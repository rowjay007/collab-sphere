package hub

import (
	"context"
	"sync"
	"testing"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func adaEntries(docIDs []string, members map[string]permissionv1.Role) map[string]PolicyEntry {
	out := make(map[string]PolicyEntry, len(docIDs))
	for _, id := range docIDs {
		out[id] = PolicyEntry{Members: members, WriteEpoch: 1, EpochState: "open"}
	}
	return out
}

func adaEditor() map[string]permissionv1.Role {
	return map[string]permissionv1.Role{"ada": permissionv1.Role_ROLE_EDITOR}
}

// slowPolicy answers every batch after delay. Once hung it never answers,
// even when the caller's context ends.
type slowPolicy struct {
	delay  time.Duration
	mu     sync.Mutex
	hung   bool
	unhang chan struct{}
}

func (p *slowPolicy) DumpBatch(ctx context.Context, docIDs []string) (map[string]PolicyEntry, error) {
	p.mu.Lock()
	hung := p.hung
	p.mu.Unlock()
	if hung {
		<-p.unhang
		return nil, context.Canceled
	}
	select {
	case <-time.After(p.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return adaEntries(docIDs, adaEditor()), nil
}

func (p *slowPolicy) hang() {
	p.mu.Lock()
	p.hung = true
	p.mu.Unlock()
}

func joinDoc(t *testing.T, ctx context.Context, reg *Registry, docID string, id crdt.ClientID) (*Session, *Peer, *stream) {
	t.Helper()
	peer := NewPeer(id, 0)
	peer.SetUser("ada")
	session, err := reg.Join(ctx, docID, peer, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Leave(context.Background()) })
	return session, peer, watch(t, ctx, peer)
}

func TestSlowPolicyBatchesKeepWritersOpen(t *testing.T) {
	reg := localRegistry(NewMemoryStore())
	reg.idleSync = time.Hour
	t.Cleanup(reg.Close)
	reg.SetSecurity(nil, &slowPolicy{delay: 800 * time.Millisecond})
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	sa, pa, a := joinDoc(t, ctx, reg, "a", 1)
	sb, pb, b := joinDoc(t, ctx, reg, "b", 2)
	var clock uint64
	for end := time.Now().Add(2 * policyStale); time.Now().Before(end); {
		clock++
		sa.SubmitOps(ctx, []crdt.Op{insert(1, clock)})
		sb.SubmitOps(ctx, []crdt.Op{insert(2, clock)})
		time.Sleep(20 * time.Millisecond)
		select {
		case <-pa.Done():
			t.Fatalf("doc a closed after %d writes: %s", clock, a.lastError(t, ctx))
		case <-pb.Done():
			t.Fatalf("doc b closed after %d writes: %s", clock, b.lastError(t, ctx))
		default:
		}
	}
	if err := a.waitSeq(ctx, clock); err != nil {
		t.Fatal(err)
	}
	if err := b.waitSeq(ctx, clock); err != nil {
		t.Fatal(err)
	}
}

func TestHungPolicyFailStopsAfterStale(t *testing.T) {
	reg := localRegistry(NewMemoryStore())
	reg.idleSync = time.Hour
	t.Cleanup(reg.Close)
	policy := &slowPolicy{unhang: make(chan struct{})}
	t.Cleanup(func() { close(policy.unhang) })
	reg.SetSecurity(nil, policy)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	_, pa, a := joinDoc(t, ctx, reg, "a", 1)
	_, pb, b := joinDoc(t, ctx, reg, "b", 2)
	policy.hang()
	hungAt := time.Now()
	for _, s := range []struct {
		peer   *Peer
		stream *stream
	}{{pa, a}, {pb, b}} {
		select {
		case <-s.peer.Done():
		case <-time.After(policyStale + 4*policyPeriod):
			t.Fatalf("peer %d stayed open %s after the policy client hung", s.peer.id, time.Since(hungAt))
		}
		if elapsed := time.Since(hungAt); elapsed < policyStale-policyPeriod {
			t.Fatalf("peer %d closed %s after the hang, before the view could be stale", s.peer.id, elapsed)
		}
		if code := s.stream.lastError(t, ctx); code != collabv1.ErrorMessage_CODE_UNAVAILABLE {
			t.Fatalf("peer %d code = %s, want CODE_UNAVAILABLE", s.peer.id, code)
		}
	}
}

// heldPolicy grants ada on every batch except a held one, which revokes
// everyone and returns only when released.
type heldPolicy struct {
	mu       sync.Mutex
	calls    int
	held     int
	newest   int
	started  chan struct{}
	release  chan struct{}
	returned chan struct{}
}

func (p *heldPolicy) DumpBatch(ctx context.Context, docIDs []string) (map[string]PolicyEntry, error) {
	p.mu.Lock()
	p.calls++
	n := p.calls
	hold := n == p.held
	p.mu.Unlock()
	members := adaEditor()
	if hold {
		close(p.started)
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		members = map[string]permissionv1.Role{}
	}
	p.mu.Lock()
	if n > p.newest {
		p.newest = n
		select {
		case p.returned <- struct{}{}:
		default:
		}
	}
	p.mu.Unlock()
	return adaEntries(docIDs, members), nil
}

func (p *heldPolicy) holdNext() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.held = p.calls + 1
	return p.held
}

func (p *heldPolicy) newestReturned() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.newest
}

func TestOlderPolicyAnswerIsNotApplied(t *testing.T) {
	reg := localRegistry(NewMemoryStore())
	reg.idleSync = time.Hour
	t.Cleanup(reg.Close)
	policy := &heldPolicy{started: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{}, 1)}
	reg.SetSecurity(nil, policy)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	_, peer, s := joinDoc(t, ctx, reg, "doc", 1)
	held := policy.holdNext()
	select {
	case <-policy.started:
	case <-ctx.Done():
		t.Fatal("held batch did not start")
	}
	for policy.newestReturned() < held+2 {
		select {
		case <-policy.returned:
		case <-ctx.Done():
			t.Fatal("no newer batch answered while one was held")
		}
	}
	close(policy.release)
	select {
	case <-peer.Done():
		t.Fatalf("older revoking answer closed the peer: %s", s.lastError(t, ctx))
	case <-time.After(300 * time.Millisecond):
	}
}
