package hub

import (
	"context"
	"errors"
	"maps"
	"sync"
	"testing"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/fanout"
)

type flakyPolicy struct {
	mu  sync.Mutex
	err error
}

func (p *flakyPolicy) DumpBatch(_ context.Context, docIDs []string) (map[string]PolicyEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return nil, p.err
	}
	entry := PolicyEntry{
		Members:    map[string]permissionv1.Role{"ada": permissionv1.Role_ROLE_EDITOR},
		WriteEpoch: 1,
		EpochState: "live",
	}
	out := make(map[string]PolicyEntry, len(docIDs))
	for _, id := range docIDs {
		out[id] = entry
	}
	return out, nil
}

func (p *flakyPolicy) fail(err error) {
	p.mu.Lock()
	p.err = err
	p.mu.Unlock()
}

func TestPolicyDumpStallStopsTheHub(t *testing.T) {
	reg := localRegistry(NewMemoryStore())
	t.Cleanup(reg.Close)
	policy := &flakyPolicy{}
	reg.SetSecurity(nil, policy)

	peer := NewPeer(1, 0)
	peer.SetUser("ada")
	if _, err := reg.Join(t.Context(), "doc", peer, false, 0); err != nil {
		t.Fatal(err)
	}
	if msg := readServerMessage(t, t.Context(), peer); msg.GetDocumentSync() == nil {
		t.Fatal("join did not send a document sync")
	}
	policy.fail(errors.New("permission service down"))

	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	msg := readServerMessage(t, ctx, peer)
	if msg.GetError().GetCode() != collabv1.ErrorMessage_CODE_UNAVAILABLE {
		t.Fatalf("code = %s, want CODE_UNAVAILABLE", msg.GetError().GetCode())
	}
	select {
	case <-peer.Done():
	case <-ctx.Done():
		t.Fatal("peer stayed open after the permission dump went stale")
	}
}

type memberPolicy struct {
	mu        sync.Mutex
	members   map[string]permissionv1.Role
	epoch     uint32
	state     string
	err       error
	calls     int
	blockCall int
	started   chan struct{}
	release   chan struct{}
	snap      map[string]permissionv1.Role
}

func newMemberPolicy(user string) *memberPolicy {
	return &memberPolicy{
		members: map[string]permissionv1.Role{user: permissionv1.Role_ROLE_EDITOR},
		epoch:   1,
		state:   "open",
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (p *memberPolicy) DumpBatch(ctx context.Context, docIDs []string) (map[string]PolicyEntry, error) {
	p.mu.Lock()
	p.calls++
	n := p.calls
	members := maps.Clone(p.members)
	epoch, state, dumpErr := p.epoch, p.state, p.err
	block := p.blockCall != 0 && n == p.blockCall
	p.mu.Unlock()
	entry := func(m map[string]permissionv1.Role) PolicyEntry {
		return PolicyEntry{Members: m, WriteEpoch: epoch, EpochState: state}
	}
	if block {
		p.mu.Lock()
		p.snap = members
		p.mu.Unlock()
		close(p.started)
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		p.mu.Lock()
		snap := maps.Clone(p.snap)
		epoch, state = p.epoch, p.state
		p.mu.Unlock()
		out := make(map[string]PolicyEntry, len(docIDs))
		for _, id := range docIDs {
			out[id] = PolicyEntry{Members: snap, WriteEpoch: epoch, EpochState: state}
		}
		return out, nil
	}
	if dumpErr != nil {
		return nil, dumpErr
	}
	out := make(map[string]PolicyEntry, len(docIDs))
	for _, id := range docIDs {
		out[id] = entry(members)
	}
	return out, nil
}

func (p *memberPolicy) setMembers(members map[string]permissionv1.Role) {
	p.mu.Lock()
	p.members = members
	p.mu.Unlock()
}

func (p *memberPolicy) fail(err error) {
	p.mu.Lock()
	p.err = err
	p.mu.Unlock()
}

func (p *memberPolicy) armNextBlock() {
	p.mu.Lock()
	p.blockCall = p.calls + 1
	p.mu.Unlock()
}

type blockingReadStore struct {
	Store
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingReadStore) ReadOperations(ctx context.Context, docID string, after uint64, limit int) (fanout.Batch, LogMeta, error) {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
	case <-ctx.Done():
		return fanout.Batch{}, LogMeta{}, ctx.Err()
	}
	return s.Store.ReadOperations(ctx, docID, after, limit)
}

func TestRevokeDuringAppendClosesPeerWithinTwoSeconds(t *testing.T) {
	store := &blockingAppendStore{
		Store:        NewMemoryStore(),
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	t.Cleanup(func() {
		select {
		case <-store.releaseFirst:
		default:
			close(store.releaseFirst)
		}
	})
	reg := localRegistry(store)
	reg.idleSync = time.Hour
	t.Cleanup(reg.Close)
	policy := newMemberPolicy("ada")
	reg.SetSecurity(nil, policy)

	peer := NewPeer(1, 0)
	peer.SetUser("ada")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := reg.Join(ctx, "doc", peer, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Leave(context.Background()) })
	if msg := readServerMessage(t, ctx, peer); msg.GetDocumentSync() == nil {
		t.Fatal("join did not send a document sync")
	}
	session.SubmitOps(ctx, []crdt.Op{crdt.Insert{ID: crdt.ID{Client: 1, Clock: 1}, Parent: crdt.Root, Value: 'a'}})
	select {
	case <-store.firstStarted:
	case <-ctx.Done():
		t.Fatal("append did not start")
	}
	policy.setMembers(map[string]permissionv1.Role{})
	revoked := time.Now()
	waitCtx, waitCancel := context.WithTimeout(ctx, 2*time.Second)
	defer waitCancel()
	msg := readServerMessage(t, waitCtx, peer)
	if msg.GetError().GetCode() != collabv1.ErrorMessage_CODE_REVOKED {
		t.Fatalf("code = %s, want CODE_REVOKED", msg.GetError().GetCode())
	}
	if time.Since(revoked) >= 2*time.Second {
		t.Fatalf("CODE_REVOKED arrived after %s", time.Since(revoked))
	}
	close(store.releaseFirst)
}

func TestDumpStartedBeforeRevokeDoesNotKeepPeer(t *testing.T) {
	reg := localRegistry(NewMemoryStore())
	reg.idleSync = time.Hour
	t.Cleanup(reg.Close)
	policy := newMemberPolicy("ada")
	reg.SetSecurity(nil, policy)

	peer := NewPeer(1, 0)
	peer.SetUser("ada")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	session, err := reg.Join(ctx, "doc", peer, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Leave(context.Background()) })
	if msg := readServerMessage(t, ctx, peer); msg.GetDocumentSync() == nil {
		t.Fatal("join did not send a document sync")
	}
	policy.armNextBlock()
	select {
	case <-policy.started:
	case <-time.After(2 * time.Second):
		t.Fatal("policy dump did not block")
	}
	started := time.Now()
	policy.setMembers(map[string]permissionv1.Role{})
	policy.fail(errors.New("permission service down"))
	time.Sleep(1400 * time.Millisecond)
	close(policy.release)
	remain := 3*time.Second - time.Since(started)
	select {
	case <-peer.Done():
	case <-time.After(remain):
		t.Fatal("stale dump kept the revoked peer")
	}
}

func TestJoinRechecksMembershipAfterBlockedRead(t *testing.T) {
	base := NewMemoryStore()
	if _, err := base.Append(context.Background(), "doc", []crdt.Op{
		crdt.Insert{ID: crdt.ID{Client: 2, Clock: 1}, Parent: crdt.Root, Value: 'a'},
	}); err != nil {
		t.Fatal(err)
	}
	store := &blockingReadStore{
		Store:   base,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	t.Cleanup(func() {
		select {
		case <-store.release:
		default:
			close(store.release)
		}
	})
	reg := localRegistry(store)
	reg.idleSync = time.Hour
	t.Cleanup(reg.Close)
	policy := newMemberPolicy("ada")
	reg.SetSecurity(nil, policy)

	peer := NewPeer(1, 0)
	peer.SetUser("ada")
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer joinCancel()
	joined := make(chan error, 1)
	go func() {
		_, err := reg.Join(joinCtx, "doc", peer, true, 0)
		joined <- err
	}()
	select {
	case <-store.started:
	case <-time.After(3 * time.Second):
		t.Fatal("resume read did not start")
	}
	policy.setMembers(map[string]permissionv1.Role{})
	time.Sleep(1200 * time.Millisecond)
	close(store.release)
	var err error
	select {
	case err = <-joined:
	case <-time.After(3 * time.Second):
		t.Fatal("join did not return")
	}
	if !errors.Is(err, errNoGrant) {
		t.Fatalf("join error = %v", err)
	}
	msgCtx, msgCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer msgCancel()
	msg := readServerMessage(t, msgCtx, peer)
	if msg.GetError().GetCode() != collabv1.ErrorMessage_CODE_REVOKED {
		t.Fatalf("code = %s", msg.GetError().GetCode())
	}
}
