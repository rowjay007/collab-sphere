package hub

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/fanout"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/presence"
)

func TestDroppedPresenceLeftIsHealed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bus := fanout.NewMemory()
	local := presence.NewLocal(bus.Loopback)
	store := &blockingAppendStore{Store: NewMemoryStore(), firstStarted: make(chan struct{}), releaseFirst: make(chan struct{})}
	reg := NewRegistry(store, bus, local)
	t.Cleanup(reg.Close)
	other := NewRegistry(NewMemoryStore(), bus, local)
	t.Cleanup(other.Close)

	alice, _, _ := join(t, ctx, reg, 1)
	_, _, bob := join(t, ctx, reg, 2)
	carol, _, _ := join(t, ctx, other, 3)
	waitView(t, ctx, bob, "bob sees carol", func(v map[uint64]bool) bool { return v[3] })

	alice.SubmitOps(ctx, []crdt.Op{insert(1, 1)})
	<-store.firstStarted
	for range presenceInbox + 10 {
		bus.Loopback("doc", &presencev1.Event{DocumentId: "doc", Client: 4})
	}
	carol.Leave(ctx)
	close(store.releaseFirst)

	healed, cancelHeal := context.WithTimeout(ctx, time.Second)
	defer cancelHeal()
	waitView(t, healed, bob, "carol's dropped Left healed", func(v map[uint64]bool) bool { return !v[3] })
}

func TestFailedPresenceJoinRetriesAndResyncs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bus := fanout.NewMemory()
	pres := &flakyPresence{Presence: presence.NewLocal(bus.Loopback)}
	reg := NewRegistry(NewMemoryStore(), bus, pres)
	t.Cleanup(reg.Close)

	_, _, alice := join(t, ctx, reg, 1)
	for !hasAwarenessSync(alice) {
		select {
		case <-ctx.Done():
			t.Fatal("alice never got her AwarenessSync")
		case <-time.After(time.Millisecond):
		}
	}
	pres.failNext(3)
	_, _, bob := join(t, ctx, reg, 2)
	waitView(t, ctx, bob, "bob sees alice after Join recovers", func(v map[uint64]bool) bool { return v[1] })

	syncs := 0
	_, _, _, frames := bob.snapshot()
	for _, f := range frames {
		if s := f.GetAwarenessSync(); s != nil {
			syncs++
			if syncs == 1 && len(s.GetStates()) != 0 {
				t.Fatalf("first AwarenessSync after a failed Join holds %v", s.GetStates())
			}
		}
	}
	if syncs != 2 {
		t.Fatalf("bob got %d AwarenessSyncs, want the empty one and the fresh one", syncs)
	}
	if got := pres.calls(); got != 5 {
		t.Fatalf("%d presence Joins, want alice's plus bob's 3 failures and 1 success", got)
	}
}

type flakyPresence struct {
	Presence
	mu    sync.Mutex
	fail  int
	joins int
}

func (f *flakyPresence) failNext(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = n
}

func (f *flakyPresence) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.joins
}

func (f *flakyPresence) Join(ctx context.Context, docID string, s PresenceSession) ([]*presencev1.State, uint64, error) {
	f.mu.Lock()
	f.joins++
	fail := f.fail > 0
	if fail {
		f.fail--
	}
	f.mu.Unlock()
	if fail {
		return nil, 0, errors.New("presence-service down")
	}
	return f.Presence.Join(ctx, docID, s)
}

func hasAwarenessSync(s *stream) bool {
	_, _, _, frames := s.snapshot()
	for _, f := range frames {
		if f.GetAwarenessSync() != nil {
			return true
		}
	}
	return false
}

// presenceView folds a peer's presence frames by the client's merge rules
// and reports who is present.
func presenceView(s *stream) map[uint64]bool {
	known := map[uint64]uint64{}
	present := map[uint64]bool{}
	_, _, _, frames := s.snapshot()
	for _, f := range frames {
		if b := f.GetPresenceBroadcast(); b != nil && b.GetVersion() > known[b.GetClient()] {
			known[b.GetClient()] = b.GetVersion()
			present[b.GetClient()] = b.GetLeft() == nil
		}
		if sync := f.GetAwarenessSync(); sync != nil {
			listed := map[uint64]bool{}
			for _, st := range sync.GetStates() {
				listed[st.GetClient()] = true
				if st.GetVersion() > known[st.GetClient()] {
					known[st.GetClient()] = st.GetVersion()
					present[st.GetClient()] = true
				}
			}
			for c := range present {
				if !listed[c] && known[c] <= sync.GetVersion() {
					present[c] = false
				}
			}
		}
	}
	return present
}

func waitView(t *testing.T, ctx context.Context, s *stream, what string, cond func(map[uint64]bool) bool) {
	t.Helper()
	for !cond(presenceView(s)) {
		s.mu.Lock()
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("%s: %v", what, ctx.Err())
		}
	}
}
