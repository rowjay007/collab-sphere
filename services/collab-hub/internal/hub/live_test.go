package hub

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/fanout"
)

func TestCanSwapLiveDocBlocksPendingAppend(t *testing.T) {
	h := &Hub{seq: fanout.NewSequencer(3), appendThrough: 4}
	if h.canSwapLiveDoc(3) {
		t.Fatal("swap allowed with pending append")
	}
	h.appendThrough = 0
	if !h.canSwapLiveDoc(3) {
		t.Fatal("swap refused when caught up")
	}
}

func TestCanSwapLiveDocBlocksGap(t *testing.T) {
	h := &Hub{seq: fanout.NewSequencer(1)}
	h.seq.Offer(fanout.Batch{FirstSeq: 3, Ops: []crdt.Op{insert(1, 3)}})
	if h.canSwapLiveDoc(3) {
		t.Fatal("swap allowed with open gap")
	}
}

func TestCollectedNewer(t *testing.T) {
	cur := crdt.VersionVector{1: 2}
	next := crdt.VersionVector{1: 3}
	if !collectedNewer(next, cur) {
		t.Fatal("expected newer")
	}
	if collectedNewer(cur, next) {
		t.Fatal("expected not newer")
	}
}

type liveRowStore struct {
	*MemoryStore
	mu        sync.Mutex
	collected crdt.VersionVector
	loadDelay time.Duration
}

func (s *liveRowStore) publishLive(docID string, seq uint64, collected crdt.VersionVector) {
	s.mu.Lock()
	s.collected = collected.Clone()
	s.mu.Unlock()
	s.SetLiveMeta(docID, seq, collected)
}

func (s *liveRowStore) Load(ctx context.Context, docID string) (*crdt.Doc, uint64, error) {
	doc, seq, err := s.MemoryStore.Load(ctx, docID)
	time.Sleep(s.loadDelay)
	s.mu.Lock()
	collected := s.collected.Clone()
	s.mu.Unlock()
	if err != nil || len(collected) == 0 {
		return doc, seq, err
	}
	if _, err := doc.GC(collected); err != nil {
		return nil, 0, err
	}
	return doc, seq, nil
}

func TestHubSwapsInCollectedLiveDoc(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	store := &liveRowStore{MemoryStore: NewMemoryStore()}
	tomb := insert(1, 1)
	seedLog(t, store.MemoryStore, []crdt.Op{
		tomb,
		insert(1, 2),
		crdt.Delete{ID: crdt.ID{Client: 1, Clock: 3}, Target: tomb.ID},
	})
	reg := localRegistry(store)
	reg.idleSync = 10 * time.Millisecond
	t.Cleanup(reg.Close)

	join(t, ctx, reg, 1)
	before := freshJoinSnapshot(t, ctx, reg, 2)
	if !before.Has(tomb.ID) {
		t.Fatal("hub collected the tombstone before any live row")
	}

	store.publishLive("doc", 3, crdt.VersionVector{1: 3})
	for {
		got := freshJoinSnapshot(t, ctx, reg, 3)
		if !got.Has(tomb.ID) {
			if c := got.Collected(); c[1] != 3 {
				t.Fatalf("swapped doc collected=%v, want {1:3}", c)
			}
			if !got.Has(insert(1, 2).ID) {
				t.Fatal("swapped doc lost the live insert")
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("hub never swapped in the collected live doc")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestBusyDocumentBoundsLiveRebuilds(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	store := &liveRowStore{MemoryStore: NewMemoryStore(), loadDelay: 20 * time.Millisecond}
	tomb := insert(1, 1)
	seedLog(t, store.MemoryStore, []crdt.Op{
		tomb,
		insert(1, 2),
		crdt.Delete{ID: crdt.ID{Client: 1, Clock: 3}, Target: tomb.ID},
	})
	reg := localRegistry(store)
	reg.idleSync = 200 * time.Millisecond
	reg.gapGrace = time.Millisecond
	t.Cleanup(reg.Close)

	session, peer, _ := join(t, ctx, reg, 2)
	store.publishLive("doc", 3, crdt.VersionVector{1: 3})
	start := store.Loads()
	const window = time.Second
	for clock, end := uint64(1), time.Now().Add(window); time.Now().Before(end); clock++ {
		if _, err := store.AppendFromOtherWriter("doc", insert(9, clock)); err != nil {
			t.Fatal(err)
		}
		session.SubmitOps(ctx, []crdt.Op{insert(2, clock)})
		time.Sleep(time.Millisecond)
	}
	loads := store.Loads() - start
	select {
	case <-peer.Done():
		t.Fatal("writer was dropped")
	default:
	}
	if limit := int64(window/reg.idleSync) + 2; loads > limit {
		t.Fatalf("%d live rebuild loads in %s, want at most %d", loads, window, limit)
	}
}

func TestCompactLiveRowBuildsOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	store := NewMemoryStore()
	tomb := insert(1, 1)
	seedLog(t, store, []crdt.Op{
		tomb,
		crdt.Delete{ID: crdt.ID{Client: 1, Clock: 2}, Target: tomb.ID},
	})
	reg := localRegistry(store)
	reg.idleSync = 10 * time.Millisecond
	t.Cleanup(reg.Close)

	join(t, ctx, reg, 2)
	start := store.Loads()
	store.SetLiveMeta("doc", 2, crdt.VersionVector{1: 2})
	time.Sleep(50 * reg.idleSync)
	if loads := store.Loads() - start; loads != 1 {
		t.Fatalf("%d loads for a live row Load never applies, want 1", loads)
	}
}

func freshJoinSnapshot(t *testing.T, ctx context.Context, reg *Registry, id crdt.ClientID) *crdt.Doc {
	t.Helper()
	peer := NewPeer(id, 0)
	session, err := reg.Join(ctx, "doc", peer, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Leave(context.Background())
	sync := readServerMessage(t, ctx, peer).GetDocumentSync()
	if sync == nil {
		t.Fatal("first frame is not DocumentSync")
	}
	doc, err := crdt.FromSnapshot(sync.GetSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	return doc
}
