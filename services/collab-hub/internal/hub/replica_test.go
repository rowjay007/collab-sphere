package hub

import (
	"context"
	"sync"
	"testing"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func TestMemoryStoreReplicaClaims(t *testing.T) {
	store := NewMemoryStore()
	ctx := t.Context()
	first, err := store.Replica(ctx, "a", 0)
	if err != nil || first == 0 {
		t.Fatalf("allocate = %d, %v", first, err)
	}
	if got, err := store.Replica(ctx, "a", 77); err != nil || got != 77 {
		t.Fatalf("claim = %d, %v", got, err)
	}
	if got, err := store.Replica(ctx, "b", 77); err != nil || got != 77 {
		t.Fatalf("unrelated claim = %d, %v", got, err)
	}
}

func TestRegistryCachesPositiveReplicaClaims(t *testing.T) {
	store := &countingReplicaStore{MemoryStore: NewMemoryStore()}
	reg := localRegistry(store)
	t.Cleanup(reg.Close)

	first, err := reg.Replica(t.Context(), "doc", 42)
	if err != nil || first != 42 {
		t.Fatalf("first claim = %d, %v", first, err)
	}
	second, err := reg.Replica(t.Context(), "doc", 42)
	if err != nil || second != 42 {
		t.Fatalf("second claim = %d, %v", second, err)
	}
	store.mu.Lock()
	calls := store.calls
	store.mu.Unlock()
	if calls != 1 {
		t.Fatalf("store claims = %d, want 1", calls)
	}
}

type countingReplicaStore struct {
	*MemoryStore
	mu    sync.Mutex
	calls int
}

func (s *countingReplicaStore) Replica(ctx context.Context, docID string, requested crdt.ClientID) (crdt.ClientID, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return s.MemoryStore.Replica(ctx, docID, requested)
}
