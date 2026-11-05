package hub

import (
	"context"
	"errors"
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

	first, err := reg.Replica(t.Context(), "doc", "ada", 42)
	if err != nil || first != 42 {
		t.Fatalf("first claim = %d, %v", first, err)
	}
	second, err := reg.Replica(t.Context(), "doc", "ada", 42)
	if err != nil || second != 42 {
		t.Fatalf("second claim = %d, %v", second, err)
	}
	store.mu.Lock()
	calls := store.calls
	store.mu.Unlock()
	if calls != 1 {
		t.Fatalf("store claims = %d, want 1", calls)
	}
	if _, err := reg.Replica(t.Context(), "doc", "bob", 42); !errors.Is(err, ErrReplicaForbidden) {
		t.Fatalf("other user = %v, want ErrReplicaForbidden", err)
	}
	store.mu.Lock()
	calls = store.calls
	store.mu.Unlock()
	if calls != 1 {
		t.Fatalf("store claims after other user = %d, want 1", calls)
	}
}

func TestRememberReplicaKeepsTheFirstUser(t *testing.T) {
	reg := localRegistry(NewMemoryStore())
	t.Cleanup(reg.Close)
	key := replicaKey{docID: "doc", client: 7}
	reg.rememberReplica(key, "ada")
	reg.rememberReplica(key, "bob")
	got, ok := reg.lookupReplica(key)
	if !ok || got != "ada" {
		t.Fatalf("cached user = %q ok=%v", got, ok)
	}
	if _, err := reg.Replica(t.Context(), "doc", "bob", 7); !errors.Is(err, ErrReplicaForbidden) {
		t.Fatalf("later claim by bob = %v", err)
	}
	id, err := reg.Replica(t.Context(), "doc", "ada", 7)
	if err != nil || id != 7 {
		t.Fatalf("ada still owns the replica: %d %v", id, err)
	}
}

func TestRegistryDoesNotCacheNegativeReplicaClaims(t *testing.T) {
	store := &scriptedReplicaStore{MemoryStore: NewMemoryStore(), fail: errors.New("unavailable")}
	reg := localRegistry(store)
	t.Cleanup(reg.Close)

	if _, err := reg.Replica(t.Context(), "doc", "ada", 42); err == nil {
		t.Fatal("first claim succeeded")
	}
	store.mu.Lock()
	store.fail = nil
	store.mu.Unlock()
	if got, err := reg.Replica(t.Context(), "doc", "ada", 42); err != nil || got != 42 {
		t.Fatalf("second claim = %d, %v", got, err)
	}
	store.mu.Lock()
	calls := store.calls
	store.mu.Unlock()
	if calls != 2 {
		t.Fatalf("store claims = %d, want 2", calls)
	}
}

type scriptedReplicaStore struct {
	*MemoryStore
	mu    sync.Mutex
	calls int
	fail  error
}

func (s *scriptedReplicaStore) Replica(ctx context.Context, docID string, requested crdt.ClientID) (crdt.ClientID, error) {
	s.mu.Lock()
	s.calls++
	err := s.fail
	s.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return s.MemoryStore.Replica(ctx, docID, requested)
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
