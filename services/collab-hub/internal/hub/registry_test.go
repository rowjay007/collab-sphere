package hub

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

type blockingLoadStore struct {
	Store
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type deadlineLoadStore struct {
	Store
}

type archivedStore struct {
	Store
}

func (s archivedStore) Load(context.Context, string) (*crdt.Doc, error) {
	return nil, ErrArchived
}

func TestArchivedDocumentJoinReportsArchived(t *testing.T) {
	reg := NewRegistry(archivedStore{Store: NewMemoryStore()})
	t.Cleanup(reg.Close)
	peer := NewPeer(1)
	_, err := reg.Join(t.Context(), "doc", peer)
	if !errors.Is(err, ErrArchived) {
		t.Fatalf("Join error = %v", err)
	}
	msg := readServerMessage(t, t.Context(), peer)
	if got := msg.GetError().GetMessage(); got != "document archived" {
		t.Fatalf("error message = %q", got)
	}
}

func (s deadlineLoadStore) Load(ctx context.Context, docID string) (*crdt.Doc, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("load context has no deadline")
	}
	return s.Store.Load(ctx, docID)
}

func TestHubLoadHasDeadline(t *testing.T) {
	reg := NewRegistry(deadlineLoadStore{Store: NewMemoryStore()})
	t.Cleanup(reg.Close)
	session, err := reg.Join(t.Context(), "doc", NewPeer(1))
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	session.Leave(context.Background())
}

func (s *blockingLoadStore) Load(ctx context.Context, docID string) (*crdt.Doc, error) {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.Store.Load(ctx, docID)
}

func TestCancelledJoinRemovesAdmittedPeer(t *testing.T) {
	store := &blockingLoadStore{
		Store:   NewMemoryStore(),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	reg := NewRegistry(store)
	t.Cleanup(reg.Close)

	cancelledCtx, cancel := context.WithCancel(t.Context())
	cancelled := make(chan error, 1)
	go func() {
		_, err := reg.Join(cancelledCtx, "doc", NewPeer(1))
		cancelled <- err
	}()
	<-store.started
	waitForQueuedEvents(t, reg, "doc", 1)

	keeper := make(chan *Session, 1)
	keeperErr := make(chan error, 1)
	go func() {
		session, err := reg.Join(t.Context(), "doc", NewPeer(2))
		if err != nil {
			keeperErr <- err
			return
		}
		keeper <- session
	}()
	waitForQueuedEvents(t, reg, "doc", 2)

	cancel()
	var cancelledErr error
	select {
	case cancelledErr = <-cancelled:
	case <-time.After(20 * time.Millisecond):
	}
	close(store.release)
	if cancelledErr == nil {
		cancelledErr = <-cancelled
	}
	if !errors.Is(cancelledErr, context.Canceled) {
		t.Fatalf("cancelled Join returned %v", cancelledErr)
	}

	var keeperSession *Session
	select {
	case keeperSession = <-keeper:
	case err := <-keeperErr:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("keeper Join timed out")
	}
	t.Cleanup(func() { keeperSession.Leave(context.Background()) })

	fresh, err := reg.Join(t.Context(), "doc", NewPeer(1))
	if err != nil {
		t.Fatalf("fresh Join after cancelled Join: %v", err)
	}
	fresh.Leave(context.Background())
}

func waitForQueuedEvents(t *testing.T, reg *Registry, docID string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		reg.mu.Lock()
		h := reg.hubs[docID]
		reg.mu.Unlock()
		if h != nil && len(h.events) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("hub has fewer than %d queued events", want)
}

func TestStaleLeaveAfterFailStopDoesNotStopNewHub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := NewMemoryStore()
	reg := NewRegistry(store)
	t.Cleanup(reg.Close)

	alice := NewPeer(1)
	aliceSession, err := reg.Join(ctx, "doc", alice)
	if err != nil {
		t.Fatal(err)
	}
	store.FailAppend(errors.New("disk on fire"))
	aliceSession.SubmitOps(ctx, []crdt.Op{crdt.Insert{ID: crdt.ID{Clock: 1, Client: 1}, Parent: crdt.Root, Value: 'a'}})
	select {
	case <-alice.Done():
	case <-ctx.Done():
		t.Fatal("fail-stop never closed alice")
	}
	store.FailAppend(nil)

	bob := NewPeer(2)
	bobSession, err := reg.Join(ctx, "doc", bob)
	if err != nil {
		t.Fatal(err)
	}
	aliceSession.Leave(ctx)

	bobSession.SubmitOps(ctx, []crdt.Op{crdt.Insert{ID: crdt.ID{Clock: 1, Client: 2}, Parent: crdt.Root, Value: 'b'}})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if doc := store.Doc("doc"); doc != nil && doc.Text() == "b" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-bob.Done():
		t.Fatal("alice's stale Leave stopped the hub bob joined")
	default:
	}
	if doc := store.Doc("doc"); doc == nil || doc.Text() != "b" {
		t.Fatalf("bob's op was not persisted by his hub")
	}
}
