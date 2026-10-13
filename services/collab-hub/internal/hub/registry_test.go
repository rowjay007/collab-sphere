package hub

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

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
