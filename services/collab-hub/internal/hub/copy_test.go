package hub

import (
	"context"
	"testing"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func TestSocketCopyInsertIsRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	store := NewMemoryStore()
	reg := localRegistry(store)
	t.Cleanup(reg.Close)
	peer := NewPeer(1, 0)
	session, err := reg.Join(ctx, "doc", peer, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	readServerMessage(t, ctx, peer)

	a := crdt.Insert{ID: crdt.ID{Client: 1, Clock: 1}, Parent: crdt.Root, Value: 'a'}
	session.SubmitOps(ctx, []crdt.Op{
		a,
		crdt.Insert{ID: crdt.ID{Client: 1, Clock: 2}, Parent: a.ID, Value: 'a', Copy: true},
	})
	msg := readServerMessage(t, ctx, peer)
	if got := msg.GetError(); got.GetCode() != collabv1.ErrorMessage_CODE_INVALID_OPERATION || got.GetMessage() != "copy inserts are server-only" {
		t.Fatalf("error = %v", got)
	}
	select {
	case <-peer.Done():
	case <-ctx.Done():
		t.Fatal("peer remained connected after a copy insert")
	}
	waitForStoredText(t, ctx, store, "doc", "a")
	if store.Doc("doc").Contains(crdt.ID{Client: 1, Clock: 2}) {
		t.Fatal("the copy insert reached the log")
	}
}
