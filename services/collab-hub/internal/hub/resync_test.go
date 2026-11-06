package hub

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func TestLapsedResumeSendsResyncDocumentSync(t *testing.T) {
	ctx := t.Context()
	store := NewMemoryStore()
	reg := localRegistry(store)
	t.Cleanup(reg.Close)

	seedLog(t, store, chain(1, 2))

	peer := NewPeer(1, 0)
	peer.SetResyncPending()
	session, err := reg.Join(ctx, "doc", peer, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Leave(context.Background()) })
	msg := readServerMessage(t, ctx, peer)
	sync := msg.GetDocumentSync()
	if sync == nil {
		t.Fatalf("first frame = %T", msg.GetPayload())
	}
	if !sync.GetResync() {
		t.Fatal("DocumentSync omitted resync for lapsed replica")
	}
}

func lapsedJoin(t *testing.T) (*MemoryStore, *Peer, *Session) {
	t.Helper()
	ctx := t.Context()
	store := NewMemoryStore()
	reg := localRegistry(store)
	t.Cleanup(reg.Close)
	seedLog(t, store, chain(1, 2))
	peer := NewPeer(1, 0)
	peer.SetResyncPending()
	session, err := reg.Join(ctx, "doc", peer, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Leave(context.Background()) })
	if !readServerMessage(t, ctx, peer).GetDocumentSync().GetResync() {
		t.Fatal("expected resync DocumentSync")
	}
	return store, peer, session
}

func TestResyncStaysPendingUntilClientReports(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	store, peer, session := lapsedJoin(t)
	stale := crdt.Insert{ID: crdt.ID{Client: 1, Clock: 3}, Parent: crdt.ID{Client: 1, Clock: 2}, Value: 's'}
	session.SubmitOps(ctx, []crdt.Op{stale})
	for {
		msg := readServerMessage(t, ctx, peer)
		if e := msg.GetError(); e != nil {
			if e.GetCode() != collabv1.ErrorMessage_CODE_RESYNC {
				t.Fatalf("error %v", e)
			}
			break
		}
	}
	_, meta, err := store.ReadOperations(ctx, "doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if meta.OpSeq != 2 {
		t.Fatalf("stale batch appended: op seq %d", meta.OpSeq)
	}
}

func TestResyncClearsAfterClientReport(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	store, peer, session := lapsedJoin(t)
	session.SubmitReport(ctx, crdt.VersionVector{1: 2})
	fresh := crdt.Insert{ID: crdt.ID{Client: 1, Clock: 3}, Parent: crdt.ID{Client: 1, Clock: 2}, Value: 'f'}
	session.SubmitOps(ctx, []crdt.Op{fresh})
	for {
		_, meta, err := store.ReadOperations(ctx, "doc", 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		if meta.OpSeq == 3 {
			return
		}
		select {
		case frame := <-peer.Frames():
			msg := &collabv1.ServerMessage{}
			if err := proto.Unmarshal(frame, msg); err != nil {
				t.Fatal(err)
			}
			if e := msg.GetError(); e != nil {
				t.Fatalf("batch after report refused: %v", e)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestResyncPendingRejectsOperationBatch(t *testing.T) {
	ctx := t.Context()
	store := NewMemoryStore()
	reg := localRegistry(store)
	t.Cleanup(reg.Close)
	seedLog(t, store, chain(1, 1))
	warm, err := reg.Join(ctx, "doc", NewPeer(2, 0), false, 0)
	if err != nil {
		t.Fatal(err)
	}
	warm.Leave(context.Background())
	h := reg.acquire("doc")
	if h == nil {
		t.Fatal("no hub")
	}
	t.Cleanup(func() { reg.release(h) })
	peer := NewPeer(1, 0)
	peer.SetResyncPending()
	h.peers[1] = peer
	op := crdt.Insert{ID: crdt.ID{Client: 1, Clock: 2}, Parent: crdt.ID{Client: 1, Clock: 1}, Value: 'q'}
	h.applyOps(opsEvent{id: 1, ops: []crdt.Op{op}})
	msg := readServerMessage(t, ctx, peer)
	if msg.GetError().GetCode() != collabv1.ErrorMessage_CODE_RESYNC {
		t.Fatalf("got %v", msg.GetError())
	}
}
