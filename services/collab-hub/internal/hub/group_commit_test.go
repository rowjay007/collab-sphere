package hub

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"google.golang.org/protobuf/proto"
)

func TestRejectedOperationDropsPeerAfterPersistingPrefix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	store := NewMemoryStore()
	reg := NewRegistry(store)
	t.Cleanup(reg.Close)
	peer := NewPeer(1)
	session, err := reg.Join(ctx, "doc", peer, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	readServerMessage(t, ctx, peer)

	session.SubmitOps(ctx, []crdt.Op{
		crdt.Insert{ID: crdt.ID{Client: 1, Clock: 1}, Parent: crdt.Root, Value: 'a'},
		crdt.Insert{ID: crdt.ID{Client: 2, Clock: 2}, Parent: crdt.Root, Value: 'x'},
	})
	msg := readServerMessage(t, ctx, peer)
	if msg.GetError().GetCode() != collabv1.ErrorMessage_CODE_CLIENT_MISMATCH {
		t.Fatalf("error code = %s", msg.GetError().GetCode())
	}
	select {
	case <-peer.Done():
	case <-ctx.Done():
		t.Fatal("peer remained connected after rejected operation")
	}
	waitForStoredText(t, ctx, store, "doc", "a")
}

func TestOperationClockBeyondHubFrontierIsRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	store := NewMemoryStore()
	reg := NewRegistry(store)
	t.Cleanup(reg.Close)
	peer := NewPeer(1)
	session, err := reg.Join(ctx, "doc", peer, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	readServerMessage(t, ctx, peer)

	session.SubmitOps(ctx, []crdt.Op{
		crdt.Insert{ID: crdt.ID{Client: 1, Clock: math.MaxUint64}, Parent: crdt.Root, Value: 'x'},
	})
	msg := readServerMessage(t, ctx, peer)
	if msg.GetError().GetCode() != collabv1.ErrorMessage_CODE_INVALID_OPERATION {
		t.Fatalf("error code = %s", msg.GetError().GetCode())
	}
	select {
	case <-peer.Done():
	case <-ctx.Done():
		t.Fatal("peer remained connected after invalid clock")
	}
	if doc := store.Doc("doc"); doc != nil && doc.Text() != "" {
		t.Fatalf("stored text = %q", doc.Text())
	}
}

func TestInvalidCursorIsIgnored(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reg := NewRegistry(NewMemoryStore())
	t.Cleanup(reg.Close)

	peer := NewPeer(1)
	session, err := reg.Join(ctx, "doc", peer, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	readServerMessage(t, ctx, peer)
	session.MoveCursor(ctx, crdt.ID{Client: 99, Clock: 1}, crdt.ID{Client: 99, Clock: 1})

	observer := NewPeer(2)
	observerSession, err := reg.Join(ctx, "doc", observer, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { observerSession.Leave(context.Background()) })
	if msg := readServerMessage(t, ctx, observer); msg.GetDocumentSync() == nil {
		t.Fatalf("observer first frame = %T, want DocumentSync", msg.GetPayload())
	}
	select {
	case frame := <-observer.Frames():
		msg := &collabv1.ServerMessage{}
		if err := proto.Unmarshal(frame, msg); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("observer got frame %T for invalid cursor", msg.GetPayload())
	default:
	}
	select {
	case frame := <-peer.Frames():
		msg := &collabv1.ServerMessage{}
		if err := proto.Unmarshal(frame, msg); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("sender got frame %T for invalid cursor", msg.GetPayload())
	case <-peer.Done():
		t.Fatal("sender was dropped for invalid cursor")
	default:
	}
}

func waitForStoredText(t *testing.T, ctx context.Context, store *MemoryStore, docID, want string) {
	t.Helper()
	for {
		if doc := store.Doc(docID); doc != nil && doc.Text() == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("stored text did not become %q", want)
		case <-time.After(time.Millisecond):
		}
	}
}

type blockingAppendStore struct {
	Store
	mu           sync.Mutex
	calls        [][]crdt.Op
	firstStarted chan struct{}
	releaseFirst chan struct{}
}

func (s *blockingAppendStore) Append(ctx context.Context, docID string, ops []crdt.Op) (AppendResult, error) {
	s.mu.Lock()
	call := len(s.calls)
	s.calls = append(s.calls, append([]crdt.Op(nil), ops...))
	s.mu.Unlock()

	if call == 0 {
		close(s.firstStarted)
		select {
		case <-s.releaseFirst:
		case <-ctx.Done():
			return AppendResult{}, ctx.Err()
		}
	}
	return s.Store.Append(ctx, docID, ops)
}

func (s *blockingAppendStore) appendCalls() [][]crdt.Op {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]crdt.Op, len(s.calls))
	for i := range s.calls {
		out[i] = append([]crdt.Op(nil), s.calls[i]...)
	}
	return out
}

func TestHubGroupsQueuedClientBatches(t *testing.T) {
	const clients = 5

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := &blockingAppendStore{
		Store:        NewMemoryStore(),
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	reg := NewRegistry(store)
	t.Cleanup(reg.Close)

	peers := make([]*Peer, clients)
	sessions := make([]*Session, clients)
	for i := range peers {
		peers[i] = NewPeer(crdt.ClientID(i + 1))
		session, err := reg.Join(ctx, "doc", peers[i], false, 0)
		if err != nil {
			t.Fatal(err)
		}
		sessions[i] = session
		msg := readServerMessage(t, ctx, peers[i])
		if msg.GetDocumentSync() == nil {
			t.Fatalf("client %d first message is %T", i+1, msg.GetPayload())
		}
	}

	sessions[0].SubmitOps(ctx, []crdt.Op{
		crdt.Insert{ID: crdt.ID{Client: 1, Clock: 1}, Parent: crdt.Root, Value: 'a'},
	})
	select {
	case <-store.firstStarted:
	case <-ctx.Done():
		t.Fatal("first append did not start")
	}

	for i := range sessions {
		client := crdt.ClientID(i + 1)
		clock := uint64(1)
		if client == 1 {
			clock = 2
		}
		sessions[i].SubmitOps(ctx, []crdt.Op{
			crdt.Insert{ID: crdt.ID{Client: client, Clock: clock}, Parent: crdt.Root, Value: rune('b' + i)},
			crdt.Insert{ID: crdt.ID{Client: 99, Clock: uint64(i + 1)}, Parent: crdt.Root, Value: 'x'},
		})
	}
	close(store.releaseFirst)

	for i, peer := range peers {
		var initialBroadcasts, errors int
		for initialBroadcasts == 0 || errors == 0 {
			msg := readServerMessage(t, ctx, peer)
			switch {
			case msg.GetError() != nil:
				if msg.GetError().GetCode() != collabv1.ErrorMessage_CODE_CLIENT_MISMATCH {
					t.Fatalf("client %d error code %s", i+1, msg.GetError().GetCode())
				}
				errors++
			case msg.GetOperationBroadcast() != nil:
				if len(msg.GetOperationBroadcast().GetOperations()) == 1 {
					initialBroadcasts++
				} else {
					t.Fatalf("client %d broadcast has %d ops", i+1, len(msg.GetOperationBroadcast().GetOperations()))
				}
			case msg.GetPresenceBroadcast() != nil:
			default:
				t.Fatalf("client %d unexpected message %T", i+1, msg.GetPayload())
			}
		}
		if initialBroadcasts != 1 || errors != 1 {
			t.Fatalf("client %d got initial=%d errors=%d", i+1, initialBroadcasts, errors)
		}
		select {
		case <-peer.Done():
		default:
			t.Fatalf("client %d was not dropped", i+1)
		}
	}

	calls := store.appendCalls()
	if len(calls) != 2 {
		t.Fatalf("Append called %d times", len(calls))
	}
	if len(calls[0]) != 1 || len(calls[1]) != clients {
		t.Fatalf("Append batch sizes are %d and %d", len(calls[0]), len(calls[1]))
	}
}

func readServerMessage(t *testing.T, ctx context.Context, peer *Peer) *collabv1.ServerMessage {
	t.Helper()
	select {
	case frame := <-peer.Frames():
		msg := &collabv1.ServerMessage{}
		if err := proto.Unmarshal(frame, msg); err != nil {
			t.Fatal(err)
		}
		return msg
	case <-ctx.Done():
		t.Fatal("timed out waiting for server message")
		return nil
	}
}
