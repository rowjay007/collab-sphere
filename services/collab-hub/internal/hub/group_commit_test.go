package hub

import (
	"context"
	"sync"
	"testing"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"google.golang.org/protobuf/proto"
)

type blockingAppendStore struct {
	Store
	mu           sync.Mutex
	calls        [][]crdt.Op
	firstStarted chan struct{}
	releaseFirst chan struct{}
}

func (s *blockingAppendStore) Append(ctx context.Context, docID string, ops []crdt.Op) error {
	s.mu.Lock()
	call := len(s.calls)
	s.calls = append(s.calls, append([]crdt.Op(nil), ops...))
	s.mu.Unlock()

	if call == 0 {
		close(s.firstStarted)
		select {
		case <-s.releaseFirst:
		case <-ctx.Done():
			return ctx.Err()
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
		session, err := reg.Join(ctx, "doc", peers[i])
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
		var initialBroadcasts, groupedBroadcasts, errors int
		for range 3 {
			msg := readServerMessage(t, ctx, peer)
			switch {
			case msg.GetError() != nil:
				if msg.GetError().GetCode() != collabv1.ErrorMessage_CODE_CLIENT_MISMATCH {
					t.Fatalf("client %d error code %s", i+1, msg.GetError().GetCode())
				}
				errors++
			case msg.GetOperationBroadcast() != nil:
				switch len(msg.GetOperationBroadcast().GetOperations()) {
				case 1:
					initialBroadcasts++
				case clients:
					groupedBroadcasts++
				default:
					t.Fatalf("client %d broadcast has %d ops", i+1, len(msg.GetOperationBroadcast().GetOperations()))
				}
			default:
				t.Fatalf("client %d unexpected message %T", i+1, msg.GetPayload())
			}
		}
		if initialBroadcasts != 1 || groupedBroadcasts != 1 || errors != 1 {
			t.Fatalf("client %d got initial=%d grouped=%d errors=%d", i+1, initialBroadcasts, groupedBroadcasts, errors)
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
