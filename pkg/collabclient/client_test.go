package collabclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func fakeHub(t *testing.T, frames ...*collabv1.ServerMessage) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for _, f := range frames {
			b, err := proto.Marshal(f)
			if err != nil {
				t.Error(err)
				return
			}
			if err := conn.Write(r.Context(), websocket.MessageBinary, b); err != nil {
				return
			}
		}
		_, _, _ = conn.Read(r.Context())
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func ops(clocks ...uint64) []*crdtv1.Operation {
	out := make([]*crdtv1.Operation, len(clocks))
	for i, clock := range clocks {
		out[i] = crdt.OpToProto(crdt.Insert{ID: crdt.ID{Client: 7, Clock: clock}, Parent: crdt.Root, Value: 'x'})
	}
	return out
}

func syncAt(seq uint64) *collabv1.ServerMessage {
	return &collabv1.ServerMessage{Payload: &collabv1.ServerMessage_DocumentSync{DocumentSync: &collabv1.DocumentSync{
		Snapshot: crdt.New().Snapshot(),
		Seq:      seq,
	}}}
}

func broadcast(firstSeq uint64, clocks ...uint64) *collabv1.ServerMessage {
	return &collabv1.ServerMessage{Payload: &collabv1.ServerMessage_OperationBroadcast{OperationBroadcast: &collabv1.OperationBroadcast{
		FirstSeq:   firstSeq,
		Operations: ops(clocks...),
	}}}
}

func TestSeqFollowsStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	missed := &collabv1.ServerMessage{Payload: &collabv1.ServerMessage_MissedOps{MissedOps: &collabv1.MissedOps{
		FirstSeq: 8, Operations: ops(3), Seq: 8,
	}}}
	url := fakeHub(t, syncAt(5), broadcast(6, 1, 2), missed, broadcast(9, 4))
	c, err := Dial(ctx, url, "doc", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.WaitUntil(ctx, func(r *crdt.Replica) bool { return r.Len() == 4 }); err != nil {
		t.Fatal(err)
	}
	if got := c.Seq(); got != 9 {
		t.Fatalf("Seq() = %d, want 9", got)
	}
}

func TestSeqGapClosesConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := fakeHub(t, syncAt(5), broadcast(6, 1), broadcast(8, 2))
	c, err := Dial(ctx, url, "doc", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	err = c.WaitUntil(ctx, func(r *crdt.Replica) bool { return r.Len() == 2 })
	if err == nil || !strings.Contains(err.Error(), "seq 8 after seq 6") {
		t.Fatalf("WaitUntil error = %v", err)
	}
	if got := c.Seq(); got != 6 {
		t.Fatalf("Seq() = %d, want 6", got)
	}
}
