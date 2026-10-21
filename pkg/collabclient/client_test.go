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
		Client:   1,
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

func cursorAt(clock uint64) *collabv1.CursorMove {
	return &collabv1.CursorMove{Anchor: &crdtv1.OpID{Client: 7, Clock: clock}, Head: &crdtv1.OpID{Client: 7, Clock: clock}}
}

func moved(client, version, clock uint64) *collabv1.ServerMessage {
	return &collabv1.ServerMessage{Payload: &collabv1.ServerMessage_PresenceBroadcast{PresenceBroadcast: &collabv1.PresenceBroadcast{
		Client: client, Version: version, State: &collabv1.PresenceBroadcast_Cursor{Cursor: cursorAt(clock)},
	}}}
}

func left(client, version uint64) *collabv1.ServerMessage {
	return &collabv1.ServerMessage{Payload: &collabv1.ServerMessage_PresenceBroadcast{PresenceBroadcast: &collabv1.PresenceBroadcast{
		Client: client, Version: version, State: &collabv1.PresenceBroadcast_Left{Left: &collabv1.PresenceLeft{
			Reason: collabv1.PresenceLeft_REASON_EXPIRED,
		}},
	}}}
}

func TestPresenceVersionRules(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	named := &collabv1.ServerMessage{Payload: &collabv1.ServerMessage_PresenceBroadcast{PresenceBroadcast: &collabv1.PresenceBroadcast{
		Client: 5, Version: 9, DisplayName: "eve",
	}}}
	sync := &collabv1.ServerMessage{Payload: &collabv1.ServerMessage_AwarenessSync{AwarenessSync: &collabv1.AwarenessSync{
		Version: 8,
		States: []*collabv1.PresenceState{
			{Client: 2, Version: 4, Cursor: cursorAt(40)},
			{Client: 4, Version: 8, DisplayName: "dan"},
			{Client: 1, Version: 8, DisplayName: "me"},
		},
	}}}
	url := fakeHub(t, syncAt(0),
		moved(2, 5, 50),
		moved(2, 3, 30),
		left(3, 7),
		moved(3, 6, 60),
		moved(6, 2, 20),
		named,
		sync,
		moved(6, 7, 70),
		moved(9, 10, 90),
	)
	c, err := Dial(ctx, url, "doc", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.WaitPresence(ctx, func(p map[crdt.ClientID]PresenceView) bool { return p[9].Version == 10 }); err != nil {
		t.Fatal(err)
	}
	want := map[crdt.ClientID]PresenceView{
		2: {Cursor: Cursor{Anchor: crdt.ID{Client: 7, Clock: 50}, Head: crdt.ID{Client: 7, Clock: 50}, Set: true}, Version: 5},
		4: {Name: "dan", Version: 8},
		5: {Name: "eve", Version: 9},
		9: {Cursor: Cursor{Anchor: crdt.ID{Client: 7, Clock: 90}, Head: crdt.ID{Client: 7, Clock: 90}, Set: true}, Version: 10},
	}
	got := c.Presence()
	if len(got) != len(want) {
		t.Fatalf("presence %v, want %v", got, want)
	}
	for id, w := range want {
		if got[id] != w {
			t.Fatalf("client %d: %+v, want %+v", id, got[id], w)
		}
	}
	if reason, ok := c.LeftReason(3); !ok || reason != collabv1.PresenceLeft_REASON_EXPIRED {
		t.Fatalf("client 3 left reason %v %v", reason, ok)
	}
	if c.JoinAwareness().GetVersion() != 8 {
		t.Fatal("JoinAwareness is not the sync")
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
