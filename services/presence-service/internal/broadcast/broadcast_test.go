package broadcast_test

import (
	"testing"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/services/presence-service/internal/broadcast"
)

func TestEncodeDecodeAndChannel(t *testing.T) {
	ev := &presencev1.Event{
		DocumentId: uuid.NewString(),
		Client:     3,
		Version:    9,
		Kind: &presencev1.Event_State{State: &presencev1.State{
			DocumentId: "d",
			Client:     3,
			Session:    1,
			Version:    9,
		}},
	}
	b, err := broadcast.EncodeEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	got, err := broadcast.DecodeEvent(b)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(ev, got) {
		t.Fatalf("round trip %+v", got)
	}
	if broadcast.Channel("abc") != "presence-events:abc" {
		t.Fatalf("channel %s", broadcast.Channel("abc"))
	}
}
