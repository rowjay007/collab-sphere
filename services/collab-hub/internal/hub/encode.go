package hub

import (
	"google.golang.org/protobuf/proto"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

// EncodeError marshals a server ErrorMessage.
func EncodeError(code collabv1.ErrorMessage_Code, message string) []byte {
	return mustMarshal(&collabv1.ServerMessage{
		Payload: &collabv1.ServerMessage_Error{Error: &collabv1.ErrorMessage{
			Code:    code,
			Message: message,
		}},
	})
}

// EncodePong marshals a Pong with the given nonce.
func EncodePong(nonce uint64) []byte {
	return mustMarshal(&collabv1.ServerMessage{
		Payload: &collabv1.ServerMessage_Pong{Pong: &collabv1.Pong{Nonce: nonce}},
	})
}

// encodeAwarenessSync leaves out self, whose own state the client already knows.
func encodeAwarenessSync(states []*presencev1.State, version uint64, self crdt.ClientID) []byte {
	sync := &collabv1.AwarenessSync{Version: version}
	for _, st := range states {
		if crdt.ClientID(st.GetClient()) == self {
			continue
		}
		ps := &collabv1.PresenceState{
			Client:      st.GetClient(),
			DisplayName: st.GetDisplayName(),
			Version:     st.GetVersion(),
		}
		if st.GetHasCursor() {
			ps.Cursor = &collabv1.CursorMove{Anchor: st.GetAnchor(), Head: st.GetHead()}
		}
		sync.States = append(sync.States, ps)
	}
	return mustMarshal(&collabv1.ServerMessage{
		Payload: &collabv1.ServerMessage_AwarenessSync{AwarenessSync: sync},
	})
}

func encodePresenceBroadcast(ev *presencev1.Event) ([]byte, bool) {
	pb := &collabv1.PresenceBroadcast{Client: ev.GetClient(), Version: ev.GetVersion()}
	switch k := ev.GetKind().(type) {
	case *presencev1.Event_State:
		pb.DisplayName = k.State.GetDisplayName()
		if k.State.GetHasCursor() {
			pb.State = &collabv1.PresenceBroadcast_Cursor{Cursor: &collabv1.CursorMove{
				Anchor: k.State.GetAnchor(),
				Head:   k.State.GetHead(),
			}}
		}
	case *presencev1.Event_Left:
		pb.State = &collabv1.PresenceBroadcast_Left{Left: &collabv1.PresenceLeft{
			Reason: collabv1.PresenceLeft_Reason(k.Left.GetReason()),
		}}
	default:
		return nil, false
	}
	return mustMarshal(&collabv1.ServerMessage{
		Payload: &collabv1.ServerMessage_PresenceBroadcast{PresenceBroadcast: pb},
	}), true
}

func mustMarshal(msg *collabv1.ServerMessage) []byte {
	b, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return b
}
