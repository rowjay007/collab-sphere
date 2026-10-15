package broadcast

import (
	"google.golang.org/protobuf/proto"

	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
)

func Channel(docID string) string {
	return "presence-events:" + docID
}

func EncodeEvent(ev *presencev1.Event) ([]byte, error) {
	return proto.Marshal(ev)
}

func DecodeEvent(b []byte) (*presencev1.Event, error) {
	ev := &presencev1.Event{}
	if err := proto.Unmarshal(b, ev); err != nil {
		return nil, err
	}
	return ev, nil
}
