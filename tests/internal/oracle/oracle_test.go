package oracle

import (
	"encoding/hex"
	"testing"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func id(client crdt.ClientID, clock uint64) crdt.ID { return crdt.ID{Client: client, Clock: clock} }

func TestEncodeADRExample(t *testing.T) {
	ops := []crdt.Op{
		crdt.Delete{ID: id(1, 3), Target: id(1, 2)},
		crdt.Insert{ID: id(2, 4), Parent: id(1, 2), Value: 'i', Copy: true},
		crdt.Insert{ID: id(1, 2), Parent: id(1, 1), Value: 'i'},
		crdt.Insert{ID: id(1, 1), Parent: crdt.Root, Value: 'é'},
	}
	s, err := Replay(ops)
	if err != nil {
		t.Fatal(err)
	}
	const want = "4353533103" + "01010000e9010000" + "010201016900010103" + "02040102690100" + "0201030204" + "00"
	if got := hex.EncodeToString(Encode(s)); got != want {
		t.Fatalf("Encode = %s, want %s", got, want)
	}
	if s.Text() != "éi" {
		t.Fatalf("Text = %q", s.Text())
	}
}

func TestReplayReportsMissingDependency(t *testing.T) {
	if _, err := Replay([]crdt.Op{crdt.Insert{ID: id(1, 2), Parent: id(1, 1), Value: 'x'}}); err == nil {
		t.Fatal("orphan insert replayed")
	}
}

func TestResolveRangeFollowsCopies(t *testing.T) {
	s, err := Replay([]crdt.Op{
		crdt.Insert{ID: id(1, 1), Parent: crdt.Root, Value: 'a'},
		crdt.Insert{ID: id(1, 2), Parent: id(1, 1), Value: 'b'},
		crdt.Delete{ID: id(1, 3), Target: id(1, 1)},
		crdt.Insert{ID: id(2, 4), Parent: id(1, 1), Value: 'a', Copy: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	r, ok := ResolveRange(s, id(1, 1), id(1, 1))
	if !ok || r.Text != "a" || r.First != id(2, 4) {
		t.Fatalf("ResolveRange = %+v, %v", r, ok)
	}
}
