package crdt_test

import (
	"testing"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"google.golang.org/protobuf/proto"
)

func TestSnapshotRoundTripWithPending(t *testing.T) {
	doc := crdt.New()
	parent := crdt.Insert{ID: crdt.ID{Clock: 2, Client: 1}, Parent: crdt.ID{Clock: 1, Client: 1}, Value: 'b'}
	deleteOp := crdt.Delete{ID: crdt.ID{Clock: 3, Client: 2}, Target: parent.ID}
	if status, err := doc.Apply(parent); err != nil || status != crdt.Pending {
		t.Fatalf("pending insert: %v, %v", status, err)
	}
	if status, err := doc.Apply(deleteOp); err != nil || status != crdt.Pending {
		t.Fatalf("pending delete: %v, %v", status, err)
	}
	restored, err := crdt.FromSnapshot(doc.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(doc.Snapshot(), restored.Snapshot()) {
		t.Fatal("round-trip snapshot differs")
	}
	first := crdt.Insert{ID: crdt.ID{Clock: 1, Client: 1}, Parent: crdt.Root, Value: 'a'}
	if _, err := doc.Apply(first); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.Apply(first); err != nil {
		t.Fatal(err)
	}
	if doc.Text() != restored.Text() || !proto.Equal(doc.Snapshot(), restored.Snapshot()) {
		t.Fatal("restored document diverged")
	}
}
