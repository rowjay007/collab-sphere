package stack

import (
	"context"
	"testing"
	"time"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func TestAllocatedReplicaReconnectsToAnotherHub(t *testing.T) {
	s := New(t, WithHubs(2))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	docID := s.CreateDocument(ctx)
	client := s.DialAllocated(ctx, docID, 0)
	id := client.ID()
	if id == 0 {
		t.Fatal("allocated replica is zero")
	}
	if err := client.InsertAt(0, 'a'); err != nil {
		t.Fatal(err)
	}
	if err := client.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}

	client.Disconnect()
	if err := client.Reconnect(ctx, s.HubURL(1)); err != nil {
		t.Fatal(err)
	}
	if client.ID() != id {
		t.Fatalf("replica changed from %d to %d", id, client.ID())
	}
	if err := client.InsertAt(1, 'b'); err != nil {
		t.Fatal(err)
	}
	if err := client.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}

	state, err := s.Docs.LoadState(ctx, &documentv1.LoadStateRequest{Id: docID})
	if err != nil {
		t.Fatal(err)
	}
	if len(state.GetOperations()) != 2 {
		t.Fatalf("operations = %d, want 2", len(state.GetOperations()))
	}
	for i, raw := range state.GetOperations() {
		op, err := crdt.OpFromProto(raw)
		if err != nil {
			t.Fatal(err)
		}
		if op.OpID().Client != id {
			t.Fatalf("operation %d client = %d, want %d", i, op.OpID().Client, id)
		}
	}
}
