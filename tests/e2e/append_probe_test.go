package e2e

import (
	"context"
	"slices"
	"testing"
	"time"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

// TestAppendLatency measures document-service AppendOperations on its own,
// one op per call, which is the hub's per-batch persistence cost.
func TestAppendLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("probe")
	}
	s := newStack(t)
	ctx := context.Background()
	docID := s.createDocument(ctx)
	r, _ := crdt.NewReplica(1, nil)

	var took []time.Duration
	for i := range 1000 {
		op, err := r.InsertAt(i, 'x')
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		_, err = s.docs.AppendOperations(ctx, &documentv1.AppendOperationsRequest{
			Id: docID, Operations: []*crdtv1.Operation{crdt.OpToProto(op)},
		})
		took = append(took, time.Since(start))
		if err != nil {
			t.Fatal(err)
		}
	}
	slices.Sort(took)
	var total time.Duration
	for _, d := range took {
		total += d
	}
	t.Logf("AppendOperations x%d: mean=%s p50=%s p99=%s max=%s", len(took), total/time.Duration(len(took)),
		took[len(took)/2], took[len(took)*99/100], took[len(took)-1])
}
