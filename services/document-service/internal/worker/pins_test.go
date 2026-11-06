package worker_test

import (
	"testing"
	"time"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	docgc "github.com/rowjay007/collab-sphere/services/document-service/internal/gc"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/testdb"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/worker"
)

func TestGCWorkerPinnedLeafSurvives(t *testing.T) {
	st, pool := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "pin", "owner")
	if err != nil {
		t.Fatal(err)
	}
	r, err := crdt.NewReplica(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	left, err := r.InsertAfter(crdt.Root, 'a')
	if err != nil {
		t.Fatal(err)
	}
	right, err := r.InsertAfter(crdt.Root, 'b')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Delete(left.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Delete(right.ID); err != nil {
		t.Fatal(err)
	}
	var protoOps []*crdtv1.Operation
	for _, op := range r.Ops() {
		protoOps = append(protoOps, crdt.OpToProto(op))
	}
	appended, err := st.Append(ctx, doc.ID, protoOps)
	if err != nil {
		t.Fatal(err)
	}
	vec, ok, err := st.DocumentCommittedVector(ctx, doc.ID)
	if err != nil || !ok {
		t.Fatalf("document vector=%v ok=%v err=%v", vec, ok, err)
	}
	full := r.Vector()
	now := time.Now()
	if err := st.UpsertReport(ctx, doc.ForkRoot, 1, full, appended.OpSeq, now); err != nil {
		t.Fatal(err)
	}
	pins := map[crdt.ID]struct{}{left.ID: {}}
	gc := worker.GC{
		Store: st,
		Floor: fakeFloor{vec: full},
		Pins:  fakePins{pins: pins},
		TTL:   time.Hour,
	}
	gc.RunDocument(ctx, doc.ID)
	d, _, err := st.BuildDocThrough(ctx, doc.ID, appended.OpSeq)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Has(left.ID) || d.Has(right.ID) {
		t.Fatalf("pinned=%v sibling=%v", d.Has(left.ID), d.Has(right.ID))
	}
	var ops int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM operations WHERE document_id = $1`, doc.ID).Scan(&ops); err != nil || ops != len(protoOps) {
		t.Fatalf("operations=%d err=%v want %d", ops, err, len(protoOps))
	}
}

func TestGCWorkerStableMinWithReport(t *testing.T) {
	doc := crdt.VersionVector{1: 4}
	low := docgc.Report{Vector: crdt.VersionVector{1: 2}, ReportedAt: time.Now()}
	stable := docgc.Stable(docgc.Input{Doc: doc, Reports: []docgc.Report{low, {Vector: doc}}, Floor: doc})
	if stable[1] != 2 {
		t.Fatalf("stable=%v", stable)
	}
}
