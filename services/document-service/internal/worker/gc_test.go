package worker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/pkg/metrics"
	docgc "github.com/rowjay007/collab-sphere/services/document-service/internal/gc"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/store"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/testdb"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/worker"
)

type fakeFloor struct {
	vec   crdt.VersionVector
	byDoc map[uuid.UUID]crdt.VersionVector
	err   error
}

func (f fakeFloor) RestoreFloor(_ context.Context, id uuid.UUID) (crdt.VersionVector, error) {
	if f.err != nil {
		return nil, f.err
	}
	vec := f.vec
	if f.byDoc != nil {
		vec = f.byDoc[id]
	}
	if vec == nil {
		return crdt.VersionVector{}, nil
	}
	return vec.Clone(), nil
}

type fakePins struct {
	pins map[crdt.ID]struct{}
	err  error
}

func (f fakePins) AnchorPins(context.Context, uuid.UUID) (map[crdt.ID]struct{}, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.pins, nil
}

func TestGCWorkerFloorErrorKeepsLiveRow(t *testing.T) {
	st, pool := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "skip", "owner")
	if err != nil {
		t.Fatal(err)
	}
	r, err := crdt.NewReplica(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	op, err := r.InsertAfter(crdt.Root, 'x')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, doc.ID, []*crdtv1.Operation{crdt.OpToProto(op)}); err != nil {
		t.Fatal(err)
	}
	d, _, err := st.BuildDocThrough(ctx, doc.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertLiveSnapshot(ctx, doc.ID, 1, d.Snapshot()); err != nil {
		t.Fatal(err)
	}
	next, err := r.InsertAfter(op.ID, 'y')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, doc.ID, []*crdtv1.Operation{crdt.OpToProto(next)}); err != nil {
		t.Fatal(err)
	}
	gc := worker.GC{Store: st, Floor: fakeFloor{err: errors.New("down")}, Pins: fakePins{}, TTL: time.Hour}
	gc.RunDocument(ctx, doc.ID)
	assertLiveSeq(t, pool, doc.ID, 1)
}

func TestGCWorkerFloorCoversForkTree(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "tree", "owner")
	if err != nil {
		t.Fatal(err)
	}
	leaf, full, opSeq := appendDeletedLeaf(t, st, doc.ID, doc.ForkRoot, time.Now())
	branch, err := st.Fork(ctx, doc.ID, opSeq, "branch", "fork-tree")
	if err != nil {
		t.Fatal(err)
	}
	gc := worker.GC{
		Store: st,
		Floor: fakeFloor{byDoc: map[uuid.UUID]crdt.VersionVector{doc.ID: full, branch.ID: {}}},
		Pins:  fakePins{},
		TTL:   time.Hour,
	}
	gc.RunDocument(ctx, doc.ID)
	if !hasNode(t, st, doc.ID, opSeq, leaf) {
		t.Fatal("leaf collected while sibling branch floor does not cover its delete")
	}
	if _, err := st.Archive(ctx, branch.ID); err != nil {
		t.Fatal(err)
	}
	gc.RunDocument(ctx, doc.ID)
	if hasNode(t, st, doc.ID, opSeq, leaf) {
		t.Fatal("leaf kept after the only uncovering branch was archived")
	}
}

func TestGCWorkerSilentClaimProtectsLeaf(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "silent", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ClaimReplica(ctx, doc.ID, 2, false, "", time.Hour); err != nil {
		t.Fatal(err)
	}
	leaf, full, opSeq := appendDeletedLeaf(t, st, doc.ID, doc.ForkRoot, time.Now())
	gc := worker.GC{Store: st, Floor: fakeFloor{vec: full}, Pins: fakePins{}, TTL: time.Hour}
	gc.RunDocument(ctx, doc.ID)
	if !hasNode(t, st, doc.ID, opSeq, leaf) {
		t.Fatal("leaf collected while a claimed replica has never reported")
	}
}

func TestStableRetiredReport(t *testing.T) {
	doc := crdt.VersionVector{1: 3}
	fresh := docgc.Report{Vector: crdt.VersionVector{1: 1}, ReportedAt: time.Now()}
	stale := docgc.Report{Vector: crdt.VersionVector{1: 0}, ReportedAt: time.Now().Add(-400 * time.Hour), Retired: true}
	stable := docgc.Stable(docgc.Input{Doc: doc, Reports: []docgc.Report{fresh, stale}, Floor: doc})
	if stable[1] != 1 {
		t.Fatalf("stable=%v", stable)
	}
}

func TestGCPassGaugesAggregateDocuments(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	young, err := st.Create(ctx, "young", "owner")
	if err != nil {
		t.Fatal(err)
	}
	_, youngFull, _ := appendDeletedLeaf(t, st, young.ID, young.ForkRoot, time.Now().Add(-time.Minute))
	old, err := st.Create(ctx, "old", "owner")
	if err != nil {
		t.Fatal(err)
	}
	_, oldFull, _ := appendDeletedLeaf(t, st, old.ID, old.ForkRoot, time.Now().Add(-2*time.Hour))

	ids, err := st.ListGCCandidates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != old.ID || ids[1] != young.ID {
		t.Fatalf("candidates=%v, want old %s collected before young %s", ids, old.ID, young.ID)
	}

	m := metrics.NewDoc(prometheus.NewRegistry())
	gc := worker.GC{
		Store:   st,
		Floor:   fakeFloor{byDoc: map[uuid.UUID]crdt.VersionVector{young.ID: youngFull, old.ID: oldFull}},
		Pins:    fakePins{},
		Metrics: m,
		TTL:     24 * time.Hour,
	}
	gc.Pass(ctx)

	age := gaugeValue(t, m.GCStableAge)
	if age < (2 * time.Hour).Seconds() {
		t.Fatalf("stable age gauge=%.0fs, want the older document's age of at least 7200s", age)
	}
	if pinned := gaugeValue(t, m.GCPinned); pinned != 2 {
		t.Fatalf("pinned gauge=%v, want 2 distinct replicas across both fork trees", pinned)
	}
}

// appendDeletedLeaf claims replica 1 on id, appends an insert and its delete,
// and stores replica 1's report covering both, reported at reportedAt.
func appendDeletedLeaf(t *testing.T, st *store.Store, id, forkRoot uuid.UUID, reportedAt time.Time) (crdt.ID, crdt.VersionVector, uint64) {
	t.Helper()
	ctx := t.Context()
	if _, _, err := st.ClaimReplica(ctx, id, 1, false, "", time.Hour); err != nil {
		t.Fatal(err)
	}
	r, err := crdt.NewReplica(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	ins, err := r.InsertAfter(crdt.Root, 'a')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Delete(ins.ID); err != nil {
		t.Fatal(err)
	}
	var protoOps []*crdtv1.Operation
	for _, op := range r.Ops() {
		protoOps = append(protoOps, crdt.OpToProto(op))
	}
	appended, err := st.Append(ctx, id, protoOps)
	if err != nil {
		t.Fatal(err)
	}
	full := r.Vector()
	if err := st.UpsertReport(ctx, forkRoot, 1, full, appended.OpSeq, false, reportedAt); err != nil {
		t.Fatal(err)
	}
	return ins.ID, full, appended.OpSeq
}

func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var pb dto.Metric
	if err := g.Write(&pb); err != nil {
		t.Fatal(err)
	}
	return pb.GetGauge().GetValue()
}

func hasNode(t *testing.T, st *store.Store, id uuid.UUID, seq uint64, node crdt.ID) bool {
	t.Helper()
	d, _, err := st.BuildDocThrough(t.Context(), id, seq)
	if err != nil {
		t.Fatal(err)
	}
	return d.Has(node)
}

func assertLiveSeq(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, want int64) {
	t.Helper()
	var seq int64
	if err := pool.QueryRow(context.Background(), `SELECT seq FROM snapshots WHERE document_id = $1 AND kind = 'live'`, id).Scan(&seq); err != nil || seq != want {
		t.Fatalf("live seq=%d err=%v want %d", seq, err, want)
	}
}
