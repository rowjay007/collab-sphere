package worker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	docgc "github.com/rowjay007/collab-sphere/services/document-service/internal/gc"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/testdb"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/worker"
)

type fakeFloor struct {
	vec crdt.VersionVector
	err error
}

func (f fakeFloor) RestoreFloor(context.Context, uuid.UUID) (crdt.VersionVector, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.vec == nil {
		return crdt.VersionVector{}, nil
	}
	return f.vec.Clone(), nil
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
	gc := worker.GC{Store: st, Floor: fakeFloor{err: errors.New("down")}, Pins: fakePins{}, TTL: time.Hour}
	gc.RunDocument(ctx, doc.ID)
	assertLiveSeq(t, pool, doc.ID, 1)
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

func assertLiveSeq(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, want int64) {
	t.Helper()
	var seq int64
	if err := pool.QueryRow(context.Background(), `SELECT seq FROM snapshots WHERE document_id = $1 AND kind = 'live'`, id).Scan(&seq); err != nil || seq != want {
		t.Fatalf("live seq=%d err=%v want %d", seq, err, want)
	}
}
