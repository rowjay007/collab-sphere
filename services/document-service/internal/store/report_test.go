package store_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"math/rand/v2"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/store"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/testdb"
)

func TestReportHomeSeqAndStaleVector(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "report", "owner")
	if err != nil {
		t.Fatal(err)
	}
	const replica = uint64(4)
	if _, _, err := st.ClaimReplica(ctx, doc.ID, replica, false, "", time.Hour); err != nil {
		t.Fatal(err)
	}
	r, err := crdt.NewReplica(crdt.ClientID(replica), nil)
	if err != nil {
		t.Fatal(err)
	}
	op, err := r.InsertAt(0, 'a')
	if err != nil {
		t.Fatal(err)
	}
	appended, err := st.Append(ctx, doc.ID, []*crdtv1.Operation{crdt.OpToProto(op)})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	vector := crdt.VersionVector{crdt.ClientID(replica): op.ID.Clock}
	if err := st.UpsertReport(ctx, doc.ForkRoot, replica, vector, appended.OpSeq, false, now); err != nil {
		t.Fatal(err)
	}
	reports, err := st.ListReports(ctx, doc.ForkRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].HomeSeq != appended.OpSeq || reports[0].HomeSeq != 1 {
		t.Fatalf("reports = %+v", reports)
	}
	if reports[0].Vector[crdt.ClientID(replica)] != op.ID.Clock {
		t.Fatalf("vector = %v", reports[0].Vector)
	}

	err = st.UpsertReport(ctx, doc.ForkRoot, replica, crdt.VersionVector{1: 1, crdt.ClientID(replica): op.ID.Clock - 1}, appended.OpSeq+1, false, now.Add(time.Second))
	if !errors.Is(err, domain.ErrStaleReport) {
		t.Fatalf("backwards report = %v", err)
	}
	reports, err = st.ListReports(ctx, doc.ForkRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].HomeSeq != appended.OpSeq || reports[0].Vector[crdt.ClientID(replica)] != op.ID.Clock || reports[0].Vector[1] != 0 {
		t.Fatalf("stale report changed the row: %+v", reports)
	}

	if err := st.UpsertReport(ctx, doc.ForkRoot, replica, vector, 12, false, now.Add(2*time.Second)); err != nil {
		t.Fatalf("equal vector: %v", err)
	}
	next, err := r.InsertAt(1, 'b')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, doc.ID, []*crdtv1.Operation{crdt.OpToProto(next)}); err != nil {
		t.Fatal(err)
	}
	above := crdt.VersionVector{crdt.ClientID(replica): next.ID.Clock}
	if err := st.UpsertReport(ctx, doc.ForkRoot, replica, above, 13, false, now.Add(3*time.Second)); err != nil {
		t.Fatalf("above vector: %v", err)
	}
	reports, err = st.ListReports(ctx, doc.ForkRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].HomeSeq != 13 || reports[0].Vector[crdt.ClientID(replica)] != next.ID.Clock {
		t.Fatalf("replaced report = %+v", reports)
	}
}

func TestReportClampedToHomeVector(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "clamp", "owner")
	if err != nil {
		t.Fatal(err)
	}
	const replica = uint64(5)
	if _, _, err := st.ClaimReplica(ctx, doc.ID, replica, false, "", time.Hour); err != nil {
		t.Fatal(err)
	}
	op := appendOne(t, st, doc.ID, crdt.ClientID(replica))
	forged := crdt.VersionVector{crdt.ClientID(replica): op.ID.Clock + 9, 99: 4}
	if err := st.UpsertReport(ctx, doc.ForkRoot, replica, forged, 1, false, time.Now()); err != nil {
		t.Fatal(err)
	}
	reports, err := st.ListReports(ctx, doc.ForkRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || len(reports[0].Vector) != 1 || reports[0].Vector[crdt.ClientID(replica)] != op.ID.Clock {
		t.Fatalf("forged report stored as %+v, want {%d:%d}", reports, replica, op.ID.Clock)
	}
}

func TestReportUnclaimedReplicaNotFound(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "unclaimed", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertReport(ctx, doc.ForkRoot, 3, crdt.VersionVector{}, 0, false, time.Now()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("unclaimed report = %v", err)
	}
}

func TestClaimLapsedFollowsResyncPending(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "pending", "owner")
	if err != nil {
		t.Fatal(err)
	}
	const replica = uint64(11)
	if _, lapsed, err := st.ClaimReplica(ctx, doc.ID, replica, false, "", time.Hour); err != nil || lapsed {
		t.Fatalf("first claim lapsed=%v err=%v", lapsed, err)
	}
	if err := st.UpsertReport(ctx, doc.ForkRoot, replica, crdt.VersionVector{}, 0, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, lapsed, err := st.ClaimReplica(ctx, doc.ID, replica, false, "", time.Hour); err != nil || !lapsed {
		t.Fatalf("claim after pending report lapsed=%v err=%v, want lapsed", lapsed, err)
	}
	if err := st.UpsertReport(ctx, doc.ForkRoot, replica, crdt.VersionVector{}, 0, false, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, lapsed, err := st.ClaimReplica(ctx, doc.ID, replica, false, "", time.Hour); err != nil || lapsed {
		t.Fatalf("claim after cleared report lapsed=%v err=%v, want not lapsed", lapsed, err)
	}
}

func TestReleasedReplicaReclaimRevives(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "revive", "owner")
	if err != nil {
		t.Fatal(err)
	}
	const replica = uint64(12)
	if _, _, err := st.ClaimReplica(ctx, doc.ID, replica, false, "", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := st.ReleaseReplica(ctx, doc.ID, replica); err != nil {
		t.Fatal(err)
	}
	if _, lapsed, err := st.ClaimReplica(ctx, doc.ID, replica, false, "", time.Hour); err != nil || !lapsed {
		t.Fatalf("re-claim after release lapsed=%v err=%v, want lapsed", lapsed, err)
	}
	reports, err := st.ListReports(ctx, doc.ForkRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].ReleasedAt != nil {
		t.Fatalf("released_at after re-claim: %+v", reports)
	}
}

func appendOne(t *testing.T, st *store.Store, id uuid.UUID, client crdt.ClientID) crdt.Insert {
	t.Helper()
	r, err := crdt.NewReplica(client, nil)
	if err != nil {
		t.Fatal(err)
	}
	op, err := r.InsertAt(0, 'x')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(t.Context(), id, []*crdtv1.Operation{crdt.OpToProto(op)}); err != nil {
		t.Fatal(err)
	}
	return op
}

func TestReleaseReplicaKeepsRow(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "release", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ClaimReplica(ctx, doc.ID, 2, false, "", time.Hour); err != nil {
		t.Fatal(err)
	}
	op := appendOne(t, st, doc.ID, 2)
	vector := crdt.VersionVector{2: op.ID.Clock}
	if err := st.UpsertReport(ctx, doc.ForkRoot, 2, vector, 4, false, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.ReleaseReplica(ctx, doc.ID, 2); err != nil {
		t.Fatal(err)
	}
	reports, err := st.ListReports(ctx, doc.ForkRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].ReleasedAt == nil || reports[0].HomeSeq != 4 || reports[0].Vector[2] != op.ID.Clock {
		t.Fatalf("released reports = %+v", reports)
	}
	released := *reports[0].ReleasedAt
	if err := st.ReleaseReplica(ctx, doc.ID, 2); err != nil {
		t.Fatal(err)
	}
	reports, err = st.ListReports(ctx, doc.ForkRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].ReleasedAt == nil || !reports[0].ReleasedAt.Equal(released) {
		t.Fatalf("second release = %+v", reports)
	}
}

func TestSystemClaimWritesInitialReport(t *testing.T) {
	st, pool := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "system", "owner")
	if err != nil {
		t.Fatal(err)
	}
	r, err := crdt.NewReplica(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	op, err := r.InsertAt(0, 's')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, doc.ID, []*crdtv1.Operation{crdt.OpToProto(op)}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ClaimReplica(ctx, doc.ID, 9, false, "", time.Hour); err != nil {
		t.Fatal(err)
	}
	reports, err := st.ListReports(ctx, doc.ForkRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].Replica != 9 || len(reports[0].Vector) != 0 || reports[0].HomeSeq != 0 || reports[0].ReleasedAt != nil {
		t.Fatalf("client claim report = %+v, want empty vector at home_seq 0", reports)
	}
	sys, _, err := st.ClaimReplica(ctx, doc.ID, 0, true, "", time.Hour)
	if err != nil || sys == 0 {
		t.Fatalf("system claim = %d, %v", sys, err)
	}
	sysReport := func() store.Report {
		t.Helper()
		reports, err := st.ListReports(ctx, doc.ForkRoot)
		if err != nil {
			t.Fatal(err)
		}
		if len(reports) != 2 {
			t.Fatalf("reports = %+v", reports)
		}
		for _, r := range reports {
			if r.Replica == sys {
				return r
			}
		}
		t.Fatalf("no system report in %+v", reports)
		return store.Report{}
	}
	if got := sysReport(); got.HomeSeq != 1 || got.Vector[1] != op.ID.Clock || got.ReleasedAt != nil {
		t.Fatalf("system report = %+v", got)
	}
	got := readVector(t, pool, doc.ID)
	if got[1] != op.ID.Clock {
		t.Fatalf("documents.vector = %v", got)
	}

	next, err := r.InsertAt(1, 't')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, doc.ID, []*crdtv1.Operation{crdt.OpToProto(next)}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ClaimReplica(ctx, doc.ID, sys, true, "", time.Hour); err != nil {
		t.Fatal(err)
	}
	if got := sysReport(); got.Vector[1] != op.ID.Clock || got.HomeSeq != 1 {
		t.Fatalf("re-claim moved the system floor: %+v", got)
	}
}

func TestBackfillReportsOnce(t *testing.T) {
	st, pool := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "backfill", "owner")
	if err != nil {
		t.Fatal(err)
	}
	const replica = uint64(6)
	if _, _, err := st.ClaimReplica(ctx, doc.ID, replica, false, "", time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM replica_reports WHERE fork_root = $1`, doc.ForkRoot); err != nil {
		t.Fatal(err)
	}
	state := crdt.New()
	r, err := crdt.NewReplica(crdt.ClientID(replica), state)
	if err != nil {
		t.Fatal(err)
	}
	op, err := r.InsertAt(0, 'b')
	if err != nil {
		t.Fatal(err)
	}
	appended, err := st.Append(ctx, doc.ID, []*crdtv1.Operation{crdt.OpToProto(op)})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.BackfillReports(ctx); err != nil {
		t.Fatal(err)
	}
	reports, err := st.ListReports(ctx, doc.ForkRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].Replica != replica || reports[0].HomeSeq != appended.OpSeq || reports[0].Vector[crdt.ClientID(replica)] != op.ID.Clock {
		t.Fatalf("backfill = %+v", reports)
	}
	if err := st.UpsertReport(ctx, doc.ForkRoot, replica, crdt.VersionVector{crdt.ClientID(replica): op.ID.Clock}, 40, false, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.BackfillReports(ctx); err != nil {
		t.Fatal(err)
	}
	reports, err = st.ListReports(ctx, doc.ForkRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].HomeSeq != 40 || reports[0].Vector[crdt.ClientID(replica)] != op.ID.Clock {
		t.Fatalf("backfill regressed the report: %+v", reports)
	}
}

func TestNullVectorBackfillsOnce(t *testing.T) {
	st, pool := testdb.Open(t)
	ctx := t.Context()
	docRow, err := st.Create(ctx, "null-vector", "owner")
	if err != nil {
		t.Fatal(err)
	}
	state := crdt.New()
	reps := make([]*crdt.Replica, 3)
	for i := range reps {
		reps[i], err = crdt.NewReplica(crdt.ClientID(i+1), state)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < int(domain.SnapshotEvery); i++ {
		r := reps[i%len(reps)]
		op, err := r.InsertAt(0, 'a')
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Append(ctx, docRow.ID, []*crdtv1.Operation{crdt.OpToProto(op)}); err != nil {
			t.Fatal(err)
		}
	}
	st.Close()
	stored, err := st.Get(ctx, docRow.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.SnapshotSeq == 0 {
		t.Fatal("snapshot was not written")
	}
	if _, err := pool.Exec(ctx, `UPDATE documents SET vector = NULL WHERE id = $1`, docRow.ID); err != nil {
		t.Fatal(err)
	}
	next, err := reps[0].InsertAt(0, 'z')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, docRow.ID, []*crdtv1.Operation{crdt.OpToProto(next)}); err != nil {
		t.Fatal(err)
	}
	assertStoredVector(t, pool, docRow.ID, state.Vector())
	again, err := reps[1].InsertAt(0, 'y')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, docRow.ID, []*crdtv1.Operation{crdt.OpToProto(again)}); err != nil {
		t.Fatal(err)
	}
	assertStoredVector(t, pool, docRow.ID, state.Vector())
}

func TestDocumentVectorMatchesReplay(t *testing.T) {
	seeds := []uint64{1, 7, 42, 99}
	for _, seed := range seeds {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			st, pool := testdb.Open(t)
			ctx := t.Context()
			docRow, err := st.Create(ctx, "vector", "owner")
			if err != nil {
				t.Fatal(err)
			}
			state := crdt.New()
			reps := make([]*crdt.Replica, 3)
			for i := range reps {
				reps[i], err = crdt.NewReplica(crdt.ClientID(i+1), state)
				if err != nil {
					t.Fatal(err)
				}
				op, err := reps[i].InsertAt(0, 'a')
				if err != nil {
					t.Fatal(err)
				}
				if _, err := st.Append(ctx, docRow.ID, []*crdtv1.Operation{crdt.OpToProto(op)}); err != nil {
					t.Fatal(err)
				}
			}
			rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
			for i := 0; i < 24; i++ {
				r := reps[rng.IntN(len(reps))]
				var op crdt.Op
				if r.Len() > 0 && rng.IntN(2) == 0 {
					del, err := r.DeleteAt(rng.IntN(r.Len()))
					if err != nil {
						t.Fatal(err)
					}
					op = del
				} else {
					ins, err := r.InsertAt(rng.IntN(r.Len()+1), rune('a'+rng.IntN(26)))
					if err != nil {
						t.Fatal(err)
					}
					op = ins
				}
				if _, err := st.Append(ctx, docRow.ID, []*crdtv1.Operation{crdt.OpToProto(op)}); err != nil {
					t.Fatal(err)
				}
			}
			if state.Len() == 0 {
				ins, err := reps[0].InsertAt(0, 'z')
				if err != nil {
					t.Fatal(err)
				}
				if _, err := st.Append(ctx, docRow.ID, []*crdtv1.Operation{crdt.OpToProto(ins)}); err != nil {
					t.Fatal(err)
				}
			}
			del, err := reps[0].DeleteAt(0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.Append(ctx, docRow.ID, []*crdtv1.Operation{crdt.OpToProto(del)}); err != nil {
				t.Fatal(err)
			}
			assertStoredVector(t, pool, docRow.ID, state.Vector())
		})
	}
}

func assertStoredVector(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, want crdt.VersionVector) {
	t.Helper()
	got := readVector(t, pool, id)
	wantN := map[uint64]uint64{}
	for client, clock := range want {
		if client != 0 && clock != 0 {
			wantN[uint64(client)] = clock
		}
	}
	if len(got) != len(wantN) {
		t.Fatalf("documents.vector = %v, replay = %v", got, wantN)
	}
	for client, clock := range wantN {
		if got[client] != clock {
			t.Fatalf("documents.vector = %v, replay = %v", got, wantN)
		}
	}
}

func readVector(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) map[uint64]uint64 {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(t.Context(), `SELECT vector FROM documents WHERE id = $1`, id).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if len(raw) == 0 {
		t.Fatal("documents.vector is null")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var obj map[string]json.Number
	if err := dec.Decode(&obj); err != nil {
		t.Fatal(err)
	}
	out := make(map[uint64]uint64, len(obj))
	for key, num := range obj {
		client, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		clock, err := strconv.ParseUint(num.String(), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		out[client] = clock
	}
	return out
}
