package store_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"math/rand/v2"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/testdb"
)

func TestAppendIdempotentAndConflict(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "doc", "owner")
	if err != nil {
		t.Fatal(err)
	}

	r, err := crdt.NewReplica(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, err := r.InsertAt(0, 'a')
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.InsertAt(1, 'b')
	if err != nil {
		t.Fatal(err)
	}
	batch := []*crdtv1.Operation{testdb.ProtoFromInsert(a), testdb.ProtoFromInsert(b)}

	seq1, err := st.Append(ctx, doc.ID, batch)
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	if seq1.OpSeq != 2 {
		t.Fatalf("op_seq %d", seq1.OpSeq)
	}
	seq2, err := st.Append(ctx, doc.ID, batch)
	if err != nil {
		t.Fatalf("second append: %v", err)
	}
	if seq2.OpSeq != seq1.OpSeq {
		t.Fatalf("idempotent seq %d vs %d", seq2.OpSeq, seq1.OpSeq)
	}

	conflict := proto.Clone(testdb.ProtoFromInsert(a)).(*crdtv1.Operation)
	conflict.GetInsert().Value = uint32('z')
	_, err = st.Append(ctx, doc.ID, []*crdtv1.Operation{conflict})
	var stored *domain.ConflictError
	if !errors.Is(err, domain.ErrConflict) || !errors.As(err, &stored) || *stored != (domain.ConflictError{Client: 1, Clock: a.ID.Clock}) {
		t.Fatalf("conflict with the log: %v", err)
	}
	_, err = st.Append(ctx, doc.ID, []*crdtv1.Operation{testdb.ProtoFromInsert(b), conflict, testdb.ProtoFromInsert(a)})
	var inBatch *domain.ConflictError
	if !errors.As(err, &inBatch) || *inBatch != (domain.ConflictError{Client: 1, Clock: a.ID.Clock}) {
		t.Fatalf("conflict within the batch: %v", err)
	}
}

func TestSnapshotCadence(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	seed := uint64(0x9e3779b97f4a7c15)
	rng := rand.New(rand.NewPCG(seed, seed))
	t.Logf("seed=%d", seed)

	doc, err := st.Create(ctx, "cadence", "owner")
	if err != nil {
		t.Fatal(err)
	}
	r, err := crdt.NewReplica(3, nil)
	if err != nil {
		t.Fatal(err)
	}
	textAtSeq := make(map[uint64]string, 250)
	for i := 0; i < 250; i++ {
		op, err := r.InsertAt(rng.IntN(r.Len()+1), rune('a'+i%26))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
		if _, err := st.Append(ctx, doc.ID, []*crdtv1.Operation{testdb.ProtoFromInsert(op)}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		textAtSeq[uint64(i+1)] = r.Text()
	}
	st.Close()
	final, err := st.Get(ctx, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.OpSeq != 250 || final.SnapshotSeq == 0 || final.OpSeq-final.SnapshotSeq >= domain.SnapshotEvery {
		t.Fatalf("final seqs op=%d snap=%d", final.OpSeq, final.SnapshotSeq)
	}
	_, snap, _, err := st.LoadState(ctx, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshotDoc, err := crdt.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	if snapshotDoc.Text() != textAtSeq[final.SnapshotSeq] {
		t.Fatalf("snapshot text %q vs %q at seq %d", snapshotDoc.Text(), textAtSeq[final.SnapshotSeq], final.SnapshotSeq)
	}

	batchDoc, err := st.Create(ctx, "batch", "owner")
	if err != nil {
		t.Fatal(err)
	}
	br, err := crdt.NewReplica(4, nil)
	if err != nil {
		t.Fatal(err)
	}
	ops := make([]*crdtv1.Operation, 0, 150)
	for i := 0; i < 150; i++ {
		op, err := br.InsertAt(br.Len(), 'x')
		if err != nil {
			t.Fatal(err)
		}
		ops = append(ops, testdb.ProtoFromInsert(op))
	}
	if _, err := st.Append(ctx, batchDoc.ID, ops); err != nil {
		t.Fatalf("150-op batch: %v", err)
	}
	if _, err := st.Compact(ctx, batchDoc.ID); err != nil {
		t.Fatalf("compact batch: %v", err)
	}
	got, err := st.Get(ctx, batchDoc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.OpSeq != 150 || got.SnapshotSeq != 150 {
		t.Fatalf("batch seqs op=%d snap=%d", got.OpSeq, got.SnapshotSeq)
	}
	_, batchSnap, _, err := st.LoadState(ctx, batchDoc.ID)
	if err != nil {
		t.Fatal(err)
	}
	batchSnapshotDoc, err := crdt.FromSnapshot(batchSnap)
	if err != nil {
		t.Fatal(err)
	}
	if batchSnapshotDoc.Text() != br.Text() {
		t.Fatalf("batch snapshot text %q vs %q", batchSnapshotDoc.Text(), br.Text())
	}
}

func TestCompactIdempotentAndConcurrent(t *testing.T) {
	st, pool := testdb.Open(t)
	ctx := t.Context()
	st.Close()

	appendHundred := func(title string, client crdt.ClientID) domain.Document {
		t.Helper()
		doc, err := st.Create(ctx, title, "owner")
		if err != nil {
			t.Fatal(err)
		}
		r, err := crdt.NewReplica(client, nil)
		if err != nil {
			t.Fatal(err)
		}
		ops := make([]*crdtv1.Operation, 0, domain.SnapshotEvery)
		for range domain.SnapshotEvery {
			op, err := r.InsertAt(r.Len(), 'x')
			if err != nil {
				t.Fatal(err)
			}
			ops = append(ops, testdb.ProtoFromInsert(op))
		}
		if _, err := st.Append(ctx, doc.ID, ops); err != nil {
			t.Fatal(err)
		}
		return doc
	}
	snapshotCount := func(id uuid.UUID) int {
		t.Helper()
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM snapshots WHERE document_id = $1`, id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}

	idempotent := appendHundred("idempotent", 30)
	first, err := st.Compact(ctx, idempotent.ID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := st.Compact(ctx, idempotent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if first != domain.SnapshotEvery || second != first || snapshotCount(idempotent.ID) != 1 {
		t.Fatalf("idempotent compact seqs %d, %d rows %d", first, second, snapshotCount(idempotent.ID))
	}

	concurrent := appendHundred("concurrent", 31)
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := st.Compact(ctx, concurrent.ID)
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent compact: %v", err)
		}
	}
	if count := snapshotCount(concurrent.ID); count != 1 {
		t.Fatalf("concurrent snapshot rows %d", count)
	}
}

func TestMissingSnapshotFailsLoadAndCompaction(t *testing.T) {
	st, pool := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "missing snapshot", "owner")
	if err != nil {
		t.Fatal(err)
	}
	replica, err := crdt.NewReplica(40, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendOps := func(count int) {
		t.Helper()
		ops := make([]*crdtv1.Operation, 0, count)
		for range count {
			op, err := replica.InsertAt(replica.Len(), 'x')
			if err != nil {
				t.Fatal(err)
			}
			ops = append(ops, testdb.ProtoFromInsert(op))
		}
		if _, err := st.Append(ctx, doc.ID, ops); err != nil {
			t.Fatal(err)
		}
	}

	appendOps(int(domain.SnapshotEvery))
	st.Close()
	compacted, err := st.Get(ctx, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if compacted.SnapshotSeq == 0 {
		t.Fatal("background compaction did not write a snapshot")
	}

	appendOps(int(domain.SnapshotEvery))
	if _, err := pool.Exec(ctx, `
		DELETE FROM snapshots
		WHERE document_id = $1 AND seq = $2`, doc.ID, int64(compacted.SnapshotSeq)); err != nil {
		t.Fatal(err)
	}

	if _, _, _, err := st.LoadState(ctx, doc.ID); err == nil {
		t.Fatal("LoadState succeeded without its snapshot")
	}
	if _, err := st.Compact(ctx, doc.ID); err == nil {
		t.Fatal("Compact succeeded without its base snapshot")
	}
	var count int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM snapshots WHERE document_id = $1`, doc.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("snapshot rows=%d", count)
	}
}

func TestRestoreEquivalence(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	seed := uint64(0x243f6a8885a308d3)
	rng := rand.New(rand.NewPCG(seed, seed))
	t.Logf("seed=%d", seed)

	doc, err := st.Create(ctx, "restore", "owner")
	if err != nil {
		t.Fatal(err)
	}
	r, err := crdt.NewReplica(5, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 120; i++ {
		op, err := r.InsertAt(rng.IntN(r.Len()+1), rune('A'+i%26))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Append(ctx, doc.ID, []*crdtv1.Operation{testdb.ProtoFromInsert(op)}); err != nil {
			t.Fatal(err)
		}
	}

	_, snap, ops, err := st.LoadState(ctx, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := crdt.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, protoOp := range ops {
		op, err := crdt.OpFromProto(protoOp)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := restored.Apply(op); err != nil {
			t.Fatal(err)
		}
	}
	if restored.Text() != r.Text() {
		t.Fatalf("text %q vs %q", restored.Text(), r.Text())
	}
	if !proto.Equal(restored.Snapshot(), r.Snapshot()) {
		t.Fatalf("snapshots differ")
	}
}

func TestConcurrentAppends(t *testing.T) {
	st, pool := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "race", "owner")
	if err != nil {
		t.Fatal(err)
	}

	const n = 8
	const per = 50
	replicas := make([]*crdt.Replica, n)
	for i := range replicas {
		r, err := crdt.NewReplica(crdt.ClientID(i+1), nil)
		if err != nil {
			t.Fatal(err)
		}
		replicas[i] = r
	}

	errCh := make(chan error, n)
	var wg sync.WaitGroup
	for i, r := range replicas {
		wg.Add(1)
		go func(r *crdt.Replica, client int) {
			defer wg.Done()
			for j := 0; j < per; j++ {
				op, err := r.InsertAt(r.Len(), rune('a'+client))
				if err != nil {
					errCh <- fmt.Errorf("replica %d insert %d: %w", client, j, err)
					return
				}
				if _, err := st.Append(ctx, doc.ID, []*crdtv1.Operation{testdb.ProtoFromInsert(op)}); err != nil {
					errCh <- fmt.Errorf("replica %d append %d: %w", client, j, err)
					return
				}
			}
		}(r, i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	got, err := st.Get(ctx, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.OpSeq != uint64(n*per) {
		t.Fatalf("op_seq %d", got.OpSeq)
	}

	rows, err := pool.Query(ctx, `SELECT seq FROM operations WHERE document_id = $1 ORDER BY seq`, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var seqs []uint64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, uint64(seq))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(seqs) != n*per {
		t.Fatalf("row count %d", len(seqs))
	}
	for i, seq := range seqs {
		if seq != uint64(i+1) {
			t.Fatalf("gap at %d: %d", i, seq)
		}
	}

	merged := crdt.New()
	for _, r := range replicas {
		if err := merged.Merge(r.Doc); err != nil {
			t.Fatal(err)
		}
	}
	_, snap, ops, err := st.LoadState(ctx, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := crdt.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, protoOp := range ops {
		op, err := crdt.OpFromProto(protoOp)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := restored.Apply(op); err != nil {
			t.Fatal(err)
		}
	}
	if restored.Text() != merged.Text() {
		t.Fatalf("merged text %q vs %q", restored.Text(), merged.Text())
	}
}

func TestAppendSeqs(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()

	t.Run("new ops", func(t *testing.T) {
		doc, err := st.Create(ctx, "new", "owner")
		if err != nil {
			t.Fatal(err)
		}
		ops := nInserts(t, 11, 3)
		got, err := st.Append(ctx, doc.ID, ops)
		if err != nil {
			t.Fatal(err)
		}
		want := []uint64{1, 2, 3}
		if got.OpSeq != 3 || got.FirstNewSeq != 1 || !equalSeqs(got.Seqs, want) {
			t.Fatalf("got %+v want seqs %v", got, want)
		}
	})

	t.Run("mix stored and new", func(t *testing.T) {
		doc, err := st.Create(ctx, "mix", "owner")
		if err != nil {
			t.Fatal(err)
		}
		first := nInserts(t, 12, 2)
		if _, err := st.Append(ctx, doc.ID, first); err != nil {
			t.Fatal(err)
		}
		r, err := crdt.NewReplica(12, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.InsertAt(0, 'a'); err != nil {
			t.Fatal(err)
		}
		if _, err := r.InsertAt(1, 'b'); err != nil {
			t.Fatal(err)
		}
		c, err := r.InsertAt(2, 'c')
		if err != nil {
			t.Fatal(err)
		}
		d, err := r.InsertAt(3, 'd')
		if err != nil {
			t.Fatal(err)
		}
		mixed := []*crdtv1.Operation{first[0], first[1], testdb.ProtoFromInsert(c), testdb.ProtoFromInsert(d)}
		got, err := st.Append(ctx, doc.ID, mixed)
		if err != nil {
			t.Fatal(err)
		}
		want := []uint64{1, 2, 3, 4}
		if got.OpSeq != 4 || got.FirstNewSeq != 3 || !equalSeqs(got.Seqs, want) {
			t.Fatalf("got %+v want seqs %v", got, want)
		}
	})

	t.Run("in-batch repeat", func(t *testing.T) {
		doc, err := st.Create(ctx, "repeat", "owner")
		if err != nil {
			t.Fatal(err)
		}
		ops := nInserts(t, 13, 2)
		batch := []*crdtv1.Operation{ops[0], ops[1], ops[0]}
		got, err := st.Append(ctx, doc.ID, batch)
		if err != nil {
			t.Fatal(err)
		}
		want := []uint64{1, 2, 1}
		if got.OpSeq != 2 || got.FirstNewSeq != 1 || !equalSeqs(got.Seqs, want) {
			t.Fatalf("got %+v want seqs %v", got, want)
		}
	})
}

func TestReadOperations(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "read", "owner")
	if err != nil {
		t.Fatal(err)
	}
	ops := nInserts(t, 21, 5)
	if _, err := st.Append(ctx, doc.ID, ops); err != nil {
		t.Fatal(err)
	}

	type want struct {
		firstSeq uint64
		n        int
		opSeq    uint64
		err      error
	}
	cases := []struct {
		name  string
		id    uuid.UUID
		after uint64
		limit int
		want  want
	}{
		{name: "after 0", id: doc.ID, after: 0, limit: 100, want: want{firstSeq: 1, n: 5, opSeq: 5}},
		{name: "after middle", id: doc.ID, after: 2, limit: 100, want: want{firstSeq: 3, n: 3, opSeq: 5}},
		{name: "after op_seq", id: doc.ID, after: 5, limit: 100, want: want{firstSeq: 0, n: 0, opSeq: 5}},
		{name: "limit truncation", id: doc.ID, after: 0, limit: 2, want: want{firstSeq: 1, n: 2, opSeq: 5}},
		{name: "limit 0 default", id: doc.ID, after: 0, limit: 0, want: want{firstSeq: 1, n: 5, opSeq: 5}},
		{name: "limit too large", id: doc.ID, after: 0, limit: 10001, want: want{err: domain.ErrInvalidArgument}},
		{name: "after_seq beyond op_seq", id: doc.ID, after: 6, limit: 10, want: want{err: domain.ErrInvalidArgument}},
		{name: "unknown doc", id: uuid.New(), after: 0, limit: 10, want: want{err: domain.ErrNotFound}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			firstSeq, got, opSeq, err := st.ReadOperations(ctx, tc.id, tc.after, tc.limit)
			if tc.want.err != nil {
				if !errors.Is(err, tc.want.err) {
					t.Fatalf("err %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if firstSeq != tc.want.firstSeq || len(got) != tc.want.n || opSeq != tc.want.opSeq {
				t.Fatalf("first=%d n=%d op_seq=%d", firstSeq, len(got), opSeq)
			}
			for i, op := range got {
				if !proto.Equal(op, ops[tc.after+uint64(i)]) {
					t.Fatalf("op %d", i)
				}
			}
		})
	}

	t.Run("archived readable", func(t *testing.T) {
		if _, err := st.Archive(ctx, doc.ID); err != nil {
			t.Fatal(err)
		}
		firstSeq, got, opSeq, err := st.ReadOperations(ctx, doc.ID, 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		if firstSeq != 1 || len(got) != 5 || opSeq != 5 {
			t.Fatalf("first=%d n=%d op_seq=%d", firstSeq, len(got), opSeq)
		}
	})

	t.Run("across compaction", func(t *testing.T) {
		compacted, err := st.Create(ctx, "compact-read", "owner")
		if err != nil {
			t.Fatal(err)
		}
		all := nInserts(t, 22, int(domain.SnapshotEvery)+20)
		if _, err := st.Append(ctx, compacted.ID, all); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Compact(ctx, compacted.ID); err != nil {
			t.Fatal(err)
		}
		gotDoc, err := st.Get(ctx, compacted.ID)
		if err != nil {
			t.Fatal(err)
		}
		if gotDoc.SnapshotSeq == 0 {
			t.Fatal("expected snapshot")
		}
		firstSeq, got, opSeq, err := st.ReadOperations(ctx, compacted.ID, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if firstSeq != 1 || len(got) != len(all) || opSeq != uint64(len(all)) {
			t.Fatalf("first=%d n=%d op_seq=%d", firstSeq, len(got), opSeq)
		}
	})

	t.Run("limit 0 caps at 1000", func(t *testing.T) {
		wide, err := st.Create(ctx, "wide", "owner")
		if err != nil {
			t.Fatal(err)
		}
		all := nInserts(t, 23, 1001)
		if _, err := st.Append(ctx, wide.ID, all); err != nil {
			t.Fatal(err)
		}
		firstSeq, got, opSeq, err := st.ReadOperations(ctx, wide.ID, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if firstSeq != 1 || len(got) != 1000 || opSeq != 1001 {
			t.Fatalf("first=%d n=%d op_seq=%d", firstSeq, len(got), opSeq)
		}
	})
}

func nInserts(t *testing.T, client crdt.ClientID, n int) []*crdtv1.Operation {
	t.Helper()
	r, err := crdt.NewReplica(client, nil)
	if err != nil {
		t.Fatal(err)
	}
	ops := make([]*crdtv1.Operation, 0, n)
	for range n {
		op, err := r.InsertAt(r.Len(), 'x')
		if err != nil {
			t.Fatal(err)
		}
		ops = append(ops, testdb.ProtoFromInsert(op))
	}
	return ops
}

func equalSeqs(got, want []uint64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
