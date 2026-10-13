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
	if seq1 != 2 {
		t.Fatalf("op_seq %d", seq1)
	}
	seq2, err := st.Append(ctx, doc.ID, batch)
	if err != nil {
		t.Fatalf("second append: %v", err)
	}
	if seq2 != seq1 {
		t.Fatalf("idempotent seq %d vs %d", seq2, seq1)
	}

	conflict := proto.Clone(testdb.ProtoFromInsert(a)).(*crdtv1.Operation)
	conflict.GetInsert().Value = uint32('z')
	if _, err := st.Append(ctx, doc.ID, []*crdtv1.Operation{conflict}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("conflict: %v", err)
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
