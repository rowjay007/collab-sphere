package store_test

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/store"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/testdb"
)

func TestForkCopiesPrefixAndStaysIndependent(t *testing.T) {
	st, pool := testdb.Open(t)
	ctx := t.Context()
	src, err := st.Create(ctx, "source", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if src.ForkRoot != src.ID || src.ForkOf != nil {
		t.Fatalf("created %+v", src)
	}

	r, err := crdt.NewReplica(21, nil)
	if err != nil {
		t.Fatal(err)
	}
	var atSeq uint64
	var textAtSeq string
	var vecAtSeq crdt.VersionVector
	for i, ch := range []rune("abcdef") {
		op, err := r.InsertAt(r.Len(), ch)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Append(ctx, src.ID, []*crdtv1.Operation{testdb.ProtoFromInsert(op)}); err != nil {
			t.Fatal(err)
		}
		if i == 3 {
			atSeq = 4
			textAtSeq = r.Text()
			vecAtSeq = maps.Clone(r.Vector())
		}
	}

	if _, err := st.ClaimReplica(ctx, src.ID, 21); err != nil {
		t.Fatal(err)
	}

	fork, err := st.Fork(ctx, src.ID, atSeq, "branch", "fork-1")
	if err != nil {
		t.Fatal(err)
	}
	if fork.ForkOf == nil || *fork.ForkOf != src.ID || fork.ForkSeq != atSeq || fork.ForkRoot != src.ID {
		t.Fatalf("fork origin %+v", fork)
	}
	if fork.OpSeq != atSeq || fork.OwnerID != "owner" || fork.Title != "branch" {
		t.Fatalf("fork meta %+v", fork)
	}

	got, meta := loadDoc(t, st, fork.ID)
	if meta.OpSeq != atSeq {
		t.Fatalf("fork op_seq %d", meta.OpSeq)
	}
	if got.Text() != textAtSeq || !maps.Equal(got.Vector(), vecAtSeq) {
		t.Fatalf("fork state %q %v, want %q %v", got.Text(), got.Vector(), textAtSeq, vecAtSeq)
	}
	srcHead, _ := loadDoc(t, st, src.ID)
	if srcHead.Text() == textAtSeq {
		t.Fatal("source was still at the fork seq")
	}

	first, ops, opSeq, err := st.ReadOperations(ctx, fork.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 || opSeq != atSeq || uint64(len(ops)) != atSeq {
		t.Fatalf("read prefix first=%d n=%d op_seq=%d", first, len(ops), opSeq)
	}
	sourceBytes := operationBytes(t, pool, src.ID)
	forkBytes := operationBytes(t, pool, fork.ID)
	for i := range forkBytes {
		if !bytes.Equal(forkBytes[i], sourceBytes[i]) {
			t.Fatalf("operation %d bytes differ", i+1)
		}
	}

	fr, err := crdt.NewReplica(22, got)
	if err != nil {
		t.Fatal(err)
	}
	fop, err := fr.InsertAt(fr.Len(), 'z')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, fork.ID, []*crdtv1.Operation{testdb.ProtoFromInsert(fop)}); err != nil {
		t.Fatal(err)
	}
	sr, err := crdt.NewReplica(21, srcHead)
	if err != nil {
		t.Fatal(err)
	}
	sop, err := sr.InsertAt(sr.Len(), 'y')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, src.ID, []*crdtv1.Operation{testdb.ProtoFromInsert(sop)}); err != nil {
		t.Fatal(err)
	}

	srcAfter, _ := loadDoc(t, st, src.ID)
	forkAfter, forkMeta := loadDoc(t, st, fork.ID)
	if srcAfter.Text() == forkAfter.Text() {
		t.Fatalf("logs coupled: source %q fork %q", srcAfter.Text(), forkAfter.Text())
	}
	if forkAfter.Text() != textAtSeq+"z" {
		t.Fatalf("fork text %q", forkAfter.Text())
	}
	if srcAfter.Text() == textAtSeq+"z" {
		t.Fatal("source absorbed the fork append")
	}

	first, ops, opSeq, err = st.ReadOperations(ctx, fork.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 || opSeq != atSeq+1 || uint64(len(ops)) != atSeq+1 {
		t.Fatalf("read after append first=%d n=%d op_seq=%d", first, len(ops), opSeq)
	}
	if forkMeta.OpSeq != atSeq+1 {
		t.Fatalf("fork moved to %d", forkMeta.OpSeq)
	}

	if _, err := st.ClaimReplica(ctx, fork.ID, 21); !errors.Is(err, domain.ErrReplicaHomedElsewhere) {
		t.Fatalf("source replica on fork: %v", err)
	}
	var replicaRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM replicas WHERE document_id = $1`, fork.ID).Scan(&replicaRows); err != nil {
		t.Fatal(err)
	}
	if replicaRows != 0 {
		t.Fatalf("copied %d replica rows", replicaRows)
	}

	repeat, err := st.Fork(ctx, src.ID, atSeq, "branch", "fork-1")
	if err != nil || repeat.ID != fork.ID {
		t.Fatalf("idempotent retry = %+v, %v", repeat, err)
	}
	if _, err := st.Fork(ctx, src.ID, atSeq-1, "other", "fork-1"); !errors.Is(err, domain.ErrAlreadyExists) {
		t.Fatalf("conflicting seq: %v", err)
	}
	other, err := st.Create(ctx, "other", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Fork(ctx, other.ID, 0, "other", "fork-1"); !errors.Is(err, domain.ErrAlreadyExists) {
		t.Fatalf("conflicting source: %v", err)
	}

	child, err := st.Fork(ctx, fork.ID, atSeq, "grandchild", "fork-2")
	if err != nil {
		t.Fatal(err)
	}
	if child.ForkRoot != src.ID || child.ForkOf == nil || *child.ForkOf != fork.ID || child.ForkSeq != atSeq {
		t.Fatalf("fork of fork %+v", child)
	}
}

func TestForkLoadStateUsesCopiedSnapshot(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	src, err := st.Create(ctx, "snap", "owner")
	if err != nil {
		t.Fatal(err)
	}
	r, err := crdt.NewReplica(23, nil)
	if err != nil {
		t.Fatal(err)
	}
	textAt := make(map[uint64]string, 170)
	vecAt := make(map[uint64]crdt.VersionVector, 170)
	ops := make([]*crdtv1.Operation, 0, 150)
	for range 150 {
		op, err := r.InsertAt(r.Len(), 'x')
		if err != nil {
			t.Fatal(err)
		}
		ops = append(ops, testdb.ProtoFromInsert(op))
		textAt[uint64(len(ops))] = r.Text()
		vecAt[uint64(len(ops))] = maps.Clone(r.Vector())
	}
	if _, err := st.Append(ctx, src.ID, ops); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Compact(ctx, src.ID); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		op, err := r.InsertAt(r.Len(), 'y')
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Append(ctx, src.ID, []*crdtv1.Operation{testdb.ProtoFromInsert(op)}); err != nil {
			t.Fatal(err)
		}
		n := uint64(len(textAt) + 1)
		textAt[n] = r.Text()
		vecAt[n] = maps.Clone(r.Vector())
	}

	below, err := st.Fork(ctx, src.ID, 120, "below", "snap-below")
	if err != nil {
		t.Fatal(err)
	}
	if below.SnapshotSeq != 0 || below.OpSeq != 120 {
		t.Fatalf("below seqs %+v", below)
	}
	got, _ := loadDoc(t, st, below.ID)
	if got.Text() != textAt[120] || !maps.Equal(got.Vector(), vecAt[120]) {
		t.Fatalf("below state %q, want %q", got.Text(), textAt[120])
	}

	atSnap, err := st.Fork(ctx, src.ID, 150, "at", "snap-at")
	if err != nil {
		t.Fatal(err)
	}
	if atSnap.SnapshotSeq != 150 || atSnap.OpSeq != 150 {
		t.Fatalf("at-snap seqs %+v", atSnap)
	}
	got, meta := loadDoc(t, st, atSnap.ID)
	if meta.SnapshotSeq != 150 || got.Text() != textAt[150] || !maps.Equal(got.Vector(), vecAt[150]) {
		t.Fatalf("at-snap state %q snap=%d", got.Text(), meta.SnapshotSeq)
	}

	above, err := st.Fork(ctx, src.ID, 160, "above", "snap-above")
	if err != nil {
		t.Fatal(err)
	}
	if above.SnapshotSeq != 150 || above.OpSeq != 160 {
		t.Fatalf("above seqs %+v", above)
	}
	got, _ = loadDoc(t, st, above.ID)
	if got.Text() != textAt[160] || !maps.Equal(got.Vector(), vecAt[160]) {
		t.Fatalf("above state %q, want %q", got.Text(), textAt[160])
	}

	srcHead, srcMeta := loadDoc(t, st, src.ID)
	if srcMeta.OpSeq != 170 || srcHead.Text() != textAt[170] {
		t.Fatalf("source head %+v %q", srcMeta, srcHead.Text())
	}
}

func TestForkRejectsBadInputs(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	src, err := st.Create(ctx, "src", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Fork(ctx, src.ID, 0, "", "r1"); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("empty title: %v", err)
	}
	if _, err := st.Fork(ctx, src.ID, 0, "t", ""); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("empty request: %v", err)
	}
	if _, err := st.Fork(ctx, uuid.New(), 0, "t", "r2"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := st.Fork(ctx, src.ID, 1, "t", "r3"); !errors.Is(err, domain.ErrOutOfRange) {
		t.Fatalf("beyond head: %v", err)
	}
	if _, err := st.Archive(ctx, src.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Fork(ctx, src.ID, 0, "t", "r4"); !errors.Is(err, domain.ErrArchived) {
		t.Fatalf("archived: %v", err)
	}
}

func TestForkIdempotentAfterSourceArchived(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	src, err := st.Create(ctx, "src", "owner")
	if err != nil {
		t.Fatal(err)
	}
	fork, err := st.Fork(ctx, src.ID, 0, "branch", "keep")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Archive(ctx, src.ID); err != nil {
		t.Fatal(err)
	}
	again, err := st.Fork(ctx, src.ID, 0, "branch", "keep")
	if err != nil || again.ID != fork.ID {
		t.Fatalf("retry after archive = %+v, %v", again, err)
	}
}

func TestForkConcurrentRequestID(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	src, err := st.Create(ctx, "src", "owner")
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan struct {
		doc domain.Document
		err error
	}, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			doc, err := st.Fork(ctx, src.ID, 0, "branch", "race")
			results <- struct {
				doc domain.Document
				err error
			}{doc, err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var id uuid.UUID
	for res := range results {
		if res.err != nil {
			t.Fatalf("race: %v", res.err)
		}
		if id == uuid.Nil {
			id = res.doc.ID
		}
		if res.doc.ID != id {
			t.Fatalf("converged to %s and %s", id, res.doc.ID)
		}
	}
}

func TestListChangedPaginationAndArchive(t *testing.T) {
	st, pool := testdb.Open(t)
	ctx := t.Context()
	a, err := st.Create(ctx, "a", "owner")
	if err != nil {
		t.Fatal(err)
	}
	b, err := st.Create(ctx, "b", "owner")
	if err != nil {
		t.Fatal(err)
	}
	c, err := st.Create(ctx, "c", "owner")
	if err != nil {
		t.Fatal(err)
	}
	archived, err := st.Create(ctx, "archived", "owner")
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, row := range []struct {
		id uuid.UUID
		at time.Time
	}{
		{a.ID, t0.Add(1 * time.Second)},
		{b.ID, t0.Add(2 * time.Second)},
		{c.ID, t0.Add(3 * time.Second)},
		{archived.ID, t0.Add(4 * time.Second)},
	} {
		if _, err := pool.Exec(ctx, `UPDATE documents SET updated_at = $2 WHERE id = $1`, row.id, row.at); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.Archive(ctx, archived.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE documents SET updated_at = $2 WHERE id = $1`, archived.ID, t0.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}

	page, next, err := st.ListChanged(ctx, t0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 || page[0].ID != a.ID || page[1].ID != b.ID {
		t.Fatalf("page1 = %v", idsOf(page))
	}
	if !next.Equal(t0.Add(2 * time.Second)) {
		t.Fatalf("next_since %v", next)
	}

	page2, next2, err := st.ListChanged(ctx, next, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 || page2[0].ID != b.ID || page2[1].ID != c.ID {
		t.Fatalf("page2 = %v", idsOf(page2))
	}
	if !next2.Equal(t0.Add(3 * time.Second)) {
		t.Fatalf("page2 next_since %v", next2)
	}

	for _, doc := range page2 {
		if doc.ID == archived.ID {
			t.Fatal("archived document in feed")
		}
	}

	empty, emptyNext, err := st.ListChanged(ctx, t0.Add(10*time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 || !emptyNext.Equal(t0.Add(10*time.Second)) {
		t.Fatalf("empty n=%d next=%v", len(empty), emptyNext)
	}

	if _, _, err := st.ListChanged(ctx, t0, 5001); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("max limit: %v", err)
	}
	if _, _, err := st.ListChanged(ctx, t0, -1); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("neg limit: %v", err)
	}
}

func loadDoc(t *testing.T, st *store.Store, id uuid.UUID) (*crdt.Doc, domain.Document) {
	t.Helper()
	meta, snap, ops, err := st.LoadState(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := crdt.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range ops {
		op, err := crdt.OpFromProto(raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := doc.Apply(op); err != nil {
			t.Fatal(err)
		}
	}
	return doc, meta
}

func idsOf(docs []domain.Document) []uuid.UUID {
	out := make([]uuid.UUID, len(docs))
	for i, doc := range docs {
		out[i] = doc.ID
	}
	return out
}

func operationBytes(t *testing.T, pool interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, id uuid.UUID) [][]byte {
	t.Helper()
	rows, err := pool.Query(t.Context(), `SELECT op FROM operations WHERE document_id = $1 ORDER BY seq`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		out = append(out, raw)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
