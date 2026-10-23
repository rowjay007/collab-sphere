package history_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/history"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/store"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/testdb"
)

func TestMutationResumeAfterEachCrashPoint(t *testing.T) {
	for _, point := range []string{history.CrashAfterPersist, history.CrashAfterAppend, history.CrashAfterNode} {
		t.Run(point, func(t *testing.T) {
			st, _ := testdb.Open(t)
			docs := newFakeDocs()
			h := history.New(st, docs)
			ctx := t.Context()
			d := docs.create(t)
			docs.edit(t, d, 50)
			if _, err := h.Materialize(ctx, d); err != nil {
				t.Fatal(err)
			}
			b, err := docs.Fork(ctx, d, 50, "b", "fork-"+point)
			if err != nil {
				t.Fatal(err)
			}
			docs.edit(t, b.ID, 30)
			if _, err := h.Materialize(ctx, b.ID); err != nil {
				t.Fatal(err)
			}

			h.SetCrashHook(point, func() error { return history.ErrInjected })
			_, err = h.Merge(ctx, b.ID, d, "merge-"+point)
			if !errors.Is(err, history.ErrInjected) {
				t.Fatalf("first merge: %v", err)
			}
			h.SetCrashHook(point, nil)

			first, err := h.Merge(ctx, b.ID, d, "merge-"+point)
			if err != nil {
				t.Fatal(err)
			}
			second, err := h.Merge(ctx, b.ID, d, "merge-"+point)
			if err != nil {
				t.Fatal(err)
			}
			if first.Version.ID != second.Version.ID || first.Version.Node.Seq != second.Version.Node.Seq {
				t.Fatalf("resume diverged: %+v vs %+v", first.Version, second.Version)
			}
			if ids := opIDs(t, docs, d); len(ids) != int(docs.seq(d)) {
				t.Fatalf("duplicate ops: %d ids, seq %d", len(ids), docs.seq(d))
			}

			again, err := h.Merge(ctx, b.ID, d, "merge-"+point+"-2")
			if err != nil {
				t.Fatal(err)
			}
			if !again.Noop || again.Version.Node.Seq != first.Version.Node.Seq {
				t.Fatalf("second payload merge: noop=%v seq=%d", again.Noop, again.Version.Node.Seq)
			}
		})
	}
}

func TestOtherCallsFinishAnInterruptedMerge(t *testing.T) {
	for _, point := range []string{history.CrashAfterPersist, history.CrashAfterAppend, history.CrashAfterNode} {
		t.Run(point, func(t *testing.T) {
			st, _ := testdb.Open(t)
			docs := newFakeDocs()
			h := history.New(st, docs)
			ctx := t.Context()
			d := docs.create(t)
			docs.edit(t, d, 50)
			if _, err := h.Materialize(ctx, d); err != nil {
				t.Fatal(err)
			}
			b, err := docs.Fork(ctx, d, 50, "b", "fork-"+point)
			if err != nil {
				t.Fatal(err)
			}
			docs.edit(t, b.ID, 30)

			h.SetCrashHook(point, func() error { return history.ErrInjected })
			if _, err := h.Merge(ctx, b.ID, d, "merge-"+point); !errors.Is(err, history.ErrInjected) {
				t.Fatalf("first merge: %v", err)
			}
			h.SetCrashHook(point, nil)

			frontier, err := h.Frontier(ctx, d)
			if err != nil {
				t.Fatal(err)
			}
			if frontier[b.ID] != 80 {
				t.Fatalf("frontier after an unrelated call: %v, want %s at 80", frontier, b.ID)
			}
		})
	}
}

func TestRestoreResumeAfterEachCrashPoint(t *testing.T) {
	for _, point := range []string{history.CrashAfterPersist, history.CrashAfterAppend, history.CrashAfterNode} {
		t.Run(point, func(t *testing.T) {
			st, _ := testdb.Open(t)
			docs := newFakeDocs()
			h := history.New(st, docs)
			ctx := t.Context()
			d := docs.create(t)
			docs.edit(t, d, 40)
			v, err := h.Checkpoint(ctx, d, "v", "cp-"+point)
			if err != nil {
				t.Fatal(err)
			}
			docs.edit(t, d, 20)

			h.SetCrashHook(point, func() error { return history.ErrInjected })
			_, err = h.Restore(ctx, d, v.ID, "restore-"+point)
			if !errors.Is(err, history.ErrInjected) {
				t.Fatalf("first restore: %v", err)
			}
			h.SetCrashHook(point, nil)

			first, err := h.Restore(ctx, d, v.ID, "restore-"+point)
			if err != nil {
				t.Fatal(err)
			}
			second, err := h.Restore(ctx, d, v.ID, "restore-"+point)
			if err != nil {
				t.Fatal(err)
			}
			if first.ID != second.ID || first.Node.Seq != second.Node.Seq {
				t.Fatalf("resume diverged: %+v vs %+v", first, second)
			}
			if ids := opIDs(t, docs, d); len(ids) != int(docs.seq(d)) {
				t.Fatalf("duplicate ops: %d ids, seq %d", len(ids), docs.seq(d))
			}
		})
	}
}

func TestMergeFrontierFollowsMergeParent(t *testing.T) {
	st, _ := testdb.Open(t)
	docs := newFakeDocs()
	h := history.New(st, docs)
	ctx := t.Context()
	d := docs.create(t)
	docs.edit(t, d, 20)
	if _, err := h.Materialize(ctx, d); err != nil {
		t.Fatal(err)
	}
	before, err := h.Frontier(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := before[d]; !ok || len(before) != 1 {
		t.Fatalf("frontier before merge: %v", before)
	}

	b, err := docs.Fork(ctx, d, 20, "b", "fork-front")
	if err != nil {
		t.Fatal(err)
	}
	docs.edit(t, b.ID, 15)
	r, err := h.Merge(ctx, b.ID, d, "merge-front")
	if err != nil {
		t.Fatal(err)
	}
	if r.Noop || r.Appended != 15 {
		t.Fatalf("merge %+v", r)
	}
	if len(r.Version.Node.Parents) != 2 {
		t.Fatalf("parents %v", r.Version.Node.Parents)
	}
	after, err := h.Frontier(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	if after[b.ID] != 35 {
		t.Fatalf("frontier after merge: %v", after)
	}
}

func TestMergeRejectsDifferentForkRoot(t *testing.T) {
	st, _ := testdb.Open(t)
	docs := newFakeDocs()
	h := history.New(st, docs)
	ctx := t.Context()
	a := docs.create(t)
	b := docs.create(t)
	docs.edit(t, a, 3)
	docs.edit(t, b, 3)
	if _, err := h.Merge(ctx, b, a, "nope"); !errors.Is(err, history.ErrFailedPrecondition) {
		t.Fatalf("got %v", err)
	}
}

func TestRequestIDPayloadMismatch(t *testing.T) {
	st, _ := testdb.Open(t)
	docs := newFakeDocs()
	h := history.New(st, docs)
	ctx := t.Context()
	d := docs.create(t)
	docs.edit(t, d, 10)
	if _, err := h.Materialize(ctx, d); err != nil {
		t.Fatal(err)
	}
	b, err := docs.Fork(ctx, d, 10, "b", "fb")
	if err != nil {
		t.Fatal(err)
	}
	c, err := docs.Fork(ctx, d, 10, "c", "fc")
	if err != nil {
		t.Fatal(err)
	}
	docs.edit(t, b.ID, 2)
	docs.edit(t, c.ID, 2)
	if _, err := h.Merge(ctx, b.ID, d, "rid"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Merge(ctx, c.ID, d, "rid"); !errors.Is(err, history.ErrAlreadyExists) {
		t.Fatalf("got %v", err)
	}
}

func TestMergeNestedAndSiblingBranches(t *testing.T) {
	st, _ := testdb.Open(t)
	docs := newFakeDocs()
	h := history.New(st, docs)
	ctx := t.Context()
	d := docs.create(t)
	docs.edit(t, d, 8)
	if _, err := h.Materialize(ctx, d); err != nil {
		t.Fatal(err)
	}
	b, err := docs.Fork(ctx, d, 8, "b", "fork-b")
	if err != nil {
		t.Fatal(err)
	}
	docs.edit(t, b.ID, 5)
	if _, err := h.Materialize(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	b2, err := docs.Fork(ctx, b.ID, 13, "b2", "fork-b2")
	if err != nil {
		t.Fatal(err)
	}
	docs.edit(t, b2.ID, 4)
	c, err := docs.Fork(ctx, d, 8, "c", "fork-c")
	if err != nil {
		t.Fatal(err)
	}
	docs.edit(t, c.ID, 3)

	for _, tc := range []struct {
		name     string
		src, dst uuid.UUID
	}{
		{"B2 into B", b2.ID, b.ID},
		{"B2 into D", b2.ID, d},
		{"C into B", c.ID, b.ID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first, err := h.Merge(ctx, tc.src, tc.dst, "merge-"+tc.name)
			if err != nil {
				t.Fatal(err)
			}
			if first.Noop {
				t.Fatal("first merge is noop")
			}
			want := unionText(t, docs, tc.src, tc.dst)
			got, _, err := docs.LoadState(ctx, tc.dst)
			if err != nil {
				t.Fatal(err)
			}
			if got.Text() != want {
				t.Fatalf("text %q, want union %q", got.Text(), want)
			}
			second, err := h.Merge(ctx, tc.src, tc.dst, "merge-"+tc.name+"-2")
			if err != nil {
				t.Fatal(err)
			}
			if !second.Noop || second.Version.Node.Seq != first.Version.Node.Seq {
				t.Fatalf("second merge: noop=%v seq=%d want %d", second.Noop, second.Version.Node.Seq, first.Version.Node.Seq)
			}
		})
	}
}

func TestRestoreNestedBranch(t *testing.T) {
	st, _ := testdb.Open(t)
	docs := newFakeDocs()
	h := history.New(st, docs)
	ctx := t.Context()
	d := docs.create(t)
	docs.edit(t, d, 6)
	if _, err := h.Materialize(ctx, d); err != nil {
		t.Fatal(err)
	}
	b, err := docs.Fork(ctx, d, 6, "b", "fork-b")
	if err != nil {
		t.Fatal(err)
	}
	docs.edit(t, b.ID, 4)
	v, err := h.Checkpoint(ctx, b.ID, "snap", "cp-b")
	if err != nil {
		t.Fatal(err)
	}
	want, _, err := docs.LoadState(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := docs.Fork(ctx, b.ID, 10, "b2", "fork-b2")
	if err != nil {
		t.Fatal(err)
	}
	docs.edit(t, b2.ID, 5)
	if _, err := h.Restore(ctx, b2.ID, v.ID, "restore-b2"); err != nil {
		t.Fatal(err)
	}
	got, _, err := docs.LoadState(ctx, b2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Text() != want.Text() {
		t.Fatalf("restored %q, want %q", got.Text(), want.Text())
	}
}

func TestMergeIntoUnstartedFork(t *testing.T) {
	st, _ := testdb.Open(t)
	docs := newFakeDocs()
	h := history.New(st, docs)
	ctx := t.Context()
	d := docs.create(t)
	docs.edit(t, d, 6)
	b, err := docs.Fork(ctx, d, 6, "b", "fork-only")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Checkpoint(ctx, d, "d", "cp-d"); err != nil {
		t.Fatal(err)
	}
	docs.edit(t, d, 4)
	r, err := h.Merge(ctx, d, b.ID, "merge-unstarted")
	if err != nil {
		t.Fatal(err)
	}
	if r.Noop {
		t.Fatal("merge into unstarted fork is noop")
	}
	want := unionText(t, docs, d, b.ID)
	got, _, err := docs.LoadState(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Text() != want {
		t.Fatalf("text %q, want union %q", got.Text(), want)
	}
}

func TestStuckAppendedRecoversOnMaterialize(t *testing.T) {
	st, pool := testdb.Open(t)
	docs := newFakeDocs()
	h := history.New(st, docs)
	ctx := t.Context()
	d := docs.create(t)
	docs.edit(t, d, 4)
	if _, err := h.Materialize(ctx, d); err != nil {
		t.Fatal(err)
	}
	b, err := docs.Fork(ctx, d, 4, "b", "fork-stuck")
	if err != nil {
		t.Fatal(err)
	}
	docs.edit(t, d, 2)
	src, err := h.Versions(ctx, d, -1, 100)
	if err != nil || len(src) == 0 {
		t.Fatalf("source versions: %d %v", len(src), err)
	}
	head := src[len(src)-1]
	if _, err := pool.Exec(ctx, `
		INSERT INTO mutations (request_id, doc, kind, state, source_doc, source_node, appended)
		VALUES ($1, $2, 'merge', 'appended', $3, $4, 0)`,
		"stuck-appended", b.ID, d, head.ID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Materialize(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	m, ok, err := st.Mutation(ctx, "stuck-appended")
	if err != nil || !ok {
		t.Fatalf("mutation: ok=%v err=%v", ok, err)
	}
	if m.State != store.StateDone {
		t.Fatalf("state %s, want done", m.State)
	}
}

func TestNoopMergeNamesFirstCoveringNode(t *testing.T) {
	st, _ := testdb.Open(t)
	docs := newFakeDocs()
	h := history.New(st, docs)
	ctx := t.Context()
	d := docs.create(t)
	docs.edit(t, d, 5)
	base, err := h.Checkpoint(ctx, d, "base", "cp-base")
	if err != nil {
		t.Fatal(err)
	}
	b, err := docs.Fork(ctx, d, 5, "b", "fork-cover")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Materialize(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	docs.edit(t, d, 7)
	if _, err := h.Checkpoint(ctx, d, "later", "cp-later"); err != nil {
		t.Fatal(err)
	}
	r, err := h.Merge(ctx, b.ID, d, "merge-cover")
	if err != nil {
		t.Fatal(err)
	}
	if !r.Noop {
		t.Fatal("expected noop")
	}
	if r.Version.ID != base.ID {
		t.Fatalf("covering node %s@%d, want base %s@%d", r.Version.ID, r.Version.Node.Seq, base.ID, base.Node.Seq)
	}
}

func TestSweepPagesSharedTimestamp(t *testing.T) {
	st, _ := testdb.Open(t)
	docs := newFakeDocs()
	h := history.New(st, docs)
	ctx := t.Context()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	const n = 501
	for range n {
		id := docs.create(t)
		docs.setUpdated(id, t0)
	}
	later := docs.create(t)
	docs.setUpdated(later, t0.Add(time.Second))
	h.SweepOnce(ctx, time.Time{})
	if _, err := st.VersionAt(ctx, later, 0); err != nil {
		t.Fatalf("later document not materialized: %v", err)
	}
}

func TestRestoreConflictFailsCleanly(t *testing.T) {
	st, _ := testdb.Open(t)
	docs := newFakeDocs()
	h := history.New(st, docs)
	ctx := t.Context()
	d := docs.create(t)
	docs.edit(t, d, 6)
	v, err := h.Checkpoint(ctx, d, "v", "cp-v")
	if err != nil {
		t.Fatal(err)
	}
	docs.edit(t, d, 4)
	docs.hijackAppend = func(id uuid.UUID, ops []crdt.Op) {
		if len(ops) == 0 {
			return
		}
		if err := docs.inject(id, crdt.Insert{ID: ops[0].OpID(), Parent: crdt.Root, Value: '\uFFFD'}); err != nil {
			t.Error(err)
		}
		docs.hijackAppend = nil
	}
	_, err = h.Restore(ctx, d, v.ID, "restore-conflict")
	if !errors.Is(err, history.ErrFailedPrecondition) {
		t.Fatalf("conflict restore: %v", err)
	}
	_, err = h.Restore(ctx, d, v.ID, "restore-conflict")
	if !errors.Is(err, history.ErrFailedPrecondition) {
		t.Fatalf("replay: %v", err)
	}
	if _, err := h.Checkpoint(ctx, d, "after-fail", "cp-after"); err != nil {
		t.Fatalf("checkpoint after failed restore: %v", err)
	}
	if _, err := h.Restore(ctx, d, v.ID, "restore-after"); err != nil {
		t.Fatalf("new restore: %v", err)
	}
}

func unionText(t *testing.T, docs *fakeDocs, ids ...uuid.UUID) string {
	t.Helper()
	acc := crdt.New()
	for _, id := range ids {
		st, _, err := docs.LoadState(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		for _, op := range st.Ops() {
			if acc.Contains(op.OpID()) {
				continue
			}
			if _, err := acc.Apply(op); err != nil {
				t.Fatal(err)
			}
		}
	}
	return acc.Text()
}

func (f *fakeDocs) seq(id uuid.UUID) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return uint64(len(f.docs[id].ops))
}

func opIDs(t *testing.T, f *fakeDocs, id uuid.UUID) map[crdt.ID]struct{} {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[crdt.ID]struct{}{}
	for _, op := range f.docs[id].ops {
		out[op.OpID()] = struct{}{}
	}
	return out
}
