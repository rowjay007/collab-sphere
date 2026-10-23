package history_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/history"
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
