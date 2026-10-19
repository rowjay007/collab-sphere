package crdt_test

import (
	"bytes"
	"testing"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func TestPlanRestoreReachesTargetAndIsIdempotent(t *testing.T) {
	var restores, copies int
	for seed := uint64(1); seed <= 300; seed++ {
		h := newHistory(t, seed).run(60)
		for _, r := range h.restores {
			restores++
			if got, want := r.result.Text(), r.target.Text(); got != want {
				t.Fatalf("seed %d: restored text %q, want %q", seed, got, want)
			}
			again, err := crdt.PlanRestore(r.result, r.target, r.system)
			if err != nil {
				t.Fatal(err)
			}
			if len(again) != 0 {
				t.Fatalf("seed %d: replanning returned %d ops", seed, len(again))
			}
		}
		for _, b := range h.branches {
			c, _ := countCopies(b.doc)
			copies += c
		}
	}
	if restores < 300 || copies == 0 {
		t.Fatalf("histories exercised %d restores and %d copies", restores, copies)
	}
}

func TestPlanRestoreCommutesWithConcurrentEdits(t *testing.T) {
	planned := 0
	for seed := uint64(1); seed <= 200; seed++ {
		h := newHistory(t, seed).run(40)
		b := h.branches[h.rng.IntN(len(h.branches))]
		var target *crdt.Doc
		for _, v := range h.versions {
			if b.doc.Vector().CoversAll(v.Vector()) {
				target = v
			}
		}
		if target == nil {
			continue
		}
		plan, err := crdt.PlanRestore(b.doc, target, b.system)
		if err != nil {
			t.Fatal(err)
		}
		concurrent := clone(t, b.doc)
		h.edit(&branch{doc: concurrent, editor: h.newClient()})
		var edits []crdt.Op
		for _, op := range concurrent.Ops() {
			if !b.doc.Contains(op.OpID()) {
				edits = append(edits, op)
			}
		}

		planFirst := clone(t, b.doc)
		applyAll(t, planFirst, plan)
		applyAll(t, planFirst, edits)
		editsFirst := clone(t, b.doc)
		applyAll(t, editsFirst, edits)
		applyAll(t, editsFirst, plan)
		x, err := crdt.CanonicalBytes(planFirst)
		if err != nil {
			t.Fatal(err)
		}
		y, err := crdt.CanonicalBytes(editsFirst)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(x, y) {
			t.Fatalf("seed %d: plan and concurrent edits do not commute: %q vs %q", seed, planFirst.Text(), editsFirst.Text())
		}
		planned++
	}
	if planned < 100 {
		t.Fatalf("only %d seeds planned a restore", planned)
	}
}

func TestPlanRestoreSkipsCopyBehindTargetText(t *testing.T) {
	a := crdt.Insert{ID: crdt.ID{Client: 1, Clock: 1}, Parent: crdt.Root, Value: 'a'}
	d := crdt.New()
	applyAll(t, d, []crdt.Op{a})
	q := clone(t, d)
	applyAll(t, q, []crdt.Op{
		crdt.Delete{ID: crdt.ID{Client: 3, Clock: 2}, Target: a.ID},
		crdt.Insert{ID: crdt.ID{Client: 4, Clock: 3}, Parent: a.ID, Value: 'a', Copy: true},
	})
	applyAll(t, d, []crdt.Op{
		crdt.Insert{ID: crdt.ID{Client: 2, Clock: 2}, Parent: crdt.Root, Value: 'x'},
		crdt.Insert{ID: crdt.ID{Client: 2, Clock: 3}, Parent: crdt.Root, Value: 'y'},
		crdt.Insert{ID: crdt.ID{Client: 2, Clock: 4}, Parent: a.ID, Value: 't'},
	})
	v := clone(t, d)
	if err := d.Merge(q); err != nil {
		t.Fatal(err)
	}
	if d.Text() != "yxta" {
		t.Fatalf("merged text = %q, want the branch copy behind t", d.Text())
	}
	plan, err := crdt.PlanRestore(d, v, 9)
	if err != nil {
		t.Fatal(err)
	}
	applyAll(t, d, plan)
	if d.Text() != v.Text() {
		t.Fatalf("restored text = %q, want %q", d.Text(), v.Text())
	}
}

func TestPlanRestoreRequiresContainment(t *testing.T) {
	a, err := crdt.NewReplica(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.InsertText(0, "abc"); err != nil {
		t.Fatal(err)
	}
	if _, err := crdt.PlanRestore(crdt.New(), a.Doc, 9); err == nil {
		t.Fatal("restore to a target the head lacks succeeded")
	}
}

func TestRestoreReattachesAnchorThroughCopies(t *testing.T) {
	r, err := crdt.NewReplica(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	ins, err := r.InsertText(0, "hello world")
	if err != nil {
		t.Fatal(err)
	}
	v := clone(t, r.Doc)
	start, end := ins[6].ID, ins[10].ID
	for range 5 {
		if _, err := r.DeleteAt(6); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := r.ResolveRange(start, end); !got.Detached {
		t.Fatalf("range over deleted text = %+v, want detached", got)
	}
	plan, err := crdt.PlanRestore(r.Doc, v, 2)
	if err != nil {
		t.Fatal(err)
	}
	applyAll(t, r.Doc, plan)
	got, err := r.ResolveRange(start, end)
	if err != nil {
		t.Fatal(err)
	}
	if got.Detached || got.Text != "world" {
		t.Fatalf("range after restore = %+v, want world", got)
	}
}
