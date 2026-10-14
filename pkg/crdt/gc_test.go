package crdt

import (
	"errors"
	"testing"
)

func TestGCKeepsTombstonesWithChildren(t *testing.T) {
	r, _ := NewReplica(1, nil)
	if _, err := r.InsertText(0, "abc"); err != nil {
		t.Fatal(err)
	}
	middle, _ := r.IDAt(1)
	if _, err := r.DeleteAt(1); err != nil {
		t.Fatal(err)
	}
	removed, err := r.GC(r.Vector())
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 || !r.Has(middle) || r.Text() != "ac" {
		t.Fatalf("removed=%d has(middle)=%v text=%q", removed, r.Has(middle), r.Text())
	}
}

func TestGCCascadesUpChains(t *testing.T) {
	r, _ := NewReplica(1, nil)
	if _, err := r.InsertText(0, "xabc"); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := r.DeleteAt(1); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := r.GC(r.Vector())
	if err != nil {
		t.Fatal(err)
	}
	if removed != 3 || r.Text() != "x" {
		t.Fatalf("removed=%d text=%q", removed, r.Text())
	}
}

func TestDeleteOfCollectedTombstoneIsApplied(t *testing.T) {
	doc := New()
	insert := Insert{ID: ID{Clock: 1, Client: 1}, Parent: Root, Value: 'x'}
	firstDelete := Delete{ID: ID{Clock: 2, Client: 2}, Target: insert.ID}
	for _, op := range []Op{insert, firstDelete} {
		if _, err := doc.Apply(op); err != nil {
			t.Fatal(err)
		}
	}
	if removed, err := doc.GC(doc.Vector()); err != nil || removed != 1 {
		t.Fatalf("GC removed=%d err=%v", removed, err)
	}

	lateDelete := Delete{ID: ID{Clock: 2, Client: 3}, Target: insert.ID}
	if !doc.Ready(lateDelete) {
		t.Fatal("delete of collected tombstone is not ready")
	}
	status, err := doc.Apply(lateDelete)
	if err != nil || status == Pending {
		t.Fatalf("late delete status=%v err=%v", status, err)
	}
	if doc.Text() != "" {
		t.Fatalf("text=%q", doc.Text())
	}
	if len(doc.pendingDeletes) != 0 || len(doc.Snapshot().Pending) != 0 {
		t.Fatalf("pending deletes=%d snapshot pending=%d", len(doc.pendingDeletes), len(doc.Snapshot().Pending))
	}
	if !doc.Vector().Covers(lateDelete.ID) {
		t.Fatalf("vector does not cover %s", lateDelete.ID)
	}
}

func TestGCRequiresFIFOReplicaReports(t *testing.T) {
	t.Run("unsafe stable rejects in-flight child", func(t *testing.T) {
		a := New()
		b := New()
		c := New()
		insert := Insert{ID: ID{Clock: 1, Client: 1}, Parent: Root, Value: 't'}
		deleteOp := Delete{ID: ID{Clock: 2, Client: 2}, Target: insert.ID}
		child := Insert{ID: ID{Clock: 2, Client: 3}, Parent: insert.ID, Value: 'x'}

		for _, doc := range []*Doc{a, b, c} {
			if _, err := doc.Apply(insert); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := a.Apply(deleteOp); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Apply(deleteOp); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Apply(child); err != nil {
			t.Fatal(err)
		}

		unsafeStable := VersionVector{1: 1, 2: 2}
		if removed, err := a.GC(unsafeStable); err != nil || removed != 1 {
			t.Fatalf("GC removed=%d err=%v", removed, err)
		}
		if _, err := a.Apply(child); !errors.Is(err, ErrInvalidOp) {
			t.Fatalf("in-flight child after unsafe GC: %v", err)
		}

		for _, doc := range []*Doc{b, c} {
			for _, op := range []Op{deleteOp, child} {
				if _, err := doc.Apply(op); err != nil {
					t.Fatal(err)
				}
			}
		}
		if a.Text() == b.Text() || b.Text() != c.Text() {
			t.Fatalf("texts after unsafe GC: A=%q B=%q C=%q", a.Text(), b.Text(), c.Text())
		}
	})

	t.Run("safe reports keep parent until child arrives", func(t *testing.T) {
		replicas := []*Doc{New(), New(), New()}
		insert := Insert{ID: ID{Clock: 1, Client: 1}, Parent: Root, Value: 't'}
		deleteOp := Delete{ID: ID{Clock: 2, Client: 2}, Target: insert.ID}
		child := Insert{ID: ID{Clock: 2, Client: 3}, Parent: insert.ID, Value: 'x'}
		for _, doc := range replicas {
			for _, op := range []Op{insert, child, deleteOp} {
				if _, err := doc.Apply(op); err != nil {
					t.Fatal(err)
				}
			}
		}

		stable := VersionVector{1: 1, 2: 2, 3: 2}
		if removed, err := replicas[0].GC(stable); err != nil || removed != 0 {
			t.Fatalf("GC removed=%d err=%v", removed, err)
		}
		if !replicas[0].Has(insert.ID) {
			t.Fatal("GC removed tombstone with a delivered child")
		}
		for i, doc := range replicas {
			if doc.Text() != "x" {
				t.Fatalf("replica %d text=%q", i, doc.Text())
			}
		}
	})
}
