package crdt

import (
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"unicode/utf8"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
)

func TestReplicaMatchesRuneModel(t *testing.T) {
	replica, err := NewReplica(7, nil)
	if err != nil {
		t.Fatal(err)
	}
	random := rand.New(rand.NewPCG(42, 1))
	var model []rune
	for step := 0; step < 1000; step++ {
		if len(model) == 0 || random.IntN(3) != 0 {
			index := random.IntN(len(model) + 1)
			value := rune('a' + random.IntN(26))
			if _, err := replica.InsertAt(index, value); err != nil {
				t.Fatal(err)
			}
			model = slices.Insert(model, index, value)
		} else {
			index := random.IntN(len(model))
			if _, err := replica.DeleteAt(index); err != nil {
				t.Fatal(err)
			}
			model = slices.Delete(model, index, index+1)
		}
		if got := replica.Text(); got != string(model) {
			t.Fatalf("step %d: got %q, want %q", step, got, string(model))
		}
	}
}

func TestFromSnapshotRejectsMalformed(t *testing.T) {
	validID := &crdtv1.OpID{Client: 1, Clock: 1}
	tests := []*crdtv1.Snapshot{
		nil,
		{Nodes: []*crdtv1.Node{{}}},
		{Nodes: []*crdtv1.Node{{Id: validID, Parent: &crdtv1.OpID{Client: 2}, Value: 'a'}}},
		{Nodes: []*crdtv1.Node{{Id: validID, Parent: &crdtv1.OpID{}, Value: uint32(utf8.MaxRune + 1)}}},
		{
			Nodes: []*crdtv1.Node{
				{Id: &crdtv1.OpID{Client: 1, Clock: 1}, Parent: &crdtv1.OpID{}, Value: 'b'},
				{Id: &crdtv1.OpID{Client: 2, Clock: 1}, Parent: &crdtv1.OpID{}, Value: 'a'},
			},
			Vector: []*crdtv1.VectorEntry{{Client: 1, Clock: 1}, {Client: 2, Clock: 1}},
		},
	}
	for i, snapshot := range tests {
		if _, err := FromSnapshot(snapshot); !errors.Is(err, ErrInvalidOp) {
			t.Fatalf("case %d: got %v", i, err)
		}
	}
}

func TestConflictsAndInvalidOperations(t *testing.T) {
	doc := New()
	insert := Insert{ID: ID{Clock: 1, Client: 1}, Parent: Root, Value: 'a'}
	if status, err := doc.Apply(insert); err != nil || status != Applied {
		t.Fatal(status, err)
	}
	if status, err := doc.Apply(insert); err != nil || status != Duplicate {
		t.Fatal(status, err)
	}
	if _, err := doc.Apply(Insert{ID: insert.ID, Parent: Root, Value: 'b'}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if _, err := doc.Apply(Delete{ID: insert.ID, Target: insert.ID}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if _, err := doc.Apply(Insert{ID: ID{Clock: 0, Client: 1}, Value: 'a'}); !errors.Is(err, ErrInvalidOp) {
		t.Fatal(err)
	}
}

func TestReplicaEditsByID(t *testing.T) {
	r, err := NewReplica(1, New())
	if err != nil {
		t.Fatal(err)
	}
	cursor := Root
	for _, v := range "helo" {
		op, err := r.InsertAfter(cursor, v)
		if err != nil {
			t.Fatal(err)
		}
		cursor = op.ID
	}
	l, err := r.IDAt(2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.InsertAfter(l, 'l'); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Delete(cursor); err != nil {
		t.Fatal(err)
	}
	if got := r.Text(); got != "hell" {
		t.Fatalf("text = %q, want %q", got, "hell")
	}

	missing := ID{Clock: 99, Client: 2}
	if _, err := r.InsertAfter(missing, 'x'); !errors.Is(err, ErrInvalidOp) {
		t.Fatalf("InsertAfter missing parent: %v", err)
	}
	if _, err := r.Delete(missing); !errors.Is(err, ErrInvalidOp) {
		t.Fatalf("Delete missing target: %v", err)
	}
	if _, err := r.Delete(Root); !errors.Is(err, ErrInvalidOp) {
		t.Fatalf("Delete root: %v", err)
	}
	if n := len(r.Snapshot().GetPending()); n != 0 {
		t.Fatalf("%d ops left pending", n)
	}
}

func TestRejectsMaximumClock(t *testing.T) {
	doc := New()
	insert := Insert{ID: ID{Clock: math.MaxUint64, Client: 1}, Parent: Root, Value: 'x'}
	if _, err := doc.Apply(insert); !errors.Is(err, ErrInvalidOp) {
		t.Fatalf("maximum-clock insert: %v", err)
	}

	deleteOp := Delete{
		ID:     ID{Clock: math.MaxUint64, Client: 2},
		Target: ID{Clock: 1, Client: 1},
	}
	if _, err := doc.Apply(deleteOp); !errors.Is(err, ErrInvalidOp) {
		t.Fatalf("maximum-clock delete: %v", err)
	}
}

func TestReplicaClockExhaustion(t *testing.T) {
	newReplica := func(t *testing.T) *Replica {
		t.Helper()
		doc := New()
		op := Insert{ID: ID{Clock: math.MaxUint64 - 1, Client: 9}, Parent: Root, Value: 'x'}
		if _, err := doc.Apply(op); err != nil {
			t.Fatal(err)
		}
		replica, err := NewReplica(1, doc)
		if err != nil {
			t.Fatal(err)
		}
		return replica
	}

	t.Run("insert", func(t *testing.T) {
		if _, err := newReplica(t).InsertAt(1, 'y'); !errors.Is(err, ErrInvalidOp) {
			t.Fatalf("InsertAt: %v", err)
		}
	})
	t.Run("insert text", func(t *testing.T) {
		if _, err := newReplica(t).InsertText(1, "y"); !errors.Is(err, ErrInvalidOp) {
			t.Fatalf("InsertText: %v", err)
		}
	})
	t.Run("delete", func(t *testing.T) {
		if _, err := newReplica(t).DeleteAt(0); !errors.Is(err, ErrInvalidOp) {
			t.Fatalf("DeleteAt: %v", err)
		}
	})
}

func TestDeepChainUsesIterativeTraversal(t *testing.T) {
	if testing.Short() {
		t.Skip("deep traversal")
	}
	doc := New()
	parent := Root
	const count = 300_000
	for clock := uint64(1); clock <= count; clock++ {
		op := Insert{ID: ID{Clock: clock, Client: 1}, Parent: parent, Value: 'x'}
		if _, err := doc.Apply(op); err != nil {
			t.Fatal(err)
		}
		parent = op.ID
	}
	if doc.Len() != count || len(doc.Text()) != count {
		t.Fatalf("len=%d bytes=%d", doc.Len(), len(doc.Text()))
	}
}

func BenchmarkRemoteInsert(b *testing.B) {
	ops := make([]Insert, b.N)
	parent := Root
	for i := range ops {
		op := Insert{ID: ID{Clock: uint64(i + 1), Client: 1}, Parent: parent, Value: 'x'}
		ops[i] = op
		parent = op.ID
	}
	doc := New()
	b.ResetTimer()
	for _, op := range ops {
		if _, err := doc.Apply(op); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkText(b *testing.B) {
	doc := New()
	parent := Root
	for clock := uint64(1); clock <= 100_000; clock++ {
		op := Insert{ID: ID{Clock: clock, Client: 1}, Parent: parent, Value: 'x'}
		if _, err := doc.Apply(op); err != nil {
			b.Fatal(err)
		}
		parent = op.ID
	}
	b.ResetTimer()
	for range b.N {
		_ = doc.Text()
	}
}
