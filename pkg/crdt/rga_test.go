package crdt

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"unicode/utf8"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"google.golang.org/protobuf/proto"
)

func TestConcurrentInsertPermutations(t *testing.T) {
	ops := []Op{
		Insert{ID: ID{Clock: 1, Client: 1}, Parent: Root, Value: 'X'},
		Insert{ID: ID{Clock: 1, Client: 2}, Parent: Root, Value: 'Y'},
		Insert{ID: ID{Clock: 1, Client: 3}, Parent: Root, Value: 'Z'},
	}
	assertAllPermutations(t, ops)
}

func TestDeleteBeforeInsert(t *testing.T) {
	insert := Insert{ID: ID{Clock: 1, Client: 1}, Parent: Root, Value: 'X'}
	deleteOp := Delete{ID: ID{Clock: 2, Client: 2}, Target: insert.ID}
	assertAllPermutations(t, []Op{insert, deleteOp})
}

func TestPartitionedTextBranches(t *testing.T) {
	base, err := NewReplica(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := base.InsertText(0, "Hello World"); err != nil {
		t.Fatal(err)
	}
	a, err := FromSnapshot(base.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	b, err := FromSnapshot(base.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	ra, _ := NewReplica(2, a)
	rb, _ := NewReplica(3, b)
	aOps, err := ra.InsertText(5, " beautiful")
	if err != nil {
		t.Fatal(err)
	}
	bOps, err := rb.InsertText(5, " cruel")
	if err != nil {
		t.Fatal(err)
	}
	var canonical *crdtv1.Snapshot
	for _, branches := range [][][]Insert{{aOps, bOps}, {bOps, aOps}} {
		doc := New()
		for _, op := range base.Ops() {
			if _, err := doc.Apply(op); err != nil {
				t.Fatalf("apply %v: %v", op, err)
			}
		}
		for _, branch := range branches {
			for _, op := range branch {
				if _, err := doc.Apply(op); err != nil {
					t.Fatalf("apply %v: %v", op, err)
				}
			}
		}
		if text := doc.Text(); text != "Hello beautiful cruel World" && text != "Hello cruel beautiful World" {
			t.Fatalf("interleaved result %q", text)
		}
		if canonical == nil {
			canonical = doc.Snapshot()
		} else if !proto.Equal(canonical, doc.Snapshot()) {
			t.Fatal("snapshots differ by arrival order")
		}
	}
}

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

func TestRandomOperationSetPermutations(t *testing.T) {
	for seed := uint64(0); seed < 12; seed++ {
		random := rand.New(rand.NewPCG(seed, seed+1))
		ops := make([]Op, 0, 7)
		inserts := make([]Insert, 0, 7)
		for i := 0; i < 7; i++ {
			id := ID{Clock: uint64(i + 1), Client: ClientID(random.IntN(3) + 1)}
			if len(inserts) > 0 && random.IntN(4) == 0 {
				target := inserts[random.IntN(len(inserts))].ID
				ops = append(ops, Delete{ID: id, Target: target})
				continue
			}
			parent := Root
			if len(inserts) > 0 && random.IntN(3) != 0 {
				parent = inserts[random.IntN(len(inserts))].ID
			}
			insert := Insert{ID: id, Parent: parent, Value: rune('a' + i)}
			inserts = append(inserts, insert)
			ops = append(ops, insert)
		}
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			assertAllPermutations(t, ops)
		})
	}
}

func TestRandomizedConvergence(t *testing.T) {
	const replicaCount = 4
	replicas := make([]*Replica, replicaCount)
	for i := range replicas {
		replicas[i], _ = NewReplica(ClientID(i+1), nil)
	}
	random := rand.New(rand.NewPCG(8675309, 1))
	var ops []Op
	for round := 0; round < 400; round++ {
		replica := replicas[random.IntN(len(replicas))]
		if replica.Len() == 0 || random.IntN(4) != 0 {
			index := random.IntN(replica.Len() + 1)
			op, err := replica.InsertAt(index, rune('a'+random.IntN(26)))
			if err != nil {
				t.Fatal(err)
			}
			ops = append(ops, op)
		} else {
			op, err := replica.DeleteAt(random.IntN(replica.Len()))
			if err != nil {
				t.Fatal(err)
			}
			ops = append(ops, op)
		}
		for delivery := 0; delivery < random.IntN(6); delivery++ {
			target := replicas[random.IntN(len(replicas))]
			op := ops[random.IntN(len(ops))]
			if _, err := target.Apply(op); err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
		}
	}
	for _, replica := range replicas {
		shuffled := append([]Op(nil), ops...)
		random.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		for _, op := range shuffled {
			if _, err := replica.Apply(op); err != nil {
				t.Fatal(err)
			}
			if _, err := replica.Apply(op); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := 1; i < len(replicas); i++ {
		if replicas[i].Text() != replicas[0].Text() || !proto.Equal(replicas[i].Snapshot(), replicas[0].Snapshot()) {
			t.Fatalf("replica %d diverged", i)
		}
	}
}

func TestSnapshotRoundTripWithPending(t *testing.T) {
	doc := New()
	parent := Insert{ID: ID{Clock: 2, Client: 1}, Parent: ID{Clock: 1, Client: 1}, Value: 'b'}
	deleteOp := Delete{ID: ID{Clock: 3, Client: 2}, Target: parent.ID}
	if status, err := doc.Apply(parent); err != nil || status != Pending {
		t.Fatalf("pending insert: %v, %v", status, err)
	}
	if status, err := doc.Apply(deleteOp); err != nil || status != Pending {
		t.Fatalf("pending delete: %v, %v", status, err)
	}
	restored, err := FromSnapshot(doc.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(doc.Snapshot(), restored.Snapshot()) {
		t.Fatal("round-trip snapshot differs")
	}
	first := Insert{ID: ID{Clock: 1, Client: 1}, Parent: Root, Value: 'a'}
	if _, err := doc.Apply(first); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.Apply(first); err != nil {
		t.Fatal(err)
	}
	if doc.Text() != restored.Text() || !proto.Equal(doc.Snapshot(), restored.Snapshot()) {
		t.Fatal("restored document diverged")
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

func TestGC(t *testing.T) {
	replica, _ := NewReplica(1, nil)
	inserts, err := replica.InsertText(0, "ab")
	if err != nil {
		t.Fatal(err)
	}
	deleteOp, err := replica.DeleteAt(1)
	if err != nil {
		t.Fatal(err)
	}
	before := replica.Text()
	if _, err := replica.GC(VersionVector{1: deleteOp.ID.Clock + 1}); !errors.Is(err, ErrInvalidOp) {
		t.Fatalf("uncovered stable vector: %v", err)
	}
	removed, err := replica.GC(replica.Vector())
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 || replica.Text() != before || replica.Has(inserts[1].ID) {
		t.Fatalf("removed=%d text=%q has=%v", removed, replica.Text(), replica.Has(inserts[1].ID))
	}
	if status, err := replica.Apply(inserts[1]); err != nil || status != Duplicate {
		t.Fatalf("late insert: %v, %v", status, err)
	}
	if status, err := replica.Apply(deleteOp); err != nil || status != Duplicate {
		t.Fatalf("late delete: %v, %v", status, err)
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

func FuzzApply(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6})
	f.Add([]byte("collabsphere"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64 {
			data = data[:64]
		}
		ops := make([]Op, 0, len(data))
		var inserts []ID
		for i, b := range data {
			id := ID{Clock: uint64(i + 1), Client: ClientID(b%4 + 1)}
			if len(inserts) > 0 && b%5 == 0 {
				ops = append(ops, Delete{ID: id, Target: inserts[int(b/5)%len(inserts)]})
				continue
			}
			parent := Root
			if len(inserts) > 0 && b%3 != 0 {
				parent = inserts[int(b/3)%len(inserts)]
			}
			ops = append(ops, Insert{ID: id, Parent: parent, Value: rune('a' + b%26)})
			inserts = append(inserts, id)
		}
		shuffled := append([]Op(nil), ops...)
		seed := uint64(len(data))
		for _, b := range data {
			seed = seed*31 + uint64(b)
		}
		rand.New(rand.NewPCG(seed, 0)).Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})
		reversed := slices.Clone(ops)
		slices.Reverse(reversed)

		var want *crdtv1.Snapshot
		for _, order := range [][]Op{ops, reversed, shuffled} {
			doc := New()
			for _, op := range order {
				if _, err := doc.Apply(op); err != nil {
					t.Fatalf("valid op %v rejected: %v", op, err)
				}
			}
			if want == nil {
				want = doc.Snapshot()
			} else if !proto.Equal(want, doc.Snapshot()) {
				t.Fatal("application order changed snapshot")
			}
		}
	})
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

func assertAllPermutations(t *testing.T, ops []Op) {
	t.Helper()
	var canonical *crdtv1.Snapshot
	var text string
	permutations(ops, func(permutation []Op) {
		doc := New()
		for _, op := range permutation {
			if _, err := doc.Apply(op); err != nil {
				t.Fatalf("apply %v: %v", op, err)
			}
		}
		if canonical == nil {
			canonical = doc.Snapshot()
			text = doc.Text()
			return
		}
		if doc.Text() != text || !proto.Equal(doc.Snapshot(), canonical) {
			t.Fatalf("arrival order diverged: %q != %q", doc.Text(), text)
		}
	})
}

func permutations(ops []Op, visit func([]Op)) {
	work := append([]Op(nil), ops...)
	var generate func(int)
	generate = func(index int) {
		if index == len(work) {
			visit(work)
			return
		}
		for i := index; i < len(work); i++ {
			work[index], work[i] = work[i], work[index]
			generate(index + 1)
			work[index], work[i] = work[i], work[index]
		}
	}
	generate(0)
}
