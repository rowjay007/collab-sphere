package crdt_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"google.golang.org/protobuf/proto"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func TestConcurrentInsertPermutations(t *testing.T) {
	ops := []crdt.Op{
		crdt.Insert{ID: crdt.ID{Clock: 1, Client: 1}, Parent: crdt.Root, Value: 'X'},
		crdt.Insert{ID: crdt.ID{Clock: 1, Client: 2}, Parent: crdt.Root, Value: 'Y'},
		crdt.Insert{ID: crdt.ID{Clock: 1, Client: 3}, Parent: crdt.Root, Value: 'Z'},
	}
	assertAllPermutations(t, ops)
}

func TestDeleteBeforeInsert(t *testing.T) {
	insert := crdt.Insert{ID: crdt.ID{Clock: 1, Client: 1}, Parent: crdt.Root, Value: 'X'}
	deleteOp := crdt.Delete{ID: crdt.ID{Clock: 2, Client: 2}, Target: insert.ID}
	assertAllPermutations(t, []crdt.Op{insert, deleteOp})
}

func TestPartitionedTextBranches(t *testing.T) {
	base, err := crdt.NewReplica(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := base.InsertText(0, "Hello World"); err != nil {
		t.Fatal(err)
	}
	a, err := crdt.FromSnapshot(base.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	b, err := crdt.FromSnapshot(base.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	ra, _ := crdt.NewReplica(2, a)
	rb, _ := crdt.NewReplica(3, b)
	aOps, err := ra.InsertText(5, " beautiful")
	if err != nil {
		t.Fatal(err)
	}
	bOps, err := rb.InsertText(5, " cruel")
	if err != nil {
		t.Fatal(err)
	}
	var canonical *crdtv1.Snapshot
	for _, branches := range [][][]crdt.Insert{{aOps, bOps}, {bOps, aOps}} {
		doc := crdt.New()
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

func TestRandomOperationSetPermutations(t *testing.T) {
	for seed := uint64(0); seed < 12; seed++ {
		random := rand.New(rand.NewPCG(seed, seed+1))
		ops := make([]crdt.Op, 0, 7)
		inserts := make([]crdt.Insert, 0, 7)
		for i := 0; i < 7; i++ {
			id := crdt.ID{Clock: uint64(i + 1), Client: crdt.ClientID(random.IntN(3) + 1)}
			if len(inserts) > 0 && random.IntN(4) == 0 {
				target := inserts[random.IntN(len(inserts))].ID
				ops = append(ops, crdt.Delete{ID: id, Target: target})
				continue
			}
			parent := crdt.Root
			if len(inserts) > 0 && random.IntN(3) != 0 {
				parent = inserts[random.IntN(len(inserts))].ID
			}
			insert := crdt.Insert{ID: id, Parent: parent, Value: rune('a' + i)}
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
	replicas := make([]*crdt.Replica, replicaCount)
	for i := range replicas {
		replicas[i], _ = crdt.NewReplica(crdt.ClientID(i+1), nil)
	}
	random := rand.New(rand.NewPCG(8675309, 1))
	var ops []crdt.Op
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
		shuffled := append([]crdt.Op(nil), ops...)
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

func assertAllPermutations(t *testing.T, ops []crdt.Op) {
	t.Helper()
	var canonical *crdtv1.Snapshot
	var text string
	permutations(ops, func(permutation []crdt.Op) {
		doc := crdt.New()
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

func permutations(ops []crdt.Op, visit func([]crdt.Op)) {
	work := append([]crdt.Op(nil), ops...)
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
