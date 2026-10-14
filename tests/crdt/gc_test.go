package crdt_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func TestGC(t *testing.T) {
	replica, _ := crdt.NewReplica(1, nil)
	inserts, err := replica.InsertText(0, "ab")
	if err != nil {
		t.Fatal(err)
	}
	deleteOp, err := replica.DeleteAt(1)
	if err != nil {
		t.Fatal(err)
	}
	before := replica.Text()
	if _, err := replica.GC(crdt.VersionVector{1: deleteOp.ID.Clock + 1}); !errors.Is(err, crdt.ErrInvalidOp) {
		t.Fatalf("uncovered stable vector: %v", err)
	}
	removed, err := replica.GC(replica.Vector())
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 || replica.Text() != before || replica.Has(inserts[1].ID) {
		t.Fatalf("removed=%d text=%q has=%v", removed, replica.Text(), replica.Has(inserts[1].ID))
	}
	if status, err := replica.Apply(inserts[1]); err != nil || status != crdt.Duplicate {
		t.Fatalf("late insert: %v, %v", status, err)
	}
	if status, err := replica.Apply(deleteOp); err != nil || status != crdt.Duplicate {
		t.Fatalf("late delete: %v, %v", status, err)
	}
}

// After a stable prefix is collected on some replicas and not others, edits
// made after the stable point must still converge to the same text.
func TestGCReplicasConvergeWithUncollectedReplicas(t *testing.T) {
	for seed := uint64(0); seed < 40; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 99))
			replicas := make([]*crdt.Replica, 4)
			for i := range replicas {
				replicas[i], _ = crdt.NewReplica(crdt.ClientID(i+1), nil)
			}
			var ops []crdt.Op
			edit := func(rounds int) {
				for range rounds {
					r := replicas[rng.IntN(len(replicas))]
					var op crdt.Op
					var err error
					if r.Len() > 0 && rng.IntN(3) == 0 {
						op, err = r.DeleteAt(rng.IntN(r.Len()))
					} else {
						op, err = r.InsertAt(rng.IntN(r.Len()+1), rune('a'+rng.IntN(26)))
					}
					if err != nil {
						t.Fatal(err)
					}
					ops = append(ops, op)
					for range rng.IntN(4) {
						if _, err := replicas[rng.IntN(len(replicas))].Apply(ops[rng.IntN(len(ops))]); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			syncAll := func() {
				for _, r := range replicas {
					shuffled := append([]crdt.Op(nil), ops...)
					rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
					for _, op := range shuffled {
						if _, err := r.Apply(op); err != nil {
							t.Fatal(err)
						}
					}
				}
			}

			edit(150)
			syncAll()
			stable := replicas[0].Vector()
			collected := 0
			for _, r := range replicas[:2] {
				n, err := r.GC(stable)
				if err != nil {
					t.Fatal(err)
				}
				collected = n
			}
			if collected == 0 {
				t.Fatal("GC collected nothing, so this seed proves nothing")
			}
			edit(150)
			syncAll()

			want := replicas[3].Text()
			for i, r := range replicas {
				if r.Text() != want {
					t.Fatalf("replica %d (collected %d tombstones) diverged:\n got %q\nwant %q", i, collected, r.Text(), want)
				}
			}
		})
	}
}
