package crdt

import (
	"fmt"
	"math/rand/v2"
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

// After a stable prefix is collected on some replicas and not others, edits
// made after the stable point must still converge to the same text.
func TestGCReplicasConvergeWithUncollectedReplicas(t *testing.T) {
	for seed := uint64(0); seed < 40; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 99))
			replicas := make([]*Replica, 4)
			for i := range replicas {
				replicas[i], _ = NewReplica(ClientID(i+1), nil)
			}
			var ops []Op
			edit := func(rounds int) {
				for range rounds {
					r := replicas[rng.IntN(len(replicas))]
					var op Op
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
					shuffled := append([]Op(nil), ops...)
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
