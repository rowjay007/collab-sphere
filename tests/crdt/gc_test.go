package crdt_test

import (
	"bytes"
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
	before := mustText(t, replica)
	if _, err := replica.GC(crdt.VersionVector{1: deleteOp.ID.Clock + 1}); !errors.Is(err, crdt.ErrInvalidOp) {
		t.Fatalf("uncovered stable vector: %v", err)
	}
	removed, err := replica.GC(replica.Vector())
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 || mustText(t, replica) != before || replica.Has(inserts[1].ID) {
		t.Fatalf("removed=%d text=%q has=%v", removed, mustText(t, replica), replica.Has(inserts[1].ID))
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

			want := mustText(t, replicas[3])
			for i, r := range replicas {
				if mustText(t, r) != want {
					t.Fatalf("replica %d (collected %d tombstones) diverged:\n got %q\nwant %q", i, collected, mustText(t, r), want)
				}
			}
		})
	}
}

func TestGCWithPinsKeepsPinnedLeafAndDropsSibling(t *testing.T) {
	r, _ := crdt.NewReplica(1, nil)
	left, err := r.InsertAfter(crdt.Root, 'a')
	if err != nil {
		t.Fatal(err)
	}
	right, err := r.InsertAfter(crdt.Root, 'b')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Delete(left.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Delete(right.ID); err != nil {
		t.Fatal(err)
	}
	removed, err := r.GCWithPins(r.Vector(), map[crdt.ID]struct{}{left.ID: {}})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 || !r.Has(left.ID) || r.Has(right.ID) {
		t.Fatalf("removed=%d has(pinned)=%v has(sibling)=%v", removed, r.Has(left.ID), r.Has(right.ID))
	}
}

func TestGCWithPinsDropsChildOfPinnedNode(t *testing.T) {
	r, _ := crdt.NewReplica(1, nil)
	parent, err := r.InsertAfter(crdt.Root, 'p')
	if err != nil {
		t.Fatal(err)
	}
	child, err := r.InsertAfter(parent.ID, 'c')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Delete(child.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Delete(parent.ID); err != nil {
		t.Fatal(err)
	}
	removed, err := r.GCWithPins(r.Vector(), map[crdt.ID]struct{}{parent.ID: {}})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 || !r.Has(parent.ID) || r.Has(child.ID) {
		t.Fatalf("removed=%d has(parent)=%v has(child)=%v", removed, r.Has(parent.ID), r.Has(child.ID))
	}
}

func TestGCWithPinsKeepsDeletedChainWhenChildPinned(t *testing.T) {
	r, _ := crdt.NewReplica(1, nil)
	parent, err := r.InsertAfter(crdt.Root, 'p')
	if err != nil {
		t.Fatal(err)
	}
	child, err := r.InsertAfter(parent.ID, 'c')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Delete(child.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Delete(parent.ID); err != nil {
		t.Fatal(err)
	}
	removed, err := r.GCWithPins(r.Vector(), map[crdt.ID]struct{}{child.ID: {}})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 || !r.Has(parent.ID) || !r.Has(child.ID) {
		t.Fatalf("removed=%d has(parent)=%v has(child)=%v", removed, r.Has(parent.ID), r.Has(child.ID))
	}
}

func TestGCWithPinsRejectsUncoveredStable(t *testing.T) {
	r, _ := crdt.NewReplica(1, nil)
	if _, err := r.InsertAfter(crdt.Root, 'a'); err != nil {
		t.Fatal(err)
	}
	if _, err := r.GCWithPins(crdt.VersionVector{1: r.Vector()[1] + 1}, nil); !errors.Is(err, crdt.ErrInvalidOp) {
		t.Fatalf("uncovered stable: %v", err)
	}
}

func TestGCWithPinsEncryptedCSS2RoundTrip(t *testing.T) {
	d := crdt.NewEncrypted()
	cipher := func(fill byte) *crdt.Ciphertext {
		return &crdt.Ciphertext{Epoch: 1, Bytes: bytes.Repeat([]byte{fill}, 17)}
	}
	a := crdt.Insert{ID: crdt.ID{Client: 1, Clock: 1}, Parent: crdt.Root, Cipher: cipher(1)}
	b := crdt.Insert{ID: crdt.ID{Client: 1, Clock: 2}, Parent: a.ID, Cipher: cipher(2)}
	c := crdt.Insert{ID: crdt.ID{Client: 1, Clock: 3}, Parent: crdt.Root, Cipher: cipher(3)}
	applyAll(t, d, []crdt.Op{
		a, b, c,
		crdt.Delete{ID: crdt.ID{Client: 1, Clock: 4}, Target: b.ID},
		crdt.Delete{ID: crdt.ID{Client: 1, Clock: 5}, Target: c.ID},
		crdt.Delete{ID: crdt.ID{Client: 1, Clock: 6}, Target: a.ID},
	})
	if _, err := d.GCWithPins(d.Vector(), map[crdt.ID]struct{}{a.ID: {}}); err != nil {
		t.Fatal(err)
	}
	if d.Has(b.ID) || d.Has(c.ID) || !d.Has(a.ID) {
		t.Fatalf("has a=%v b=%v c=%v", d.Has(a.ID), d.Has(b.ID), d.Has(c.ID))
	}
	if _, err := d.Text(); !errors.Is(err, crdt.ErrEncrypted) {
		t.Fatalf("Text = %v", err)
	}
	for _, n := range d.Snapshot().GetNodes() {
		if n.GetValue() != 0 || len(n.GetCipher()) == 0 {
			t.Fatalf("node produced a rune or lost cipher: value=%d cipher=%d", n.GetValue(), len(n.GetCipher()))
		}
	}
	encoded, err := crdt.CanonicalCSS2(d)
	if err != nil {
		t.Fatal(err)
	}
	back, err := crdt.FromCanonical(encoded)
	if err != nil {
		t.Fatal(err)
	}
	again, err := crdt.CanonicalCSS2(back)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, again) {
		t.Fatalf("re-encode %x != %x", again, encoded)
	}
}

func TestStableMissingClientIsZero(t *testing.T) {
	doc := crdt.VersionVector{1: 5, 2: 4}
	got := crdt.Stable(doc, []crdt.VersionVector{{1: 3}}, doc)
	if got[2] != 0 {
		t.Fatalf("missing client clock=%d, want 0", got[2])
	}
	if got[1] != 3 {
		t.Fatalf("client 1 clock=%d, want 3", got[1])
	}
}

func TestStableFloorMissingClientKeepsLaterDelete(t *testing.T) {
	a, _ := crdt.NewReplica(1, nil)
	x, err := a.InsertAfter(crdt.Root, 'x')
	if err != nil {
		t.Fatal(err)
	}
	floor := a.Vector()
	b, _ := crdt.NewReplica(2, nil)
	if _, err := b.Apply(x); err != nil {
		t.Fatal(err)
	}
	del, err := b.Delete(x.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Apply(del); err != nil {
		t.Fatal(err)
	}
	doc := a.Vector()
	stable := crdt.Stable(doc, []crdt.VersionVector{doc, doc}, floor)
	if stable[2] != 0 {
		t.Fatalf("floor without client 2 gave clock %d, want 0", stable[2])
	}
	if _, err := a.GCWithPins(stable, nil); err != nil {
		t.Fatal(err)
	}
	if !a.Has(x.ID) {
		t.Fatal("tombstone deleted after the restore floor was collected")
	}
}

func TestGCWithPinsPreservesTextAndPinnedIDs(t *testing.T) {
	for seed := uint64(0); seed < 40; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 17))
			r, err := crdt.NewReplica(1, nil)
			if err != nil {
				t.Fatal(err)
			}
			var ids []crdt.ID
			for range 80 {
				if r.Len() > 0 && rng.IntN(3) == 0 {
					if _, err := r.DeleteAt(rng.IntN(r.Len())); err != nil {
						t.Fatal(err)
					}
					continue
				}
				ins, err := r.InsertAt(rng.IntN(r.Len()+1), rune('a'+rng.IntN(26)))
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, ins.ID)
			}
			pins := make(map[crdt.ID]struct{})
			for _, id := range ids {
				if r.Has(id) && rng.IntN(3) == 0 {
					pins[id] = struct{}{}
				}
			}
			before := mustText(t, r)
			if _, err := r.GCWithPins(r.Vector(), pins); err != nil {
				t.Fatal(err)
			}
			if got := mustText(t, r); got != before {
				t.Fatalf("text changed: got %q want %q", got, before)
			}
			for id := range pins {
				if !r.Has(id) {
					t.Fatalf("pinned %s was removed", id)
				}
			}
		})
	}
}
