package crdt_test

import (
	"math/rand/v2"
	"slices"
	"testing"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"google.golang.org/protobuf/proto"
)

func FuzzApply(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6})
	f.Add([]byte("collabsphere"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64 {
			data = data[:64]
		}
		ops := make([]crdt.Op, 0, len(data))
		var inserts []crdt.ID
		for i, b := range data {
			id := crdt.ID{Clock: uint64(i + 1), Client: crdt.ClientID(b%4 + 1)}
			if len(inserts) > 0 && b%5 == 0 {
				ops = append(ops, crdt.Delete{ID: id, Target: inserts[int(b/5)%len(inserts)]})
				continue
			}
			parent := crdt.Root
			if len(inserts) > 0 && b%3 != 0 {
				parent = inserts[int(b/3)%len(inserts)]
			}
			ops = append(ops, crdt.Insert{ID: id, Parent: parent, Value: rune('a' + b%26)})
			inserts = append(inserts, id)
		}
		shuffled := append([]crdt.Op(nil), ops...)
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
		for _, order := range [][]crdt.Op{ops, reversed, shuffled} {
			doc := crdt.New()
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
