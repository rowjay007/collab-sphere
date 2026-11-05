package oracle

import (
	"bytes"
	"encoding/hex"
	"math/rand/v2"
	"testing"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func TestCSS2GoldenVectors(t *testing.T) {
	one := crdt.NewEncrypted()
	cipher := &crdt.Ciphertext{Epoch: 1, Bytes: bytes.Repeat([]byte{0xab}, 17)}
	if _, err := one.Apply(crdt.Insert{
		ID: crdt.ID{Client: 1, Clock: 1}, Parent: crdt.Root, Cipher: cipher,
	}); err != nil {
		t.Fatal(err)
	}
	const oneHex = "4353533201010100000111" +
		"ababababababababababababababababab" +
		"000001010100"

	for _, tc := range []struct {
		name string
		doc  *crdt.Doc
		hex  string
	}{
		{"empty", crdt.NewEncrypted(), "43535332000000"},
		{"one node", one, oneHex},
	} {
		want, err := hex.DecodeString(tc.hex)
		if err != nil {
			t.Fatal(err)
		}
		got, err := crdt.CanonicalCSS2(tc.doc)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s: CanonicalCSS2 = %x, want %x", tc.name, got, want)
		}
		if o := EncodeCSS2(FromSnapshot(tc.doc.Snapshot())); !bytes.Equal(o, want) {
			t.Errorf("%s: oracle = %x, want %x", tc.name, o, want)
		}
		back, err := crdt.FromCanonical(want)
		if err != nil {
			t.Fatalf("%s: FromCanonical: %v", tc.name, err)
		}
		if back.Mode() != crdt.Encrypted {
			t.Errorf("%s: mode = %d", tc.name, back.Mode())
		}
		again, err := crdt.CanonicalCSS2(back)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(again, want) {
			t.Errorf("%s: re-encode = %x", tc.name, again)
		}
	}
}

func TestCSS2Agreement(t *testing.T) {
	var copies, multiDeletes int
	for seed := uint64(1); seed <= 500; seed++ {
		d := randomEncrypted(t, seed)
		got, err := crdt.CanonicalCSS2(d)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		state := FromSnapshot(d.Snapshot())
		if want := EncodeCSS2(state); !bytes.Equal(got, want) {
			t.Fatalf("seed %d: encoders disagree:\ncrdt   %x\noracle %x", seed, got, want)
		}
		replayed, err := Replay(d.Ops())
		if err != nil {
			t.Fatalf("seed %d: replay: %v", seed, err)
		}
		if want := EncodeCSS2(replayed); !bytes.Equal(got, want) {
			t.Fatalf("seed %d: replay encoder disagrees", seed)
		}
		back, err := crdt.FromCanonical(got)
		if err != nil {
			t.Fatalf("seed %d: FromCanonical: %v", seed, err)
		}
		again, err := crdt.CanonicalCSS2(back)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(again, got) {
			t.Fatalf("seed %d: re-encode mismatch", seed)
		}
		c, multi := countCipherShape(d)
		copies += c
		multiDeletes += multi
	}
	if copies < 500 || multiDeletes < 500 {
		t.Fatalf("documents held %d copies and %d nodes with two deletes", copies, multiDeletes)
	}
}

func randomEncrypted(t *testing.T, seed uint64) *crdt.Doc {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, 11))
	d := crdt.NewEncrypted()
	var clock uint64
	next := func(client crdt.ClientID) crdt.ID {
		clock++
		return crdt.ID{Client: client, Clock: clock}
	}
	parent := crdt.Root
	n := 4 + rng.IntN(8)
	var leaf crdt.ID
	for range n {
		id := next(1)
		op := crdt.Insert{ID: id, Parent: parent, Cipher: randCipher(rng, uint32(1+rng.IntN(3)))}
		if _, err := d.Apply(op); err != nil {
			t.Fatalf("seed %d insert: %v", seed, err)
		}
		leaf = id
		parent = id
	}
	target, err := crdt.FromSnapshot(d.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := d.Apply(crdt.Delete{ID: next(1), Target: leaf}); err != nil {
			t.Fatalf("seed %d delete: %v", seed, err)
		}
	}
	ops, err := crdt.PlanRestore(d, target, 2)
	if err != nil {
		t.Fatalf("seed %d plan: %v", seed, err)
	}
	for _, op := range ops {
		if _, err := d.Apply(op); err != nil {
			t.Fatalf("seed %d restore: %v", seed, err)
		}
	}
	return d
}

func randCipher(rng *rand.Rand, epoch uint32) *crdt.Ciphertext {
	n := 17 + rng.IntN(4)
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.IntN(256))
	}
	return &crdt.Ciphertext{Epoch: epoch, Bytes: b}
}

func countCipherShape(d *crdt.Doc) (copies, multiDeletes int) {
	for _, n := range d.Snapshot().GetNodes() {
		if n.GetCopy() {
			copies++
		}
		if len(n.GetDeletedBy()) >= 2 {
			multiDeletes++
		}
	}
	return copies, multiDeletes
}
