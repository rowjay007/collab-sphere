package crdt_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/oracle"
)

// adrExample is the worked example in ADR 0010: "éi" with the i deleted and
// then restored as a copy, so the text is "éi".
const adrExample = "4353533103" +
	"01010000e9010000" +
	"010201016900010103" +
	"02040102690100" +
	"0201030204" +
	"00"

func adrExampleDoc(t *testing.T) *crdt.Doc {
	t.Helper()
	d := crdt.New()
	applyAll(t, d, []crdt.Op{
		crdt.Insert{ID: crdt.ID{Client: 1, Clock: 1}, Parent: crdt.Root, Value: 'é'},
		crdt.Insert{ID: crdt.ID{Client: 1, Clock: 2}, Parent: crdt.ID{Client: 1, Clock: 1}, Value: 'i'},
		crdt.Delete{ID: crdt.ID{Client: 1, Clock: 3}, Target: crdt.ID{Client: 1, Clock: 2}},
		crdt.Insert{ID: crdt.ID{Client: 2, Clock: 4}, Parent: crdt.ID{Client: 1, Clock: 2}, Value: 'i', Copy: true},
	})
	return d
}

func TestCanonicalGoldenVectors(t *testing.T) {
	collected := crdt.New()
	applyAll(t, collected, []crdt.Op{
		crdt.Insert{ID: crdt.ID{Client: 1, Clock: 1}, Parent: crdt.Root, Value: 'a'},
		crdt.Insert{ID: crdt.ID{Client: 1, Clock: 2}, Parent: crdt.ID{Client: 1, Clock: 1}, Value: 'b'},
		crdt.Delete{ID: crdt.ID{Client: 1, Clock: 3}, Target: crdt.ID{Client: 1, Clock: 2}},
	})
	if _, err := collected.GC(crdt.VersionVector{1: 3}); err != nil {
		t.Fatal(err)
	}
	wide := crdt.New()
	applyAll(t, wide, []crdt.Op{crdt.Insert{ID: crdt.ID{Client: 300, Clock: 1}, Parent: crdt.Root, Value: 'x'}})

	for _, tc := range []struct {
		name string
		doc  *crdt.Doc
		hex  string
	}{
		{"empty", crdt.New(), "43535331000000"},
		{"adr example", adrExampleDoc(t), adrExample},
		{"collected", collected, "4353533101" + "01010000610000" + "010103" + "010103"},
		{"multi-byte client", wide, "4353533101" + "ac02010000780000" + "01ac0201" + "00"},
	} {
		want, err := hex.DecodeString(tc.hex)
		if err != nil {
			t.Fatal(err)
		}
		got, err := crdt.CanonicalBytes(tc.doc)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s: CanonicalBytes = %x, want %x", tc.name, got, want)
		}
		if o := oracle.Encode(oracle.FromSnapshot(tc.doc.Snapshot())); !bytes.Equal(o, want) {
			t.Errorf("%s: oracle = %x, want %x", tc.name, o, want)
		}
		back, err := crdt.FromCanonical(want)
		if err != nil {
			t.Fatalf("%s: FromCanonical: %v", tc.name, err)
		}
		if back.Text() != tc.doc.Text() {
			t.Errorf("%s: decoded text %q, want %q", tc.name, back.Text(), tc.doc.Text())
		}
	}
}

func TestFromCanonicalRejectsNonCanonicalBytes(t *testing.T) {
	for _, tc := range []struct{ name, hex string }{
		{"wrong magic", strings.Replace(adrExample, "43535331", "43535332", 1)},
		{"non-minimal count", strings.Replace(adrExample, "4353533103", "435353318300", 1)},
		{"trailing byte", adrExample + "00"},
		{"truncated", adrExample[:len(adrExample)-2]},
		{"unknown flag", strings.Replace(adrExample, "01010000e9010000", "01010000e9010200", 1)},
		{"copy value differs", strings.Replace(adrExample, "02040102690100", "020401026a0100", 1)},
		{"copy of root", "4353533101" + "01010000610100" + "010101" + "00"},
		{"siblings ascending", "4353533102" + "01010000610000" + "01020000620000" + "010102" + "00"},
		{"deletes descending", "4353533101" + "01010000610002" + "01030102" + "010103" + "00"},
		{"vector descending", "4353533101" + "01010000610000" + "0202010101" + "00"},
		{"vector misses an op", "4353533101" + "01020000610000" + "010101" + "00"},
		{"count overflows input", "43535331ffffffff0f"},
	} {
		b, err := hex.DecodeString(tc.hex)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if _, err := crdt.FromCanonical(b); !errors.Is(err, crdt.ErrInvalidOp) {
			t.Errorf("%s: FromCanonical error = %v, want ErrInvalidOp", tc.name, err)
		}
	}
}

func TestCanonicalBytesRefusesPendingOps(t *testing.T) {
	d := crdt.New()
	applyAll(t, d, []crdt.Op{crdt.Insert{ID: crdt.ID{Client: 1, Clock: 2}, Parent: crdt.ID{Client: 1, Clock: 1}, Value: 'x'}})
	if _, err := crdt.CanonicalBytes(d); !errors.Is(err, crdt.ErrPending) {
		t.Fatalf("CanonicalBytes error = %v, want ErrPending", err)
	}
}

func TestCanonicalBytesAgreeWithIndependentEncoder(t *testing.T) {
	var copies, deleted, collected int
	for seed := uint64(1); seed <= 500; seed++ {
		h := newHistory(t, seed).run(40)
		d := h.pick()
		assertEncodersAgree(t, d, replay(t, d))
		c, del := countCopies(d)
		copies += c
		deleted += del
		if seed%5 == 0 {
			g := clone(t, d)
			stable := crdt.VersionVector{}
			for client, clock := range g.Vector() {
				stable[client] = clock / 2
			}
			if _, err := g.GC(stable); err != nil {
				t.Fatal(err)
			}
			assertEncodersAgree(t, g, oracle.FromSnapshot(g.Snapshot()))
			collected++
		}
	}
	if copies < 500 || deleted < 2000 || collected != 100 {
		t.Fatalf("documents held %d copies, %d tombstones, %d collected", copies, deleted, collected)
	}
}

func assertEncodersAgree(t *testing.T, d *crdt.Doc, s oracle.State) {
	t.Helper()
	got, err := crdt.CanonicalBytes(d)
	if err != nil {
		t.Fatal(err)
	}
	if want := oracle.Encode(s); !bytes.Equal(got, want) {
		t.Fatalf("encoders disagree:\ncrdt   %x\noracle %x", got, want)
	}
	back, err := crdt.FromCanonical(got)
	if err != nil {
		t.Fatal(err)
	}
	again, err := crdt.CanonicalBytes(back)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again, got) || back.Text() != d.Text() {
		t.Fatal("FromCanonical does not round-trip")
	}
}
