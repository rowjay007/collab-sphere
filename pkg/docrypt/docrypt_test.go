package docrypt

import (
	"bytes"
	"crypto/ecdh"
	"crypto/rand"
	"testing"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func testKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	return key
}

func TestSealRoundTrip(t *testing.T) {
	key := testKey(t)
	doc := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	root := crdt.ID{Client: 7, Clock: 3}
	for _, r := range []rune{'a', 'é', '你', '😀'} {
		ct, err := Seal(key, doc, 1, root, root, r)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(ct); got != utf8.RuneLen(r)+16 {
			t.Fatalf("rune %q ciphertext length %d", r, got)
		}
		opened, err := Open(key, doc, 1, root, ct)
		if err != nil {
			t.Fatal(err)
		}
		if opened != r {
			t.Fatalf("opened %q, want %q", opened, r)
		}
		for i := range ct {
			bad := bytes.Clone(ct)
			bad[i] ^= 0xff
			if _, err := Open(key, doc, 1, root, bad); err == nil {
				t.Fatalf("rune %q byte %d tamper opened", r, i)
			}
		}
		if _, err := Open(key, doc, 2, root, ct); err == nil {
			t.Fatal("wrong epoch opened")
		}
		if _, err := Open(key, uuid.New(), 1, root, ct); err == nil {
			t.Fatal("wrong doc opened")
		}
	}
}

func TestCopyOpensWithRoot(t *testing.T) {
	key := testKey(t)
	doc := uuid.New()
	root := crdt.ID{Client: 1, Clock: 1}
	copy := crdt.ID{Client: 2, Clock: 4}
	ct, err := Seal(key, doc, 3, copy, root, 'q')
	if err != nil {
		t.Fatal(err)
	}
	got, err := Open(key, doc, 3, root, ct)
	if err != nil {
		t.Fatal(err)
	}
	if got != 'q' {
		t.Fatalf("opened %q", got)
	}
	if _, err := Open(key, doc, 3, copy, ct); err == nil {
		t.Fatal("copy id opened a seal bound to the lineage root")
	}
}

func TestDistinctRootsGiveDistinctNonces(t *testing.T) {
	doc := uuid.New()
	a := crdt.ID{Client: 1, Clock: 1}
	b := crdt.ID{Client: 1, Clock: 2}
	c := crdt.ID{Client: 2, Clock: 1}
	if bytes.Equal(Nonce(doc, 1, a), Nonce(doc, 1, b)) || bytes.Equal(Nonce(doc, 1, a), Nonce(doc, 1, c)) {
		t.Fatal("distinct roots share a nonce")
	}
	if bytes.Equal(Nonce(doc, 1, a), Nonce(doc, 2, a)) || bytes.Equal(Nonce(doc, 1, a), Nonce(uuid.New(), 1, a)) {
		t.Fatal("epoch or doc dropped from the nonce")
	}
}

func TestWrapRoundTrip(t *testing.T) {
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	docKey := testKey(t)
	doc := uuid.New()
	keyID := uuid.New()
	user := "ada"
	w, err := WrapKey(docKey, doc, 4, user, keyID, priv.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	if len(w.EphPub) != 32 || len(w.Nonce) != 12 || len(w.Ciphertext) != 48 {
		t.Fatalf("wrap lengths eph=%d nonce=%d ct=%d", len(w.EphPub), len(w.Nonce), len(w.Ciphertext))
	}
	got, err := UnwrapKey(w, doc, 4, user, keyID, priv)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, docKey) {
		t.Fatal("unwrapped key differs")
	}
	for _, blob := range []struct {
		name string
		flip func(*Wrap)
	}{
		{"eph", func(w *Wrap) { w.EphPub = bytes.Clone(w.EphPub); w.EphPub[0] ^= 0xff }},
		{"nonce", func(w *Wrap) { w.Nonce = bytes.Clone(w.Nonce); w.Nonce[0] ^= 0xff }},
		{"ciphertext", func(w *Wrap) {
			w.Ciphertext = bytes.Clone(w.Ciphertext)
			for i := range w.Ciphertext {
				bad := bytes.Clone(w.Ciphertext)
				bad[i] ^= 0xff
				trial := *w
				trial.Ciphertext = bad
				if _, err := UnwrapKey(trial, doc, 4, user, keyID, priv); err == nil {
					t.Fatalf("ciphertext byte %d tamper opened", i)
				}
			}
		}},
	} {
		trial := w
		blob.flip(&trial)
		if blob.name == "ciphertext" {
			continue
		}
		if _, err := UnwrapKey(trial, doc, 4, user, keyID, priv); err == nil {
			t.Fatalf("%s tamper opened", blob.name)
		}
	}
	if _, err := UnwrapKey(w, doc, 5, user, keyID, priv); err == nil {
		t.Fatal("wrong epoch opened")
	}
	if _, err := UnwrapKey(w, uuid.New(), 4, user, keyID, priv); err == nil {
		t.Fatal("wrong doc opened")
	}
	if _, err := UnwrapKey(w, doc, 4, "bob", keyID, priv); err == nil {
		t.Fatal("wrong user opened")
	}
	if _, err := UnwrapKey(w, doc, 4, user, uuid.New(), priv); err == nil {
		t.Fatal("wrong key id opened")
	}
}
