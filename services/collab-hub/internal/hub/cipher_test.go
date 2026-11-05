package hub

import (
	"bytes"
	"context"
	"testing"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func docText(t testing.TB, d *crdt.Doc) string {
	t.Helper()
	s, err := d.Text()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type encryptedStore struct {
	*MemoryStore
}

func (s encryptedStore) Load(ctx context.Context, docID string) (*crdt.Doc, uint64, error) {
	doc, seq, err := s.MemoryStore.Load(ctx, docID)
	if err != nil || seq != 0 {
		return doc, seq, err
	}
	return crdt.NewEncrypted(), 0, nil
}

func TestHubRejectsCipherShape(t *testing.T) {
	cipher := func(n int) *crdt.Ciphertext {
		return &crdt.Ciphertext{Epoch: 1, Bytes: bytes.Repeat([]byte{0x11}, n)}
	}
	id := crdt.ID{Client: 1, Clock: 1}
	cases := []struct {
		name      string
		encrypted bool
		op        crdt.Insert
		message   string
	}{
		{"plain cipher", false, crdt.Insert{ID: id, Parent: crdt.Root, Cipher: cipher(17)}, "operation does not match document mode"},
		{"encrypted rune", true, crdt.Insert{ID: id, Parent: crdt.Root, Value: 'a'}, "operation does not match document mode"},
		{"both", true, crdt.Insert{ID: id, Parent: crdt.Root, Value: 'a', Cipher: cipher(17)}, "operation does not match document mode"},
		{"short", true, crdt.Insert{ID: id, Parent: crdt.Root, Cipher: cipher(16)}, "cipher length out of range"},
		{"long", true, crdt.Insert{ID: id, Parent: crdt.Root, Cipher: cipher(21)}, "cipher length out of range"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			base := NewMemoryStore()
			var store Store = base
			if tc.encrypted {
				store = encryptedStore{base}
			}
			reg := localRegistry(store)
			t.Cleanup(reg.Close)
			peer := NewPeer(1, 0)
			session, err := reg.Join(ctx, "doc", peer, false, 0)
			if err != nil {
				t.Fatal(err)
			}
			readServerMessage(t, ctx, peer)
			session.SubmitOps(ctx, []crdt.Op{tc.op})
			msg := readServerMessage(t, ctx, peer)
			got := msg.GetError()
			if got.GetCode() != collabv1.ErrorMessage_CODE_INVALID_OPERATION || got.GetMessage() != tc.message {
				t.Fatalf("error = %v", got)
			}
		})
	}
}

func TestHubAcceptsEncryptedCipher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	store := encryptedStore{NewMemoryStore()}
	reg := localRegistry(store)
	t.Cleanup(reg.Close)
	peer := NewPeer(1, 0)
	session, err := reg.Join(ctx, "doc", peer, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	readServerMessage(t, ctx, peer)
	op := crdt.Insert{
		ID: crdt.ID{Client: 1, Clock: 1}, Parent: crdt.Root,
		Cipher: &crdt.Ciphertext{Epoch: 1, Bytes: bytes.Repeat([]byte{0x22}, 20)},
	}
	session.SubmitOps(ctx, []crdt.Op{op})
	msg := readServerMessage(t, ctx, peer)
	b := msg.GetOperationBroadcast()
	if b == nil || len(b.GetOperations()) != 1 {
		t.Fatalf("broadcast = %v", msg)
	}
	got := b.GetOperations()[0].GetInsert()
	if len(got.GetCipher()) != 20 || got.GetEpoch() != 1 || got.GetValue() != 0 {
		t.Fatalf("insert = %+v", got)
	}
}
