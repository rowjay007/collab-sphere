package crdt

import (
	"bytes"
	"errors"
	"testing"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
)

func testCipher(epoch uint32, n int, fill byte) *Ciphertext {
	return &Ciphertext{Epoch: epoch, Bytes: bytes.Repeat([]byte{fill}, n)}
}

func TestInsertRejectsRuneAndCipher(t *testing.T) {
	both := Insert{
		ID: ID{Client: 1, Clock: 1}, Parent: Root, Value: 'a',
		Cipher: testCipher(1, 17, 1),
	}
	for _, d := range []*Doc{New(), NewEncrypted()} {
		if _, err := d.Apply(both); !errors.Is(err, ErrInvalidOp) {
			t.Fatalf("Apply error = %v, want ErrInvalidOp", err)
		}
	}
	if _, err := OpFromProto(OpToProto(both)); !errors.Is(err, ErrInvalidOp) {
		t.Fatalf("OpFromProto error = %v, want ErrInvalidOp", err)
	}
	pb := OpToProto(Insert{ID: both.ID, Parent: Root, Value: 'a'})
	pb.GetInsert().Cipher = bytes.Repeat([]byte{1}, 17)
	pb.GetInsert().Epoch = 1
	if _, err := OpFromProto(pb); !errors.Is(err, ErrInvalidOp) {
		t.Fatalf("proto both: %v", err)
	}
}

func TestInsertRejectsNeitherAndBadLength(t *testing.T) {
	pb := OpToProto(Insert{ID: ID{Client: 1, Clock: 1}, Parent: Root, Value: 'a'})
	pb.GetInsert().Value = 0
	pb.GetInsert().Epoch = 3
	if _, err := OpFromProto(pb); !errors.Is(err, ErrInvalidOp) {
		t.Fatalf("epoch without cipher: %v", err)
	}
	for _, n := range []int{16, 21} {
		op := Insert{ID: ID{Client: 1, Clock: 1}, Parent: Root, Cipher: testCipher(1, n, 2)}
		if _, err := NewEncrypted().Apply(op); !errors.Is(err, ErrInvalidOp) {
			t.Fatalf("length %d: %v", n, err)
		}
		if _, err := OpFromProto(OpToProto(op)); !errors.Is(err, ErrInvalidOp) {
			t.Fatalf("proto length %d: %v", n, err)
		}
	}
}

func TestApplyRejectsModeMismatch(t *testing.T) {
	runeOp := Insert{ID: ID{Client: 1, Clock: 1}, Parent: Root, Value: 'a'}
	cipherOp := Insert{ID: ID{Client: 1, Clock: 1}, Parent: Root, Cipher: testCipher(1, 17, 3)}
	if _, err := NewEncrypted().Apply(runeOp); !errors.Is(err, ErrInvalidOp) {
		t.Fatalf("rune in encrypted: %v", err)
	}
	if _, err := New().Apply(cipherOp); !errors.Is(err, ErrInvalidOp) {
		t.Fatalf("cipher in plain: %v", err)
	}
}

func TestCopyComparesCipher(t *testing.T) {
	parent := Insert{ID: ID{Client: 1, Clock: 1}, Parent: Root, Cipher: testCipher(2, 17, 4)}
	d := NewEncrypted()
	if _, err := d.Apply(parent); err != nil {
		t.Fatal(err)
	}
	for _, op := range []Insert{
		{ID: ID{Client: 2, Clock: 2}, Parent: parent.ID, Copy: true, Cipher: testCipher(2, 17, 5)},
		{ID: ID{Client: 2, Clock: 2}, Parent: parent.ID, Copy: true, Cipher: testCipher(9, 17, 4)},
	} {
		if _, err := d.Apply(op); !errors.Is(err, ErrInvalidOp) {
			t.Fatalf("copy mismatch accepted: %+v", op.Cipher)
		}
	}
	ok := Insert{ID: ID{Client: 2, Clock: 2}, Parent: parent.ID, Copy: true, Cipher: testCipher(2, 17, 4)}
	if _, err := d.Apply(ok); err != nil {
		t.Fatal(err)
	}
	again := Insert{ID: ok.ID, Parent: ok.Parent, Copy: true, Cipher: testCipher(2, 17, 4)}
	if status, err := d.Apply(again); err != nil || status != Duplicate {
		t.Fatalf("rebuilt cipher insert: %v %v", status, err)
	}
}

func TestEncryptedRefusesPlaintextViews(t *testing.T) {
	d := NewEncrypted()
	op := Insert{ID: ID{Client: 1, Clock: 1}, Parent: Root, Cipher: testCipher(1, 18, 6)}
	if _, err := d.Apply(op); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Text(); !errors.Is(err, ErrEncrypted) {
		t.Fatalf("Text error = %v", err)
	}
	if _, err := NewEncrypted().Text(); !errors.Is(err, ErrEncrypted) {
		t.Fatalf("empty Text error = %v", err)
	}
	if _, err := CanonicalBytes(d); !errors.Is(err, ErrEncrypted) {
		t.Fatalf("CanonicalBytes error = %v", err)
	}
	if _, err := Diff(d, NewEncrypted()); !errors.Is(err, ErrEncrypted) {
		t.Fatalf("Diff error = %v", err)
	}
	if _, err := Diff(New(), d); !errors.Is(err, ErrEncrypted) {
		t.Fatalf("mixed Diff error = %v", err)
	}
	got, err := d.ResolveRange(op.ID, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Detached || got.Text != "" || !got.TextOmitted || got.First != op.ID || got.Last != op.ID {
		t.Fatalf("range = %+v", got)
	}
}

func TestPlanRestoreCopiesCipher(t *testing.T) {
	d := NewEncrypted()
	first := Insert{ID: ID{Client: 1, Clock: 1}, Parent: Root, Cipher: testCipher(1, 17, 7)}
	second := Insert{ID: ID{Client: 1, Clock: 2}, Parent: first.ID, Cipher: testCipher(4, 20, 8)}
	if _, err := d.Apply(first); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Apply(second); err != nil {
		t.Fatal(err)
	}
	target, err := FromSnapshot(d.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Apply(Delete{ID: ID{Client: 1, Clock: 3}, Target: second.ID}); err != nil {
		t.Fatal(err)
	}
	ops, err := PlanRestore(d, target, 2)
	if err != nil {
		t.Fatal(err)
	}
	var copy Insert
	for _, op := range ops {
		ins, ok := op.(Insert)
		if !ok || !ins.Copy {
			continue
		}
		copy = ins
	}
	if !copy.Copy || copy.Parent != second.ID || copy.Value != 0 || !sameCipher(copy.Cipher, second.Cipher) {
		t.Fatalf("restore copy = %+v", copy)
	}
	if _, err := d.Apply(copy); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotCarriesEncryptedMode(t *testing.T) {
	d := NewEncrypted()
	if _, err := d.Apply(Insert{ID: ID{Client: 1, Clock: 1}, Parent: Root, Cipher: testCipher(1, 17, 9)}); err != nil {
		t.Fatal(err)
	}
	snap := d.Snapshot()
	if !snap.GetEncrypted() || len(snap.GetNodes()[0].GetCipher()) != 17 {
		t.Fatalf("snapshot = %+v", snap)
	}
	back, err := FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	if back.Mode() != Encrypted {
		t.Fatal("mode dropped")
	}
	plain := &crdtv1.Snapshot{Encrypted: false, Nodes: []*crdtv1.Node{{
		Id: &crdtv1.OpID{Client: 1, Clock: 1}, Parent: &crdtv1.OpID{},
		Cipher: bytes.Repeat([]byte{1}, 17), Epoch: 1,
	}}, Vector: []*crdtv1.VectorEntry{{Client: 1, Clock: 1}}}
	if _, err := FromSnapshot(plain); !errors.Is(err, ErrInvalidOp) {
		t.Fatalf("plain snapshot with cipher: %v", err)
	}
}
