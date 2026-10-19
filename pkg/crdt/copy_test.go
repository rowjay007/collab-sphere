package crdt

import (
	"encoding/hex"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestApplyValidatesCopies(t *testing.T) {
	a := ID{Client: 1, Clock: 1}
	for _, tc := range []struct {
		name string
		op   Insert
	}{
		{"copy of root", Insert{ID: ID{Client: 2, Clock: 2}, Parent: Root, Value: 'a', Copy: true}},
		{"value differs", Insert{ID: ID{Client: 2, Clock: 2}, Parent: a, Value: 'b', Copy: true}},
	} {
		d := New()
		if _, err := d.Apply(Insert{ID: a, Parent: Root, Value: 'a'}); err != nil {
			t.Fatal(err)
		}
		if _, err := d.Apply(tc.op); !errors.Is(err, ErrInvalidOp) {
			t.Errorf("%s: Apply error = %v, want ErrInvalidOp", tc.name, err)
		}
		if d.Contains(tc.op.ID) {
			t.Errorf("%s: rejected copy was recorded", tc.name)
		}
	}
	d := New()
	if _, err := d.Apply(Insert{ID: a, Parent: Root, Value: 'a'}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Apply(Insert{ID: ID{Client: 2, Clock: 2}, Parent: a, Value: 'a', Copy: true}); err != nil {
		t.Fatalf("valid copy rejected: %v", err)
	}
}

func TestCopyRoundTripsThroughProto(t *testing.T) {
	op := Insert{ID: ID{Client: 2, Clock: 5}, Parent: ID{Client: 1, Clock: 1}, Value: 'a', Copy: true}
	back, err := OpFromProto(OpToProto(op))
	if err != nil {
		t.Fatal(err)
	}
	if back != op {
		t.Fatalf("round trip = %+v, want %+v", back, op)
	}
	d := New()
	for _, op := range []Op{Insert{ID: op.Parent, Parent: Root, Value: 'a'}, op} {
		if _, err := d.Apply(op); err != nil {
			t.Fatal(err)
		}
	}
	restored, err := FromSnapshot(d.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if !restored.nodes[op.ID].copy {
		t.Fatal("snapshot dropped the copy flag")
	}
}

func TestNonCopyInsertBytesUnchanged(t *testing.T) {
	op := Insert{ID: ID{Client: 1, Clock: 2}, Parent: ID{Client: 1, Clock: 1}, Value: 'a'}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(OpToProto(op))
	if err != nil {
		t.Fatal(err)
	}
	const want = "0a0408011002" + "1208" + "0a0408011001" + "1061"
	if got := hex.EncodeToString(b); got != want {
		t.Fatalf("insert bytes = %s, want %s", got, want)
	}
}

func TestCoversAll(t *testing.T) {
	v := VersionVector{1: 5, 2: 3}
	for _, tc := range []struct {
		o    VersionVector
		want bool
	}{
		{VersionVector{}, true},
		{VersionVector{1: 5, 2: 3}, true},
		{VersionVector{1: 4}, true},
		{VersionVector{1: 6}, false},
		{VersionVector{3: 1}, false},
	} {
		if got := v.CoversAll(tc.o); got != tc.want {
			t.Errorf("CoversAll(%v) = %v, want %v", tc.o, got, tc.want)
		}
	}
}
