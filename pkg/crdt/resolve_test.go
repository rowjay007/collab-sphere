package crdt

import "testing"

func newTestReplica(t *testing.T, client ClientID, doc *Doc) *Replica {
	t.Helper()
	r, err := NewReplica(client, doc)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestResolveFollowsCopyLineageInDocumentOrder(t *testing.T) {
	r := newTestReplica(t, 1, nil)
	ins, err := r.InsertText(0, "ab")
	if err != nil {
		t.Fatal(err)
	}
	a := ins[0].ID
	if got, ok := r.Resolve(a); !ok || got != a {
		t.Fatalf("Resolve(visible) = %s, %v", got, ok)
	}
	if _, err := r.Delete(a); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Resolve(a); ok {
		t.Fatal("deleted node without copies resolved")
	}
	first := Insert{ID: ID{Client: 9, Clock: 10}, Parent: a, Value: 'a', Copy: true}
	second := Insert{ID: ID{Client: 9, Clock: 11}, Parent: first.ID, Value: 'a', Copy: true}
	sibling := Insert{ID: ID{Client: 9, Clock: 12}, Parent: a, Value: 'a', Copy: true}
	for _, op := range []Op{first, second, sibling, Delete{ID: ID{Client: 9, Clock: 13}, Target: first.ID}} {
		if _, err := r.Apply(op); err != nil {
			t.Fatal(err)
		}
	}
	if got, ok := r.Resolve(a); !ok || got != sibling.ID {
		t.Fatalf("Resolve = %s, want the first copy in document order %s", got, sibling.ID)
	}
	if _, err := r.Delete(sibling.ID); err != nil {
		t.Fatal(err)
	}
	if got, ok := r.Resolve(a); !ok || got != second.ID {
		t.Fatalf("Resolve = %s, want the copy of a copy %s", got, second.ID)
	}
	if got, ok := r.Resolve(first.ID); !ok || got != second.ID {
		t.Fatalf("Resolve(copy) = %s, want %s", got, second.ID)
	}
	if _, ok := r.Resolve(ID{Client: 77, Clock: 1}); ok {
		t.Fatal("unknown ID resolved")
	}
}

func TestResolveRangeEdges(t *testing.T) {
	r := newTestReplica(t, 1, nil)
	ins, err := r.InsertText(0, "abcde")
	if err != nil {
		t.Fatal(err)
	}
	start, end := ins[1].ID, ins[3].ID
	expect := func(want string) {
		t.Helper()
		got, err := r.ResolveRange(start, end)
		if err != nil {
			t.Fatal(err)
		}
		if got.Detached || got.Text != want {
			t.Fatalf("range = %+v, want %q in %q", got, want, r.Text())
		}
	}
	expect("bcd")
	if _, err := r.InsertAfter(ins[0].ID, 'X'); err != nil {
		t.Fatal(err)
	}
	if _, err := r.InsertAfter(end, 'Y'); err != nil {
		t.Fatal(err)
	}
	expect("bcd")
	if _, err := r.InsertAfter(ins[2].ID, 'Z'); err != nil {
		t.Fatal(err)
	}
	expect("bcZd")
	if _, err := r.Delete(start); err != nil {
		t.Fatal(err)
	}
	expect("cZd")
	for _, id := range []ID{ins[2].ID, ins[3].ID} {
		if _, err := r.Delete(id); err != nil {
			t.Fatal(err)
		}
	}
	expect("Z")
	z, err := r.IDAt(2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Delete(z); err != nil {
		t.Fatal(err)
	}
	if got, err := r.ResolveRange(start, end); err != nil || !got.Detached {
		t.Fatalf("range over deleted text = %+v, %v", got, err)
	}
	if _, err := r.ResolveRange(Root, end); err == nil {
		t.Fatal("Root anchor resolved")
	}
}
