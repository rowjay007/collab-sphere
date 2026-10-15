package crdt

import "testing"

func TestContains(t *testing.T) {
	gone := Insert{ID: ID{Clock: 1, Client: 1}, Parent: Root, Value: 'g'}
	goneDelete := Delete{ID: ID{Clock: 2, Client: 1}, Target: gone.ID}
	kept := Insert{ID: ID{Clock: 3, Client: 1}, Parent: Root, Value: 'k'}
	keptDelete := Delete{ID: ID{Clock: 4, Client: 2}, Target: kept.ID}
	orphan := Insert{ID: ID{Clock: 6, Client: 3}, Parent: ID{Clock: 5, Client: 9}, Value: 'o'}
	orphanDelete := Delete{ID: ID{Clock: 7, Client: 3}, Target: ID{Clock: 5, Client: 8}}

	doc := New()
	for _, op := range []Op{gone, goneDelete} {
		if _, err := doc.Apply(op); err != nil {
			t.Fatal(err)
		}
	}
	if removed, err := doc.GC(doc.Vector()); err != nil || removed != 1 {
		t.Fatalf("GC removed=%d err=%v", removed, err)
	}
	for _, op := range []Op{kept, keptDelete, orphan, orphanDelete} {
		if _, err := doc.Apply(op); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name string
		id   ID
		want bool
	}{
		{"integrated insert", kept.ID, true},
		{"applied delete", keptDelete.ID, true},
		{"pending insert", orphan.ID, true},
		{"pending delete", orphanDelete.ID, true},
		{"collected insert", gone.ID, true},
		{"collected delete", goneDelete.ID, true},
		{"missing parent of pending insert", orphan.Parent, false},
		{"unseen op", ID{Clock: 1, Client: 42}, false},
		{"root", Root, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := doc.Contains(tt.id); got != tt.want {
				t.Fatalf("Contains(%s) = %v, want %v", tt.id, got, tt.want)
			}
		})
	}

	var nilDoc *Doc
	if nilDoc.Contains(kept.ID) {
		t.Fatal("nil doc contains an op")
	}
}
