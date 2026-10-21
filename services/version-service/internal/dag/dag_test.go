package dag

import (
	"bytes"
	"testing"

	"github.com/google/uuid"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

const (
	emptyHash = "bb321de70311ee002c0ebfb4ee481aeb9c593d692de17082c898890f192d0f60"
	genesisID = "468d554dbb6dbb2f9ea76683868c6037b5c0706ab9c8bafc7eac54dece9c5707"
	autoID    = "d095aa9edf2478da77f6f0154356c35a52b69bcf729522a1a456ae2c60375cf4"
	mergeID   = "6317677dcb9cdeeaf91117c0af78da5852045438f61a615f9767d112adbf241c"
)

func TestEmptyContentHash(t *testing.T) {
	canonical, err := crdt.CanonicalBytes(crdt.New())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(canonical, []byte("CSS1\x00\x00\x00")) {
		t.Fatalf("empty canonical = %x", canonical)
	}
	if got := ContentHash(canonical).String(); got != emptyHash {
		t.Fatalf("empty content hash = %s, want %s", got, emptyHash)
	}
}

// The expected IDs were computed outside Go from the §2 byte layout:
// "CSV1" | doc[16] | uvarint(seq) | content[32] | uvarint(n) | parents.
func TestNodeIDGolden(t *testing.T) {
	docA := uuid.MustParse("6f1c2a3b-4d5e-4f60-8172-839405a6b7c8")
	docB := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	empty := ContentHash([]byte("CSS1\x00\x00\x00"))

	genesis := Node{Doc: docA, Seq: 0, Content: empty}
	auto := Node{Doc: docA, Seq: 300, Content: ContentHash([]byte("x")), Parents: []NodeID{genesis.ID()}}
	merge := Node{Doc: docB, Seq: 1 << 35, Content: empty, Parents: []NodeID{auto.ID(), genesis.ID()}}

	for name, tc := range map[string]struct {
		node Node
		want string
	}{
		"genesis":         {genesis, genesisID},
		"auto at 300":     {auto, autoID},
		"two-parent 2^35": {merge, mergeID},
	} {
		if got := tc.node.ID().String(); got != tc.want {
			t.Errorf("%s: id %s, want %s", name, got, tc.want)
		}
	}
}

func TestNodeIDCoversEveryField(t *testing.T) {
	base := Node{Doc: uuid.New(), Seq: 100, Content: ContentHash([]byte("a")), Parents: []NodeID{{1}}}
	variants := map[string]Node{
		"doc":          {Doc: uuid.New(), Seq: base.Seq, Content: base.Content, Parents: base.Parents},
		"seq":          {Doc: base.Doc, Seq: 200, Content: base.Content, Parents: base.Parents},
		"content":      {Doc: base.Doc, Seq: base.Seq, Content: ContentHash([]byte("b")), Parents: base.Parents},
		"parent":       {Doc: base.Doc, Seq: base.Seq, Content: base.Content, Parents: []NodeID{{2}}},
		"no parents":   {Doc: base.Doc, Seq: base.Seq, Content: base.Content},
		"parent order": {Doc: base.Doc, Seq: base.Seq, Content: base.Content, Parents: []NodeID{{1}, {2}}},
	}
	swapped := Node{Doc: base.Doc, Seq: base.Seq, Content: base.Content, Parents: []NodeID{{2}, {1}}}
	if swapped.ID() == variants["parent order"].ID() {
		t.Error("parent order does not change the id")
	}
	for name, v := range variants {
		if v.ID() == base.ID() {
			t.Errorf("changing %s does not change the id", name)
		}
	}
}
