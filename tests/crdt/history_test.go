package crdt_test

import (
	"math/rand/v2"
	"testing"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

// history drives one fork tree through random edits, forks, merges, saved
// versions and restores. Every client authors ops in exactly one document,
// as homed replicas do, so each document holds a prefix of every client's
// ops and merges never conflict.
type history struct {
	t        testing.TB
	rng      *rand.Rand
	branches []*branch
	versions []*crdt.Doc
	clients  crdt.ClientID
	restores []restore
}

type branch struct {
	doc            *crdt.Doc
	editor, system crdt.ClientID
}

type restore struct {
	target, result *crdt.Doc
	system         crdt.ClientID
}

func newHistory(t testing.TB, seed uint64) *history {
	h := &history{t: t, rng: rand.New(rand.NewPCG(seed, 3))}
	h.branches = append(h.branches, h.newBranch(crdt.New()))
	return h
}

func (h *history) newBranch(doc *crdt.Doc) *branch {
	return &branch{doc: doc, editor: h.newClient(), system: h.newClient()}
}

func (h *history) newClient() crdt.ClientID {
	h.clients++
	return h.clients
}

func (h *history) run(steps int) *history {
	for range steps {
		b := h.branches[h.rng.IntN(len(h.branches))]
		switch k := h.rng.IntN(20); {
		case k < 11:
			h.edit(b)
		case k < 13:
			h.branches = append(h.branches, h.newBranch(clone(h.t, b.doc)))
		case k < 15:
			other := h.branches[h.rng.IntN(len(h.branches))]
			if err := b.doc.Merge(other.doc); err != nil {
				h.t.Fatalf("merge: %v", err)
			}
		case k < 17:
			h.versions = append(h.versions, clone(h.t, b.doc))
		default:
			h.restore(b)
		}
	}
	return h
}

func (h *history) edit(b *branch) {
	r, err := crdt.NewReplica(b.editor, b.doc)
	if err != nil {
		h.t.Fatal(err)
	}
	for range 1 + h.rng.IntN(6) {
		if n := b.doc.Len(); n > 0 && h.rng.IntN(3) == 0 {
			_, err = r.DeleteAt(h.rng.IntN(n))
		} else {
			_, err = r.InsertAt(h.rng.IntN(n+1), rune('a'+h.rng.IntN(26)))
		}
		if err != nil {
			h.t.Fatal(err)
		}
	}
}

func (h *history) restore(b *branch) {
	var candidates []*crdt.Doc
	for _, v := range h.versions {
		if b.doc.Vector().CoversAll(v.Vector()) {
			candidates = append(candidates, v)
		}
	}
	if len(candidates) == 0 {
		return
	}
	target := candidates[h.rng.IntN(len(candidates))]
	ops, err := crdt.PlanRestore(b.doc, target, b.system)
	if err != nil {
		h.t.Fatalf("plan restore: %v", err)
	}
	applyAll(h.t, b.doc, ops)
	h.restores = append(h.restores, restore{target: target, result: clone(h.t, b.doc), system: b.system})
}

// states returns every live branch and saved version.
func (h *history) states() []*crdt.Doc {
	out := make([]*crdt.Doc, 0, len(h.branches)+len(h.versions))
	for _, b := range h.branches {
		out = append(out, b.doc)
	}
	return append(out, h.versions...)
}

func (h *history) pick() *crdt.Doc {
	states := h.states()
	return states[h.rng.IntN(len(states))]
}

func clone(t testing.TB, d *crdt.Doc) *crdt.Doc {
	t.Helper()
	c, err := crdt.FromSnapshot(d.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func applyAll(t testing.TB, d *crdt.Doc, ops []crdt.Op) {
	t.Helper()
	for _, op := range ops {
		if _, err := d.Apply(op); err != nil {
			t.Fatalf("apply %v: %v", op, err)
		}
	}
}

func countCopies(d *crdt.Doc) (copies, deleted int) {
	for _, n := range d.Snapshot().GetNodes() {
		if n.GetCopy() {
			copies++
		}
		if len(n.GetDeletedBy()) != 0 {
			deleted++
		}
	}
	return copies, deleted
}
