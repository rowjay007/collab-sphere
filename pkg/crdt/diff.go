package crdt

import (
	"fmt"
	"sort"
	"strings"
)

// SpanKind classifies a run of characters in a Diff.
type SpanKind uint8

const (
	Retained SpanKind = iota + 1
	Inserted
	Deleted
)

// String returns the kind name.
func (k SpanKind) String() string {
	switch k {
	case Retained:
		return "retained"
	case Inserted:
		return "inserted"
	case Deleted:
		return "deleted"
	default:
		return "unknown"
	}
}

// Span is a maximal run of one kind. First and Last are b's nodes for
// Retained and Inserted spans and a's nodes for Deleted spans.
type Span struct {
	Kind        SpanKind
	Text        string
	First, Last ID
}

type diffChar struct {
	id    ID
	root  ID
	value rune
}

// Diff compares the visible text of a and b, which share one ID space. It
// walks their union once in document order. A node visible in both is
// Retained. A copy counts as its original: between two nodes visible in both,
// characters visible only in a and only in b whose lineage roots match pair
// up as Retained along their longest common subsequence, so Diff(v,
// restore-of-v) has only Retained spans. Retained and Deleted spans spell a's
// text; Retained and Inserted spans spell b's.
func Diff(a, b *Doc) ([]Span, error) {
	if a == nil || b == nil {
		return nil, fmt.Errorf("%w: nil document", ErrInvalidOp)
	}
	union := New()
	if err := union.Merge(a); err != nil {
		return nil, err
	}
	if err := union.Merge(b); err != nil {
		return nil, err
	}
	var w spanWriter
	var onlyA, onlyB []diffChar
	for _, id := range union.nodeIDsDFS() {
		inA, inB := a.visibleNode(id), b.visibleNode(id)
		if !inA && !inB {
			continue
		}
		c := diffChar{id: id, root: union.lineageRoot(id), value: union.nodes[id].value}
		switch {
		case inA && inB:
			w.gap(onlyA, onlyB)
			onlyA, onlyB = onlyA[:0], onlyB[:0]
			w.add(Retained, c)
		case inA:
			onlyA = append(onlyA, c)
		default:
			onlyB = append(onlyB, c)
		}
	}
	w.gap(onlyA, onlyB)
	return w.finish(), nil
}

func (d *Doc) lineageRoot(id ID) ID {
	for n := d.nodes[id]; n.copy; n = d.nodes[id] {
		id = n.parent
	}
	return id
}

type spanWriter struct {
	spans []Span
	text  strings.Builder
}

func (w *spanWriter) add(kind SpanKind, c diffChar) {
	if last := len(w.spans) - 1; last >= 0 && w.spans[last].Kind == kind {
		w.spans[last].Last = c.id
	} else {
		w.flushText()
		w.spans = append(w.spans, Span{Kind: kind, First: c.id, Last: c.id})
	}
	w.text.WriteRune(c.value)
}

func (w *spanWriter) flushText() {
	if last := len(w.spans) - 1; last >= 0 {
		w.spans[last].Text = w.text.String()
	}
	w.text.Reset()
}

func (w *spanWriter) finish() []Span {
	w.flushText()
	return w.spans
}

// gap emits the characters between two nodes visible in both documents.
// Only a sees onlyA and only b sees onlyB, so any interleaving that keeps
// each side's order spells both texts; pairing along the longest common
// subsequence of lineage roots maximizes Retained.
func (w *spanWriter) gap(onlyA, onlyB []diffChar) {
	i, j := 0, 0
	for _, m := range commonRoots(onlyA, onlyB) {
		for ; i < m.a; i++ {
			w.add(Deleted, onlyA[i])
		}
		for ; j < m.b; j++ {
			w.add(Inserted, onlyB[j])
		}
		w.add(Retained, onlyB[j])
		i, j = i+1, j+1
	}
	for ; i < len(onlyA); i++ {
		w.add(Deleted, onlyA[i])
	}
	for ; j < len(onlyB); j++ {
		w.add(Inserted, onlyB[j])
	}
}

type match struct{ a, b int }

// commonRoots returns a longest common subsequence of the two root
// sequences as index pairs, by reduction to a longest increasing
// subsequence (Hunt-Szymanski).
func commonRoots(as, bs []diffChar) []match {
	if len(as) == 0 || len(bs) == 0 {
		return nil
	}
	positions := make(map[ID][]int)
	for j, c := range bs {
		positions[c.root] = append(positions[c.root], j)
	}
	var candidates []match
	for i, c := range as {
		js := positions[c.root]
		for k := len(js) - 1; k >= 0; k-- {
			candidates = append(candidates, match{a: i, b: js[k]})
		}
	}
	tails := make([]int, 0)
	prev := make([]int, len(candidates))
	for p, m := range candidates {
		length := sort.Search(len(tails), func(k int) bool { return candidates[tails[k]].b >= m.b })
		prev[p] = -1
		if length > 0 {
			prev[p] = tails[length-1]
		}
		if length == len(tails) {
			tails = append(tails, p)
		} else {
			tails[length] = p
		}
	}
	if len(tails) == 0 {
		return nil
	}
	out := make([]match, len(tails))
	for k, p := len(out)-1, tails[len(tails)-1]; k >= 0; k-- {
		out[k] = candidates[p]
		p = prev[p]
	}
	return out
}
