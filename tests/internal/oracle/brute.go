package oracle

import (
	"strings"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func (s State) byID() map[crdt.ID]Node {
	m := make(map[crdt.ID]Node, len(s.Nodes))
	for _, n := range s.Nodes {
		m[n.ID] = n
	}
	return m
}

func copyOf(nodes map[crdt.ID]Node, x, id crdt.ID) bool {
	for {
		if x == id {
			return true
		}
		n := nodes[x]
		if !n.Copy {
			return false
		}
		x = n.Parent
	}
}

func lineageRoot(nodes map[crdt.ID]Node, id crdt.ID) crdt.ID {
	for nodes[id].Copy {
		id = nodes[id].Parent
	}
	return id
}

// ResolveRange recomputes the anchor resolution of design §6 by scanning
// every node: each endpoint is the first visible node in document order whose
// copy chain reaches it, else its own position. It reports false when an
// endpoint is not in s.
func ResolveRange(s State, start, end crdt.ID) (crdt.Range, bool) {
	nodes := s.byID()
	pos := make(map[crdt.ID]int, len(s.Nodes))
	for i, n := range s.Nodes {
		pos[n.ID] = i
	}
	resolve := func(id crdt.ID) (int, bool) {
		at, ok := pos[id]
		if !ok {
			return 0, false
		}
		for i, n := range s.Nodes {
			if visible(n) && copyOf(nodes, n.ID, id) {
				return i, true
			}
		}
		return at, true
	}
	si, okS := resolve(start)
	ei, okE := resolve(end)
	if !okS || !okE {
		return crdt.Range{}, false
	}
	var r crdt.Range
	var text strings.Builder
	for i := si; i <= ei; i++ {
		if n := s.Nodes[i]; visible(n) {
			if text.Len() == 0 {
				r.First = n.ID
			}
			r.Last = n.ID
			text.WriteRune(n.Value)
		}
	}
	if text.Len() == 0 {
		return crdt.Range{Detached: true}, true
	}
	r.Text = text.String()
	return r, true
}

type diffChar struct {
	id, root crdt.ID
	value    rune
}

// Diff recomputes the lineage-aware diff of design §5 by brute force: the
// union is replayed from scratch, and each gap between nodes visible in both
// pairs a-only and b-only characters by an O(mn) dynamic-programming longest
// common subsequence of lineage roots.
func Diff(a, b State) ([]crdt.Span, error) {
	union, err := Replay(append(a.Ops(), b.Ops()...))
	if err != nil {
		return nil, err
	}
	nodes := union.byID()
	visA, visB := visibleSet(a), visibleSet(b)
	var spans []crdt.Span
	emit := func(kind crdt.SpanKind, c diffChar) {
		if last := len(spans) - 1; last >= 0 && spans[last].Kind == kind {
			spans[last].Last = c.id
			spans[last].Text += string(c.value)
			return
		}
		spans = append(spans, crdt.Span{Kind: kind, Text: string(c.value), First: c.id, Last: c.id})
	}
	var onlyA, onlyB []diffChar
	flush := func() {
		pairs := lcs(onlyA, onlyB)
		i, j := 0, 0
		for _, p := range pairs {
			for ; i < p[0]; i++ {
				emit(crdt.Deleted, onlyA[i])
			}
			for ; j < p[1]; j++ {
				emit(crdt.Inserted, onlyB[j])
			}
			emit(crdt.Retained, onlyB[j])
			i, j = i+1, j+1
		}
		for ; i < len(onlyA); i++ {
			emit(crdt.Deleted, onlyA[i])
		}
		for ; j < len(onlyB); j++ {
			emit(crdt.Inserted, onlyB[j])
		}
		onlyA, onlyB = nil, nil
	}
	for _, n := range union.Nodes {
		c := diffChar{id: n.ID, root: lineageRoot(nodes, n.ID), value: n.Value}
		switch inA, inB := visA[n.ID], visB[n.ID]; {
		case inA && inB:
			flush()
			emit(crdt.Retained, c)
		case inA:
			onlyA = append(onlyA, c)
		case inB:
			onlyB = append(onlyB, c)
		}
	}
	flush()
	return spans, nil
}

func visibleSet(s State) map[crdt.ID]bool {
	m := make(map[crdt.ID]bool)
	for _, n := range s.Nodes {
		if visible(n) {
			m[n.ID] = true
		}
	}
	return m
}

func lcs(as, bs []diffChar) [][2]int {
	table := make([][]int, len(as)+1)
	for i := range table {
		table[i] = make([]int, len(bs)+1)
	}
	for i := len(as) - 1; i >= 0; i-- {
		for j := len(bs) - 1; j >= 0; j-- {
			if as[i].root == bs[j].root {
				table[i][j] = table[i+1][j+1] + 1
			} else {
				table[i][j] = max(table[i+1][j], table[i][j+1])
			}
		}
	}
	var pairs [][2]int
	for i, j := 0, 0; i < len(as) && j < len(bs); {
		switch {
		case as[i].root == bs[j].root && table[i][j] == table[i+1][j+1]+1:
			pairs = append(pairs, [2]int{i, j})
			i, j = i+1, j+1
		case table[i+1][j] >= table[i][j+1]:
			i++
		default:
			j++
		}
	}
	return pairs
}
