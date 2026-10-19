package crdt

import (
	"fmt"
	"strings"
)

// Range is the resolution of an inclusive anchor. First and Last are the
// first and last visible nodes in it; Detached means there are none.
type Range struct {
	Detached    bool
	First, Last ID
	Text        string
}

// Resolve returns id if it is visible, else the first visible node of its
// copy lineage (a copy of id, or a copy of a copy) in document order.
func (d *Doc) Resolve(id ID) (ID, bool) {
	if d == nil {
		return Root, false
	}
	if _, ok := d.nodes[id]; !ok {
		return Root, false
	}
	stack := []ID{id}
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		n := d.nodes[id]
		if len(n.deletedBy) == 0 {
			return id, true
		}
		for i := len(n.children) - 1; i >= 0; i-- {
			if child := n.children[i]; d.nodes[child].copy {
				stack = append(stack, child)
			}
		}
	}
	return Root, false
}

// ResolveRange resolves the inclusive anchor [start, end]. Each endpoint maps
// through Resolve, falling back to its own tombstone when its lineage has no
// visible node, and the range is every visible node in document order from
// the start position to the end position inclusive. Text inserted strictly
// inside the range joins it. Text typed right before start or right after end
// does not, because it lands before start's position or after end's. The
// range is Detached when no visible node lies in it.
func (d *Doc) ResolveRange(start, end ID) (Range, error) {
	if start.IsRoot() || end.IsRoot() || !d.Has(start) || !d.Has(end) {
		return Range{}, fmt.Errorf("%w: anchor %s..%s is not integrated", ErrInvalidOp, start, end)
	}
	if id, ok := d.Resolve(start); ok {
		start = id
	}
	if id, ok := d.Resolve(end); ok {
		end = id
	}
	var r Range
	var text strings.Builder
	inside := false
	stack := make([]ID, 0, len(d.nodes))
	pushReverse(&stack, d.rootChildren)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		n := d.nodes[id]
		if id == start {
			inside = true
		}
		if inside && len(n.deletedBy) == 0 {
			if text.Len() == 0 {
				r.First = id
			}
			r.Last = id
			text.WriteRune(n.value)
		}
		if id == end {
			break
		}
		pushReverse(&stack, n.children)
	}
	if text.Len() == 0 {
		return Range{Detached: true}, nil
	}
	r.Text = text.String()
	return r, nil
}

func (d *Doc) visibleNode(id ID) bool {
	n, ok := d.nodes[id]
	return ok && len(n.deletedBy) == 0
}
