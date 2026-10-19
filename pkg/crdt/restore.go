package crdt

import (
	"fmt"
	"math"
)

// PlanRestore returns ops authored by r that make head's visible text equal
// target's. head must contain target. Each visible character c of target
// keeps one representative in head: c itself if visible, else the first
// visible node of c's copy lineage that comes before every other visible
// target node in c's subtree. A character with no representative gets a Copy
// insert with Parent c and a clock above head.Clock(), which makes it c's
// first child, so it sits right after c in target's order. Every other
// visible node of head is deleted. Planning against the result is empty.
func PlanRestore(head, target *Doc, r ClientID) ([]Op, error) {
	if head == nil || target == nil {
		return nil, fmt.Errorf("%w: nil document", ErrInvalidOp)
	}
	if r == 0 {
		return nil, fmt.Errorf("%w: zero client", ErrInvalidOp)
	}
	if !head.vector.CoversAll(target.vector) {
		return nil, fmt.Errorf("%w: head does not contain target", ErrInvalidOp)
	}
	clock := head.clock
	next := func() (ID, error) {
		if clock >= math.MaxUint64-1 {
			return ID{}, fmt.Errorf("%w: restore clock is exhausted", ErrInvalidOp)
		}
		clock++
		return ID{Clock: clock, Client: r}, nil
	}

	var ops []Op
	kept := make(map[ID]bool)
	for _, c := range target.nodeIDsDFS() {
		if !target.visibleNode(c) {
			continue
		}
		if !head.Has(c) {
			return nil, fmt.Errorf("%w: head lacks target character %s", ErrInvalidOp, c)
		}
		if rep, ok := head.representative(c, target); ok {
			kept[rep] = true
			continue
		}
		id, err := next()
		if err != nil {
			return nil, err
		}
		ops = append(ops, Insert{ID: id, Parent: c, Value: target.nodes[c].value, Copy: true})
	}
	for _, id := range head.nodeIDsDFS() {
		if !head.visibleNode(id) || kept[id] {
			continue
		}
		deleteID, err := next()
		if err != nil {
			return nil, err
		}
		ops = append(ops, Delete{ID: deleteID, Target: id})
	}
	return ops, nil
}

// representative walks c's subtree in document order up to the first node
// visible in target. Stopping there keeps the chosen node in c's slot of
// target's order, and gives each candidate at most one claimant.
func (d *Doc) representative(c ID, target *Doc) (ID, bool) {
	if d.visibleNode(c) {
		return c, true
	}
	lineage := map[ID]bool{c: true}
	stack := make([]ID, 0)
	pushReverse(&stack, d.nodes[c].children)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if target.visibleNode(id) {
			return Root, false
		}
		n := d.nodes[id]
		if n.copy && lineage[n.parent] {
			lineage[id] = true
			if len(n.deletedBy) == 0 {
				return id, true
			}
		}
		pushReverse(&stack, n.children)
	}
	return Root, false
}
