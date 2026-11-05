package crdt

import "fmt"

// GC removes stable leaf tombstones. Stable must not exceed the pointwise
// minimum of vectors reported by every replica. Each report must follow that
// replica's operations on the same FIFO channel, so all earlier operations
// have arrived before the report.
func (d *Doc) GC(stable VersionVector) (int, error) {
	return d.GCWithPins(stable, nil)
}

// GCWithPins is GC with an exception set. A pinned node is never removed.
// Its ancestors stay because they still have a child.
func (d *Doc) GCWithPins(stable VersionVector, pins map[ID]struct{}) (int, error) {
	if d == nil {
		return 0, fmt.Errorf("%w: nil document", ErrInvalidOp)
	}
	for client, clock := range stable {
		if client == 0 || d.vector[client] < clock {
			return 0, fmt.Errorf("%w: stable vector is not covered", ErrInvalidOp)
		}
	}

	stack := make([]ID, 0)
	queued := make(map[ID]bool)
	for id, n := range d.nodes {
		if gcEligible(n, stable, pins) {
			stack = append(stack, id)
			queued[id] = true
		}
	}

	removed := 0
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		n, ok := d.nodes[id]
		if !ok || !gcEligible(n, stable, pins) {
			continue
		}
		if n.parent.IsRoot() {
			d.rootChildren = removeID(d.rootChildren, id)
		} else {
			parent := d.nodes[n.parent]
			parent.children = removeID(parent.children, id)
		}
		delete(d.nodes, id)
		delete(d.inserts, id)
		for _, deleteID := range n.deletedBy {
			delete(d.deletes, deleteID)
		}
		removed++

		if !n.parent.IsRoot() {
			parent := d.nodes[n.parent]
			if !queued[parent.id] && gcEligible(parent, stable, pins) {
				stack = append(stack, parent.id)
				queued[parent.id] = true
			}
		}
	}

	for client, clock := range stable {
		if clock > d.collected[client] {
			d.collected[client] = clock
		}
	}
	return removed, nil
}

// Stable is the pointwise minimum of doc, reports, and floor. A client
// missing from a report or from floor contributes zero. Callers with no
// restore floor pass doc.
func Stable(doc VersionVector, reports []VersionVector, floor VersionVector) VersionVector {
	out := make(VersionVector)
	for client, clock := range doc {
		if client == 0 || clock == 0 {
			continue
		}
		for _, report := range reports {
			var reported uint64
			if report != nil {
				reported = report[client]
			}
			if reported < clock {
				clock = reported
			}
		}
		if limited := floor[client]; limited < clock {
			clock = limited
		}
		if clock != 0 {
			out[client] = clock
		}
	}
	return out
}

func gcEligible(n *node, stable VersionVector, pins map[ID]struct{}) bool {
	if _, pinned := pins[n.id]; pinned {
		return false
	}
	if len(n.children) != 0 || len(n.deletedBy) == 0 || !stable.Covers(n.id) {
		return false
	}
	for _, deleteID := range n.deletedBy {
		if stable.Covers(deleteID) {
			return true
		}
	}
	return false
}

func removeID(ids []ID, id ID) []ID {
	for i := range ids {
		if ids[i] == id {
			copy(ids[i:], ids[i+1:])
			return ids[:len(ids)-1]
		}
	}
	return ids
}
