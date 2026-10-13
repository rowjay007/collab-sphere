package crdt

import "fmt"

// GC removes stable leaf tombstones. Stable must be a prefix every replica has
// fully received under per-client FIFO delivery.
func (d *Doc) GC(stable VersionVector) (int, error) {
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
		if gcEligible(n, stable) {
			stack = append(stack, id)
			queued[id] = true
		}
	}

	removed := 0
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		n, ok := d.nodes[id]
		if !ok || !gcEligible(n, stable) {
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
			if !queued[parent.id] && gcEligible(parent, stable) {
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

func gcEligible(n *node, stable VersionVector) bool {
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
