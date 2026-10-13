package crdt

import (
	"fmt"
	"sort"
	"unicode/utf8"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
)

// Snapshot returns a canonical protobuf snapshot.
func (d *Doc) Snapshot() *crdtv1.Snapshot {
	snapshot := &crdtv1.Snapshot{}
	if d == nil {
		return snapshot
	}
	for _, id := range d.nodeIDsDFS() {
		n := d.nodes[id]
		pbNode := &crdtv1.Node{
			Id:     idToProto(n.id),
			Parent: idToProto(n.parent),
			Value:  uint32(n.value),
		}
		for _, deleteID := range n.deletedBy {
			pbNode.DeletedBy = append(pbNode.DeletedBy, idToProto(deleteID))
		}
		snapshot.Nodes = append(snapshot.Nodes, pbNode)
	}

	pending := make([]Op, 0, len(d.pendingInserts)+len(d.pendingDeletes))
	for _, op := range d.pendingInserts {
		pending = append(pending, op)
	}
	for _, op := range d.pendingDeletes {
		pending = append(pending, op)
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].OpID().Less(pending[j].OpID()) })
	for _, op := range pending {
		snapshot.Pending = append(snapshot.Pending, OpToProto(op))
	}
	snapshot.Vector = vectorToProto(d.vector)
	snapshot.Collected = vectorToProto(d.collected)
	return snapshot
}

// FromSnapshot validates and restores a protobuf snapshot.
func FromSnapshot(snapshot *crdtv1.Snapshot) (*Doc, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("%w: nil snapshot", ErrInvalidOp)
	}
	vector, err := vectorFromProto(snapshot.Vector)
	if err != nil {
		return nil, err
	}
	collected, err := vectorFromProto(snapshot.Collected)
	if err != nil {
		return nil, err
	}
	for client, clock := range collected {
		if vector[client] < clock {
			return nil, fmt.Errorf("%w: collected vector is not covered", ErrInvalidOp)
		}
	}

	d := New()
	d.vector = vector
	d.collected = collected
	seen := make(map[ID]struct{})
	nodeOrder := make([]ID, 0, len(snapshot.Nodes))
	lastChild := make(map[ID]ID)
	for _, pbNode := range snapshot.Nodes {
		if pbNode == nil || pbNode.Id == nil || pbNode.Parent == nil {
			return nil, fmt.Errorf("%w: incomplete node", ErrInvalidOp)
		}
		id := idFromProto(pbNode.Id)
		parent := idFromProto(pbNode.Parent)
		op := Insert{ID: id, Parent: parent, Value: rune(pbNode.Value)}
		if uint32(op.Value) != pbNode.Value {
			return nil, fmt.Errorf("%w: invalid rune", ErrInvalidOp)
		}
		if err := validateInsert(op); err != nil {
			return nil, err
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("%w: duplicate operation ID %s", ErrInvalidOp, id)
		}
		if !parent.IsRoot() {
			if _, exists := d.nodes[parent]; !exists {
				return nil, fmt.Errorf("%w: parent does not precede child", ErrInvalidOp)
			}
		}
		if previous, ok := lastChild[parent]; ok && !id.Less(previous) {
			return nil, fmt.Errorf("%w: siblings are not descending", ErrInvalidOp)
		}
		lastChild[parent] = id
		seen[id] = struct{}{}
		n := &node{id: id, parent: parent, value: op.Value}
		var previousDelete ID
		for i, pbDeleteID := range pbNode.DeletedBy {
			if pbDeleteID == nil {
				return nil, fmt.Errorf("%w: nil delete ID", ErrInvalidOp)
			}
			deleteID := idFromProto(pbDeleteID)
			if deleteID.Client == 0 || deleteID.Clock == 0 {
				return nil, fmt.Errorf("%w: zero delete ID", ErrInvalidOp)
			}
			if i > 0 && !previousDelete.Less(deleteID) {
				return nil, fmt.Errorf("%w: delete IDs are not ascending", ErrInvalidOp)
			}
			if _, exists := seen[deleteID]; exists {
				return nil, fmt.Errorf("%w: duplicate operation ID %s", ErrInvalidOp, deleteID)
			}
			seen[deleteID] = struct{}{}
			n.deletedBy = append(n.deletedBy, deleteID)
			d.deletes[deleteID] = Delete{ID: deleteID, Target: id}
			previousDelete = deleteID
		}
		d.nodes[id] = n
		d.inserts[id] = op
		if parent.IsRoot() {
			d.rootChildren = append(d.rootChildren, id)
		} else {
			d.nodes[parent].children = append(d.nodes[parent].children, id)
		}
		if len(n.deletedBy) == 0 {
			d.visible++
		}
		nodeOrder = append(nodeOrder, id)
	}
	actualOrder := d.nodeIDsDFS()
	if len(actualOrder) != len(nodeOrder) {
		return nil, fmt.Errorf("%w: malformed node order", ErrInvalidOp)
	}
	for i := range actualOrder {
		if actualOrder[i] != nodeOrder[i] {
			return nil, fmt.Errorf("%w: nodes are not in document order", ErrInvalidOp)
		}
	}

	var previousPending ID
	for i, pbOp := range snapshot.Pending {
		op, err := OpFromProto(pbOp)
		if err != nil {
			return nil, err
		}
		id := op.OpID()
		if i > 0 && !previousPending.Less(id) {
			return nil, fmt.Errorf("%w: pending operations are not ascending", ErrInvalidOp)
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("%w: duplicate operation ID %s", ErrInvalidOp, id)
		}
		seen[id] = struct{}{}
		switch op := op.(type) {
		case Insert:
			if d.Has(op.Parent) || d.collected.Covers(op.Parent) {
				return nil, fmt.Errorf("%w: insert is not pending", ErrInvalidOp)
			}
			d.inserts[op.ID] = op
			d.pendingInserts[op.ID] = op
			d.waitingInserts[op.Parent] = append(d.waitingInserts[op.Parent], op.ID)
		case Delete:
			if d.Has(op.Target) {
				return nil, fmt.Errorf("%w: delete is not pending", ErrInvalidOp)
			}
			d.deletes[op.ID] = op
			d.pendingDeletes[op.ID] = op
			d.waitingDeletes[op.Target] = append(d.waitingDeletes[op.Target], op.ID)
		}
		previousPending = id
	}

	for id := range seen {
		if !d.vector.Covers(id) {
			return nil, fmt.Errorf("%w: vector does not cover operation %s", ErrInvalidOp, id)
		}
	}
	for _, clock := range d.vector {
		if clock > d.clock {
			d.clock = clock
		}
	}
	return d, nil
}

// OpToProto converts an operation to protobuf form.
func OpToProto(op Op) *crdtv1.Operation {
	switch op := op.(type) {
	case Insert:
		return &crdtv1.Operation{
			Id: idToProto(op.ID),
			Kind: &crdtv1.Operation_Insert{Insert: &crdtv1.Insert{
				Parent: idToProto(op.Parent),
				Value:  uint32(op.Value),
			}},
		}
	case *Insert:
		if op == nil {
			return nil
		}
		return OpToProto(*op)
	case Delete:
		return &crdtv1.Operation{
			Id: idToProto(op.ID),
			Kind: &crdtv1.Operation_Delete{Delete: &crdtv1.Delete{
				Target: idToProto(op.Target),
			}},
		}
	case *Delete:
		if op == nil {
			return nil
		}
		return OpToProto(*op)
	default:
		return nil
	}
}

// OpFromProto validates and converts a protobuf operation.
func OpFromProto(pbOp *crdtv1.Operation) (Op, error) {
	if pbOp == nil || pbOp.Id == nil {
		return nil, fmt.Errorf("%w: missing operation ID", ErrInvalidOp)
	}
	id := idFromProto(pbOp.Id)
	if id.Client == 0 || id.Clock == 0 {
		return nil, fmt.Errorf("%w: zero operation ID", ErrInvalidOp)
	}
	switch kind := pbOp.Kind.(type) {
	case *crdtv1.Operation_Insert:
		if kind.Insert == nil || kind.Insert.Parent == nil {
			return nil, fmt.Errorf("%w: missing insert", ErrInvalidOp)
		}
		value := rune(kind.Insert.Value)
		if uint32(value) != kind.Insert.Value || !utf8.ValidRune(value) {
			return nil, fmt.Errorf("%w: invalid rune", ErrInvalidOp)
		}
		op := Insert{ID: id, Parent: idFromProto(kind.Insert.Parent), Value: value}
		if err := validateInsert(op); err != nil {
			return nil, err
		}
		return op, nil
	case *crdtv1.Operation_Delete:
		if kind.Delete == nil || kind.Delete.Target == nil {
			return nil, fmt.Errorf("%w: missing delete", ErrInvalidOp)
		}
		target := idFromProto(kind.Delete.Target)
		if target.IsRoot() || target.Client == 0 {
			return nil, fmt.Errorf("%w: malformed delete target", ErrInvalidOp)
		}
		return Delete{ID: id, Target: target}, nil
	default:
		return nil, fmt.Errorf("%w: missing operation kind", ErrInvalidOp)
	}
}

func idToProto(id ID) *crdtv1.OpID {
	return &crdtv1.OpID{Client: uint64(id.Client), Clock: id.Clock}
}

func idFromProto(id *crdtv1.OpID) ID {
	return ID{Client: ClientID(id.Client), Clock: id.Clock}
}

func vectorToProto(vector VersionVector) []*crdtv1.VectorEntry {
	clients := make([]ClientID, 0, len(vector))
	for client, clock := range vector {
		if client != 0 && clock != 0 {
			clients = append(clients, client)
		}
	}
	sort.Slice(clients, func(i, j int) bool { return clients[i] < clients[j] })
	entries := make([]*crdtv1.VectorEntry, 0, len(clients))
	for _, client := range clients {
		entries = append(entries, &crdtv1.VectorEntry{
			Client: uint64(client),
			Clock:  vector[client],
		})
	}
	return entries
}

func vectorFromProto(entries []*crdtv1.VectorEntry) (VersionVector, error) {
	vector := make(VersionVector, len(entries))
	var previous ClientID
	for i, entry := range entries {
		if entry == nil || entry.Client == 0 || entry.Clock == 0 {
			return nil, fmt.Errorf("%w: malformed vector entry", ErrInvalidOp)
		}
		client := ClientID(entry.Client)
		if i > 0 && client <= previous {
			return nil, fmt.Errorf("%w: vector is not ascending", ErrInvalidOp)
		}
		vector[client] = entry.Clock
		previous = client
	}
	return vector, nil
}
