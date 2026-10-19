// Package oracle holds test oracles that share no logic with pkg/crdt: a
// canonical snapshot encoder written from ADR 0010's byte spec, a minimal RGA
// replay, and brute-force anchor range and diff oracles. It uses pkg/crdt
// only for its value types.
package oracle

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

// Node is one RGA element. DeletedBy is ascending by (clock, client).
type Node struct {
	ID, Parent crdt.ID
	Value      rune
	Copy       bool
	DeletedBy  []crdt.ID
}

// Entry is one version vector entry.
type Entry struct {
	Client crdt.ClientID
	Clock  uint64
}

// State is a document as plain data. Nodes are in document order; Vector and
// Collected are ascending by client.
type State struct {
	Nodes     []Node
	Vector    []Entry
	Collected []Entry
}

func idLess(a, b crdt.ID) bool {
	return a.Clock < b.Clock || a.Clock == b.Clock && a.Client < b.Client
}

func compareID(a, b crdt.ID) int {
	switch {
	case idLess(a, b):
		return -1
	case a == b:
		return 0
	default:
		return 1
	}
}

// Replay builds the state of an op set with a from-scratch RGA. Ops may come
// in any order; an op whose dependency never arrives is an error.
func Replay(ops []crdt.Op) (State, error) {
	inserts := make(map[crdt.ID]crdt.Insert)
	deletes := make(map[crdt.ID][]crdt.ID)
	vector := make(map[crdt.ClientID]uint64)
	for _, op := range ops {
		id := op.OpID()
		vector[id.Client] = max(vector[id.Client], id.Clock)
		switch op := op.(type) {
		case crdt.Insert:
			inserts[op.ID] = op
		case crdt.Delete:
			if !slices.Contains(deletes[op.Target], op.ID) {
				deletes[op.Target] = append(deletes[op.Target], op.ID)
			}
		default:
			return State{}, fmt.Errorf("oracle: unsupported op %T", op)
		}
	}
	children := make(map[crdt.ID][]crdt.ID)
	for id, ins := range inserts {
		children[ins.Parent] = append(children[ins.Parent], id)
	}
	for parent := range children {
		slices.SortFunc(children[parent], func(a, b crdt.ID) int { return compareID(b, a) })
	}

	var s State
	stack := slices.Clone(children[crdt.Root])
	slices.Reverse(stack)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		ins := inserts[id]
		dels := slices.Clone(deletes[id])
		slices.SortFunc(dels, compareID)
		delete(deletes, id)
		s.Nodes = append(s.Nodes, Node{ID: id, Parent: ins.Parent, Value: ins.Value, Copy: ins.Copy, DeletedBy: dels})
		kids := slices.Clone(children[id])
		slices.Reverse(kids)
		stack = append(stack, kids...)
	}
	if len(s.Nodes) != len(inserts) || len(deletes) != 0 {
		return State{}, fmt.Errorf("oracle: %d ops have a missing dependency", len(inserts)-len(s.Nodes)+len(deletes))
	}
	s.Vector = sortedEntries(vector)
	return s, nil
}

// FromSnapshot reads a state from snapshot proto fields, for documents whose
// ops were collected and cannot be replayed.
func FromSnapshot(pb *crdtv1.Snapshot) State {
	var s State
	for _, n := range pb.GetNodes() {
		node := Node{ID: fromProto(n.GetId()), Parent: fromProto(n.GetParent()), Value: rune(n.GetValue()), Copy: n.GetCopy()}
		for _, d := range n.GetDeletedBy() {
			node.DeletedBy = append(node.DeletedBy, fromProto(d))
		}
		s.Nodes = append(s.Nodes, node)
	}
	for _, e := range pb.GetVector() {
		s.Vector = append(s.Vector, Entry{Client: crdt.ClientID(e.GetClient()), Clock: e.GetClock()})
	}
	for _, e := range pb.GetCollected() {
		s.Collected = append(s.Collected, Entry{Client: crdt.ClientID(e.GetClient()), Clock: e.GetClock()})
	}
	return s
}

func fromProto(id *crdtv1.OpID) crdt.ID {
	return crdt.ID{Client: crdt.ClientID(id.GetClient()), Clock: id.GetClock()}
}

func sortedEntries(m map[crdt.ClientID]uint64) []Entry {
	var out []Entry
	for client, clock := range m {
		if client != 0 && clock != 0 {
			out = append(out, Entry{Client: client, Clock: clock})
		}
	}
	slices.SortFunc(out, func(a, b Entry) int { return cmp.Compare(a.Client, b.Client) })
	return out
}

// Ops returns the state's inserts in document order, then its deletes.
func (s State) Ops() []crdt.Op {
	var ops, dels []crdt.Op
	for _, n := range s.Nodes {
		ops = append(ops, crdt.Insert{ID: n.ID, Parent: n.Parent, Value: n.Value, Copy: n.Copy})
		for _, d := range n.DeletedBy {
			dels = append(dels, crdt.Delete{ID: d, Target: n.ID})
		}
	}
	return append(ops, dels...)
}

func visible(n Node) bool { return len(n.DeletedBy) == 0 }

// Text returns the visible text.
func (s State) Text() string {
	var b strings.Builder
	for _, n := range s.Nodes {
		if visible(n) {
			b.WriteRune(n.Value)
		}
	}
	return b.String()
}
