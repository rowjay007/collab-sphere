package crdt

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

type node struct {
	id        ID
	parent    ID
	value     rune
	children  []ID
	deletedBy []ID
}

// Doc is an RGA document.
type Doc struct {
	nodes          map[ID]*node
	rootChildren   []ID
	inserts        map[ID]Insert
	deletes        map[ID]Delete
	pendingInserts map[ID]Insert
	waitingInserts map[ID][]ID
	pendingDeletes map[ID]Delete
	waitingDeletes map[ID][]ID
	vector         VersionVector
	collected      VersionVector
	clock          uint64
	visible        int
}

// New returns an empty document.
func New() *Doc {
	return &Doc{
		nodes:          make(map[ID]*node),
		inserts:        make(map[ID]Insert),
		deletes:        make(map[ID]Delete),
		pendingInserts: make(map[ID]Insert),
		waitingInserts: make(map[ID][]ID),
		pendingDeletes: make(map[ID]Delete),
		waitingDeletes: make(map[ID][]ID),
		vector:         make(VersionVector),
		collected:      make(VersionVector),
	}
}

// Apply validates and adds an operation.
func (d *Doc) Apply(op Op) (Status, error) {
	if d == nil {
		return 0, fmt.Errorf("%w: nil document", ErrInvalidOp)
	}
	switch op := op.(type) {
	case Insert:
		return d.applyInsert(op)
	case *Insert:
		if op == nil {
			return 0, fmt.Errorf("%w: nil insert", ErrInvalidOp)
		}
		return d.applyInsert(*op)
	case Delete:
		return d.applyDelete(op)
	case *Delete:
		if op == nil {
			return 0, fmt.Errorf("%w: nil delete", ErrInvalidOp)
		}
		return d.applyDelete(*op)
	default:
		return 0, fmt.Errorf("%w: unsupported operation", ErrInvalidOp)
	}
}

func (d *Doc) applyInsert(op Insert) (Status, error) {
	if err := validateInsert(op); err != nil {
		return 0, err
	}
	if existing, ok := d.inserts[op.ID]; ok {
		if existing == op {
			return Duplicate, nil
		}
		return 0, fmt.Errorf("%w: insert ID %s", ErrConflict, op.ID)
	}
	if _, ok := d.deletes[op.ID]; ok {
		return 0, fmt.Errorf("%w: operation ID %s", ErrConflict, op.ID)
	}
	if d.collected.Covers(op.ID) {
		return Duplicate, nil
	}
	if !d.Has(op.Parent) && d.collected.Covers(op.Parent) {
		return 0, fmt.Errorf("%w: parent %s was collected", ErrInvalidOp, op.Parent)
	}

	d.observe(op.ID)
	d.inserts[op.ID] = op
	if !d.Has(op.Parent) {
		d.pendingInserts[op.ID] = op
		d.waitingInserts[op.Parent] = append(d.waitingInserts[op.Parent], op.ID)
		return Pending, nil
	}

	d.integrate(op)
	return Applied, nil
}

func validateInsert(op Insert) error {
	if op.ID.Client == 0 || op.ID.Clock == 0 {
		return fmt.Errorf("%w: zero insert ID", ErrInvalidOp)
	}
	if op.Parent.Client == 0 && op.Parent.Clock != 0 {
		return fmt.Errorf("%w: malformed parent", ErrInvalidOp)
	}
	if op.ID.Clock <= op.Parent.Clock {
		return fmt.Errorf("%w: insert clock does not follow parent", ErrInvalidOp)
	}
	if !utf8.ValidRune(op.Value) {
		return fmt.Errorf("%w: invalid rune", ErrInvalidOp)
	}
	return nil
}

func (d *Doc) integrate(first Insert) {
	queue := []Insert{first}
	for len(queue) > 0 {
		op := queue[0]
		queue = queue[1:]
		delete(d.pendingInserts, op.ID)

		n := &node{id: op.ID, parent: op.Parent, value: op.Value}
		d.nodes[op.ID] = n
		if op.Parent.IsRoot() {
			d.rootChildren = insertIDDescending(d.rootChildren, op.ID)
		} else {
			parent := d.nodes[op.Parent]
			parent.children = insertIDDescending(parent.children, op.ID)
		}

		if deleteIDs := d.waitingDeletes[op.ID]; len(deleteIDs) != 0 {
			sortIDsAscending(deleteIDs)
			n.deletedBy = append(n.deletedBy, deleteIDs...)
			for _, id := range deleteIDs {
				delete(d.pendingDeletes, id)
			}
			delete(d.waitingDeletes, op.ID)
		} else {
			d.visible++
		}

		waiting := d.waitingInserts[op.ID]
		delete(d.waitingInserts, op.ID)
		sortIDsAscending(waiting)
		for _, id := range waiting {
			if child, ok := d.pendingInserts[id]; ok {
				queue = append(queue, child)
			}
		}
	}
}

func (d *Doc) applyDelete(op Delete) (Status, error) {
	if op.ID.Client == 0 || op.ID.Clock == 0 || op.Target.IsRoot() || op.Target.Client == 0 {
		return 0, fmt.Errorf("%w: malformed delete", ErrInvalidOp)
	}
	if existing, ok := d.deletes[op.ID]; ok {
		if existing == op {
			return Duplicate, nil
		}
		return 0, fmt.Errorf("%w: delete ID %s", ErrConflict, op.ID)
	}
	if _, ok := d.inserts[op.ID]; ok {
		return 0, fmt.Errorf("%w: operation ID %s", ErrConflict, op.ID)
	}
	if d.collected.Covers(op.ID) {
		return Duplicate, nil
	}

	d.observe(op.ID)
	d.deletes[op.ID] = op
	if target, ok := d.nodes[op.Target]; ok {
		if len(target.deletedBy) == 0 {
			d.visible--
		}
		target.deletedBy = insertIDAscending(target.deletedBy, op.ID)
		return Applied, nil
	}

	d.pendingDeletes[op.ID] = op
	d.waitingDeletes[op.Target] = append(d.waitingDeletes[op.Target], op.ID)
	return Pending, nil
}

func (d *Doc) observe(id ID) {
	if id.Clock > d.clock {
		d.clock = id.Clock
	}
	if id.Clock > d.vector[id.Client] {
		d.vector[id.Client] = id.Clock
	}
}

// Ready reports whether an operation's dependency is present.
func (d *Doc) Ready(op Op) bool {
	if d == nil {
		return false
	}
	switch op := op.(type) {
	case Insert:
		return d.Has(op.Parent)
	case *Insert:
		return op != nil && d.Has(op.Parent)
	case Delete:
		return d.Has(op.Target)
	case *Delete:
		return op != nil && d.Has(op.Target)
	default:
		return false
	}
}

// Has reports whether id is Root or an integrated node.
func (d *Doc) Has(id ID) bool {
	if id.IsRoot() {
		return true
	}
	if d == nil {
		return false
	}
	_, ok := d.nodes[id]
	return ok
}

// Text returns the visible document text.
func (d *Doc) Text() string {
	if d == nil || d.visible == 0 {
		return ""
	}
	var text strings.Builder
	stack := make([]ID, 0, len(d.nodes))
	pushReverse(&stack, d.rootChildren)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		n := d.nodes[id]
		if len(n.deletedBy) == 0 {
			text.WriteRune(n.value)
		}
		pushReverse(&stack, n.children)
	}
	return text.String()
}

// Len returns the number of visible runes.
func (d *Doc) Len() int {
	if d == nil {
		return 0
	}
	return d.visible
}

// IDAt returns the ID of the index-th visible rune.
func (d *Doc) IDAt(index int) (ID, error) {
	if d == nil || index < 0 || index >= d.visible {
		return Root, ErrIndex
	}
	stack := make([]ID, 0, len(d.nodes))
	pushReverse(&stack, d.rootChildren)
	visible := 0
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		n := d.nodes[id]
		if len(n.deletedBy) == 0 {
			if visible == index {
				return id, nil
			}
			visible++
		}
		pushReverse(&stack, n.children)
	}
	return Root, ErrIndex
}

// Clock returns the greatest operation clock received by the document.
func (d *Doc) Clock() uint64 {
	if d == nil {
		return 0
	}
	return d.clock
}

// Vector returns a copy of the document's version vector.
func (d *Doc) Vector() VersionVector {
	if d == nil {
		return VersionVector{}
	}
	return d.vector.Clone()
}

// Ops returns every operation held by the document in canonical order.
func (d *Doc) Ops() []Op {
	if d == nil {
		return nil
	}
	ops := make([]Op, 0, len(d.inserts)+len(d.deletes))
	for _, id := range d.nodeIDsDFS() {
		ops = append(ops, d.inserts[id])
	}
	pending := make([]Op, 0, len(d.pendingInserts)+len(d.pendingDeletes))
	for _, op := range d.pendingInserts {
		pending = append(pending, op)
	}
	for _, op := range d.pendingDeletes {
		pending = append(pending, op)
	}
	sort.Slice(pending, func(i, j int) bool {
		return pending[i].OpID().Less(pending[j].OpID())
	})
	ops = append(ops, pending...)
	deleteIDs := make([]ID, 0, len(d.deletes)-len(d.pendingDeletes))
	for id := range d.deletes {
		if _, pending := d.pendingDeletes[id]; !pending {
			deleteIDs = append(deleteIDs, id)
		}
	}
	sortIDsAscending(deleteIDs)
	for _, id := range deleteIDs {
		ops = append(ops, d.deletes[id])
	}
	return ops
}

// Merge applies every operation held by other.
func (d *Doc) Merge(other *Doc) error {
	if other == nil {
		return fmt.Errorf("%w: nil document", ErrInvalidOp)
	}
	for _, op := range other.Ops() {
		if _, err := d.Apply(op); err != nil {
			return err
		}
	}
	return nil
}

func (d *Doc) nodeIDsDFS() []ID {
	ids := make([]ID, 0, len(d.nodes))
	stack := make([]ID, 0, len(d.nodes))
	pushReverse(&stack, d.rootChildren)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		ids = append(ids, id)
		pushReverse(&stack, d.nodes[id].children)
	}
	return ids
}

func pushReverse(stack *[]ID, ids []ID) {
	for i := len(ids) - 1; i >= 0; i-- {
		*stack = append(*stack, ids[i])
	}
}

func insertIDDescending(ids []ID, id ID) []ID {
	index := sort.Search(len(ids), func(i int) bool { return ids[i].Less(id) })
	ids = append(ids, ID{})
	copy(ids[index+1:], ids[index:])
	ids[index] = id
	return ids
}

func insertIDAscending(ids []ID, id ID) []ID {
	index := sort.Search(len(ids), func(i int) bool { return !ids[i].Less(id) })
	ids = append(ids, ID{})
	copy(ids[index+1:], ids[index:])
	ids[index] = id
	return ids
}

func sortIDsAscending(ids []ID) {
	sort.Slice(ids, func(i, j int) bool { return ids[i].Less(ids[j]) })
}

// Replica creates local operations for one client.
type Replica struct {
	*Doc
	client ClientID
}

// NewReplica returns a replica for a nonzero client.
func NewReplica(client ClientID, doc *Doc) (*Replica, error) {
	if client == 0 {
		return nil, fmt.Errorf("%w: zero client", ErrInvalidOp)
	}
	if doc == nil {
		doc = New()
	}
	return &Replica{Doc: doc, client: client}, nil
}

// Client returns the replica's client ID.
func (r *Replica) Client() ClientID { return r.client }

// InsertAt inserts one rune at a visible index.
func (r *Replica) InsertAt(index int, value rune) (Insert, error) {
	if r == nil || r.Doc == nil || index < 0 || index > r.Len() {
		return Insert{}, ErrIndex
	}
	parent := Root
	if index > 0 {
		var err error
		parent, err = r.IDAt(index - 1)
		if err != nil {
			return Insert{}, err
		}
	}
	op := Insert{ID: ID{Clock: r.Clock() + 1, Client: r.client}, Parent: parent, Value: value}
	if _, err := r.Apply(op); err != nil {
		return Insert{}, err
	}
	return op, nil
}

// InsertText inserts a chain of runes at a visible index.
func (r *Replica) InsertText(index int, text string) ([]Insert, error) {
	if r == nil || r.Doc == nil || index < 0 || index > r.Len() {
		return nil, ErrIndex
	}
	parent := Root
	if index > 0 {
		var err error
		parent, err = r.IDAt(index - 1)
		if err != nil {
			return nil, err
		}
	}
	ops := make([]Insert, 0, utf8.RuneCountInString(text))
	for _, value := range text {
		op := Insert{ID: ID{Clock: r.Clock() + 1, Client: r.client}, Parent: parent, Value: value}
		if _, err := r.Apply(op); err != nil {
			return ops, err
		}
		ops = append(ops, op)
		parent = op.ID
	}
	return ops, nil
}

// DeleteAt deletes the rune at a visible index.
func (r *Replica) DeleteAt(index int) (Delete, error) {
	if r == nil || r.Doc == nil {
		return Delete{}, ErrIndex
	}
	target, err := r.IDAt(index)
	if err != nil {
		return Delete{}, err
	}
	op := Delete{ID: ID{Clock: r.Clock() + 1, Client: r.client}, Target: target}
	if _, err := r.Apply(op); err != nil {
		return Delete{}, err
	}
	return op, nil
}
