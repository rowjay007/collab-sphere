package crdt

import "errors"

// Op is a CRDT operation.
type Op interface {
	OpID() ID
	isOp()
}

// Insert adds one rune after Parent. A Copy insert re-creates Parent's
// character under a new ID, so Value equals Parent's value; restore writes
// copies because RGA has no undelete.
type Insert struct {
	ID     ID
	Parent ID
	Value  rune
	Copy   bool
}

// OpID returns the operation ID.
func (op Insert) OpID() ID { return op.ID }
func (Insert) isOp()       {}

// Delete tombstones Target.
type Delete struct {
	ID     ID
	Target ID
}

// OpID returns the operation ID.
func (op Delete) OpID() ID { return op.ID }
func (Delete) isOp()       {}

// Status describes the result of applying an operation.
type Status int

const (
	Applied Status = iota + 1
	Pending
	Duplicate
)

// String returns the status name.
func (s Status) String() string {
	switch s {
	case Applied:
		return "applied"
	case Pending:
		return "pending"
	case Duplicate:
		return "duplicate"
	default:
		return "unknown"
	}
}

var (
	ErrInvalidOp = errors.New("crdt: invalid operation")
	ErrConflict  = errors.New("crdt: conflicting operation")
	ErrIndex     = errors.New("crdt: index out of range")
	ErrPending   = errors.New("crdt: document holds pending operations")
)
