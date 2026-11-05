package crdt

import (
	"bytes"
	"errors"
	"fmt"
)

// Op is a CRDT operation.
type Op interface {
	OpID() ID
	isOp()
}

// Mode is fixed for the life of a document.
type Mode uint8

const (
	Plain Mode = iota
	Encrypted
)

// Ciphertext is one sealed rune. Bytes are AES-256-GCM output with the tag
// and without the nonce, so the length is 17 to 20.
type Ciphertext struct {
	Epoch uint32
	Bytes []byte
}

// Insert adds one character after Parent. A plain insert carries Value and a
// nil Cipher. An encrypted insert carries Cipher and a zero Value. A Copy
// insert re-creates Parent's character under a new ID, so it repeats Parent's
// rune or cipher. Restore writes copies because RGA has no undelete.
type Insert struct {
	ID     ID
	Parent ID
	Value  rune
	Cipher *Ciphertext
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
	ErrEncrypted = errors.New("crdt: document is encrypted")
)

const (
	minCipher = 17
	maxCipher = 20
)

func cloneCipher(c *Ciphertext) *Ciphertext {
	if c == nil {
		return nil
	}
	return &Ciphertext{Epoch: c.Epoch, Bytes: append([]byte(nil), c.Bytes...)}
}

func sameCipher(a, b *Ciphertext) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Epoch == b.Epoch && bytes.Equal(a.Bytes, b.Bytes)
}

func sameInsert(a, b Insert) bool {
	return a.ID == b.ID && a.Parent == b.Parent && a.Value == b.Value && a.Copy == b.Copy && sameCipher(a.Cipher, b.Cipher)
}

func validateInsertShape(op Insert) error {
	if op.Cipher == nil {
		return nil
	}
	if op.Value != 0 {
		return fmt.Errorf("%w: insert carries a rune and a cipher", ErrInvalidOp)
	}
	n := len(op.Cipher.Bytes)
	if n < minCipher || n > maxCipher {
		return fmt.Errorf("%w: cipher length out of range", ErrInvalidOp)
	}
	return nil
}
