package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusActive   Status = "active"
	StatusArchived Status = "archived"
)

const SnapshotEvery uint64 = 100

type Document struct {
	ID          uuid.UUID
	Title       string
	OwnerID     string
	Status      Status
	OpSeq       uint64
	SnapshotSeq uint64
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

var (
	ErrNotFound        = errors.New("document: not found")
	ErrArchived        = errors.New("document: archived")
	ErrInvalidArgument = errors.New("document: invalid argument")
	ErrConflict        = errors.New("document: conflict")
)

// ConflictError names the op whose (client, clock) is already stored, or
// repeated in the batch, with different bytes. It matches ErrConflict.
type ConflictError struct {
	Client, Clock uint64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("document: op (%d, %d) conflicts with the log", e.Client, e.Clock)
}

func (e *ConflictError) Unwrap() error { return ErrConflict }
