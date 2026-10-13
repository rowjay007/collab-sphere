package domain

import (
	"errors"
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
