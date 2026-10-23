package history

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

var (
	ErrNotFound           = errors.New("history: not found")
	ErrAlreadyExists      = errors.New("history: already exists")
	ErrInvalidArgument    = errors.New("history: invalid argument")
	ErrOutOfRange         = errors.New("history: out of range")
	ErrFailedPrecondition = errors.New("history: failed precondition")
)

type Document struct {
	ID       uuid.UUID
	OpSeq    uint64
	ForkOf   *uuid.UUID
	ForkSeq  uint64
	ForkRoot uuid.UUID
}

type AppendResult struct {
	Seqs        []uint64
	FirstNewSeq uint64
}

// Docs is the narrow port onto document-service.
type Docs interface {
	Get(ctx context.Context, doc uuid.UUID) (Document, error)
	// Read returns up to limit ops after seq after; they hold contiguous seqs
	// starting at after+1.
	Read(ctx context.Context, doc uuid.UUID, after uint64, limit int) ([]crdt.Op, error)
	Fork(ctx context.Context, src uuid.UUID, seq uint64, title, requestID string) (Document, error)
	// ListChanged returns active documents updated at or after since and the
	// updated_at of the last one returned.
	ListChanged(ctx context.Context, since time.Time, limit int) ([]uuid.UUID, time.Time, error)
	LoadState(ctx context.Context, doc uuid.UUID) (*crdt.Doc, uint64, error)
	Append(ctx context.Context, doc uuid.UUID, ops []crdt.Op) (AppendResult, error)
	ClaimReplica(ctx context.Context, doc uuid.UUID, client uint64) (crdt.ClientID, error)
}
