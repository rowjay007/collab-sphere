package comments

import (
	"context"

	"github.com/google/uuid"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

type Docs interface {
	LoadState(ctx context.Context, doc uuid.UUID) (*crdt.Doc, uint64, error)
}

type Versions interface {
	Frontier(ctx context.Context, doc uuid.UUID) (map[uuid.UUID]uint64, error)
}
