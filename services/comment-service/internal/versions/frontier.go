package versions

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/rowjay007/collab-sphere/services/comment-service/internal/comments"
)

// FrontierClient implements comments.Versions until version-service exposes
// version.v1.VersionService/Frontier. It never invents {doc: head}.
type FrontierClient struct {
	addr string
}

func NewFrontierClient(addr string) *FrontierClient {
	return &FrontierClient{addr: addr}
}

func (c *FrontierClient) Frontier(context.Context, uuid.UUID) (map[uuid.UUID]uint64, error) {
	return nil, fmt.Errorf("%w: version-service Frontier at %s is not bound yet", comments.ErrUnavailable, c.addr)
}
