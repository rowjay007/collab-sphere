package versions

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/services/comment-service/internal/comments"
)

type FrontierClient struct {
	rpc versionv1.VersionServiceClient
}

func NewFrontierClient(rpc versionv1.VersionServiceClient) *FrontierClient {
	return &FrontierClient{rpc: rpc}
}

func (c *FrontierClient) Frontier(ctx context.Context, doc uuid.UUID) (map[uuid.UUID]uint64, error) {
	resp, err := c.rpc.Frontier(ctx, &versionv1.FrontierRequest{DocId: doc.String()})
	if err != nil {
		return nil, mapError(err)
	}
	out := make(map[uuid.UUID]uint64, len(resp.GetEntries()))
	for _, e := range resp.GetEntries() {
		id, err := uuid.Parse(e.GetDocumentId())
		if err != nil {
			return nil, fmt.Errorf("frontier document_id %q: %w", e.GetDocumentId(), err)
		}
		out[id] = e.GetSeq()
	}
	return out, nil
}

func mapError(err error) error {
	switch status.Code(err) {
	case codes.Unavailable:
		return fmt.Errorf("%w: version-service: %s", comments.ErrUnavailable, status.Convert(err).Message())
	case codes.NotFound:
		return fmt.Errorf("%w: version-service: %s", comments.ErrNotFound, status.Convert(err).Message())
	default:
		return err
	}
}
