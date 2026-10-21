package docs

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/comment-service/internal/comments"
)

type Client struct {
	rpc documentv1.DocumentServiceClient
}

func New(rpc documentv1.DocumentServiceClient) *Client {
	return &Client{rpc: rpc}
}

func (c *Client) LoadState(ctx context.Context, docID uuid.UUID) (*crdt.Doc, uint64, error) {
	resp, err := c.rpc.LoadState(ctx, &documentv1.LoadStateRequest{Id: docID.String()})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, 0, comments.ErrNotFound
		}
		return nil, 0, err
	}
	var doc *crdt.Doc
	if resp.GetSnapshot() == nil {
		doc = crdt.New()
	} else {
		doc, err = crdt.FromSnapshot(resp.GetSnapshot())
		if err != nil {
			return nil, 0, fmt.Errorf("decode snapshot: %w", err)
		}
	}
	for i, pop := range resp.GetOperations() {
		op, err := crdt.OpFromProto(pop)
		if err != nil {
			return nil, 0, fmt.Errorf("decode operation %d: %w", i, err)
		}
		if _, err := doc.Apply(op); err != nil {
			return nil, 0, fmt.Errorf("apply operation %d: %w", i, err)
		}
	}
	return doc, resp.GetDocument().GetOpSeq(), nil
}
