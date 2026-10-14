package docstore

import (
	"context"
	"fmt"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/hub"
)

// Store loads and appends document state through DocumentService.
type Store struct {
	client documentv1.DocumentServiceClient
}

// New wraps a document-service gRPC client as a hub.Store.
func New(client documentv1.DocumentServiceClient) *Store {
	return &Store{client: client}
}

// Load restores a document from the latest snapshot plus subsequent ops.
func (s *Store) Load(ctx context.Context, docID string) (*crdt.Doc, error) {
	resp, err := s.client.LoadState(ctx, &documentv1.LoadStateRequest{Id: docID})
	if err != nil {
		return nil, err
	}
	if resp.GetDocument().GetStatus() == documentv1.Document_STATUS_ARCHIVED {
		return nil, hub.ErrArchived
	}
	var doc *crdt.Doc
	if resp.GetSnapshot() == nil {
		doc = crdt.New()
	} else {
		doc, err = crdt.FromSnapshot(resp.GetSnapshot())
		if err != nil {
			return nil, fmt.Errorf("decode snapshot: %w", err)
		}
	}
	for i, pb := range resp.GetOperations() {
		op, err := crdt.OpFromProto(pb)
		if err != nil {
			return nil, fmt.Errorf("decode operation %d: %w", i, err)
		}
		if _, err := doc.Apply(op); err != nil {
			return nil, fmt.Errorf("apply operation %d: %w", i, err)
		}
	}
	return doc, nil
}

// Append persists ops through AppendOperations.
func (s *Store) Append(ctx context.Context, docID string, ops []crdt.Op) error {
	protoOps := make([]*crdtv1.Operation, len(ops))
	for i, op := range ops {
		protoOps[i] = crdt.OpToProto(op)
	}
	_, err := s.client.AppendOperations(ctx, &documentv1.AppendOperationsRequest{
		Id:         docID,
		Operations: protoOps,
	})
	return err
}
