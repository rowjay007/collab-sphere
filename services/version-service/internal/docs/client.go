// Package docs adapts document-service's gRPC API to history.Docs.
package docs

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/history"
)

type Client struct {
	rpc documentv1.DocumentServiceClient
}

func New(rpc documentv1.DocumentServiceClient) *Client {
	return &Client{rpc: rpc}
}

func (c *Client) Get(ctx context.Context, doc uuid.UUID) (history.Document, error) {
	resp, err := c.rpc.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: doc.String()})
	if err != nil {
		return history.Document{}, mapError(err)
	}
	return fromProto(resp)
}

func (c *Client) Read(ctx context.Context, doc uuid.UUID, after uint64, limit int) ([]crdt.Op, error) {
	resp, err := c.rpc.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: doc.String(), AfterSeq: after, Limit: uint32(limit)})
	if err != nil {
		return nil, mapError(err)
	}
	if len(resp.GetOperations()) > 0 && resp.GetFirstSeq() != after+1 {
		return nil, fmt.Errorf("read %s after %d starts at %d", doc, after, resp.GetFirstSeq())
	}
	ops := make([]crdt.Op, len(resp.GetOperations()))
	for i, p := range resp.GetOperations() {
		if ops[i], err = crdt.OpFromProto(p); err != nil {
			return nil, fmt.Errorf("decode %s@%d: %w", doc, after+uint64(i)+1, err)
		}
	}
	return ops, nil
}

func (c *Client) Fork(ctx context.Context, src uuid.UUID, seq uint64, title, requestID string) (history.Document, error) {
	resp, err := c.rpc.ForkDocument(ctx, &documentv1.ForkDocumentRequest{SourceId: src.String(), Seq: seq, Title: title, RequestId: requestID})
	if err != nil {
		return history.Document{}, mapError(err)
	}
	return fromProto(resp)
}

func (c *Client) ListChanged(ctx context.Context, since time.Time, limit int) ([]uuid.UUID, time.Time, error) {
	req := &documentv1.ListChangedRequest{Limit: uint32(limit)}
	if !since.IsZero() {
		req.Since = timestamppb.New(since)
	}
	resp, err := c.rpc.ListChangedDocuments(ctx, req)
	if err != nil {
		return nil, time.Time{}, mapError(err)
	}
	ids := make([]uuid.UUID, len(resp.GetDocuments()))
	for i, d := range resp.GetDocuments() {
		if ids[i], err = uuid.Parse(d.GetId()); err != nil {
			return nil, time.Time{}, fmt.Errorf("document id %q: %w", d.GetId(), err)
		}
	}
	return ids, resp.GetNextSince().AsTime(), nil
}

func fromProto(d *documentv1.Document) (history.Document, error) {
	id, err := uuid.Parse(d.GetId())
	if err != nil {
		return history.Document{}, fmt.Errorf("document id %q: %w", d.GetId(), err)
	}
	out := history.Document{ID: id, OpSeq: d.GetOpSeq(), ForkSeq: d.GetForkSeq()}
	if d.GetForkOf() != "" {
		src, err := uuid.Parse(d.GetForkOf())
		if err != nil {
			return history.Document{}, fmt.Errorf("fork_of %q: %w", d.GetForkOf(), err)
		}
		out.ForkOf = &src
	}
	return out, nil
}

func mapError(err error) error {
	sentinel := map[codes.Code]error{
		codes.NotFound:        history.ErrNotFound,
		codes.AlreadyExists:   history.ErrAlreadyExists,
		codes.InvalidArgument: history.ErrInvalidArgument,
		codes.OutOfRange:      history.ErrOutOfRange,
	}[status.Code(err)]
	if sentinel == nil {
		return err
	}
	return fmt.Errorf("%w: document-service: %s", sentinel, status.Convert(err).Message())
}
