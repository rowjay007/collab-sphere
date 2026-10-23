package versiongrpc

import (
	"context"
	"errors"
	"log/slog"
	"strconv"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/dag"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/history"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/store"
)

const (
	defaultPageSize = 100
	maxPageSize     = 1000
)

type Server struct {
	versionv1.UnimplementedVersionServiceServer
	history *history.History
}

func New(h *history.History) *Server {
	return &Server{history: h}
}

func (s *Server) ListVersions(ctx context.Context, req *versionv1.ListVersionsRequest) (*versionv1.ListVersionsResponse, error) {
	doc, err := parseDoc(req.GetDocId())
	if err != nil {
		return nil, err
	}
	size := int(req.GetPageSize())
	if size == 0 {
		size = defaultPageSize
	}
	if size > maxPageSize {
		return nil, status.Errorf(codes.InvalidArgument, "page_size above %d", maxPageSize)
	}
	after := int64(-1)
	if tok := req.GetPageToken(); tok != "" {
		if after, err = strconv.ParseInt(tok, 10, 64); err != nil || after < 0 {
			return nil, status.Error(codes.InvalidArgument, "invalid page_token")
		}
	}
	versions, err := s.history.Versions(ctx, doc, after, size)
	if err != nil {
		return nil, mapError(err)
	}
	resp := &versionv1.ListVersionsResponse{Versions: make([]*versionv1.Version, len(versions))}
	for i, v := range versions {
		resp.Versions[i] = toProto(v)
	}
	if len(versions) == size {
		resp.NextPageToken = strconv.FormatUint(versions[len(versions)-1].Node.Seq, 10)
	}
	return resp, nil
}

func (s *Server) GetVersion(ctx context.Context, req *versionv1.GetVersionRequest) (*versionv1.Version, error) {
	id, err := parseNodeID(req.GetId())
	if err != nil {
		return nil, err
	}
	v, err := s.history.Version(ctx, id)
	if err != nil {
		return nil, mapError(err)
	}
	return toProto(v), nil
}

func (s *Server) GetBlob(ctx context.Context, req *versionv1.GetBlobRequest) (*versionv1.GetBlobResponse, error) {
	if len(req.GetHash()) != len(dag.Hash{}) {
		return nil, status.Error(codes.InvalidArgument, "hash must be 32 bytes")
	}
	b, err := s.history.Blob(ctx, dag.Hash(req.GetHash()))
	if err != nil {
		return nil, mapError(err)
	}
	return &versionv1.GetBlobResponse{Bytes: b}, nil
}

func (s *Server) Checkpoint(ctx context.Context, req *versionv1.CheckpointRequest) (*versionv1.Version, error) {
	doc, err := parseDoc(req.GetDocId())
	if err != nil {
		return nil, err
	}
	v, err := s.history.Checkpoint(ctx, doc, req.GetName(), req.GetRequestId())
	if err != nil {
		return nil, mapError(err)
	}
	return toProto(v), nil
}

func (s *Server) Branch(ctx context.Context, req *versionv1.BranchRequest) (*versionv1.BranchResponse, error) {
	id, err := parseNodeID(req.GetVersionId())
	if err != nil {
		return nil, err
	}
	fork, v, err := s.history.Branch(ctx, id, req.GetTitle(), req.GetRequestId())
	if err != nil {
		return nil, mapError(err)
	}
	return &versionv1.BranchResponse{DocumentId: fork.String(), Version: toProto(v)}, nil
}

func (s *Server) Diff(ctx context.Context, req *versionv1.DiffRequest) (*versionv1.DiffResponse, error) {
	a, err := parseNodeID(req.GetFromVersionId())
	if err != nil {
		return nil, err
	}
	b, err := parseNodeID(req.GetToVersionId())
	if err != nil {
		return nil, err
	}
	spans, err := s.history.Diff(ctx, a, b)
	if err != nil {
		return nil, mapError(err)
	}
	resp := &versionv1.DiffResponse{Spans: make([]*versionv1.Span, len(spans))}
	for i, sp := range spans {
		resp.Spans[i] = &versionv1.Span{Kind: spanKinds[sp.Kind], Text: sp.Text, First: opID(sp.First), Last: opID(sp.Last)}
	}
	return resp, nil
}

func (s *Server) Merge(ctx context.Context, req *versionv1.MergeRequest) (*versionv1.MergeResponse, error) {
	src, err := parseDoc(req.GetSourceDocId())
	if err != nil {
		return nil, err
	}
	dst, err := parseDoc(req.GetTargetDocId())
	if err != nil {
		return nil, err
	}
	r, err := s.history.Merge(ctx, src, dst, req.GetRequestId())
	if err != nil {
		return nil, mapError(err)
	}
	return &versionv1.MergeResponse{Version: toProto(r.Version), Appended: uint32(r.Appended), Noop: r.Noop}, nil
}

func (s *Server) Restore(ctx context.Context, req *versionv1.RestoreRequest) (*versionv1.Version, error) {
	doc, err := parseDoc(req.GetDocId())
	if err != nil {
		return nil, err
	}
	id, err := parseNodeID(req.GetVersionId())
	if err != nil {
		return nil, err
	}
	v, err := s.history.Restore(ctx, doc, id, req.GetRequestId())
	if err != nil {
		return nil, mapError(err)
	}
	return toProto(v), nil
}

func (s *Server) Frontier(ctx context.Context, req *versionv1.FrontierRequest) (*versionv1.FrontierResponse, error) {
	doc, err := parseDoc(req.GetDocId())
	if err != nil {
		return nil, err
	}
	frontier, err := s.history.Frontier(ctx, doc)
	if err != nil {
		return nil, mapError(err)
	}
	resp := &versionv1.FrontierResponse{}
	for d, seq := range frontier {
		resp.Entries = append(resp.Entries, &versionv1.FrontierEntry{DocumentId: d.String(), Seq: seq})
	}
	return resp, nil
}

var spanKinds = map[crdt.SpanKind]versionv1.Span_Kind{
	crdt.Retained: versionv1.Span_KIND_RETAINED,
	crdt.Inserted: versionv1.Span_KIND_INSERTED,
	crdt.Deleted:  versionv1.Span_KIND_DELETED,
}

func opID(id crdt.ID) *crdtv1.OpID {
	return &crdtv1.OpID{Client: uint64(id.Client), Clock: id.Clock}
}

func toProto(v store.Version) *versionv1.Version {
	out := &versionv1.Version{
		Id:           v.ID[:],
		DocId:        v.Node.Doc.String(),
		Seq:          v.Node.Seq,
		ContentHash:  v.Node.Content[:],
		Labels:       v.Meta.Labels,
		RestoredFrom: v.Meta.RestoredFrom,
		CreatedAt:    timestamppb.New(v.CreatedAt),
	}
	for _, p := range v.Node.Parents {
		out.Parents = append(out.Parents, p[:])
	}
	return out
}

func parseDoc(raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.UUID{}, status.Error(codes.InvalidArgument, "invalid document id")
	}
	return id, nil
}

func parseNodeID(raw []byte) (dag.NodeID, error) {
	if len(raw) != len(dag.NodeID{}) {
		return dag.NodeID{}, status.Error(codes.InvalidArgument, "version id must be 32 bytes")
	}
	return dag.NodeID(raw), nil
}

func mapError(err error) error {
	for sentinel, code := range map[error]codes.Code{
		history.ErrNotFound:           codes.NotFound,
		history.ErrAlreadyExists:      codes.AlreadyExists,
		history.ErrInvalidArgument:    codes.InvalidArgument,
		history.ErrOutOfRange:         codes.OutOfRange,
		history.ErrFailedPrecondition: codes.FailedPrecondition,
	} {
		if errors.Is(err, sentinel) {
			return status.Error(code, err.Error())
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	slog.Error("internal error", "err", err)
	return status.Error(codes.Internal, "internal error")
}
