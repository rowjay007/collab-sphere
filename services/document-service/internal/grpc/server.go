package docgrpc

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/store"
)

type Server struct {
	documentv1.UnimplementedDocumentServiceServer
	store *store.Store
}

func New(s *store.Store) *Server {
	return &Server{store: s}
}

func (s *Server) CreateDocument(ctx context.Context, req *documentv1.CreateDocumentRequest) (*documentv1.Document, error) {
	doc, err := s.store.Create(ctx, req.GetTitle(), req.GetOwnerId())
	if err != nil {
		return nil, mapError(err)
	}
	return toProto(doc), nil
}

func (s *Server) GetDocument(ctx context.Context, req *documentv1.GetDocumentRequest) (*documentv1.Document, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	doc, err := s.store.Get(ctx, id)
	if err != nil {
		return nil, mapError(err)
	}
	return toProto(doc), nil
}

func (s *Server) ArchiveDocument(ctx context.Context, req *documentv1.ArchiveDocumentRequest) (*documentv1.Document, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	doc, err := s.store.Archive(ctx, id)
	if err != nil {
		return nil, mapError(err)
	}
	return toProto(doc), nil
}

func (s *Server) LoadState(ctx context.Context, req *documentv1.LoadStateRequest) (*documentv1.LoadStateResponse, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	doc, snap, ops, err := s.store.LoadState(ctx, id)
	if err != nil {
		return nil, mapError(err)
	}
	return &documentv1.LoadStateResponse{
		Document:   toProto(doc),
		Snapshot:   snap,
		Operations: ops,
	}, nil
}

func (s *Server) AppendOperations(ctx context.Context, req *documentv1.AppendOperationsRequest) (*documentv1.AppendOperationsResponse, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	result, err := s.store.Append(ctx, id, req.GetOperations())
	if err != nil {
		return nil, mapError(err)
	}
	return &documentv1.AppendOperationsResponse{
		OpSeq:       result.OpSeq,
		Seqs:        result.Seqs,
		FirstNewSeq: result.FirstNewSeq,
	}, nil
}

func (s *Server) ReadOperations(ctx context.Context, req *documentv1.ReadOperationsRequest) (*documentv1.ReadOperationsResponse, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	firstSeq, ops, opSeq, err := s.store.ReadOperations(ctx, id, req.GetAfterSeq(), int(req.GetLimit()))
	if err != nil {
		return nil, mapError(err)
	}
	return &documentv1.ReadOperationsResponse{
		FirstSeq:   firstSeq,
		Operations: ops,
		OpSeq:      opSeq,
	}, nil
}

func (s *Server) ClaimReplica(ctx context.Context, req *documentv1.ClaimReplicaRequest) (*documentv1.ClaimReplicaResponse, error) {
	id, err := parseID(req.GetDocumentId())
	if err != nil {
		return nil, err
	}
	client, err := s.store.ClaimReplica(ctx, id, req.GetClient(), req.GetSystem())
	if err != nil {
		return nil, mapError(err)
	}
	return &documentv1.ClaimReplicaResponse{Client: client}, nil
}

func (s *Server) ForkDocument(ctx context.Context, req *documentv1.ForkDocumentRequest) (*documentv1.Document, error) {
	id, err := parseID(req.GetSourceId())
	if err != nil {
		return nil, err
	}
	doc, err := s.store.Fork(ctx, id, req.GetSeq(), req.GetTitle(), req.GetRequestId())
	if err != nil {
		return nil, mapError(err)
	}
	return toProto(doc), nil
}

func (s *Server) ListChangedDocuments(ctx context.Context, req *documentv1.ListChangedRequest) (*documentv1.ListChangedResponse, error) {
	var since time.Time
	if ts := req.GetSince(); ts != nil {
		since = ts.AsTime()
	}
	var afterID uuid.UUID
	if raw := req.GetAfterId(); raw != "" {
		id, err := parseID(raw)
		if err != nil {
			return nil, err
		}
		afterID = id
	}
	docs, next, nextAfter, err := s.store.ListChanged(ctx, since, afterID, int(req.GetLimit()))
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*documentv1.Document, len(docs))
	for i, doc := range docs {
		out[i] = toProto(doc)
	}
	resp := &documentv1.ListChangedResponse{
		Documents: out,
		NextSince: timestamppb.New(next),
	}
	if nextAfter != uuid.Nil {
		resp.NextAfterId = nextAfter.String()
	}
	return resp, nil
}

func parseID(raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.UUID{}, status.Error(codes.InvalidArgument, "invalid document id")
	}
	return id, nil
}

// ConflictReason is the ErrorInfo reason on an AlreadyExists append; its
// metadata names the conflicting op's client and clock.
const ConflictReason = "OP_CONFLICT"

const ReplicaHomedElsewhereReason = "REPLICA_HOMED_ELSEWHERE"

func mapError(err error) error {
	var conflict *domain.ConflictError
	switch {
	case errors.As(err, &conflict):
		st, detailErr := status.New(codes.AlreadyExists, err.Error()).WithDetails(&errdetails.ErrorInfo{
			Reason: ConflictReason,
			Domain: "document-service",
			Metadata: map[string]string{
				"client": strconv.FormatUint(conflict.Client, 10),
				"clock":  strconv.FormatUint(conflict.Clock, 10),
			},
		})
		if detailErr != nil {
			return status.Error(codes.Internal, "internal error")
		}
		return st.Err()
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrArchived):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrReplicaHomedElsewhere):
		st, detailErr := status.New(codes.FailedPrecondition, err.Error()).WithDetails(&errdetails.ErrorInfo{
			Reason: ReplicaHomedElsewhereReason,
			Domain: "document-service",
		})
		if detailErr != nil {
			return status.Error(codes.Internal, "internal error")
		}
		return st.Err()
	case errors.Is(err, domain.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrOutOfRange):
		return status.Error(codes.OutOfRange, err.Error())
	case errors.Is(err, domain.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

func toProto(doc domain.Document) *documentv1.Document {
	status := documentv1.Document_STATUS_ACTIVE
	if doc.Status == domain.StatusArchived {
		status = documentv1.Document_STATUS_ARCHIVED
	}
	out := &documentv1.Document{
		Id:          doc.ID.String(),
		Title:       doc.Title,
		OwnerId:     doc.OwnerID,
		Status:      status,
		OpSeq:       doc.OpSeq,
		SnapshotSeq: doc.SnapshotSeq,
		CreatedAt:   timestamppb.New(doc.CreatedAt),
		UpdatedAt:   timestamppb.New(doc.UpdatedAt),
		ForkSeq:     doc.ForkSeq,
		ForkRoot:    doc.ForkRoot.String(),
	}
	if doc.ForkOf != nil {
		out.ForkOf = doc.ForkOf.String()
	}
	return out
}
