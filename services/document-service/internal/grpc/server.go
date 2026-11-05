package docgrpc

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/pkg/metrics"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/store"
)

type permissions interface {
	EnsureOwner(ctx context.Context, docID, userID string, encrypted bool) error
	Require(ctx context.Context, docID, userID string, min permissionv1.Role) error
	CopyGrants(ctx context.Context, src, dst string) error
}

type Server struct {
	documentv1.UnimplementedDocumentServiceServer
	store     *store.Store
	perm      permissions
	appendObs interface{ Observe(float64) }
}

func New(s *store.Store) *Server {
	return &Server{store: s}
}

func (s *Server) UsePermissions(p permissions) { s.perm = p }

func (s *Server) UseAppend(obs interface{ Observe(float64) }) { s.appendObs = obs }

func Methods() map[string]auth.Class {
	return map[string]auth.Class{
		documentv1.DocumentService_CreateDocument_FullMethodName:       auth.User,
		documentv1.DocumentService_GetDocument_FullMethodName:          auth.Either,
		documentv1.DocumentService_ArchiveDocument_FullMethodName:      auth.User,
		documentv1.DocumentService_LoadState_FullMethodName:            auth.Internal,
		documentv1.DocumentService_AppendOperations_FullMethodName:     auth.Internal,
		documentv1.DocumentService_ReadOperations_FullMethodName:       auth.Internal,
		documentv1.DocumentService_ClaimReplica_FullMethodName:         auth.Internal,
		documentv1.DocumentService_ForkDocument_FullMethodName:         auth.Internal,
		documentv1.DocumentService_ListChangedDocuments_FullMethodName: auth.Internal,
		documentv1.DocumentService_SetEpoch_FullMethodName:             auth.Internal,
		documentv1.DocumentService_ReportVector_FullMethodName:         auth.Internal,
		documentv1.DocumentService_ListReports_FullMethodName:          auth.Internal,
		documentv1.DocumentService_ReleaseReplica_FullMethodName:       auth.Internal,
	}
}

func (s *Server) CreateDocument(ctx context.Context, req *documentv1.CreateDocumentRequest) (*documentv1.Document, error) {
	owner := req.GetOwnerId()
	subject := auth.Subject(ctx)
	if subject != "" {
		if owner != "" && owner != subject {
			return nil, status.Error(codes.InvalidArgument, "owner_id does not match token")
		}
		owner = subject
	} else if !auth.IsInternal(ctx) {
		return nil, status.Error(codes.Unauthenticated, "unauthenticated")
	}
	doc, err := s.store.CreateWithEncryption(ctx, req.GetTitle(), owner, req.GetEncrypted())
	if err != nil {
		return nil, mapError(err)
	}
	if auth.IsInternal(ctx) {
		return toProto(doc), nil
	}
	if s.perm == nil {
		_ = s.store.Delete(ctx, doc.ID)
		return nil, status.Error(codes.Unavailable, "permission service unavailable")
	}
	if err := s.perm.EnsureOwner(ctx, doc.ID.String(), owner, req.GetEncrypted()); err != nil {
		_ = s.store.Delete(ctx, doc.ID)
		return nil, err
	}
	return toProto(doc), nil
}

func (s *Server) GetDocument(ctx context.Context, req *documentv1.GetDocumentRequest) (*documentv1.Document, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	if !auth.IsInternal(ctx) {
		if err := s.allow(ctx, id, permissionv1.Role_ROLE_VIEWER); err != nil {
			return nil, err
		}
	}
	doc, err := s.store.Get(ctx, id)
	if err != nil {
		if !auth.IsInternal(ctx) && errors.Is(err, domain.ErrNotFound) {
			return nil, status.Error(codes.PermissionDenied, "permission denied")
		}
		return nil, mapError(err)
	}
	return toProto(doc), nil
}

func (s *Server) ArchiveDocument(ctx context.Context, req *documentv1.ArchiveDocumentRequest) (*documentv1.Document, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.allow(ctx, id, permissionv1.Role_ROLE_OWNER); err != nil {
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
	start := time.Now()
	result, err := s.store.Append(ctx, id, req.GetOperations())
	metrics.ObserveSince(s.appendObs, start)
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
	client, err := s.store.ClaimReplica(ctx, id, req.GetClient(), req.GetSystem(), req.GetUserId())
	if err != nil {
		return nil, mapError(err)
	}
	return &documentv1.ClaimReplicaResponse{Client: client}, nil
}

func (s *Server) ReportVector(ctx context.Context, req *documentv1.ReportVectorRequest) (*documentv1.ReportVectorResponse, error) {
	id, err := parseID(req.GetDocumentId())
	if err != nil {
		return nil, err
	}
	if req.GetReplicaId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid replica id")
	}
	vector, err := vectorFromEntries(req.GetVector())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid vector")
	}
	doc, err := s.store.Get(ctx, id)
	if err != nil {
		return nil, mapError(err)
	}
	if err := s.store.UpsertReport(ctx, doc.ForkRoot, req.GetReplicaId(), vector, req.GetHomeSeq(), time.Now()); err != nil {
		return nil, mapError(err)
	}
	return &documentv1.ReportVectorResponse{}, nil
}

func (s *Server) ListReports(ctx context.Context, req *documentv1.ListReportsRequest) (*documentv1.ListReportsResponse, error) {
	root, err := parseID(req.GetForkRoot())
	if err != nil {
		return nil, err
	}
	rows, err := s.store.ListReports(ctx, root)
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*documentv1.ReplicaReport, len(rows))
	for i, row := range rows {
		report := &documentv1.ReplicaReport{
			ReplicaId:  row.Replica,
			Vector:     entriesFromVector(row.Vector),
			HomeSeq:    row.HomeSeq,
			ReportedAt: timestamppb.New(row.ReportedAt),
		}
		if row.ReleasedAt != nil {
			report.ReleasedAt = timestamppb.New(*row.ReleasedAt)
		}
		out[i] = report
	}
	return &documentv1.ListReportsResponse{Reports: out}, nil
}

func (s *Server) ReleaseReplica(ctx context.Context, req *documentv1.ReleaseReplicaRequest) (*documentv1.ReleaseReplicaResponse, error) {
	id, err := parseID(req.GetDocumentId())
	if err != nil {
		return nil, err
	}
	if req.GetReplicaId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid replica id")
	}
	if err := s.store.ReleaseReplica(ctx, id, req.GetReplicaId()); err != nil {
		return nil, mapError(err)
	}
	return &documentv1.ReleaseReplicaResponse{}, nil
}

func (s *Server) SetEpoch(ctx context.Context, req *documentv1.SetEpochRequest) (*documentv1.Document, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	doc, err := s.store.SetEpoch(ctx, id, req.GetWriteEpoch(), req.GetEpochState())
	if err != nil {
		return nil, mapError(err)
	}
	return toProto(doc), nil
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
	if s.perm != nil {
		if err := s.perm.CopyGrants(ctx, id.String(), doc.ID.String()); err != nil {
			return nil, err
		}
	}
	return toProto(doc), nil
}

func (s *Server) allow(ctx context.Context, id uuid.UUID, min permissionv1.Role) error {
	if auth.IsInternal(ctx) {
		return nil
	}
	subject := auth.Subject(ctx)
	if subject == "" {
		return status.Error(codes.Unauthenticated, "unauthenticated")
	}
	if s.perm == nil {
		return status.Error(codes.PermissionDenied, "permission denied")
	}
	return s.perm.Require(ctx, id.String(), subject, min)
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

const ReplicaOwnedReason = "REPLICA_OWNED"

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
		return failedPrecondition(err, ReplicaHomedElsewhereReason)
	case errors.Is(err, domain.ErrReplicaOwned):
		return failedPrecondition(err, ReplicaOwnedReason)
	case errors.Is(err, domain.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrOutOfRange):
		return status.Error(codes.OutOfRange, err.Error())
	case errors.Is(err, domain.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, domain.ErrEncrypted):
		return failedPrecondition(err, "ENCRYPTED")
	case errors.Is(err, domain.ErrEpoch):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrKeyRotation):
		return failedPrecondition(err, "KEY_ROTATION")
	case errors.Is(err, domain.ErrStaleReport):
		return failedPrecondition(err, "STALE_REPORT")
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

func failedPrecondition(err error, reason string) error {
	st, detailErr := status.New(codes.FailedPrecondition, err.Error()).WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: "document-service",
	})
	if detailErr != nil {
		return status.Error(codes.Internal, "internal error")
	}
	return st.Err()
}

func vectorFromEntries(entries []*crdtv1.VectorEntry) (crdt.VersionVector, error) {
	sorted := make([]*crdtv1.VectorEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i] == nil {
			return false
		}
		if sorted[j] == nil {
			return true
		}
		return sorted[i].Client < sorted[j].Client
	})
	doc, err := crdt.FromSnapshot(&crdtv1.Snapshot{Vector: sorted})
	if err != nil {
		return nil, err
	}
	return doc.Vector(), nil
}

func entriesFromVector(v crdt.VersionVector) []*crdtv1.VectorEntry {
	clients := make([]crdt.ClientID, 0, len(v))
	for client, clock := range v {
		if client != 0 && clock != 0 {
			clients = append(clients, client)
		}
	}
	sort.Slice(clients, func(i, j int) bool { return clients[i] < clients[j] })
	out := make([]*crdtv1.VectorEntry, 0, len(clients))
	for _, client := range clients {
		out = append(out, &crdtv1.VectorEntry{Client: uint64(client), Clock: v[client]})
	}
	return out
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
		Encryption:  doc.Encryption,
		WriteEpoch:  doc.WriteEpoch,
		EpochState:  doc.EpochState,
	}
	if doc.ForkOf != nil {
		out.ForkOf = doc.ForkOf.String()
	}
	return out
}
