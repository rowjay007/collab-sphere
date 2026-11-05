package permgrpc

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/services/permission-service/internal/perms"
)

const domain = "permission-service"

type Server struct {
	permissionv1.UnimplementedPermissionServiceServer
	svc *perms.Service
}

func New(svc *perms.Service) *Server {
	return &Server{svc: svc}
}

func Methods() map[string]auth.Class {
	return map[string]auth.Class{
		permissionv1.PermissionService_EnsureOwner_FullMethodName: auth.Internal,
		permissionv1.PermissionService_Grant_FullMethodName:       auth.User,
		permissionv1.PermissionService_Revoke_FullMethodName:      auth.User,
		permissionv1.PermissionService_CreateLink_FullMethodName:  auth.User,
		permissionv1.PermissionService_RevokeLink_FullMethodName:  auth.User,
		permissionv1.PermissionService_Redeem_FullMethodName:      auth.User,
		permissionv1.PermissionService_PolicyDump_FullMethodName:  auth.Internal,
	}
}

func (s *Server) EnsureOwner(ctx context.Context, req *permissionv1.EnsureOwnerRequest) (*permissionv1.GrantRecord, error) {
	doc, err := parseID(req.GetDocId(), "doc id")
	if err != nil {
		return nil, err
	}
	g, err := s.svc.EnsureOwner(ctx, doc, req.GetUserId())
	if err != nil {
		return nil, mapError(err)
	}
	return grantToProto(g), nil
}

func (s *Server) Grant(ctx context.Context, req *permissionv1.GrantRequest) (*permissionv1.GrantRecord, error) {
	doc, err := parseID(req.GetDocId(), "doc id")
	if err != nil {
		return nil, err
	}
	role, err := roleFromProto(req.GetRole())
	if err != nil {
		return nil, err
	}
	g, err := s.svc.Grant(ctx, auth.Subject(ctx), doc, req.GetUserId(), role)
	if err != nil {
		return nil, mapError(err)
	}
	return grantToProto(g), nil
}

func (s *Server) Revoke(ctx context.Context, req *permissionv1.RevokeRequest) (*permissionv1.RevokeResponse, error) {
	doc, err := parseID(req.GetDocId(), "doc id")
	if err != nil {
		return nil, err
	}
	if err := s.svc.Revoke(ctx, auth.Subject(ctx), doc, req.GetUserId()); err != nil {
		return nil, mapError(err)
	}
	return &permissionv1.RevokeResponse{}, nil
}

func (s *Server) CreateLink(ctx context.Context, req *permissionv1.CreateLinkRequest) (*permissionv1.ShareLink, error) {
	doc, err := parseID(req.GetDocId(), "doc id")
	if err != nil {
		return nil, err
	}
	role, err := roleFromProto(req.GetRole())
	if err != nil {
		return nil, err
	}
	if req.GetExpiresAt() == nil {
		return nil, status.Error(codes.InvalidArgument, "expires_at is required")
	}
	l, tok, err := s.svc.CreateLink(ctx, auth.Subject(ctx), doc, role, req.GetExpiresAt().AsTime())
	if err != nil {
		return nil, mapError(err)
	}
	return &permissionv1.ShareLink{
		Id:        l.ID.String(),
		DocId:     l.DocID.String(),
		Role:      roleToProto(l.Role),
		ExpiresAt: timestamppb.New(l.ExpiresAt),
		Token:     tok,
	}, nil
}

func (s *Server) RevokeLink(ctx context.Context, req *permissionv1.RevokeLinkRequest) (*permissionv1.RevokeLinkResponse, error) {
	id, err := parseID(req.GetId(), "link id")
	if err != nil {
		return nil, err
	}
	if _, err := s.svc.RevokeLink(ctx, auth.Subject(ctx), id); err != nil {
		return nil, mapError(err)
	}
	return &permissionv1.RevokeLinkResponse{}, nil
}

func (s *Server) Redeem(ctx context.Context, req *permissionv1.RedeemRequest) (*permissionv1.GrantRecord, error) {
	g, err := s.svc.Redeem(ctx, auth.Subject(ctx), req.GetToken())
	if err != nil {
		return nil, mapError(err)
	}
	return grantToProto(g), nil
}

func (s *Server) PolicyDump(ctx context.Context, req *permissionv1.PolicyDumpRequest) (*permissionv1.PolicyDumpResponse, error) {
	doc, err := parseID(req.GetDocId(), "doc id")
	if err != nil {
		return nil, err
	}
	p, err := s.svc.PolicyDump(ctx, doc)
	if err != nil {
		return nil, mapError(err)
	}
	members := make(map[string]permissionv1.Role, len(p.Members))
	for user, role := range p.Members {
		members[user] = roleToProto(role)
	}
	return &permissionv1.PolicyDumpResponse{
		Members:    members,
		WriteEpoch: p.WriteEpoch,
		EpochState: p.EpochState,
	}, nil
}

func parseID(raw, what string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.UUID{}, status.Errorf(codes.InvalidArgument, "invalid %s", what)
	}
	return id, nil
}

func roleFromProto(r permissionv1.Role) (perms.Role, error) {
	switch r {
	case permissionv1.Role_ROLE_OWNER:
		return perms.RoleOwner, nil
	case permissionv1.Role_ROLE_EDITOR:
		return perms.RoleEditor, nil
	case permissionv1.Role_ROLE_COMMENTER:
		return perms.RoleCommenter, nil
	case permissionv1.Role_ROLE_VIEWER:
		return perms.RoleViewer, nil
	default:
		return "", status.Error(codes.InvalidArgument, "invalid role")
	}
}

func roleToProto(r perms.Role) permissionv1.Role {
	switch r {
	case perms.RoleOwner:
		return permissionv1.Role_ROLE_OWNER
	case perms.RoleEditor:
		return permissionv1.Role_ROLE_EDITOR
	case perms.RoleCommenter:
		return permissionv1.Role_ROLE_COMMENTER
	case perms.RoleViewer:
		return permissionv1.Role_ROLE_VIEWER
	default:
		return permissionv1.Role_ROLE_UNSPECIFIED
	}
}

func grantToProto(g perms.Grant) *permissionv1.GrantRecord {
	return &permissionv1.GrantRecord{
		DocId:  g.DocID.String(),
		UserId: g.UserID,
		Role:   roleToProto(g.Role),
	}
}

func mapError(err error) error {
	switch {
	case errors.Is(err, perms.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, perms.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, perms.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, perms.ErrPermissionDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, perms.ErrOwnerNotRevocable):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, perms.ErrLinkBad):
		return failedPrecondition(perms.ReasonLinkBad, err)
	case errors.Is(err, perms.ErrLinkRevoked):
		return failedPrecondition(perms.ReasonLinkRevoked, err)
	case errors.Is(err, perms.ErrLinkExpired):
		return failedPrecondition(perms.ReasonLinkExpired, err)
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

func failedPrecondition(reason string, err error) error {
	st, detailErr := status.New(codes.FailedPrecondition, err.Error()).WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: domain,
	})
	if detailErr != nil {
		return status.Error(codes.Internal, "internal error")
	}
	return st.Err()
}
