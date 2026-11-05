package exportgrpc

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	exportv1 "github.com/rowjay007/collab-sphere/gen/export/v1"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/services/export-service/internal/export"
)

const (
	domain          = "export-service"
	reasonEncrypted = "ENCRYPTED"
)

type Server struct {
	exportv1.UnimplementedExportServiceServer
	svc *export.Service
}

func New(svc *export.Service) *Server {
	return &Server{svc: svc}
}

func Methods() map[string]auth.Class {
	return map[string]auth.Class{
		exportv1.ExportService_RequestExport_FullMethodName: auth.User,
		exportv1.ExportService_GetExport_FullMethodName:     auth.User,
	}
}

func (s *Server) RequestExport(ctx context.Context, req *exportv1.RequestExportRequest) (*exportv1.ExportJob, error) {
	docID, err := uuid.Parse(req.GetDocId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "doc id")
	}
	format, err := export.ParseFormat(req.GetFormat())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "format")
	}
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id")
	}
	if vid := req.GetVersionId(); len(vid) != 0 && len(vid) != 32 {
		return nil, status.Error(codes.InvalidArgument, "version_id")
	}
	job, err := s.svc.Request(ctx, export.Request{
		DocID:     docID,
		VersionID: req.GetVersionId(),
		Format:    format,
		RequestID: req.GetRequestId(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return toProto(job), nil
}

func (s *Server) GetExport(ctx context.Context, req *exportv1.GetExportRequest) (*exportv1.ExportJob, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "id")
	}
	job, err := s.svc.Get(ctx, id)
	if err != nil {
		return nil, mapError(err)
	}
	return toProto(job), nil
}

func toProto(j export.Job) *exportv1.ExportJob {
	return &exportv1.ExportJob{
		Id:        j.ID.String(),
		RequestId: j.RequestID,
		DocId:     j.DocID.String(),
		VersionId: j.NodeID,
		Format:    string(j.Format),
		UserId:    j.UserID,
		State:     string(j.State),
		Error:     j.Error,
		Url:       j.URL,
	}
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, export.ErrNotFound):
		return status.Error(codes.NotFound, "export not found")
	case errors.Is(err, export.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, export.ErrEncrypted):
		st, detailErr := status.New(codes.FailedPrecondition, err.Error()).WithDetails(&errdetails.ErrorInfo{
			Reason: reasonEncrypted,
			Domain: domain,
		})
		if detailErr != nil {
			return status.Error(codes.Internal, "internal error")
		}
		return st.Err()
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
