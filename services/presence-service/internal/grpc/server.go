package presgrpc

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/services/presence-service/internal/store"
)

type Server struct {
	presencev1.UnimplementedPresenceServiceServer
	store *store.Store
}

func New(s *store.Store) *Server {
	return &Server{store: s}
}

func (s *Server) Join(ctx context.Context, req *presencev1.JoinRequest) (*presencev1.ListResponse, error) {
	self := req.GetSelf()
	if err := validateState(self); err != nil {
		return nil, err
	}
	self.HasCursor = false
	self.Anchor = nil
	self.Head = nil
	if err := s.store.Upsert(ctx, self); err != nil {
		return nil, status.Error(codes.Internal, "internal error")
	}
	states, ver, err := s.store.List(ctx, self.GetDocumentId())
	if err != nil {
		return nil, status.Error(codes.Internal, "internal error")
	}
	others := make([]*presencev1.State, 0, len(states))
	for _, st := range states {
		if st.GetClient() != self.GetClient() {
			others = append(others, st)
		}
	}
	return &presencev1.ListResponse{States: others, Version: ver}, nil
}

func (s *Server) Apply(ctx context.Context, req *presencev1.ApplyRequest) (*presencev1.ApplyResponse, error) {
	for _, ch := range req.GetChanges() {
		if err := validateChange(ch); err != nil {
			return nil, err
		}
	}

	var missing []*presencev1.Touch
	var superseded []*presencev1.Touch
	for _, ch := range req.GetChanges() {
		switch k := ch.GetKind().(type) {
		case *presencev1.Change_Upsert:
			if err := s.store.Upsert(ctx, k.Upsert); err != nil {
				return nil, status.Error(codes.Internal, "internal error")
			}
		case *presencev1.Change_Touch:
			ok, err := s.store.Touch(ctx, k.Touch)
			if err != nil {
				return nil, status.Error(codes.Internal, "internal error")
			}
			switch ok {
			case store.TouchMissing:
				missing = append(missing, k.Touch)
			case store.TouchSuperseded:
				superseded = append(superseded, k.Touch)
			}
		case *presencev1.Change_Leave:
			if err := s.store.Remove(ctx, k.Leave); err != nil {
				return nil, status.Error(codes.Internal, "internal error")
			}
		}
	}
	return &presencev1.ApplyResponse{Missing: missing, Superseded: superseded}, nil
}

func validateChange(ch *presencev1.Change) error {
	switch k := ch.GetKind().(type) {
	case *presencev1.Change_Upsert:
		return validateState(k.Upsert)
	case *presencev1.Change_Touch:
		docID, err := validateRef(k.Touch.GetDocumentId(), k.Touch.GetClient(), k.Touch.GetSession())
		if err != nil {
			return err
		}
		k.Touch.DocumentId = docID
		return nil
	case *presencev1.Change_Leave:
		docID, err := validateRef(k.Leave.GetDocumentId(), k.Leave.GetClient(), k.Leave.GetSession())
		if err != nil {
			return err
		}
		k.Leave.DocumentId = docID
		return nil
	default:
		return status.Error(codes.InvalidArgument, "empty change")
	}
}

func (s *Server) List(ctx context.Context, req *presencev1.ListRequest) (*presencev1.ListResponse, error) {
	docID, err := parseDocID(req.GetDocumentId())
	if err != nil {
		return nil, err
	}
	states, ver, err := s.store.List(ctx, docID)
	if err != nil {
		return nil, status.Error(codes.Internal, "internal error")
	}
	return &presencev1.ListResponse{States: states, Version: ver}, nil
}

func validateState(st *presencev1.State) error {
	if st == nil {
		return status.Error(codes.InvalidArgument, "missing state")
	}
	docID, err := parseDocID(st.GetDocumentId())
	if err != nil {
		return err
	}
	st.DocumentId = docID
	if st.GetClient() == 0 || st.GetSession() == 0 {
		return status.Error(codes.InvalidArgument, "client and session must be nonzero")
	}
	return nil
}

func validateRef(docID string, client, session uint64) (string, error) {
	canonical, err := parseDocID(docID)
	if err != nil {
		return "", err
	}
	if client == 0 || session == 0 {
		return "", status.Error(codes.InvalidArgument, "client and session must be nonzero")
	}
	return canonical, nil
}

func parseDocID(raw string) (string, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return "", status.Error(codes.InvalidArgument, "invalid document id")
	}
	return id.String(), nil
}
