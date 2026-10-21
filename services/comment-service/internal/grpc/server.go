package commentgrpc

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commentv1 "github.com/rowjay007/collab-sphere/gen/comment/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/comment-service/internal/comments"
)

type Server struct {
	commentv1.UnimplementedCommentServiceServer
	svc *comments.Service
}

func New(svc *comments.Service) *Server {
	return &Server{svc: svc}
}

func (s *Server) CreateThread(ctx context.Context, req *commentv1.CreateThreadRequest) (*commentv1.Thread, error) {
	home, err := parseID(req.GetDocumentId(), "document id")
	if err != nil {
		return nil, err
	}
	anchor, err := parseAnchor(req.GetAnchor())
	if err != nil {
		return nil, err
	}
	th, err := s.svc.Create(ctx, home, anchor, req.GetAuthor(), req.GetBody(), req.GetRequestId())
	if err != nil {
		return nil, mapError(err)
	}
	return threadToProto(th, comments.Resolution{}), nil
}

func (s *Server) Reply(ctx context.Context, req *commentv1.ReplyRequest) (*commentv1.Comment, error) {
	threadID, err := parseID(req.GetThreadId(), "thread id")
	if err != nil {
		return nil, err
	}
	c, err := s.svc.Reply(ctx, threadID, req.GetAuthor(), req.GetBody(), req.GetRequestId())
	if err != nil {
		return nil, mapError(err)
	}
	return commentToProto(c), nil
}

func (s *Server) SetResolved(ctx context.Context, req *commentv1.SetResolvedRequest) (*commentv1.Thread, error) {
	threadID, err := parseID(req.GetThreadId(), "thread id")
	if err != nil {
		return nil, err
	}
	th, err := s.svc.SetResolved(ctx, threadID, req.GetResolved(), req.GetActor(), req.GetRequestId())
	if err != nil {
		return nil, mapError(err)
	}
	return threadToProto(th, comments.Resolution{}), nil
}

func (s *Server) ListThreads(ctx context.Context, req *commentv1.ListThreadsRequest) (*commentv1.ListThreadsResponse, error) {
	docID, err := parseID(req.GetDocumentId(), "document id")
	if err != nil {
		return nil, err
	}
	threads, err := s.svc.List(ctx, docID, req.GetIncludeResolved())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*commentv1.Thread, len(threads))
	for i, th := range threads {
		out[i] = threadToProto(th.Thread, th.Resolution)
	}
	return &commentv1.ListThreadsResponse{Threads: out}, nil
}

func (s *Server) ListNotifications(ctx context.Context, req *commentv1.ListNotificationsRequest) (*commentv1.ListNotificationsResponse, error) {
	notes, err := s.svc.ListNotifications(ctx, req.GetUser(), req.GetAfterId(), int(req.GetLimit()))
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*commentv1.Notification, len(notes))
	for i, n := range notes {
		out[i] = notificationToProto(n)
	}
	return &commentv1.ListNotificationsResponse{Notifications: out}, nil
}

func parseID(raw, what string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.UUID{}, status.Errorf(codes.InvalidArgument, "invalid %s", what)
	}
	return id, nil
}

func parseAnchor(a *commentv1.Anchor) (comments.Anchor, error) {
	if a == nil || a.GetStart() == nil || a.GetEnd() == nil {
		return comments.Anchor{}, status.Error(codes.InvalidArgument, "anchor start and end are required")
	}
	return comments.Anchor{Start: idFromProto(a.GetStart()), End: idFromProto(a.GetEnd())}, nil
}

func mapError(err error) error {
	switch {
	case errors.Is(err, comments.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, comments.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, comments.ErrInvalidArgument), errors.Is(err, comments.ErrInvalidAnchor):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, comments.ErrUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

func threadToProto(th comments.Thread, res comments.Resolution) *commentv1.Thread {
	commentsPB := make([]*commentv1.Comment, len(th.Comments))
	for i, c := range th.Comments {
		commentsPB[i] = commentToProto(c)
	}
	return &commentv1.Thread{
		Id:         th.ID.String(),
		DocumentId: th.Home.String(),
		CreatedSeq: th.CreatedSeq,
		Anchor: &commentv1.Anchor{
			Start: idToProto(th.Anchor.Start),
			End:   idToProto(th.Anchor.End),
		},
		Resolved:   th.Resolved,
		Author:     th.Author,
		Comments:   commentsPB,
		Resolution: resolutionToProto(res),
		CreatedAt:  timestamppb.New(th.CreatedAt),
	}
}

func commentToProto(c comments.Comment) *commentv1.Comment {
	return &commentv1.Comment{
		Id:        c.ID.String(),
		Author:    c.Author,
		Body:      c.Body,
		Mentions:  c.Mentions,
		CreatedAt: timestamppb.New(c.At),
	}
}

func resolutionToProto(r comments.Resolution) *commentv1.Resolution {
	if r.State == 0 {
		return nil
	}
	out := &commentv1.Resolution{Text: r.Text}
	switch r.State {
	case comments.Attached:
		out.State = commentv1.Resolution_STATE_ATTACHED
		out.First = idToProto(r.First)
		out.Last = idToProto(r.Last)
	case comments.Detached:
		out.State = commentv1.Resolution_STATE_DETACHED
	}
	return out
}

func notificationToProto(n comments.Notification) *commentv1.Notification {
	out := &commentv1.Notification{
		Id:        n.ID,
		User:      n.User,
		ThreadId:  n.ThreadID.String(),
		Kind:      kindToProto(n.Kind),
		CreatedAt: timestamppb.New(n.CreatedAt),
	}
	if n.CommentID != uuid.Nil {
		out.CommentId = n.CommentID.String()
	}
	return out
}

func kindToProto(k comments.Kind) commentv1.Notification_Kind {
	switch k {
	case comments.Mention:
		return commentv1.Notification_KIND_MENTION
	case comments.ReplyKind:
		return commentv1.Notification_KIND_REPLY
	case comments.ResolvedKind:
		return commentv1.Notification_KIND_RESOLVED
	case comments.UnresolvedKind:
		return commentv1.Notification_KIND_UNRESOLVED
	default:
		return commentv1.Notification_KIND_UNSPECIFIED
	}
}

func idToProto(id crdt.ID) *crdtv1.OpID {
	return &crdtv1.OpID{Client: uint64(id.Client), Clock: id.Clock}
}

func idFromProto(id *crdtv1.OpID) crdt.ID {
	return crdt.ID{Client: crdt.ClientID(id.GetClient()), Clock: id.GetClock()}
}
