package commentgrpc_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	commentv1 "github.com/rowjay007/collab-sphere/gen/comment/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/comment-service/internal/comments"
	"github.com/rowjay007/collab-sphere/services/comment-service/internal/comments/testdb"
	commentgrpc "github.com/rowjay007/collab-sphere/services/comment-service/internal/grpc"
)

type stubDocs struct {
	doc *crdt.Doc
	seq uint64
}

func (s stubDocs) LoadState(context.Context, uuid.UUID) (*crdt.Doc, uint64, error) {
	if s.doc == nil {
		return nil, 0, comments.ErrNotFound
	}
	return s.doc, s.seq, nil
}

func (s stubDocs) ForkRoot(_ context.Context, doc uuid.UUID) (uuid.UUID, error) {
	if s.doc == nil {
		return uuid.UUID{}, comments.ErrNotFound
	}
	return doc, nil
}

type stubVersions struct {
	frontier map[uuid.UUID]uint64
}

func (s stubVersions) Frontier(context.Context, uuid.UUID) (map[uuid.UUID]uint64, error) {
	return s.frontier, nil
}

func TestGRPCMapsDomainErrors(t *testing.T) {
	pool := testdb.Open(t)
	r, err := crdt.NewReplica(3, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range "xyz" {
		if _, err := r.InsertAt(r.Len(), ch); err != nil {
			t.Fatal(err)
		}
	}
	start, err := r.IDAt(0)
	if err != nil {
		t.Fatal(err)
	}
	end, err := r.IDAt(2)
	if err != nil {
		t.Fatal(err)
	}
	home := uuid.New()
	client, stop := startGRPC(t, comments.New(pool, stubDocs{doc: r.Doc, seq: 3}, stubVersions{
		frontier: map[uuid.UUID]uint64{home: 3},
	}))
	defer stop()
	ctx := t.Context()

	created, err := client.CreateThread(ctx, &commentv1.CreateThreadRequest{
		DocumentId: home.String(),
		Anchor:     &commentv1.Anchor{Start: idPB(start), End: idPB(end)},
		Author:     "ann",
		Body:       "hi @bob",
		RequestId:  "g1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.GetAuthor() != "ann" || len(created.GetComments()) != 1 {
		t.Fatalf("created %+v", created)
	}

	_, err = client.CreateThread(ctx, &commentv1.CreateThreadRequest{
		DocumentId: home.String(),
		Anchor:     &commentv1.Anchor{Start: idPB(start), End: idPB(end)},
		Author:     "ann",
		Body:       "other",
		RequestId:  "g1",
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflict: %v", err)
	}

	_, err = client.CreateThread(ctx, &commentv1.CreateThreadRequest{
		DocumentId: "not-a-uuid",
		Anchor:     &commentv1.Anchor{Start: idPB(start), End: idPB(end)},
		Author:     "ann",
		Body:       "x",
		RequestId:  "g2",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad id: %v", err)
	}

	listed, err := client.ListThreads(ctx, &commentv1.ListThreadsRequest{DocumentId: home.String()})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.GetThreads()) != 1 || listed.GetThreads()[0].GetResolution().GetState() != commentv1.Resolution_STATE_ATTACHED {
		t.Fatalf("list %+v", listed)
	}

	notes, err := client.ListNotifications(ctx, &commentv1.ListNotificationsRequest{User: "bob"})
	if err != nil {
		t.Fatal(err)
	}
	if len(notes.GetNotifications()) != 1 || notes.GetNotifications()[0].GetKind() != commentv1.Notification_KIND_MENTION {
		t.Fatalf("notes %+v", notes)
	}
}

func TestUserMethodRejectsBareContext(t *testing.T) {
	s := commentgrpc.New(nil)
	_, err := s.ListNotifications(context.Background(), &commentv1.ListNotificationsRequest{User: "eve"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("bare list: %v", err)
	}
}

func TestAnchorPinsRejectsNonInternal(t *testing.T) {
	pub, priv, err := auth.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	const secret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(auth.UnaryInterceptor(pub, secret, commentgrpc.Methods())))
	commentv1.RegisterCommentServiceServer(server, commentgrpc.New(nil))
	go server.Serve(lis)
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := commentv1.NewCommentServiceClient(conn)
	tok, err := auth.Mint(priv, "ada", time.Now(), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.AnchorPins(authCtx(t.Context(), tok), &commentv1.AnchorPinsRequest{ForkRoot: uuid.NewString()})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("user token: %v", err)
	}
	_, err = client.AnchorPins(authCtx(t.Context(), auth.FormatInternal(secret)), &commentv1.AnchorPinsRequest{ForkRoot: "not-a-uuid"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("internal token: %v", err)
	}
}

func authCtx(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func idPB(id crdt.ID) *crdtv1.OpID {
	return &crdtv1.OpID{Client: uint64(id.Client), Clock: id.Clock}
}

func startGRPC(t *testing.T, svc *comments.Service) (commentv1.CommentServiceClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(auth.WithInternal(ctx), req)
	}))
	commentv1.RegisterCommentServiceServer(server, commentgrpc.New(svc))
	go server.Serve(lis)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return commentv1.NewCommentServiceClient(conn), func() {
		_ = conn.Close()
		server.Stop()
		_ = lis.Close()
	}
}
