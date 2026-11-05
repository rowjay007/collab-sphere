package docgrpc_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	docgrpc "github.com/rowjay007/collab-sphere/services/document-service/internal/grpc"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/testdb"
)

type failOwner struct {
	docID string
}

func (f *failOwner) EnsureOwner(_ context.Context, docID, _ string, _ bool) error {
	f.docID = docID
	return status.Error(codes.Unavailable, "ensure failed")
}

func (f *failOwner) Require(context.Context, string, string, permissionv1.Role) error {
	return nil
}

func (f *failOwner) CopyGrants(context.Context, string, string) error { return nil }

func TestCreateDocumentDeletesRowWhenEnsureOwnerFails(t *testing.T) {
	st, _ := testdb.Open(t)
	pub, priv, err := auth.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	const internal = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(
		grpc.UnaryInterceptor(auth.UnaryInterceptor(pub, internal, docgrpc.Methods())),
	)
	api := docgrpc.New(st)
	owner := &failOwner{}
	api.UsePermissions(owner)
	documentv1.RegisterDocumentServiceServer(server, api)
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
	client := documentv1.NewDocumentServiceClient(conn)
	ctx := t.Context()

	tok, err := auth.Mint(priv, "ada", time.Now(), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CreateDocument(authCtx(ctx, tok), &documentv1.CreateDocumentRequest{Title: "doomed", OwnerId: "ada"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("create = %v", err)
	}
	if owner.docID == "" {
		t.Fatal("EnsureOwner was not called")
	}
	_, err = client.GetDocument(authCtx(ctx, "internal."+internal), &documentv1.GetDocumentRequest{Id: owner.docID})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("get after failed EnsureOwner = %v, want NotFound", err)
	}

	_, err = client.CreateDocument(authCtx(ctx, "internal."+internal), &documentv1.CreateDocumentRequest{Title: "nope"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("internal token on CreateDocument = %v", err)
	}
}

func TestUserMethodRejectsBareContext(t *testing.T) {
	s := docgrpc.New(nil)
	_, err := s.CreateDocument(context.Background(), &documentv1.CreateDocumentRequest{Title: "x", OwnerId: "eve"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("bare create: %v", err)
	}
}

func authCtx(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}
