package docgrpc_test

import (
	"context"
	"crypto/ed25519"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	docgrpc "github.com/rowjay007/collab-sphere/services/document-service/internal/grpc"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/store"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/testdb"
)

func TestReportRPCsRejectNonInternal(t *testing.T) {
	st, _ := testdb.Open(t)
	pub, priv, err := auth.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	const internal = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	client := dialDocs(t, pub, internal, st)
	ctx := t.Context()
	doc, err := st.Create(ctx, "auth", "owner")
	if err != nil {
		t.Fatal(err)
	}
	user, err := auth.Mint(priv, "ada", time.Now(), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	report := &documentv1.ReportVectorRequest{DocumentId: doc.ID.String(), ReplicaId: 1, HomeSeq: 1}
	list := &documentv1.ListReportsRequest{ForkRoot: doc.ForkRoot.String()}
	release := &documentv1.ReleaseReplicaRequest{DocumentId: doc.ID.String(), ReplicaId: 1}
	for _, call := range []struct {
		name string
		fn   func(context.Context) error
	}{
		{"report", func(ctx context.Context) error { _, err := client.ReportVector(ctx, report); return err }},
		{"list", func(ctx context.Context) error { _, err := client.ListReports(ctx, list); return err }},
		{"release", func(ctx context.Context) error { _, err := client.ReleaseReplica(ctx, release); return err }},
	} {
		if err := call.fn(ctx); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("%s without a token: %v", call.name, err)
		}
		if err := call.fn(authCtx(ctx, user)); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("%s with a user token: %v", call.name, err)
		}
	}

	in := authCtx(ctx, "internal."+internal)
	const replica = uint64(3)
	if _, err := client.ClaimReplica(in, &documentv1.ClaimReplicaRequest{DocumentId: doc.ID.String(), Client: replica}); err != nil {
		t.Fatal(err)
	}
	r, err := crdt.NewReplica(crdt.ClientID(replica), nil)
	if err != nil {
		t.Fatal(err)
	}
	op, err := r.InsertAt(0, 'q')
	if err != nil {
		t.Fatal(err)
	}
	appended, err := client.AppendOperations(in, &documentv1.AppendOperationsRequest{
		Id:         doc.ID.String(),
		Operations: []*crdtv1.Operation{crdt.OpToProto(op)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReportVector(in, &documentv1.ReportVectorRequest{
		DocumentId: doc.ID.String(),
		ReplicaId:  replica,
		Vector:     []*crdtv1.VectorEntry{{Client: replica, Clock: op.ID.Clock}},
		HomeSeq:    appended.GetOpSeq(),
	}); err != nil {
		t.Fatal(err)
	}
	listed, err := client.ListReports(in, list)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.GetReports()) != 1 || listed.GetReports()[0].GetHomeSeq() != appended.GetOpSeq() {
		t.Fatalf("reports = %+v", listed.GetReports())
	}
	if _, err := client.ReleaseReplica(in, &documentv1.ReleaseReplicaRequest{DocumentId: doc.ID.String(), ReplicaId: replica}); err != nil {
		t.Fatal(err)
	}
	listed, err = client.ListReports(in, list)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.GetReports()) != 1 || listed.GetReports()[0].GetReleasedAt() == nil {
		t.Fatalf("released reports = %+v", listed.GetReports())
	}
}

func dialDocs(t *testing.T, pub ed25519.PublicKey, internal string, st *store.Store) documentv1.DocumentServiceClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(auth.UnaryInterceptor(pub, internal, docgrpc.Methods())))
	documentv1.RegisterDocumentServiceServer(server, docgrpc.New(st))
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
	return documentv1.NewDocumentServiceClient(conn)
}
