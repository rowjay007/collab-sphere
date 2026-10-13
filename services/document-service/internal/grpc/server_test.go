package docgrpc_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	docgrpc "github.com/rowjay007/collab-sphere/services/document-service/internal/grpc"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/store"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/testdb"
)

func TestGRPCHappyAndErrors(t *testing.T) {
	st, _ := testdb.Open(t)
	client, stop := startGRPC(t, st)
	defer stop()
	ctx := t.Context()

	created, err := client.CreateDocument(ctx, &documentv1.CreateDocumentRequest{
		Title:   "grpc",
		OwnerId: "owner",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.GetTitle() != "grpc" || created.GetStatus() != documentv1.Document_STATUS_ACTIVE {
		t.Fatalf("created %+v", created)
	}

	got, err := client.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: created.GetId()})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.GetId() != created.GetId() {
		t.Fatalf("get id %s", got.GetId())
	}

	r, err := crdt.NewReplica(9, nil)
	if err != nil {
		t.Fatal(err)
	}
	op, err := r.InsertAt(0, 'g')
	if err != nil {
		t.Fatal(err)
	}
	appended, err := client.AppendOperations(ctx, &documentv1.AppendOperationsRequest{
		Id:         created.GetId(),
		Operations: []*crdtv1.Operation{testdb.ProtoFromInsert(op)},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if appended.GetOpSeq() != 1 {
		t.Fatalf("op_seq %d", appended.GetOpSeq())
	}

	state, err := client.LoadState(ctx, &documentv1.LoadStateRequest{Id: created.GetId()})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if state.GetDocument().GetOpSeq() != 1 || len(state.GetOperations()) != 1 {
		t.Fatalf("state %+v ops=%d", state.GetDocument(), len(state.GetOperations()))
	}

	archived, err := client.ArchiveDocument(ctx, &documentv1.ArchiveDocumentRequest{Id: created.GetId()})
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if archived.GetStatus() != documentv1.Document_STATUS_ARCHIVED {
		t.Fatalf("status %s", archived.GetStatus())
	}

	_, err = client.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: "not-a-uuid"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad uuid: %v", err)
	}
	_, err = client.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing: %v", err)
	}
	_, err = client.CreateDocument(ctx, &documentv1.CreateDocumentRequest{Title: "", OwnerId: "o"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty title: %v", err)
	}
	_, err = client.AppendOperations(ctx, &documentv1.AppendOperationsRequest{
		Id:         created.GetId(),
		Operations: []*crdtv1.Operation{testdb.ProtoFromInsert(op)},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("append archived: %v", err)
	}

	active, err := client.CreateDocument(ctx, &documentv1.CreateDocumentRequest{Title: "live", OwnerId: "o"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.AppendOperations(ctx, &documentv1.AppendOperationsRequest{
		Id:         active.GetId(),
		Operations: []*crdtv1.Operation{{}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad op: %v", err)
	}

	good, err := crdt.NewReplica(10, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := good.InsertAt(0, 'a')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AppendOperations(ctx, &documentv1.AppendOperationsRequest{
		Id:         active.GetId(),
		Operations: []*crdtv1.Operation{testdb.ProtoFromInsert(first)},
	}); err != nil {
		t.Fatal(err)
	}
	clash := testdb.ProtoFromInsert(first)
	clash.GetInsert().Value = uint32('b')
	_, err = client.AppendOperations(ctx, &documentv1.AppendOperationsRequest{
		Id:         active.GetId(),
		Operations: []*crdtv1.Operation{clash},
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflict: %v", err)
	}
}

func startGRPC(t *testing.T, st *store.Store) (documentv1.DocumentServiceClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	documentv1.RegisterDocumentServiceServer(server, docgrpc.New(st))
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
	return documentv1.NewDocumentServiceClient(conn), func() {
		_ = conn.Close()
		server.Stop()
		_ = lis.Close()
	}
}
