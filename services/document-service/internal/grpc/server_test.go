package docgrpc_test

import (
	"context"
	"net"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
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
	var info *errdetails.ErrorInfo
	for _, d := range status.Convert(err).Details() {
		if i, ok := d.(*errdetails.ErrorInfo); ok {
			info = i
		}
	}
	if info.GetReason() != docgrpc.ConflictReason || info.GetMetadata()["client"] != "10" || info.GetMetadata()["clock"] != "1" {
		t.Fatalf("conflict detail = %v", info)
	}
}

func TestGRPCAppendSeqsAndReadOperations(t *testing.T) {
	st, _ := testdb.Open(t)
	client, stop := startGRPC(t, st)
	defer stop()
	ctx := t.Context()

	doc, err := client.CreateDocument(ctx, &documentv1.CreateDocumentRequest{Title: "seqs", OwnerId: "o"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := crdt.NewReplica(31, nil)
	if err != nil {
		t.Fatal(err)
	}
	var protoOps []*crdtv1.Operation
	for range 4 {
		op, err := r.InsertAt(r.Len(), 's')
		if err != nil {
			t.Fatal(err)
		}
		protoOps = append(protoOps, testdb.ProtoFromInsert(op))
	}

	first, err := client.AppendOperations(ctx, &documentv1.AppendOperationsRequest{
		Id:         doc.GetId(),
		Operations: protoOps[:2],
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.GetOpSeq() != 2 || first.GetFirstNewSeq() != 1 || len(first.GetSeqs()) != 2 || first.GetSeqs()[0] != 1 || first.GetSeqs()[1] != 2 {
		t.Fatalf("first %+v", first)
	}

	mixed, err := client.AppendOperations(ctx, &documentv1.AppendOperationsRequest{
		Id:         doc.GetId(),
		Operations: []*crdtv1.Operation{protoOps[0], protoOps[2], protoOps[2]},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mixed.GetOpSeq() != 3 || mixed.GetFirstNewSeq() != 3 || len(mixed.GetSeqs()) != 3 {
		t.Fatalf("mixed %+v", mixed)
	}
	if mixed.GetSeqs()[0] != 1 || mixed.GetSeqs()[1] != 3 || mixed.GetSeqs()[2] != 3 {
		t.Fatalf("mixed seqs %v", mixed.GetSeqs())
	}

	after0, err := client.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: doc.GetId(), AfterSeq: 0, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if after0.GetFirstSeq() != 1 || after0.GetOpSeq() != 3 || len(after0.GetOperations()) != 3 {
		t.Fatalf("after0 %+v n=%d", after0, len(after0.GetOperations()))
	}

	afterMid, err := client.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: doc.GetId(), AfterSeq: 1, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if afterMid.GetFirstSeq() != 2 || len(afterMid.GetOperations()) != 2 {
		t.Fatalf("after mid %+v n=%d", afterMid, len(afterMid.GetOperations()))
	}

	empty, err := client.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: doc.GetId(), AfterSeq: 3})
	if err != nil {
		t.Fatal(err)
	}
	if empty.GetFirstSeq() != 0 || len(empty.GetOperations()) != 0 || empty.GetOpSeq() != 3 {
		t.Fatalf("empty %+v n=%d", empty, len(empty.GetOperations()))
	}

	trunc, err := client.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: doc.GetId(), AfterSeq: 0, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if trunc.GetFirstSeq() != 1 || len(trunc.GetOperations()) != 1 || trunc.GetOpSeq() != 3 {
		t.Fatalf("trunc %+v n=%d", trunc, len(trunc.GetOperations()))
	}

	_, err = client.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: doc.GetId(), AfterSeq: 0, Limit: 10001})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("limit too large: %v", err)
	}
	_, err = client.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: doc.GetId(), AfterSeq: 4})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("after beyond: %v", err)
	}
	_, err = client.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("unknown: %v", err)
	}

	if _, err := client.ArchiveDocument(ctx, &documentv1.ArchiveDocumentRequest{Id: doc.GetId()}); err != nil {
		t.Fatal(err)
	}
	archived, err := client.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: doc.GetId(), AfterSeq: 0})
	if err != nil {
		t.Fatal(err)
	}
	if archived.GetFirstSeq() != 1 || len(archived.GetOperations()) != 3 {
		t.Fatalf("archived n=%d", len(archived.GetOperations()))
	}
}

func TestGRPCClaimReplica(t *testing.T) {
	st, pool := testdb.Open(t)
	client, stop := startGRPC(t, st)
	defer stop()
	ctx := t.Context()
	home, err := client.CreateDocument(ctx, &documentv1.CreateDocumentRequest{Title: "home", OwnerId: "o"})
	if err != nil {
		t.Fatal(err)
	}
	allocated, err := client.ClaimReplica(ctx, &documentv1.ClaimReplicaRequest{DocumentId: home.GetId()})
	if err != nil {
		t.Fatal(err)
	}
	if allocated.GetClient() == 0 {
		t.Fatal("allocated zero replica")
	}
	claimed, err := client.ClaimReplica(ctx, &documentv1.ClaimReplicaRequest{DocumentId: home.GetId(), Client: 90})
	if err != nil || claimed.GetClient() != 90 {
		t.Fatalf("claim = %v, %v", claimed, err)
	}

	branch := uuid.New()
	if _, err := pool.Exec(ctx, `
		INSERT INTO documents (id, title, owner_id, fork_root)
		VALUES ($1, 'branch', 'o', $2)`, branch, home.GetId()); err != nil {
		t.Fatal(err)
	}
	_, err = client.ClaimReplica(ctx, &documentv1.ClaimReplicaRequest{DocumentId: branch.String(), Client: 90})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("branch claim error = %v", err)
	}
	var info *errdetails.ErrorInfo
	for _, detail := range status.Convert(err).Details() {
		if candidate, ok := detail.(*errdetails.ErrorInfo); ok {
			info = candidate
		}
	}
	if info.GetReason() != docgrpc.ReplicaHomedElsewhereReason {
		t.Fatalf("claim detail = %v", info)
	}
}

func TestGRPCForkAndListChanged(t *testing.T) {
	st, _ := testdb.Open(t)
	client, stop := startGRPC(t, st)
	defer stop()
	ctx := t.Context()

	src, err := client.CreateDocument(ctx, &documentv1.CreateDocumentRequest{Title: "src", OwnerId: "o"})
	if err != nil {
		t.Fatal(err)
	}
	if src.GetForkRoot() != src.GetId() || src.GetForkOf() != "" || src.GetForkSeq() != 0 {
		t.Fatalf("created %+v", src)
	}

	r, err := crdt.NewReplica(41, nil)
	if err != nil {
		t.Fatal(err)
	}
	op, err := r.InsertAt(0, 'f')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AppendOperations(ctx, &documentv1.AppendOperationsRequest{
		Id:         src.GetId(),
		Operations: []*crdtv1.Operation{testdb.ProtoFromInsert(op)},
	}); err != nil {
		t.Fatal(err)
	}

	fork, err := client.ForkDocument(ctx, &documentv1.ForkDocumentRequest{
		SourceId:  src.GetId(),
		Seq:       1,
		Title:     "branch",
		RequestId: "grpc-fork",
	})
	if err != nil {
		t.Fatal(err)
	}
	if fork.GetForkOf() != src.GetId() || fork.GetForkSeq() != 1 || fork.GetForkRoot() != src.GetId() || fork.GetOpSeq() != 1 {
		t.Fatalf("fork %+v", fork)
	}

	got, err := client.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: fork.GetId()})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetForkOf() != src.GetId() || got.GetForkSeq() != 1 || got.GetForkRoot() != src.GetId() {
		t.Fatalf("get fork %+v", got)
	}

	again, err := client.ForkDocument(ctx, &documentv1.ForkDocumentRequest{
		SourceId:  src.GetId(),
		Seq:       1,
		Title:     "branch",
		RequestId: "grpc-fork",
	})
	if err != nil || again.GetId() != fork.GetId() {
		t.Fatalf("retry = %v, %v", again, err)
	}
	_, err = client.ForkDocument(ctx, &documentv1.ForkDocumentRequest{
		SourceId:  src.GetId(),
		Seq:       0,
		Title:     "other",
		RequestId: "grpc-fork",
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflict: %v", err)
	}
	_, err = client.ForkDocument(ctx, &documentv1.ForkDocumentRequest{
		SourceId:  src.GetId(),
		Seq:       2,
		Title:     "beyond",
		RequestId: "grpc-beyond",
	})
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("beyond: %v", err)
	}
	_, err = client.ForkDocument(ctx, &documentv1.ForkDocumentRequest{
		SourceId:  src.GetId(),
		Seq:       1,
		Title:     "branch",
		RequestId: "",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing request_id: %v", err)
	}

	archived, err := client.CreateDocument(ctx, &documentv1.CreateDocumentRequest{Title: "old", OwnerId: "o"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ArchiveDocument(ctx, &documentv1.ArchiveDocumentRequest{Id: archived.GetId()}); err != nil {
		t.Fatal(err)
	}
	_, err = client.ForkDocument(ctx, &documentv1.ForkDocumentRequest{
		SourceId:  archived.GetId(),
		Seq:       0,
		Title:     "nope",
		RequestId: "grpc-archived",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("archived: %v", err)
	}

	listed, err := client.ListChangedDocuments(ctx, &documentv1.ListChangedRequest{Limit: 5000})
	if err != nil {
		t.Fatal(err)
	}
	if listed.GetNextSince() == nil {
		t.Fatal("missing next_since")
	}
	var sawFork, sawArchived bool
	for _, doc := range listed.GetDocuments() {
		if doc.GetId() == fork.GetId() {
			sawFork = true
		}
		if doc.GetId() == archived.GetId() {
			sawArchived = true
		}
	}
	if !sawFork || sawArchived {
		t.Fatalf("feed fork=%v archived=%v n=%d", sawFork, sawArchived, len(listed.GetDocuments()))
	}
	_, err = client.ListChangedDocuments(ctx, &documentv1.ListChangedRequest{Limit: 5001})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("limit: %v", err)
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
