package docstore

import (
	"context"
	"errors"
	"strings"
	"testing"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/hub"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type archivedClient struct {
	documentv1.DocumentServiceClient
}

func (archivedClient) LoadState(context.Context, *documentv1.LoadStateRequest, ...grpc.CallOption) (*documentv1.LoadStateResponse, error) {
	return &documentv1.LoadStateResponse{
		Document: &documentv1.Document{Status: documentv1.Document_STATUS_ARCHIVED},
	}, nil
}

func TestLoadRejectsArchivedDocument(t *testing.T) {
	_, _, err := New(archivedClient{}).Load(t.Context(), "doc")
	if err == nil || !strings.Contains(err.Error(), "document archived") {
		t.Fatalf("Load error = %v", err)
	}
}

var (
	opA = crdt.Insert{ID: crdt.ID{Client: 1, Clock: 1}, Parent: crdt.Root, Value: 'a'}
	opB = crdt.Insert{ID: crdt.ID{Client: 1, Clock: 2}, Parent: opA.ID, Value: 'b'}
)

type logClient struct {
	documentv1.DocumentServiceClient
	seqs     []uint64
	firstSeq uint64
}

func (logClient) LoadState(context.Context, *documentv1.LoadStateRequest, ...grpc.CallOption) (*documentv1.LoadStateResponse, error) {
	return &documentv1.LoadStateResponse{
		Document:   &documentv1.Document{Status: documentv1.Document_STATUS_ACTIVE, OpSeq: 7},
		Operations: []*crdtv1.Operation{crdt.OpToProto(opA)},
	}, nil
}

func (c logClient) AppendOperations(context.Context, *documentv1.AppendOperationsRequest, ...grpc.CallOption) (*documentv1.AppendOperationsResponse, error) {
	return &documentv1.AppendOperationsResponse{OpSeq: 9, Seqs: c.seqs, FirstNewSeq: 9}, nil
}

func (c logClient) ReadOperations(context.Context, *documentv1.ReadOperationsRequest, ...grpc.CallOption) (*documentv1.ReadOperationsResponse, error) {
	return &documentv1.ReadOperationsResponse{
		FirstSeq:   c.firstSeq,
		Operations: []*crdtv1.Operation{crdt.OpToProto(opA), crdt.OpToProto(opB)},
		OpSeq:      9,
	}, nil
}

func TestLoadReturnsOpSeq(t *testing.T) {
	doc, seq, err := New(logClient{}).Load(t.Context(), "doc")
	if err != nil {
		t.Fatal(err)
	}
	if seq != 7 || doc.Text() != "a" {
		t.Fatalf("seq=%d text=%q", seq, doc.Text())
	}
}

func TestAppendReturnsSeqs(t *testing.T) {
	res, err := New(logClient{seqs: []uint64{4, 9}}).Append(t.Context(), "doc", []crdt.Op{opA, opB})
	if err != nil {
		t.Fatal(err)
	}
	if res.OpSeq != 9 || res.FirstNewSeq != 9 || len(res.Seqs) != 2 || res.Seqs[0] != 4 || res.Seqs[1] != 9 {
		t.Fatalf("result = %+v", res)
	}
	if _, err := New(logClient{seqs: []uint64{9}}).Append(t.Context(), "doc", []crdt.Op{opA, opB}); err == nil {
		t.Fatal("Append accepted a response with too few seqs")
	}
}

type conflictingClient struct {
	documentv1.DocumentServiceClient
	err error
}

type replicaClient struct {
	documentv1.DocumentServiceClient
	client uint64
	err    error
}

func (c replicaClient) ClaimReplica(context.Context, *documentv1.ClaimReplicaRequest, ...grpc.CallOption) (*documentv1.ClaimReplicaResponse, error) {
	return &documentv1.ClaimReplicaResponse{Client: c.client}, c.err
}

func TestReplicaClaim(t *testing.T) {
	got, err := New(replicaClient{client: 44}).Replica(t.Context(), "doc", 44)
	if err != nil || got != 44 {
		t.Fatalf("Replica = %d, %v", got, err)
	}
	st, err := status.New(codes.FailedPrecondition, "homed elsewhere").WithDetails(&errdetails.ErrorInfo{
		Reason: replicaHomedElsewhereReason,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(replicaClient{err: st.Err()}).Replica(t.Context(), "doc", 44)
	if !errors.Is(err, hub.ErrUnknownReplica) {
		t.Fatalf("Replica error = %v", err)
	}
}

func (c conflictingClient) AppendOperations(context.Context, *documentv1.AppendOperationsRequest, ...grpc.CallOption) (*documentv1.AppendOperationsResponse, error) {
	return nil, c.err
}

func TestAppendConflictNamesClient(t *testing.T) {
	st, err := status.New(codes.AlreadyExists, "conflict").WithDetails(&errdetails.ErrorInfo{
		Reason:   conflictReason,
		Metadata: map[string]string{"client": "50", "clock": "3"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(conflictingClient{err: st.Err()}).Append(t.Context(), "doc", []crdt.Op{opA})
	var conflict hub.ConflictError
	if !errors.As(err, &conflict) || conflict.Client != 50 {
		t.Fatalf("Append error = %v, want a conflict for client 50", err)
	}

	_, err = New(conflictingClient{err: status.Error(codes.AlreadyExists, "conflict")}).Append(t.Context(), "doc", []crdt.Op{opA})
	if err == nil || errors.As(err, &conflict) {
		t.Fatalf("Append error = %v, want a plain error without the detail", err)
	}
}

func TestReadOperations(t *testing.T) {
	b, opSeq, err := New(logClient{firstSeq: 8}).ReadOperations(t.Context(), "doc", 7, 10)
	if err != nil {
		t.Fatal(err)
	}
	if b.FirstSeq != 8 || len(b.Ops) != 2 || b.Ops[1] != crdt.Op(opB) || opSeq != 9 {
		t.Fatalf("batch=%+v opSeq=%d", b, opSeq)
	}
	if _, _, err := New(logClient{firstSeq: 5}).ReadOperations(t.Context(), "doc", 7, 10); err == nil {
		t.Fatal("ReadOperations accepted a range that does not start after the requested seq")
	}
}
