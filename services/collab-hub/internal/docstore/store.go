package docstore

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/fanout"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/hub"
)

const (
	conflictReason              = "OP_CONFLICT"
	replicaHomedElsewhereReason = "REPLICA_HOMED_ELSEWHERE"
	replicaOwnedReason          = "REPLICA_OWNED"
)

// Store loads, appends and reads document state through DocumentService.
type Store struct {
	client documentv1.DocumentServiceClient
}

// New wraps a document-service gRPC client as a hub.Store.
func New(client documentv1.DocumentServiceClient) *Store {
	return &Store{client: client}
}

func (s *Store) Replica(ctx context.Context, docID string, requested crdt.ClientID) (crdt.ClientID, bool, error) {
	resp, err := s.client.ClaimReplica(ctx, &documentv1.ClaimReplicaRequest{
		DocumentId: docID,
		Client:     uint64(requested),
		UserId:     hub.ReplicaUser(ctx),
	})
	if err != nil {
		if hasErrorReason(err, codes.FailedPrecondition, replicaOwnedReason) {
			return 0, false, hub.ErrReplicaForbidden
		}
		if hasErrorReason(err, codes.FailedPrecondition, replicaHomedElsewhereReason) {
			return 0, false, hub.ErrUnknownReplica
		}
		return 0, false, err
	}
	return crdt.ClientID(resp.GetClient()), resp.GetLapsed(), nil
}

// Load restores a document from the latest snapshot plus subsequent ops.
func (s *Store) Load(ctx context.Context, docID string) (*crdt.Doc, uint64, error) {
	resp, err := s.client.LoadState(ctx, &documentv1.LoadStateRequest{Id: docID})
	if err != nil {
		return nil, 0, err
	}
	if resp.GetDocument().GetStatus() == documentv1.Document_STATUS_ARCHIVED {
		return nil, 0, hub.ErrArchived
	}
	var doc *crdt.Doc
	if resp.GetSnapshot() == nil {
		if resp.GetDocument().GetEncryption() == "encrypted" {
			doc = crdt.NewEncrypted()
		} else {
			doc = crdt.New()
		}
	} else {
		doc, err = crdt.FromSnapshot(resp.GetSnapshot())
		if err != nil {
			return nil, 0, fmt.Errorf("decode snapshot: %w", err)
		}
	}
	ops, err := decodeOps(resp.GetOperations())
	if err != nil {
		return nil, 0, err
	}
	for i, op := range ops {
		if _, err := doc.Apply(op); err != nil {
			return nil, 0, fmt.Errorf("apply operation %d: %w", i, err)
		}
	}
	return doc, resp.GetDocument().GetOpSeq(), nil
}

// Append persists ops through AppendOperations.
func (s *Store) Append(ctx context.Context, docID string, ops []crdt.Op) (hub.AppendResult, error) {
	protoOps := make([]*crdtv1.Operation, len(ops))
	for i, op := range ops {
		protoOps[i] = crdt.OpToProto(op)
	}
	resp, err := s.client.AppendOperations(ctx, &documentv1.AppendOperationsRequest{
		Id:         docID,
		Operations: protoOps,
	})
	if err != nil {
		if client, ok := conflictClient(err); ok {
			return hub.AppendResult{}, hub.ConflictError{Client: client}
		}
		return hub.AppendResult{}, err
	}
	if len(resp.GetSeqs()) != len(ops) {
		return hub.AppendResult{}, fmt.Errorf("append returned %d seqs for %d ops", len(resp.GetSeqs()), len(ops))
	}
	return hub.AppendResult{
		OpSeq:       resp.GetOpSeq(),
		FirstNewSeq: resp.GetFirstNewSeq(),
		Seqs:        resp.GetSeqs(),
	}, nil
}

// conflictClient returns the client named by document-service's OP_CONFLICT
// ErrorInfo on an AlreadyExists append.
func conflictClient(err error) (crdt.ClientID, bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.AlreadyExists {
		return 0, false
	}
	for _, d := range st.Details() {
		info, ok := d.(*errdetails.ErrorInfo)
		if !ok || info.GetReason() != conflictReason {
			continue
		}
		client, err := strconv.ParseUint(info.GetMetadata()["client"], 10, 64)
		if err != nil {
			return 0, false
		}
		return crdt.ClientID(client), true
	}
	return 0, false
}

func hasErrorReason(err error, code codes.Code, reason string) bool {
	st, ok := status.FromError(err)
	if !ok || st.Code() != code {
		return false
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == reason {
			return true
		}
	}
	return false
}

// ReadOperations reads up to limit ops after seq after.
func (s *Store) ReadOperations(ctx context.Context, docID string, after uint64, limit int) (fanout.Batch, hub.LogMeta, error) {
	resp, err := s.client.ReadOperations(ctx, &documentv1.ReadOperationsRequest{
		Id:       docID,
		AfterSeq: after,
		Limit:    uint32(limit),
	})
	if err != nil {
		return fanout.Batch{}, hub.LogMeta{}, err
	}
	ops, err := decodeOps(resp.GetOperations())
	if err != nil {
		return fanout.Batch{}, hub.LogMeta{}, err
	}
	if len(ops) > 0 && resp.GetFirstSeq() != after+1 {
		return fanout.Batch{}, hub.LogMeta{}, fmt.Errorf("read after %d started at seq %d", after, resp.GetFirstSeq())
	}
	meta := hub.LogMeta{
		OpSeq:     resp.GetOpSeq(),
		LiveSeq:   resp.GetLiveSeq(),
		Collected: vectorFromEntries(resp.GetCollected()),
	}
	return fanout.Batch{FirstSeq: resp.GetFirstSeq(), Ops: ops}, meta, nil
}

func vectorFromEntries(entries []*crdtv1.VectorEntry) crdt.VersionVector {
	out := make(crdt.VersionVector, len(entries))
	for _, e := range entries {
		if e.GetClient() == 0 || e.GetClock() == 0 {
			continue
		}
		out[crdt.ClientID(e.GetClient())] = e.GetClock()
	}
	return out
}

// ReportVector stores one replica's vector. A stale row comes back as hub.ErrStaleReport.
func (s *Store) ReportVector(ctx context.Context, docID string, replica crdt.ClientID, vector crdt.VersionVector, homeSeq uint64, resyncPending bool) error {
	_, err := s.client.ReportVector(ctx, &documentv1.ReportVectorRequest{
		DocumentId:    docID,
		ReplicaId:     uint64(replica),
		Vector:        vectorEntries(vector),
		HomeSeq:       homeSeq,
		ResyncPending: resyncPending,
	})
	if hasErrorReason(err, codes.FailedPrecondition, "STALE_REPORT") {
		return fmt.Errorf("%w: %v", hub.ErrStaleReport, err)
	}
	return err
}

func vectorEntries(v crdt.VersionVector) []*crdtv1.VectorEntry {
	clients := make([]crdt.ClientID, 0, len(v))
	for id, clock := range v {
		if id == 0 || clock == 0 {
			continue
		}
		clients = append(clients, id)
	}
	sort.Slice(clients, func(i, j int) bool { return clients[i] < clients[j] })
	out := make([]*crdtv1.VectorEntry, 0, len(clients))
	for _, id := range clients {
		out = append(out, &crdtv1.VectorEntry{Client: uint64(id), Clock: v[id]})
	}
	return out
}

func decodeOps(pbs []*crdtv1.Operation) ([]crdt.Op, error) {
	ops := make([]crdt.Op, len(pbs))
	for i, pb := range pbs {
		op, err := crdt.OpFromProto(pb)
		if err != nil {
			return nil, fmt.Errorf("decode operation %d: %w", i, err)
		}
		ops[i] = op
	}
	return ops, nil
}
