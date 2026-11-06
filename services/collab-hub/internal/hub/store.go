package hub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/fanout"
)

var ErrArchived = errors.New("document archived")
var ErrUnknownReplica = errors.New("unknown replica")
var ErrReplicaForbidden = errors.New("replica owned by another user")
var ErrStaleReport = errors.New("stale replica report")

type replicaUserKey struct{}

func WithReplicaUser(ctx context.Context, user string) context.Context {
	return context.WithValue(ctx, replicaUserKey{}, user)
}

func ReplicaUser(ctx context.Context) string {
	s, _ := ctx.Value(replicaUserKey{}).(string)
	return s
}

// ConflictError is an Append refused because the log holds one of Client's
// op IDs with different content.
type ConflictError struct {
	Client crdt.ClientID
}

func (e ConflictError) Error() string {
	return fmt.Sprintf("an op from client %d conflicts with the log", e.Client)
}

// AppendResult reports where Append put each op. Seqs is parallel to the
// appended ops. An op already in the log keeps its existing seq. Newly stored
// ops hold exactly FirstNewSeq..OpSeq, and FirstNewSeq is 0 when none were new.
type AppendResult struct {
	OpSeq       uint64
	FirstNewSeq uint64
	Seqs        []uint64
}

// LogMeta is the log head plus live snapshot metadata from ReadOperations.
type LogMeta struct {
	OpSeq     uint64
	LiveSeq   uint64
	Collected crdt.VersionVector
}

// Store is the consumer-side persistence seam for a document hub.
type Store interface {
	Replica(ctx context.Context, docID string, requested crdt.ClientID) (crdt.ClientID, bool, error)
	// Load returns the document and the seq of the last op it holds.
	Load(ctx context.Context, docID string) (*crdt.Doc, uint64, error)
	Append(ctx context.Context, docID string, ops []crdt.Op) (AppendResult, error)
	// ReadOperations returns up to limit ops after seq after and log metadata.
	ReadOperations(ctx context.Context, docID string, after uint64, limit int) (fanout.Batch, LogMeta, error)
	// ReportVector records one replica's version vector at homeSeq.
	// resyncPending marks a stamp whose resync DocumentSync the client has
	// not yet acknowledged with its own report.
	// ErrStaleReport means the stored row was left unchanged.
	ReportVector(ctx context.Context, docID string, replica crdt.ClientID, vector crdt.VersionVector, homeSeq uint64, resyncPending bool) error
}

// StoredReport is one replica's recorded vector.
type StoredReport struct {
	Replica       crdt.ClientID
	Vector        crdt.VersionVector
	HomeSeq       uint64
	ResyncPending bool
}

// MemoryStore is an in-process Store for tests. Each document is an ordered
// op log deduplicated by op ID, like document-service.
type MemoryStore struct {
	mu        sync.Mutex
	logs      map[string]*memoryLog
	replicas  map[string]map[crdt.ClientID]struct{}
	reports   map[string]map[crdt.ClientID]StoredReport
	nextID    crdt.ClientID
	appendErr error
	loads     atomic.Int64
}

type memoryLog struct {
	ops           []crdt.Op
	seq           map[crdt.ID]uint64
	liveSeq       uint64
	liveCollected crdt.VersionVector
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		logs:     make(map[string]*memoryLog),
		replicas: make(map[string]map[crdt.ClientID]struct{}),
		reports:  make(map[string]map[crdt.ClientID]StoredReport),
	}
}

func (s *MemoryStore) Replica(_ context.Context, docID string, requested crdt.ClientID) (crdt.ClientID, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	homes := s.replicas[docID]
	if homes == nil {
		homes = make(map[crdt.ClientID]struct{})
		s.replicas[docID] = homes
	}
	if requested != 0 {
		homes[requested] = struct{}{}
		return requested, false, nil
	}
	for {
		s.nextID++
		if s.nextID == 0 {
			continue
		}
		if _, exists := homes[s.nextID]; exists {
			continue
		}
		homes[s.nextID] = struct{}{}
		return s.nextID, false, nil
	}
}

func (s *MemoryStore) log(docID string) *memoryLog {
	if s.logs[docID] == nil {
		s.logs[docID] = &memoryLog{seq: make(map[crdt.ID]uint64)}
	}
	return s.logs[docID]
}

// Load replays the document's log, creating an empty one if needed.
func (s *MemoryStore) Load(_ context.Context, docID string) (*crdt.Doc, uint64, error) {
	s.loads.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.log(docID)
	doc, err := replay(l.ops)
	return doc, uint64(len(l.ops)), err
}

// Append sequences ops unless FailAppend is set.
func (s *MemoryStore) Append(_ context.Context, docID string, ops []crdt.Op) (AppendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.appendErr != nil {
		return AppendResult{}, s.appendErr
	}
	return s.log(docID).append(ops)
}

// AppendFromOtherWriter sequences ops as if another hub had appended them.
// It ignores FailAppend.
func (s *MemoryStore) AppendFromOtherWriter(docID string, ops ...crdt.Op) (AppendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log(docID).append(ops)
}

func (l *memoryLog) append(ops []crdt.Op) (AppendResult, error) {
	for _, op := range ops {
		if seq, ok := l.seq[op.OpID()]; ok && l.ops[seq-1] != op {
			return AppendResult{}, ConflictError{Client: op.OpID().Client}
		}
	}
	res := AppendResult{Seqs: make([]uint64, len(ops))}
	for i, op := range ops {
		seq, ok := l.seq[op.OpID()]
		if !ok {
			l.ops = append(l.ops, op)
			seq = uint64(len(l.ops))
			l.seq[op.OpID()] = seq
			if res.FirstNewSeq == 0 {
				res.FirstNewSeq = seq
			}
		}
		res.Seqs[i] = seq
	}
	res.OpSeq = uint64(len(l.ops))
	return res, nil
}

// ReportVector records the report, replacing any row for the same replica.
func (s *MemoryStore) ReportVector(_ context.Context, docID string, replica crdt.ClientID, vector crdt.VersionVector, homeSeq uint64, resyncPending bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	byReplica := s.reports[docID]
	if byReplica == nil {
		byReplica = make(map[crdt.ClientID]StoredReport)
		s.reports[docID] = byReplica
	}
	byReplica[replica] = StoredReport{Replica: replica, Vector: vector.Clone(), HomeSeq: homeSeq, ResyncPending: resyncPending}
	return nil
}

// Report returns the recorded report for replica.
func (s *MemoryStore) Report(docID string, replica crdt.ClientID) (StoredReport, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rep, ok := s.reports[docID][replica]
	if !ok {
		return StoredReport{}, false
	}
	rep.Vector = rep.Vector.Clone()
	return rep, true
}

// ReadOperations returns up to limit ops after seq after.
func (s *MemoryStore) ReadOperations(_ context.Context, docID string, after uint64, limit int) (fanout.Batch, LogMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.log(docID)
	opSeq := uint64(len(l.ops))
	if after > opSeq {
		return fanout.Batch{}, LogMeta{}, fmt.Errorf("after %d is beyond op seq %d", after, opSeq)
	}
	end := min(opSeq, after+uint64(limit))
	if end == after {
		return fanout.Batch{}, LogMeta{OpSeq: opSeq, Collected: l.liveCollected.Clone()}, nil
	}
	return fanout.Batch{FirstSeq: after + 1, Ops: append([]crdt.Op(nil), l.ops[after:end]...)},
		LogMeta{OpSeq: opSeq, LiveSeq: l.liveSeq, Collected: l.liveCollected.Clone()}, nil
}

// SetLiveMeta sets live snapshot metadata returned by ReadOperations.
func (s *MemoryStore) SetLiveMeta(docID string, seq uint64, collected crdt.VersionVector) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.log(docID)
	l.liveSeq = seq
	l.liveCollected = collected.Clone()
}

// FailAppend makes the next Append calls return err. Pass nil to resume.
func (s *MemoryStore) FailAppend(err error) {
	s.mu.Lock()
	s.appendErr = err
	s.mu.Unlock()
}

// Loads returns how many times Load has been called.
func (s *MemoryStore) Loads() int64 {
	return s.loads.Load()
}

// Doc replays the stored log, or returns nil if the document has none.
func (s *MemoryStore) Doc(docID string) *crdt.Doc {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.logs[docID]
	if l == nil {
		return nil
	}
	doc, err := replay(l.ops)
	if err != nil {
		return nil
	}
	return doc
}

func replay(ops []crdt.Op) (*crdt.Doc, error) {
	doc := crdt.New()
	for i, op := range ops {
		if _, err := doc.Apply(op); err != nil {
			return nil, fmt.Errorf("replay seq %d: %w", i+1, err)
		}
	}
	return doc, nil
}
