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

// AppendResult reports where Append put each op. Seqs is parallel to the
// appended ops. An op already in the log keeps its existing seq. Newly stored
// ops hold exactly FirstNewSeq..OpSeq, and FirstNewSeq is 0 when none were new.
type AppendResult struct {
	OpSeq       uint64
	FirstNewSeq uint64
	Seqs        []uint64
}

// Store is the consumer-side persistence seam for a document hub.
type Store interface {
	// Load returns the document and the seq of the last op it holds.
	Load(ctx context.Context, docID string) (*crdt.Doc, uint64, error)
	Append(ctx context.Context, docID string, ops []crdt.Op) (AppendResult, error)
	// ReadOperations returns up to limit ops after seq after, and the log's last seq.
	ReadOperations(ctx context.Context, docID string, after uint64, limit int) (fanout.Batch, uint64, error)
}

// MemoryStore is an in-process Store for tests. Each document is an ordered
// op log deduplicated by op ID, like document-service.
type MemoryStore struct {
	mu        sync.Mutex
	logs      map[string]*memoryLog
	appendErr error
	loads     atomic.Int64
}

type memoryLog struct {
	ops []crdt.Op
	seq map[crdt.ID]uint64
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{logs: make(map[string]*memoryLog)}
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
			return AppendResult{}, fmt.Errorf("op %s conflicts with stored op", op.OpID())
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

// ReadOperations returns up to limit ops after seq after.
func (s *MemoryStore) ReadOperations(_ context.Context, docID string, after uint64, limit int) (fanout.Batch, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.log(docID)
	opSeq := uint64(len(l.ops))
	if after > opSeq {
		return fanout.Batch{}, opSeq, fmt.Errorf("after %d is beyond op seq %d", after, opSeq)
	}
	end := min(opSeq, after+uint64(limit))
	if end == after {
		return fanout.Batch{}, opSeq, nil
	}
	return fanout.Batch{FirstSeq: after + 1, Ops: append([]crdt.Op(nil), l.ops[after:end]...)}, opSeq, nil
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
