package hub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

var ErrArchived = errors.New("document archived")

// Store is the consumer-side persistence seam for a document hub.
type Store interface {
	Load(ctx context.Context, docID string) (*crdt.Doc, error)
	Append(ctx context.Context, docID string, ops []crdt.Op) error
}

// MemoryStore is an in-process Store for tests.
type MemoryStore struct {
	mu        sync.Mutex
	docs      map[string]*crdt.Doc
	appendErr error
	loads     atomic.Int64
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{docs: make(map[string]*crdt.Doc)}
}

// Load returns a snapshot clone of the document, creating an empty one if needed.
func (s *MemoryStore) Load(_ context.Context, docID string) (*crdt.Doc, error) {
	s.loads.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.docs[docID] == nil {
		s.docs[docID] = crdt.New()
	}
	return cloneDoc(s.docs[docID])
}

// Append applies ops to the stored document unless FailAppend is set.
func (s *MemoryStore) Append(_ context.Context, docID string, ops []crdt.Op) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.appendErr != nil {
		return s.appendErr
	}
	if s.docs[docID] == nil {
		s.docs[docID] = crdt.New()
	}
	for _, op := range ops {
		if _, err := s.docs[docID].Apply(op); err != nil {
			return err
		}
	}
	return nil
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

// Doc returns a snapshot clone of the stored document, or nil if none exists.
func (s *MemoryStore) Doc(docID string) *crdt.Doc {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.docs[docID] == nil {
		return nil
	}
	doc, err := cloneDoc(s.docs[docID])
	if err != nil {
		return nil
	}
	return doc
}

func cloneDoc(doc *crdt.Doc) (*crdt.Doc, error) {
	return crdt.FromSnapshot(doc.Snapshot())
}
