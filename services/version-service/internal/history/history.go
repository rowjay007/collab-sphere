// Package history derives the version DAG of design §3 from document-service
// logs: a chain start per document, an auto node at every multiple of 100,
// and nodes at arbitrary seqs for checkpoints and fork bases.
package history

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/dag"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/store"
)

const (
	AutoEvery = 100
	readLimit = 10_000

	LabelGenesis  = "genesis"
	LabelAuto     = "auto"
	LabelFork     = "fork"
	LabelForkBase = "fork-base"
	LabelMerge    = "merge"
	LabelRestore  = "restore"
)

func CheckpointLabel(name string) string { return "checkpoint:" + name }

type History struct {
	store *store.Store
	docs  Docs
	hooks map[string]func() error
}

func New(st *store.Store, docs Docs) *History {
	return &History{store: st, docs: docs}
}

// state caches the document state of one node across consecutive puts.
type state struct {
	node dag.NodeID
	doc  *crdt.Doc
}

// Materialize emits every auto node at or below the document's current
// op_seq and returns the document as read.
func (h *History) Materialize(ctx context.Context, id uuid.UUID) (Document, error) {
	doc, err := h.docs.Get(ctx, id)
	if err != nil {
		return Document{}, err
	}
	if holdFrom(ctx) == nil {
		if err := h.startChain(ctx, id); err != nil {
			return Document{}, err
		}
	} else if err := h.ensureStarted(ctx, doc); err != nil {
		return Document{}, err
	}
	var cache state
	for {
		done := false
		err := h.withDoc(ctx, []uuid.UUID{id}, func(tx store.Tx) error {
			tip, ok, err := tx.Tip(ctx, id)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("chain of %s has no start", id)
			}
			next := (tip.Node.Seq/AutoEvery + 1) * AutoEvery
			if next > doc.OpSeq {
				done = true
				return nil
			}
			_, err = h.put(ctx, tx, id, next, "", &cache)
			return err
		})
		if err != nil {
			return Document{}, err
		}
		if done {
			return doc, nil
		}
	}
}

// startChain creates the document's first node and, for a fork, the
// ancestor chain that node names as its parent. Each document is started
// under its own lock. Call it before taking a merge or restore hold.
func (h *History) startChain(ctx context.Context, id uuid.UUID) error {
	if holdFrom(ctx) != nil {
		return fmt.Errorf("startChain under an existing hold")
	}
	doc, err := h.docs.Get(ctx, id)
	if err != nil {
		return err
	}
	if doc.ForkOf != nil {
		if err := h.startChain(ctx, *doc.ForkOf); err != nil {
			return err
		}
	}
	return h.ensureStarted(ctx, doc)
}

func (h *History) hasTip(ctx context.Context, id uuid.UUID) (bool, error) {
	var ok bool
	err := h.do(ctx, []uuid.UUID{id}, func(tx store.Tx) error {
		_, found, err := tx.Tip(ctx, id)
		ok = found
		return err
	})
	return ok, err
}

// ensureStarted creates the document's first node: genesis at 0 for a root
// document, or for a fork B of S at s the node B@s whose only parent is
// S@s. If the chain already exists it returns without touching the parent.
func (h *History) ensureStarted(ctx context.Context, doc Document) error {
	started, err := h.hasTip(ctx, doc.ID)
	if err != nil || started {
		return err
	}
	var (
		start     dag.Node
		canonical []byte
		labels    []string
	)
	if doc.ForkOf == nil {
		empty, err := crdt.CanonicalBytes(crdt.New())
		if err != nil {
			return err
		}
		start = dag.Node{Doc: doc.ID, Seq: 0, Content: dag.ContentHash(empty)}
		canonical, labels = empty, []string{LabelGenesis}
	} else {
		base, err := h.forkBase(ctx, *doc.ForkOf, doc.ForkSeq)
		if err != nil {
			return fmt.Errorf("fork base of %s: %w", doc.ID, err)
		}
		start = dag.Node{Doc: doc.ID, Seq: doc.ForkSeq, Content: base.Node.Content, Parents: []dag.NodeID{base.ID}}
		labels = labelsAt(doc.ForkSeq, LabelFork)
	}
	return h.do(ctx, []uuid.UUID{doc.ID}, func(tx store.Tx) error {
		if _, ok, err := tx.Tip(ctx, doc.ID); ok || err != nil {
			return err
		}
		_, err := tx.PutNode(ctx, start, canonical, store.Meta{Labels: labels})
		return err
	})
}

func (h *History) forkBase(ctx context.Context, parent uuid.UUID, seq uint64) (store.Version, error) {
	if hold := holdFrom(ctx); hold != nil && !hold.Covers([]uuid.UUID{parent}) {
		return h.store.VersionAt(ctx, parent, seq)
	}
	return h.nodeAt(ctx, parent, seq, LabelForkBase)
}

// nodeAt materializes the document and returns its node at seq, creating it
// with createLabel if it is not a node yet.
func (h *History) nodeAt(ctx context.Context, id uuid.UUID, seq uint64, createLabel string) (store.Version, error) {
	doc, err := h.Materialize(ctx, id)
	if err != nil {
		return store.Version{}, err
	}
	if seq > doc.OpSeq {
		return store.Version{}, fmt.Errorf("%w: seq %d exceeds op_seq %d of %s", ErrOutOfRange, seq, doc.OpSeq, id)
	}
	var v store.Version
	err = h.withDoc(ctx, []uuid.UUID{id}, func(tx store.Tx) error {
		v, err = h.put(ctx, tx, id, seq, createLabel, &state{})
		return err
	})
	return v, err
}

// put returns the node at doc@seq, creating it if missing. A new node's only
// parent is the document's node with the greatest seq below it, and its
// content is that node's state with the log ops up to seq applied.
func (h *History) put(ctx context.Context, tx store.Tx, id uuid.UUID, seq uint64, createLabel string, cache *state) (store.Version, error) {
	return h.putNode(ctx, tx, id, seq, nil, store.Meta{Labels: labelsAt(seq, createLabel)}, cache)
}

func (h *History) putNode(ctx context.Context, tx store.Tx, id uuid.UUID, seq uint64, extra []dag.NodeID, meta store.Meta, cache *state) (store.Version, error) {
	prev, ok, err := tx.NodeAtOrBelow(ctx, id, seq)
	if err != nil {
		return store.Version{}, err
	}
	if !ok {
		return store.Version{}, fmt.Errorf("chain of %s starts above %d", id, seq)
	}
	if prev.Node.Seq == seq {
		return prev, nil
	}
	doc, err := h.stateOf(ctx, tx, prev, cache)
	if err != nil {
		return store.Version{}, err
	}
	for after := prev.Node.Seq; after < seq; {
		ops, err := h.docs.Read(ctx, id, after, int(min(seq-after, readLimit)))
		if err != nil {
			return store.Version{}, err
		}
		if len(ops) == 0 {
			return store.Version{}, fmt.Errorf("log of %s ends at %d, before %d", id, after, seq)
		}
		for _, op := range ops {
			st, err := doc.Apply(op)
			if err != nil {
				return store.Version{}, fmt.Errorf("apply %s@%d: %w", id, after+1, err)
			}
			if st == crdt.Pending {
				return store.Version{}, fmt.Errorf("op %s at %s@%d has a missing dependency", op.OpID(), id, after+1)
			}
			after++
		}
	}
	canonical, err := crdt.CanonicalBytes(doc)
	if err != nil {
		return store.Version{}, err
	}
	parents := append([]dag.NodeID{prev.ID}, extra...)
	if meta.Labels == nil {
		meta.Labels = labelsAt(seq, "")
	}
	node := dag.Node{Doc: id, Seq: seq, Content: dag.ContentHash(canonical), Parents: parents}
	v, err := tx.PutNode(ctx, node, canonical, meta)
	if err != nil {
		return store.Version{}, err
	}
	*cache = state{node: v.ID, doc: doc}
	return v, nil
}

func (h *History) stateOf(ctx context.Context, tx store.Tx, v store.Version, cache *state) (*crdt.Doc, error) {
	if cache.doc != nil && cache.node == v.ID {
		doc := cache.doc
		*cache = state{}
		return doc, nil
	}
	b, err := tx.Blob(ctx, v.Node.Content)
	if err != nil {
		return nil, err
	}
	return crdt.FromCanonical(b)
}

func labelsAt(seq uint64, label string) []string {
	var labels []string
	if label != "" {
		labels = append(labels, label)
	}
	if seq > 0 && seq%AutoEvery == 0 {
		labels = append(labels, LabelAuto)
	}
	return labels
}

// Checkpoint names the node at the document's current op_seq.
func (h *History) Checkpoint(ctx context.Context, id uuid.UUID, name, requestID string) (store.Version, error) {
	if name == "" || requestID == "" {
		return store.Version{}, fmt.Errorf("%w: name and request_id are required", ErrInvalidArgument)
	}
	if cp, ok, err := h.store.Checkpoint(ctx, id, name); err != nil {
		return store.Version{}, err
	} else if ok && cp.RequestID == requestID {
		return h.version(ctx, cp.Node)
	}
	doc, err := h.Materialize(ctx, id)
	if err != nil {
		return store.Version{}, err
	}
	var node dag.NodeID
	err = h.withDoc(ctx, []uuid.UUID{id}, func(tx store.Tx) error {
		v, err := h.put(ctx, tx, id, doc.OpSeq, "", &state{})
		if err != nil {
			return err
		}
		cp, err := tx.PutCheckpoint(ctx, id, name, v.ID, requestID)
		if err != nil {
			return err
		}
		node = cp.Node
		switch {
		case cp.Node == v.ID:
			return tx.AddLabel(ctx, v.ID, CheckpointLabel(name))
		case cp.RequestID == requestID:
			return nil
		default:
			return fmt.Errorf("%w: checkpoint %q of %s names %s", ErrAlreadyExists, name, id, cp.Node)
		}
	})
	if err != nil {
		return store.Version{}, err
	}
	return h.version(ctx, node)
}

// Branch forks the version's document at the version's seq and returns the
// fork and its first node.
func (h *History) Branch(ctx context.Context, vid dag.NodeID, title, requestID string) (uuid.UUID, store.Version, error) {
	v, err := h.version(ctx, vid)
	if err != nil {
		return uuid.UUID{}, store.Version{}, err
	}
	fork, err := h.docs.Fork(ctx, v.Node.Doc, v.Node.Seq, title, requestID)
	if err != nil {
		return uuid.UUID{}, store.Version{}, err
	}
	if _, err := h.Materialize(ctx, fork.ID); err != nil {
		return uuid.UUID{}, store.Version{}, err
	}
	start, err := h.store.VersionAt(ctx, fork.ID, fork.ForkSeq)
	if err != nil {
		return uuid.UUID{}, store.Version{}, mapStore(err)
	}
	return fork.ID, start, nil
}

// Diff compares the visible text of two versions.
func (h *History) Diff(ctx context.Context, a, b dag.NodeID) ([]crdt.Span, error) {
	va, err := h.version(ctx, a)
	if err != nil {
		return nil, err
	}
	vb, err := h.version(ctx, b)
	if err != nil {
		return nil, err
	}
	if va.Node.Content == vb.Node.Content {
		return nil, nil
	}
	da, err := h.load(ctx, va.Node.Content)
	if err != nil {
		return nil, err
	}
	db, err := h.load(ctx, vb.Node.Content)
	if err != nil {
		return nil, err
	}
	return crdt.Diff(da, db)
}

// Frontier returns the document at its head seq plus every document its
// nodes reach through cross-document parents, at the highest seq reached.
func (h *History) Frontier(ctx context.Context, id uuid.UUID) (map[uuid.UUID]uint64, error) {
	doc, err := h.Materialize(ctx, id)
	if err != nil {
		return nil, err
	}
	reach, err := h.store.Reach(ctx, id)
	if err != nil {
		return nil, err
	}
	reach[id] = doc.OpSeq
	return reach, nil
}

func (h *History) Versions(ctx context.Context, id uuid.UUID, after int64, limit int) ([]store.Version, error) {
	if _, err := h.Materialize(ctx, id); err != nil {
		return nil, err
	}
	return h.store.Versions(ctx, id, after, limit)
}

func (h *History) Version(ctx context.Context, id dag.NodeID) (store.Version, error) {
	return h.version(ctx, id)
}

func (h *History) Blob(ctx context.Context, hash dag.Hash) ([]byte, error) {
	b, err := h.store.Blob(ctx, hash)
	return b, mapStore(err)
}

func (h *History) version(ctx context.Context, id dag.NodeID) (store.Version, error) {
	v, err := h.store.Version(ctx, id)
	return v, mapStore(err)
}

func (h *History) load(ctx context.Context, hash dag.Hash) (*crdt.Doc, error) {
	b, err := h.Blob(ctx, hash)
	if err != nil {
		return nil, err
	}
	return crdt.FromCanonical(b)
}

func mapStore(err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("%w: %v", ErrNotFound, err)
	}
	return err
}
