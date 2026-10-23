package history

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/dag"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/store"
)

const (
	appendChunk = 10_000

	CrashAfterPersist = "after_persist"
	CrashAfterAppend  = "after_append"
	CrashAfterNode    = "after_node"
)

// ErrInjected is returned by a crash hook so a unit test can resume.
var ErrInjected = errors.New("history: injected crash")

type MergeResult struct {
	Version  store.Version
	Appended int
	Noop     bool
}

func (h *History) SetCrashHook(point string, fn func() error) {
	if h.hooks == nil {
		h.hooks = map[string]func() error{}
	}
	h.hooks[point] = fn
}

func (h *History) crash(point string) error {
	if h.hooks != nil {
		if fn := h.hooks[point]; fn != nil {
			return fn()
		}
	}
	if os.Getenv("VERSION_CRASH_AT") == point {
		os.Exit(1)
	}
	return nil
}

func (h *History) Merge(ctx context.Context, src, dst uuid.UUID, requestID string) (MergeResult, error) {
	if requestID == "" {
		return MergeResult{}, fmt.Errorf("%w: request_id is required", ErrInvalidArgument)
	}
	if src == dst {
		return MergeResult{}, fmt.Errorf("%w: source and target are the same document", ErrInvalidArgument)
	}
	if err := h.startChain(ctx, src); err != nil {
		return MergeResult{}, err
	}
	if err := h.startChain(ctx, dst); err != nil {
		return MergeResult{}, err
	}
	ctx, unlock, err := h.ensureHold(ctx, []uuid.UUID{src, dst})
	if err != nil {
		return MergeResult{}, err
	}
	defer unlock()

	if r, ok, err := h.mergeIfKnown(ctx, requestID, src, dst); err != nil || ok {
		return r, err
	}

	srcDoc, err := h.docs.Get(ctx, src)
	if err != nil {
		return MergeResult{}, err
	}
	dstDoc, err := h.docs.Get(ctx, dst)
	if err != nil {
		return MergeResult{}, err
	}
	if srcDoc.ForkRoot != dstDoc.ForkRoot {
		return MergeResult{}, fmt.Errorf("%w: %s and %s are not in the same fork tree", ErrFailedPrecondition, src, dst)
	}

	if _, err := h.Materialize(ctx, src); err != nil {
		return MergeResult{}, err
	}
	srcDoc, err = h.docs.Get(ctx, src)
	if err != nil {
		return MergeResult{}, err
	}
	nb, err := h.nodeAt(ctx, src, srcDoc.OpSeq, "")
	if err != nil {
		return MergeResult{}, err
	}
	nbState, err := h.load(ctx, nb.Node.Content)
	if err != nil {
		return MergeResult{}, err
	}
	head, _, err := h.docs.LoadState(ctx, dst)
	if err != nil {
		return MergeResult{}, err
	}

	if head.Vector().CoversAll(nbState.Vector()) {
		covered, err := h.firstCovering(ctx, dst, nbState.Vector())
		if err != nil {
			return MergeResult{}, err
		}
		err = h.withDoc(ctx, []uuid.UUID{src, dst}, func(tx store.Tx) error {
			if _, ok, err := tx.Mutation(ctx, requestID); ok || err != nil {
				return err
			}
			src := src
			id := nb.ID
			return tx.InsertMutation(ctx, store.Mutation{
				RequestID:  requestID,
				Doc:        dst,
				Kind:       store.KindMerge,
				State:      store.StateDone,
				SourceDoc:  &src,
				SourceNode: &id,
				ResultNode: &covered.ID,
				Noop:       true,
			})
		})
		if err != nil {
			return MergeResult{}, err
		}
		if r, ok, err := h.mergeIfKnown(ctx, requestID, src, dst); err != nil || ok {
			return r, err
		}
		return MergeResult{Version: covered, Noop: true}, nil
	}

	plan, err := mergePlan(head, nbState, func() ([]crdt.Op, error) {
		return h.readAll(ctx, src, nb.Node.Seq)
	})
	if err != nil {
		return MergeResult{}, err
	}
	raw, err := encodePlan(plan)
	if err != nil {
		return MergeResult{}, err
	}
	srcCopy := src
	nbID := nb.ID
	err = h.withDoc(ctx, []uuid.UUID{src, dst}, func(tx store.Tx) error {
		if _, ok, err := tx.Mutation(ctx, requestID); ok || err != nil {
			return err
		}
		return tx.InsertMutation(ctx, store.Mutation{
			RequestID:  requestID,
			Doc:        dst,
			Kind:       store.KindMerge,
			State:      store.StatePlanned,
			SourceDoc:  &srcCopy,
			SourceNode: &nbID,
			Plan:       raw,
			Appended:   len(plan),
		})
	})
	if err != nil {
		return MergeResult{}, err
	}
	if err := h.crash(CrashAfterPersist); err != nil {
		return MergeResult{}, err
	}
	return h.mergeIfKnownErr(ctx, requestID, src, dst)
}

func (h *History) Restore(ctx context.Context, doc uuid.UUID, vid dag.NodeID, requestID string) (store.Version, error) {
	if requestID == "" {
		return store.Version{}, fmt.Errorf("%w: request_id is required", ErrInvalidArgument)
	}
	if err := h.startChain(ctx, doc); err != nil {
		return store.Version{}, err
	}
	ctx, unlock, err := h.ensureHold(ctx, []uuid.UUID{doc})
	if err != nil {
		return store.Version{}, err
	}
	defer unlock()

	if v, ok, err := h.restoreIfKnown(ctx, requestID, doc, vid); err != nil || ok {
		return v, err
	}

	target, err := h.version(ctx, vid)
	if err != nil {
		return store.Version{}, err
	}
	dstDoc, err := h.docs.Get(ctx, doc)
	if err != nil {
		return store.Version{}, err
	}
	srcDoc, err := h.docs.Get(ctx, target.Node.Doc)
	if err != nil {
		return store.Version{}, err
	}
	if srcDoc.ForkRoot != dstDoc.ForkRoot {
		return store.Version{}, fmt.Errorf("%w: version %s is not in %s's fork tree", ErrFailedPrecondition, vid, doc)
	}

	head, _, err := h.docs.LoadState(ctx, doc)
	if err != nil {
		return store.Version{}, err
	}
	wanted, err := h.load(ctx, target.Node.Content)
	if err != nil {
		return store.Version{}, err
	}
	if !head.Vector().CoversAll(wanted.Vector()) {
		return store.Version{}, fmt.Errorf("%w: head of %s does not contain %s", ErrFailedPrecondition, doc, vid)
	}

	replica, err := h.systemReplica(ctx, doc)
	if err != nil {
		return store.Version{}, err
	}
	plan, err := crdt.PlanRestore(head, wanted, replica)
	if err != nil {
		return store.Version{}, err
	}
	if len(plan) == 0 {
		cur, err := h.nodeAtHead(ctx, doc)
		if err != nil {
			return store.Version{}, err
		}
		err = h.withDoc(ctx, []uuid.UUID{doc}, func(tx store.Tx) error {
			if _, ok, err := tx.Mutation(ctx, requestID); ok || err != nil {
				return err
			}
			return tx.InsertMutation(ctx, store.Mutation{
				RequestID:  requestID,
				Doc:        doc,
				Kind:       store.KindRestore,
				State:      store.StateDone,
				SourceNode: &vid,
				ResultNode: &cur.ID,
			})
		})
		if err != nil {
			return store.Version{}, err
		}
		return cur, nil
	}

	raw, err := encodePlan(plan)
	if err != nil {
		return store.Version{}, err
	}
	err = h.withDoc(ctx, []uuid.UUID{doc}, func(tx store.Tx) error {
		if _, ok, err := tx.Mutation(ctx, requestID); ok || err != nil {
			return err
		}
		return tx.InsertMutation(ctx, store.Mutation{
			RequestID:  requestID,
			Doc:        doc,
			Kind:       store.KindRestore,
			State:      store.StatePlanned,
			SourceNode: &vid,
			Plan:       raw,
			Appended:   len(plan),
		})
	})
	if err != nil {
		return store.Version{}, err
	}
	if err := h.crash(CrashAfterPersist); err != nil {
		return store.Version{}, err
	}
	v, _, err := h.restoreIfKnown(ctx, requestID, doc, vid)
	return v, err
}

func (h *History) mergeIfKnownErr(ctx context.Context, requestID string, src, dst uuid.UUID) (MergeResult, error) {
	r, ok, err := h.mergeIfKnown(ctx, requestID, src, dst)
	if err != nil {
		return MergeResult{}, err
	}
	if !ok {
		return MergeResult{}, fmt.Errorf("merge %s vanished after persist", requestID)
	}
	return r, nil
}

func (h *History) mergeIfKnown(ctx context.Context, requestID string, src, dst uuid.UUID) (MergeResult, bool, error) {
	m, ok, err := h.store.Mutation(ctx, requestID)
	if err != nil || !ok {
		return MergeResult{}, false, err
	}
	if m.Kind != store.KindMerge || m.Doc != dst || m.SourceDoc == nil || *m.SourceDoc != src {
		return MergeResult{}, true, fmt.Errorf("%w: request_id %s is a different mutation", ErrAlreadyExists, requestID)
	}
	if m.State == store.StateFailed {
		return MergeResult{}, true, fmt.Errorf("%w: mutation %s failed", ErrFailedPrecondition, requestID)
	}
	if m.State != store.StateDone {
		if err := h.drive(ctx, m); err != nil {
			return MergeResult{}, true, err
		}
		m, ok, err = h.store.Mutation(ctx, requestID)
		if err != nil || !ok {
			return MergeResult{}, true, err
		}
	}
	if m.ResultNode == nil {
		return MergeResult{}, true, fmt.Errorf("merge %s has no result", requestID)
	}
	v, err := h.version(ctx, *m.ResultNode)
	if err != nil {
		return MergeResult{}, true, err
	}
	return MergeResult{Version: v, Appended: m.Appended, Noop: m.Noop}, true, nil
}

func (h *History) restoreIfKnown(ctx context.Context, requestID string, doc uuid.UUID, vid dag.NodeID) (store.Version, bool, error) {
	m, ok, err := h.store.Mutation(ctx, requestID)
	if err != nil || !ok {
		return store.Version{}, false, err
	}
	if m.Kind != store.KindRestore || m.Doc != doc || m.SourceNode == nil || *m.SourceNode != vid {
		return store.Version{}, true, fmt.Errorf("%w: request_id %s is a different mutation", ErrAlreadyExists, requestID)
	}
	if m.State == store.StateFailed {
		return store.Version{}, true, fmt.Errorf("%w: mutation %s failed", ErrFailedPrecondition, requestID)
	}
	if m.State != store.StateDone {
		if err := h.drive(ctx, m); err != nil {
			return store.Version{}, true, err
		}
		m, ok, err = h.store.Mutation(ctx, requestID)
		if err != nil || !ok {
			return store.Version{}, true, err
		}
	}
	if m.ResultNode == nil {
		return store.Version{}, true, fmt.Errorf("restore %s has no result", requestID)
	}
	v, err := h.version(ctx, *m.ResultNode)
	return v, true, err
}

func (h *History) drive(ctx context.Context, m store.Mutation) error {
	if m.State == store.StateDone {
		return nil
	}
	if m.State == store.StateFailed {
		return fmt.Errorf("%w: mutation %s failed", ErrFailedPrecondition, m.RequestID)
	}
	doc, err := h.docs.Get(ctx, m.Doc)
	if err != nil {
		return err
	}
	if holdFrom(ctx) == nil {
		if err := h.startChain(ctx, m.Doc); err != nil {
			return err
		}
	} else if err := h.ensureStarted(ctx, doc); err != nil {
		return err
	}
	ctx, unlock, err := h.ensureHold(ctx, []uuid.UUID{m.Doc})
	if err != nil {
		return err
	}
	defer unlock()

	ops, err := decodePlan(m.Plan)
	if err != nil {
		return err
	}
	var seqs []uint64
	for i := 0; i < len(ops); i += appendChunk {
		end := min(i+appendChunk, len(ops))
		res, err := h.docs.Append(ctx, m.Doc, ops[i:end])
		if err != nil {
			if isOpConflict(err) {
				if markErr := h.do(ctx, []uuid.UUID{m.Doc}, func(tx store.Tx) error {
					return tx.SetMutationState(ctx, m.RequestID, store.StateFailed)
				}); markErr != nil {
					return markErr
				}
				return fmt.Errorf("%w: %v", ErrFailedPrecondition, err)
			}
			return err
		}
		seqs = append(seqs, res.Seqs...)
	}
	if err := h.crash(CrashAfterAppend); err != nil {
		return err
	}
	if err := h.do(ctx, []uuid.UUID{m.Doc}, func(tx store.Tx) error {
		return tx.SetMutationState(ctx, m.RequestID, store.StateAppended)
	}); err != nil {
		return err
	}

	hseq := maxSeq(seqs)
	if len(ops) == 0 {
		doc, err := h.docs.Get(ctx, m.Doc)
		if err != nil {
			return err
		}
		hseq = doc.OpSeq
	}

	var result store.Version
	err = h.do(ctx, []uuid.UUID{m.Doc}, func(tx store.Tx) error {
		if err := h.materializeBelow(ctx, tx, m.Doc, hseq); err != nil {
			return err
		}
		var extra []dag.NodeID
		meta := store.Meta{Labels: labelsAt(hseq, LabelRestore)}
		if m.Kind == store.KindMerge {
			meta.Labels = labelsAt(hseq, LabelMerge)
			if m.SourceNode != nil {
				extra = []dag.NodeID{*m.SourceNode}
			}
		} else if m.SourceNode != nil {
			meta.RestoredFrom = m.SourceNode[:]
		}
		result, err = h.putNode(ctx, tx, m.Doc, hseq, extra, meta, &state{})
		return err
	})
	if err != nil {
		return err
	}
	if err := h.crash(CrashAfterNode); err != nil {
		return err
	}
	return h.do(ctx, []uuid.UUID{m.Doc}, func(tx store.Tx) error {
		return tx.FinishMutation(ctx, m.RequestID, result.ID, m.Appended, m.Noop)
	})
}

func (h *History) materializeBelow(ctx context.Context, tx store.Tx, id uuid.UUID, hseq uint64) error {
	for {
		tip, ok, err := tx.Tip(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("chain of %s has no start", id)
		}
		next := (tip.Node.Seq/AutoEvery + 1) * AutoEvery
		if next >= hseq {
			return nil
		}
		if _, err := h.put(ctx, tx, id, next, "", &state{}); err != nil {
			return err
		}
	}
}

func (h *History) firstCovering(ctx context.Context, doc uuid.UUID, vec crdt.VersionVector) (store.Version, error) {
	head, err := h.nodeAtHead(ctx, doc)
	if err != nil {
		return store.Version{}, err
	}
	d, err := h.load(ctx, head.Node.Content)
	if err != nil {
		return store.Version{}, err
	}
	if !d.Vector().CoversAll(vec) {
		return store.Version{}, fmt.Errorf("head of %s does not cover the source", doc)
	}
	// Containment is monotonic along one document's chain: if i < j then
	// state@j covers state@i. Binary search finds the earliest covering seq.
	lo, hi := uint64(0), head.Node.Seq
	found := head
	for lo <= hi {
		mid := lo + (hi-lo)/2
		v, ok, err := h.store.NodeAtOrBelow(ctx, doc, mid)
		if err != nil {
			return store.Version{}, err
		}
		if !ok {
			return store.Version{}, fmt.Errorf("no versions of %s at or below %d", doc, mid)
		}
		d, err := h.load(ctx, v.Node.Content)
		if err != nil {
			return store.Version{}, err
		}
		if d.Vector().CoversAll(vec) {
			found = v
			if v.Node.Seq == 0 {
				break
			}
			hi = v.Node.Seq - 1
		} else {
			lo = mid + 1
		}
	}
	return found, nil
}

func (h *History) nodeAtHead(ctx context.Context, id uuid.UUID) (store.Version, error) {
	doc, err := h.Materialize(ctx, id)
	if err != nil {
		return store.Version{}, err
	}
	return h.nodeAt(ctx, id, doc.OpSeq, "")
}

func isOpConflict(err error) bool {
	return errors.Is(err, ErrConflict) || errors.Is(err, crdt.ErrConflict)
}

func (h *History) systemReplica(ctx context.Context, doc uuid.UUID) (crdt.ClientID, error) {
	var id uint64
	var found bool
	err := h.withDoc(ctx, []uuid.UUID{doc}, func(tx store.Tx) error {
		var err error
		id, found, err = tx.SystemReplica(ctx, doc)
		return err
	})
	if err != nil {
		return 0, err
	}
	if found {
		return crdt.ClientID(id), nil
	}
	claimed, err := h.docs.ClaimReplica(ctx, doc, 0, true)
	if err != nil {
		return 0, err
	}
	err = h.withDoc(ctx, []uuid.UUID{doc}, func(tx store.Tx) error {
		cur, ok, err := tx.SystemReplica(ctx, doc)
		if err != nil {
			return err
		}
		if ok {
			id = cur
			return nil
		}
		if err := tx.SetSystemReplica(ctx, doc, uint64(claimed)); err != nil {
			return err
		}
		id = uint64(claimed)
		return nil
	})
	return crdt.ClientID(id), err
}

func (h *History) readAll(ctx context.Context, id uuid.UUID, upTo uint64) ([]crdt.Op, error) {
	var ops []crdt.Op
	for after := uint64(0); after < upTo; {
		chunk, err := h.docs.Read(ctx, id, after, int(min(upTo-after, readLimit)))
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			return nil, fmt.Errorf("log of %s ends at %d, before %d", id, after, upTo)
		}
		ops = append(ops, chunk...)
		after += uint64(len(chunk))
	}
	return ops, nil
}

func mergePlan(head, src *crdt.Doc, readSrc func() ([]crdt.Op, error)) ([]crdt.Op, error) {
	fromBlob := missing(head, src.Ops())
	if coversAfter(head, fromBlob, src.Vector()) {
		return fromBlob, nil
	}
	ops, err := readSrc()
	if err != nil {
		return nil, err
	}
	fromLog := missing(head, ops)
	if coversAfter(head, fromLog, src.Vector()) {
		return fromLog, nil
	}
	return nil, fmt.Errorf("neither canonical Ops() nor the source log reproduces the merge plan")
}

func missing(head *crdt.Doc, ops []crdt.Op) []crdt.Op {
	var out []crdt.Op
	for _, op := range ops {
		if !head.Contains(op.OpID()) {
			out = append(out, op)
		}
	}
	return out
}

func coversAfter(head *crdt.Doc, plan []crdt.Op, want crdt.VersionVector) bool {
	d, err := crdt.FromCanonical(mustCanonical(head))
	if err != nil {
		return false
	}
	for _, op := range plan {
		if _, err := d.Apply(op); err != nil {
			return false
		}
	}
	return d.Vector().CoversAll(want)
}

func mustCanonical(d *crdt.Doc) []byte {
	b, err := crdt.CanonicalBytes(d)
	if err != nil {
		return nil
	}
	return b
}

func encodePlan(ops []crdt.Op) ([]byte, error) {
	p := &versionv1.OpPlan{Operations: make([]*crdtv1.Operation, len(ops))}
	for i, op := range ops {
		p.Operations[i] = crdt.OpToProto(op)
	}
	return proto.Marshal(p)
}

func decodePlan(raw []byte) ([]crdt.Op, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	p := &versionv1.OpPlan{}
	if err := proto.Unmarshal(raw, p); err != nil {
		return nil, fmt.Errorf("decode plan: %w", err)
	}
	ops := make([]crdt.Op, len(p.GetOperations()))
	for i, pb := range p.GetOperations() {
		op, err := crdt.OpFromProto(pb)
		if err != nil {
			return nil, fmt.Errorf("decode plan op %d: %w", i, err)
		}
		ops[i] = op
	}
	return ops, nil
}

func maxSeq(seqs []uint64) uint64 {
	var h uint64
	for _, s := range seqs {
		if s > h {
			h = s
		}
	}
	return h
}
