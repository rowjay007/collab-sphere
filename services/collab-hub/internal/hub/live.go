package hub

import (
	"context"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func collectedNewer(next, cur crdt.VersionVector) bool {
	for client, clock := range next {
		if clock > cur[client] {
			return true
		}
	}
	return false
}

// liveBuild is the loop's live doc rebuild state. tried is the store's live
// collected vector behind the last build that was not swapped in. retryAt is
// set when that build only lost a race with delivery; a build that could
// never help leaves it zero and waits for a newer collected vector.
type liveBuild struct {
	building bool
	tried    crdt.VersionVector
	retryAt  time.Time
}

type liveBuildEvent struct {
	doc       *crdt.Doc
	seq       uint64
	collected crdt.VersionVector
	err       error
}

func (liveBuildEvent) isEvent() {}

func (h *Hub) maybeStartLiveBuild(collected crdt.VersionVector) {
	if h.dead || h.live.building || h.doc == nil || len(collected) == 0 {
		return
	}
	if !collectedNewer(collected, h.doc.Collected()) {
		return
	}
	if h.live.tried != nil && !collectedNewer(collected, h.live.tried) &&
		(h.live.retryAt.IsZero() || time.Now().Before(h.live.retryAt)) {
		return
	}
	h.live.building = true
	docID := h.docID
	store := h.store
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), loadTimeout)
		doc, seq, err := store.Load(ctx, docID)
		cancel()
		_ = h.send(context.Background(), liveBuildEvent{doc: doc, seq: seq, collected: collected, err: err})
	}()
}

func (h *Hub) handleLiveBuild(ev liveBuildEvent) bool {
	h.live.building = false
	if h.dead || h.doc == nil {
		return false
	}
	switch {
	case ev.err == nil && ev.doc != nil && !collectedNewer(ev.doc.Collected(), h.doc.Collected()):
		h.live = liveBuild{tried: ev.collected}
	case ev.err != nil || ev.doc == nil || !h.canSwapLiveDoc(ev.seq):
		h.live = liveBuild{tried: ev.collected, retryAt: time.Now().Add(h.registry.idleSync)}
	default:
		h.doc = ev.doc
		h.live = liveBuild{}
	}
	return false
}

func (h *Hub) canSwapLiveDoc(builtSeq uint64) bool {
	if h.seq == nil {
		return false
	}
	if open, _ := h.seq.Gap(); open {
		return false
	}
	return h.delivered() == builtSeq && h.appendThrough <= h.delivered()
}
