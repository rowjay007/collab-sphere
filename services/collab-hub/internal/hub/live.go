package hub

import (
	"context"

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

type liveBuildEvent struct {
	doc *crdt.Doc
	seq uint64
	err error
}

func (liveBuildEvent) isEvent() {}

func (h *Hub) maybeStartLiveBuild(collected crdt.VersionVector) {
	if h.dead || h.liveBuilding || h.doc == nil || len(collected) == 0 {
		return
	}
	if !collectedNewer(collected, h.doc.Collected()) {
		return
	}
	h.liveBuilding = true
	docID := h.docID
	store := h.store
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), loadTimeout)
		doc, seq, err := store.Load(ctx, docID)
		cancel()
		_ = h.send(context.Background(), liveBuildEvent{doc: doc, seq: seq, err: err})
	}()
}

func (h *Hub) handleLiveBuild(ev liveBuildEvent) bool {
	h.liveBuilding = false
	if h.dead || ev.err != nil || ev.doc == nil {
		return false
	}
	if h.canSwapLiveDoc(ev.seq) {
		h.doc = ev.doc
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
