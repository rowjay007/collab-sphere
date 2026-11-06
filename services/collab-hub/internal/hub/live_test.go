package hub

import (
	"testing"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/fanout"
)

func TestCanSwapLiveDocBlocksPendingAppend(t *testing.T) {
	h := &Hub{seq: fanout.NewSequencer(3), appendThrough: 4}
	if h.canSwapLiveDoc(3) {
		t.Fatal("swap allowed with pending append")
	}
	h.appendThrough = 0
	if !h.canSwapLiveDoc(3) {
		t.Fatal("swap refused when caught up")
	}
}

func TestCanSwapLiveDocBlocksGap(t *testing.T) {
	h := &Hub{seq: fanout.NewSequencer(1)}
	h.seq.Offer(fanout.Batch{FirstSeq: 3, Ops: []crdt.Op{insert(1, 3)}})
	if h.canSwapLiveDoc(3) {
		t.Fatal("swap allowed with open gap")
	}
}

func TestCollectedNewer(t *testing.T) {
	cur := crdt.VersionVector{1: 2}
	next := crdt.VersionVector{1: 3}
	if !collectedNewer(next, cur) {
		t.Fatal("expected newer")
	}
	if collectedNewer(cur, next) {
		t.Fatal("expected not newer")
	}
}
