// Package fanout orders sequenced ops for delivery and defines the seam
// through which hubs learn about ops other hubs appended.
package fanout

import (
	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

// Batch holds ops with the contiguous seqs FirstSeq..FirstSeq+len(Ops)-1.
type Batch struct {
	FirstSeq uint64
	Ops      []crdt.Op
}

// Next returns the seq after the batch's last op.
func (b Batch) Next() uint64 { return b.FirstSeq + uint64(len(b.Ops)) }

// clip keeps the ops whose seqs lie in [lo, hi).
func (b Batch) clip(lo, hi uint64) Batch {
	lo, hi = max(lo, b.FirstSeq), min(hi, b.Next())
	if lo >= hi {
		return Batch{}
	}
	return Batch{FirstSeq: lo, Ops: b.Ops[lo-b.FirstSeq : hi-b.FirstSeq]}
}

// Sink receives what a Bus learns for one document.
type Sink interface {
	RemoteOps(Batch)
	Presence(*presencev1.Event)
	// Resync means the bus may have lost messages and the hub should read the log.
	Resync()
}

// Subscription is one hub's attachment to a document's channel.
type Subscription interface {
	Ready() <-chan struct{}
	Close()
}

// Bus carries sequenced batches between hubs. It is a lossy hint: the log in
// document-service stays the source of truth.
type Bus interface {
	Join(docID string, sink Sink) Subscription
	Publish(docID string, b Batch)
}
