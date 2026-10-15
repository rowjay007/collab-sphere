package fanout

import (
	"cmp"
	"math"
	"slices"
)

// Sequencer releases ops in seq order. It holds D, the highest seq delivered,
// and the disjoint ranges offered above D that wait for the gap below them.
type Sequencer struct {
	delivered uint64
	pending   []Batch
}

// NewSequencer starts with every seq up to delivered already delivered.
func NewSequencer(delivered uint64) *Sequencer {
	return &Sequencer{delivered: delivered}
}

// Offer buffers the part of b that is neither delivered nor buffered, then
// returns every batch that is now contiguous with D, in order.
func (s *Sequencer) Offer(b Batch) []Batch {
	var fresh []Batch
	lo := s.delivered + 1
	for _, p := range s.pending {
		fresh = appendNonEmpty(fresh, b.clip(lo, p.FirstSeq))
		lo = max(lo, p.Next())
	}
	fresh = appendNonEmpty(fresh, b.clip(lo, math.MaxUint64))
	s.pending = mergeAdjacent(append(s.pending, fresh...))

	var ready []Batch
	for len(s.pending) > 0 && s.pending[0].FirstSeq == s.delivered+1 {
		ready = append(ready, s.pending[0])
		s.delivered = s.pending[0].Next() - 1
		s.pending = s.pending[1:]
	}
	return ready
}

// Delivered returns D.
func (s *Sequencer) Delivered() uint64 { return s.delivered }

// Gap reports whether ranges wait above a missing D+1, and how many.
func (s *Sequencer) Gap() (open bool, buffered int) {
	return len(s.pending) > 0, len(s.pending)
}

func appendNonEmpty(batches []Batch, b Batch) []Batch {
	if len(b.Ops) == 0 {
		return batches
	}
	return append(batches, b)
}

func mergeAdjacent(batches []Batch) []Batch {
	slices.SortFunc(batches, func(a, b Batch) int { return cmp.Compare(a.FirstSeq, b.FirstSeq) })
	merged := batches[:0]
	for _, b := range batches {
		if n := len(merged); n > 0 && merged[n-1].Next() == b.FirstSeq {
			merged[n-1].Ops = slices.Concat(merged[n-1].Ops, b.Ops)
			continue
		}
		merged = append(merged, b)
	}
	return merged
}
