package fanout

import (
	"slices"
	"testing"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

// opAt is the op the log holds at seq, so a test can check every released op
// sits at its own seq.
func opAt(seq uint64) crdt.Op {
	return crdt.Insert{ID: crdt.ID{Client: 1, Clock: seq}, Parent: crdt.Root, Value: 'x'}
}

func batch(first, last uint64) Batch {
	b := Batch{FirstSeq: first}
	for seq := first; seq <= last; seq++ {
		b.Ops = append(b.Ops, opAt(seq))
	}
	return b
}

type span struct{ first, last uint64 }

func TestSequencer(t *testing.T) {
	type step struct {
		offer     Batch
		ready     []span
		delivered uint64
		buffered  int
	}
	tests := []struct {
		name  string
		start uint64
		steps []step
	}{
		{
			name: "in order",
			steps: []step{
				{offer: batch(1, 2), ready: []span{{1, 2}}, delivered: 2},
				{offer: batch(3, 3), ready: []span{{3, 3}}, delivered: 3},
			},
		},
		{
			name: "out of order",
			steps: []step{
				{offer: batch(4, 5), delivered: 0, buffered: 1},
				{offer: batch(2, 3), delivered: 0, buffered: 1},
				{offer: batch(1, 1), ready: []span{{1, 5}}, delivered: 5},
			},
		},
		{
			name: "two gaps fill one at a time",
			steps: []step{
				{offer: batch(3, 3), buffered: 1},
				{offer: batch(6, 7), buffered: 2},
				{offer: batch(1, 2), ready: []span{{1, 3}}, delivered: 3, buffered: 1},
				{offer: batch(4, 5), ready: []span{{4, 7}}, delivered: 7},
			},
		},
		{
			name:  "starts after load",
			start: 10,
			steps: []step{
				{offer: batch(11, 12), ready: []span{{11, 12}}, delivered: 12},
			},
		},
		{
			name:  "at or below delivered is dropped",
			start: 5,
			steps: []step{
				{offer: batch(1, 5), delivered: 5},
				{offer: batch(5, 5), delivered: 5},
			},
		},
		{
			name:  "overlap with delivered is trimmed",
			start: 5,
			steps: []step{
				{offer: batch(3, 7), ready: []span{{6, 7}}, delivered: 7},
			},
		},
		{
			name: "duplicate of buffered range",
			steps: []step{
				{offer: batch(3, 4), buffered: 1},
				{offer: batch(3, 4), buffered: 1},
				{offer: batch(1, 2), ready: []span{{1, 4}}, delivered: 4},
			},
		},
		{
			name: "overlap with buffered ranges is trimmed on both sides",
			steps: []step{
				{offer: batch(3, 4), buffered: 1},
				{offer: batch(7, 8), buffered: 2},
				{offer: batch(2, 9), buffered: 1},
				{offer: batch(1, 1), ready: []span{{1, 9}}, delivered: 9},
			},
		},
		{
			name: "fill straddling delivered and buffered",
			steps: []step{
				{offer: batch(1, 2), ready: []span{{1, 2}}, delivered: 2},
				{offer: batch(5, 6), delivered: 2, buffered: 1},
				{offer: batch(1, 6), ready: []span{{3, 6}}, delivered: 6},
			},
		},
		{
			name: "empty batch",
			steps: []step{
				{offer: Batch{}},
				{offer: Batch{FirstSeq: 4}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSequencer(tt.start)
			for i, st := range tt.steps {
				var got []span
				for _, b := range s.Offer(st.offer) {
					for j, op := range b.Ops {
						if want := opAt(b.FirstSeq + uint64(j)); op != want {
							t.Fatalf("step %d: seq %d holds %v, want %v", i, b.FirstSeq+uint64(j), op, want)
						}
					}
					got = append(got, span{b.FirstSeq, b.Next() - 1})
				}
				if !slices.Equal(got, st.ready) {
					t.Fatalf("step %d: ready %v, want %v", i, got, st.ready)
				}
				if s.Delivered() != st.delivered {
					t.Fatalf("step %d: delivered %d, want %d", i, s.Delivered(), st.delivered)
				}
				open, buffered := s.Gap()
				if open != (st.buffered > 0) || buffered != st.buffered {
					t.Fatalf("step %d: gap open=%v buffered=%d, want %d", i, open, buffered, st.buffered)
				}
			}
		})
	}
}

func TestSequencerReadyIsContiguous(t *testing.T) {
	s := NewSequencer(0)
	offers := []Batch{batch(9, 12), batch(2, 3), batch(5, 5), batch(13, 13), batch(4, 4), batch(6, 8), batch(1, 1)}
	next := uint64(1)
	for _, b := range offers {
		for _, r := range s.Offer(b) {
			if r.FirstSeq != next {
				t.Fatalf("ready batch starts at %d, want %d", r.FirstSeq, next)
			}
			next = r.Next()
		}
	}
	if next != 14 || s.Delivered() != 13 {
		t.Fatalf("delivered %d, next %d", s.Delivered(), next)
	}
	if open, _ := s.Gap(); open {
		t.Fatal("gap still open")
	}
}
