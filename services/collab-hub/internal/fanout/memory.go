package fanout

import (
	"sync"
	"time"
)

// Fault decides what happens to one delivery of a published batch: drop it,
// or deliver it after delay. Delaying some deliveries reorders them.
type Fault func(docID string, b Batch) (drop bool, delay time.Duration)

// Memory is an in-process Bus. It serves single-instance mode and tests that
// run several registries in one process. Every joined sink receives every
// batch, its own publisher's included.
type Memory struct {
	mu    sync.Mutex
	subs  map[string]map[*memorySub]struct{}
	fault Fault
}

type memorySub struct {
	m     *Memory
	docID string
	sink  Sink
}

var closedReady = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// NewMemory returns a bus that delivers every batch at once.
func NewMemory() *Memory {
	return &Memory{subs: make(map[string]map[*memorySub]struct{})}
}

func (m *Memory) Join(docID string, sink Sink) Subscription {
	s := &memorySub{m: m, docID: docID, sink: sink}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.subs[docID] == nil {
		m.subs[docID] = make(map[*memorySub]struct{})
	}
	m.subs[docID][s] = struct{}{}
	return s
}

func (m *Memory) Publish(docID string, b Batch) {
	sinks, fault := m.sinks(docID)
	for _, sink := range sinks {
		if fault == nil {
			sink.RemoteOps(b)
			continue
		}
		drop, delay := fault(docID, b)
		switch {
		case drop:
		case delay > 0:
			time.AfterFunc(delay, func() { sink.RemoteOps(b) })
		default:
			sink.RemoteOps(b)
		}
	}
}

// SetFault applies f to every later delivery. Pass nil to deliver everything.
func (m *Memory) SetFault(f Fault) {
	m.mu.Lock()
	m.fault = f
	m.mu.Unlock()
}

// Resync tells every sink on docID that messages may have been lost, as a
// Redis resubscribe would.
func (m *Memory) Resync(docID string) {
	sinks, _ := m.sinks(docID)
	for _, sink := range sinks {
		sink.Resync()
	}
}

func (m *Memory) sinks(docID string) ([]Sink, Fault) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sinks := make([]Sink, 0, len(m.subs[docID]))
	for s := range m.subs[docID] {
		sinks = append(sinks, s.sink)
	}
	return sinks, m.fault
}

func (s *memorySub) Ready() <-chan struct{} { return closedReady }

func (s *memorySub) Close() {
	s.m.mu.Lock()
	defer s.m.mu.Unlock()
	delete(s.m.subs[s.docID], s)
	if len(s.m.subs[s.docID]) == 0 {
		delete(s.m.subs, s.docID)
	}
}
