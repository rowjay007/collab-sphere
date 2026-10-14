package hub

import (
	"sync"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

const sendBuf = 256

// Peer is one connected client. Only the write pump reads Frames.
type Peer struct {
	id       crdt.ClientID
	out      chan []byte
	done     chan struct{}
	once     sync.Once
	reasonMu sync.Mutex
	reason   string
}

// NewPeer constructs a peer with a bounded send buffer.
func NewPeer(id crdt.ClientID) *Peer {
	return &Peer{
		id:   id,
		out:  make(chan []byte, sendBuf),
		done: make(chan struct{}),
	}
}

// ID returns the connection's client ID.
func (p *Peer) ID() crdt.ClientID { return p.id }

// Frames is the outbound byte stream drained by the write pump.
func (p *Peer) Frames() <-chan []byte { return p.out }

// Done closes when the peer is dropped.
func (p *Peer) Done() <-chan struct{} { return p.done }

// Close marks the peer dead. It is idempotent.
func (p *Peer) Close() {
	p.CloseWithReason("dropped")
}

func (p *Peer) CloseWithReason(reason string) {
	p.once.Do(func() {
		p.reasonMu.Lock()
		p.reason = reason
		p.reasonMu.Unlock()
		close(p.done)
	})
}

func (p *Peer) DropReason() string {
	p.reasonMu.Lock()
	defer p.reasonMu.Unlock()
	return p.reason
}

// TrySend enqueues a frame. It returns false if the buffer is full or the peer is dead.
func (p *Peer) TrySend(frame []byte) bool {
	select {
	case <-p.done:
		return false
	default:
	}
	select {
	case p.out <- frame:
		return true
	case <-p.done:
		return false
	default:
		return false
	}
}
