package hub

import (
	"math/rand/v2"
	"sync"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

// sendBuf is about 1.5s of a 30-editor room's frames. A smaller queue drops
// readers on an ordinary GC pause or network stall.
const sendBuf = 4096

// Peer is one connected client. Only the write pump reads Frames.
type Peer struct {
	id       crdt.ClientID
	session  uint64
	epoch    uint64
	out      chan []byte
	done     chan struct{}
	once     sync.Once
	reasonMu sync.Mutex
	reason   string

	// Owned by the hub loop. joined closes when the presence Join for this
	// peer has returned, and stays nil until the hub starts that Join.
	name   string
	joined chan struct{}
}

// NewPeer constructs a peer with a bounded send buffer and a fresh presence
// session. epoch is the client's connection count, or zero if it sent none.
func NewPeer(id crdt.ClientID, epoch uint64) *Peer {
	session := rand.Uint64()
	for session == 0 {
		session = rand.Uint64()
	}
	return &Peer{
		id:      id,
		session: session,
		epoch:   epoch,
		out:     make(chan []byte, sendBuf),
		done:    make(chan struct{}),
	}
}

func (p *Peer) presence() PresenceSession {
	return PresenceSession{Client: p.id, Session: p.session, Epoch: p.epoch, Name: p.name}
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
