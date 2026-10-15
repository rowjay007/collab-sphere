// Package presence carries a hub's presence writes to presence-service
// (Writer), or keeps presence in process when there is none (Local). Either
// way, events come back to the hub through fanout.Sink.Presence.
package presence

import (
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

// Session is one connection's presence identity. Session is random per
// connection and nonzero, so a reconnect on any hub replaces the old state
// and a Leave from the old connection no longer matches. Epoch is the
// client's connection count: a Join replaces another session only with a
// higher epoch, and epoch zero keeps last-writer-wins.
type Session struct {
	Client  crdt.ClientID
	Session uint64
	Epoch   uint64
	Name    string
}

// key holds at most one live session per client and document. A newer
// session for the same client replaces the entry, as it does in
// presence-service.
type key struct {
	doc    string
	client crdt.ClientID
}

// phase is where a session stands with presence-service.
type phase uint8

const (
	// pending holds Move and Rename that reach the hub before the session's
	// Join has succeeded.
	pending phase = iota
	joined
	// forgotten is a session presence-service gave to a newer connection.
	// Nothing more is written for it, not even its Leave.
	forgotten
)

// entry is the last known state of one session.
type entry struct {
	session   uint64
	epoch     uint64
	name      string
	anchor    crdt.ID
	head      crdt.ID
	hasCursor bool
	phase     phase
	version   uint64
}

// hold returns the entry for s, replacing one that belongs to another session.
func hold(live map[key]*entry, k key, s Session) *entry {
	e := live[k]
	if e == nil || e.session != s.Session {
		e = &entry{session: s.Session, epoch: s.Epoch, name: s.Name}
		live[k] = e
	}
	return e
}

// yields reports whether s may replace e: presence-service's Join rule.
func (e *entry) yields(s Session) bool {
	return e.session == s.Session || s.Epoch > e.epoch || s.Epoch == 0 && e.epoch == 0
}

func (e *entry) state(k key) *presencev1.State {
	st := &presencev1.State{
		DocumentId:  k.doc,
		Client:      uint64(k.client),
		Session:     e.session,
		Epoch:       e.epoch,
		DisplayName: e.name,
		Version:     e.version,
		HasCursor:   e.hasCursor,
	}
	if e.hasCursor {
		st.Anchor = opID(e.anchor)
		st.Head = opID(e.head)
	}
	return st
}

func opID(id crdt.ID) *crdtv1.OpID {
	return &crdtv1.OpID{Client: uint64(id.Client), Clock: id.Clock}
}
