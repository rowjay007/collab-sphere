package presence

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

const (
	minBackoff   = 50 * time.Millisecond
	maxBackoff   = 2 * time.Second
	applyTimeout = 2 * time.Second
	closeTimeout = time.Second
	maxChanges   = 512
)

// Writer sends a hub's presence changes to presence-service. Move, Rename
// and Leave only record the change; every flush interval the latest state of
// each changed session goes out at once, so a burst of cursor moves costs
// one write. Every heartbeat it touches every joined session and
// re-upserts any that presence-service reports missing. A session that
// presence-service reports superseded, on Join, upsert or touch, is
// forgotten: it gets no more writes and no Leave. Failed writes are kept and
// retried with backoff; callers never wait on presence-service.
type Writer struct {
	client    presencev1.PresenceServiceClient
	flush     time.Duration
	heartbeat time.Duration

	mu       sync.Mutex
	live     map[key]*entry
	dirty    map[key]struct{}
	leaves   []*presencev1.Leave
	lastWarn time.Time

	stop chan struct{}
	done chan struct{}
}

// NewWriter starts flushing at once. Close stops it.
func NewWriter(client presencev1.PresenceServiceClient, flush, heartbeat time.Duration) *Writer {
	w := &Writer{
		client:    client,
		flush:     flush,
		heartbeat: heartbeat,
		live:      make(map[key]*entry),
		dirty:     make(map[key]struct{}),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	go w.run()
	return w
}

// Join upserts s with no cursor and returns everyone else. Changes recorded
// for s before Join are flushed after it. Until a Join succeeds, s is
// neither upserted nor touched; the caller retries it.
func (w *Writer) Join(ctx context.Context, docID string, s Session) ([]*presencev1.State, uint64, error) {
	k := key{doc: docID, client: s.Client}
	w.mu.Lock()
	e := hold(w.live, k, s)
	self := e.state(k)
	w.mu.Unlock()

	resp, err := w.client.Join(ctx, &presencev1.JoinRequest{Self: self})
	if err != nil {
		return nil, 0, err
	}

	w.mu.Lock()
	switch {
	case w.live[k] != e || e.phase == forgotten:
	case resp.GetSuperseded():
		slog.Debug("presence join superseded", "doc", docID, "client", s.Client, "epoch", s.Epoch)
		e.phase = forgotten
		delete(w.dirty, k)
	default:
		e.phase = joined
		if e.hasCursor || e.name != self.GetDisplayName() {
			w.dirty[k] = struct{}{}
		}
	}
	w.mu.Unlock()
	return resp.GetStates(), resp.GetVersion(), nil
}

func (w *Writer) Move(docID string, s Session, anchor, head crdt.ID) {
	w.update(docID, s, func(e *entry) {
		e.anchor, e.head, e.hasCursor = anchor, head, true
		e.name = s.Name
	})
}

func (w *Writer) Rename(docID string, s Session, name string) {
	w.update(docID, s, func(e *entry) { e.name = name })
}

func (w *Writer) update(docID string, s Session, fn func(*entry)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	k := key{doc: docID, client: s.Client}
	e := hold(w.live, k, s)
	if e.phase == forgotten {
		return
	}
	fn(e)
	if e.phase == joined {
		w.dirty[k] = struct{}{}
	}
}

// Leave removes s. presence-service ignores it if another session has
// since replaced s.
func (w *Writer) Leave(docID string, s Session) {
	w.mu.Lock()
	defer w.mu.Unlock()
	k := key{doc: docID, client: s.Client}
	if e := w.live[k]; e != nil && e.session == s.Session {
		delete(w.live, k)
		delete(w.dirty, k)
		if e.phase == forgotten {
			return
		}
	}
	w.leaves = append(w.leaves, &presencev1.Leave{DocumentId: docID, Client: uint64(s.Client), Session: s.Session})
}

func (w *Writer) forget(docID string, client uint64, session uint64) {
	k := key{doc: docID, client: crdt.ClientID(client)}
	if e := w.live[k]; e != nil && e.session == session {
		e.phase = forgotten
		delete(w.dirty, k)
	}
}

func (w *Writer) List(ctx context.Context, docID string) ([]*presencev1.State, uint64, error) {
	resp, err := w.client.List(ctx, &presencev1.ListRequest{DocumentId: docID})
	if err != nil {
		return nil, 0, err
	}
	return resp.GetStates(), resp.GetVersion(), nil
}

// Close stops flushing and sends the pending Leaves, waiting at most a second.
func (w *Writer) Close() {
	close(w.stop)
	<-w.done
	w.mu.Lock()
	leaves := w.leaves
	w.leaves = nil
	w.mu.Unlock()
	if len(leaves) == 0 {
		return
	}
	changes := make([]*presencev1.Change, len(leaves))
	for i, l := range leaves {
		changes[i] = &presencev1.Change{Kind: &presencev1.Change_Leave{Leave: l}}
	}
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()
	if _, err := w.client.Apply(ctx, &presencev1.ApplyRequest{Changes: changes}); err != nil {
		slog.Warn("presence leaves dropped on close", "err", err, "leaves", len(leaves))
	}
}

func (w *Writer) run() {
	defer close(w.done)
	tick := time.NewTicker(w.flush)
	defer tick.Stop()
	nextBeat := time.Now().Add(w.heartbeat)
	var retryAt time.Time
	backoff := minBackoff
	for {
		select {
		case <-w.stop:
			return
		case <-tick.C:
		}
		now := time.Now()
		if now.Before(retryAt) {
			continue
		}
		beat := !now.Before(nextBeat)
		if err := w.send(beat); err != nil {
			w.warn(err)
			retryAt = now.Add(backoff)
			backoff = min(2*backoff, maxBackoff)
			continue
		}
		backoff = minBackoff
		if beat {
			nextBeat = now.Add(w.heartbeat)
		}
	}
}

// send applies the latest state of every changed session, then the Leaves,
// then a touch of every joined session when beat is set, in Applies of at
// most maxChanges in that order. Upserts go before Leaves so a replaced
// session's Leave lands after its replacement and does nothing. When an
// Apply fails, the upserts and Leaves it and the later Applies held are
// queued again.
func (w *Writer) send(beat bool) error {
	w.mu.Lock()
	var changes []*presencev1.Change
	var upserted []key
	for k := range w.dirty {
		e := w.live[k]
		if e == nil || e.phase != joined {
			continue
		}
		changes = append(changes, &presencev1.Change{Kind: &presencev1.Change_Upsert{Upsert: e.state(k)}})
		upserted = append(upserted, k)
	}
	clear(w.dirty)
	leaves := w.leaves
	w.leaves = nil
	for _, l := range leaves {
		changes = append(changes, &presencev1.Change{Kind: &presencev1.Change_Leave{Leave: l}})
	}
	if beat {
		for k, e := range w.live {
			if e.phase != joined {
				continue
			}
			changes = append(changes, &presencev1.Change{Kind: &presencev1.Change_Touch{Touch: &presencev1.Touch{
				DocumentId: k.doc, Client: uint64(k.client), Session: e.session,
			}}})
		}
	}
	w.mu.Unlock()

	for sent := 0; sent < len(changes); sent += maxChanges {
		if err := w.apply(changes[sent:min(sent+maxChanges, len(changes))]); err != nil {
			w.requeue(upserted[min(sent, len(upserted)):], leaves[min(max(sent-len(upserted), 0), len(leaves)):])
			return err
		}
	}
	return nil
}

func (w *Writer) apply(changes []*presencev1.Change) error {
	ctx, cancel := context.WithTimeout(context.Background(), applyTimeout)
	resp, err := w.client.Apply(ctx, &presencev1.ApplyRequest{Changes: changes})
	cancel()
	if status.Code(err) == codes.InvalidArgument {
		// Retrying a batch presence-service rejects would block every
		// document's presence behind it.
		slog.Warn("presence batch rejected", "err", err, "changes", len(changes))
		return nil
	}
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for _, t := range resp.GetMissing() {
		k := key{doc: t.GetDocumentId(), client: crdt.ClientID(t.GetClient())}
		if e := w.live[k]; e != nil && e.session == t.GetSession() && e.phase == joined {
			w.dirty[k] = struct{}{}
		}
	}
	for _, t := range resp.GetSuperseded() {
		w.forget(t.GetDocumentId(), t.GetClient(), t.GetSession())
	}
	for _, st := range resp.GetSupersededUpserts() {
		w.forget(st.GetDocumentId(), st.GetClient(), st.GetSession())
	}
	return nil
}

func (w *Writer) requeue(upserted []key, leaves []*presencev1.Leave) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, k := range upserted {
		if _, ok := w.live[k]; ok {
			w.dirty[k] = struct{}{}
		}
	}
	w.leaves = append(leaves, w.leaves...)
}

func (w *Writer) warn(err error) {
	w.mu.Lock()
	now := time.Now()
	quiet := now.Sub(w.lastWarn) < time.Second
	if !quiet {
		w.lastWarn = now
	}
	w.mu.Unlock()
	if !quiet {
		slog.Warn("presence write failed, retrying", "err", err)
	}
}
