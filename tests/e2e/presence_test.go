package e2e

import (
	"context"
	"slices"
	"testing"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
)

func TestPresenceAcrossHubs(t *testing.T) {
	const ttl = 1500 * time.Millisecond
	s := stack.New(t, stack.WithHubs(3), stack.WithPresenceTTL(ttl))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	docID := s.CreateDocument(ctx)
	const idA, idB, idC, idD = crdt.ClientID(1), crdt.ClientID(2), crdt.ClientID(3), crdt.ClientID(4)

	a := s.DialHub(ctx, docID, idA, 0)
	a.SetName("ann")
	b := s.DialAuto(ctx, docID, idB, 1)
	b.SetName("bob")

	const text = "presence across three hubs"
	for i, r := range text {
		if err := a.InsertAt(i, r); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.WaitUntil(ctx, func(r *crdt.Replica) bool { return r.Len() == len(text) }); err != nil {
		t.Fatal(err)
	}
	var ids []crdt.ID
	a.Read(func(r *crdt.Replica) {
		for i := range r.Len() {
			id, err := r.IDAt(i)
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
	})
	waitPresent(t, ctx, b, idA, "ann")

	latencies := make([]time.Duration, 0, 20)
	for i := range 20 {
		head := ids[i+1]
		start := time.Now()
		if err := a.MoveCursor(ids[0], head); err != nil {
			t.Fatal(err)
		}
		if err := b.WaitPresence(ctx, func(m map[crdt.ClientID]collabclient.PresenceView) bool {
			c := m[idA].Cursor
			return c.Set && c.Anchor == ids[0] && c.Head == head
		}); err != nil {
			t.Fatalf("move %d: %v", i, err)
		}
		latencies = append(latencies, time.Since(start))
	}
	sorted := slices.Sorted(slices.Values(latencies))
	t.Logf("cursor hub0 to hub1 over %d moves: p50 %v, max %v", len(sorted), sorted[len(sorted)/2], sorted[len(sorted)-1])
	if worst := sorted[len(sorted)-1]; worst >= 200*time.Millisecond {
		t.Fatalf("slowest cursor move took %v, want under 200ms", worst)
	}
	lastHead := ids[20]

	c := s.DialAuto(ctx, docID, idC, 2)
	if err := c.WaitPresence(ctx, func(m map[crdt.ClientID]collabclient.PresenceView) bool { return len(m) >= 2 }); err != nil {
		t.Fatal(err)
	}
	sync := c.JoinAwareness()
	if sync == nil {
		t.Fatal("C has presence but no AwarenessSync")
	}
	byClient := map[crdt.ClientID]*collabv1.PresenceState{}
	for _, st := range sync.GetStates() {
		byClient[crdt.ClientID(st.GetClient())] = st
	}
	if byClient[idA].GetDisplayName() != "ann" || byClient[idB].GetDisplayName() != "bob" {
		t.Fatalf("C's first AwarenessSync lacks A or B by name: %v", sync)
	}
	if h := byClient[idA].GetCursor().GetHead(); h.GetClient() != uint64(lastHead.Client) || h.GetClock() != lastHead.Clock {
		t.Fatalf("C's first AwarenessSync has A's head at %v, want %v", h, lastHead)
	}

	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	for name, peer := range map[string]*collabclient.Client{"B": b, "C": c} {
		waitLeft(t, ctx, peer, idA, collabv1.PresenceLeft_REASON_LEAVE, name)
	}

	d := s.DialHub(ctx, docID, idD, 1)
	d.SetName("dee")
	waitPresent(t, ctx, c, idD, "dee")
	s.KillHub(1)
	killed := time.Now()
	a2 := s.DialHub(ctx, docID, idA, 0)

	expiry, cancelExpiry := context.WithTimeout(ctx, ttl+time.Second)
	defer cancelExpiry()
	waitPresent(t, expiry, a2, idD, "dee")
	for name, peer := range map[string]*collabclient.Client{"A'": a2, "C": c} {
		waitLeft(t, expiry, peer, idD, collabv1.PresenceLeft_REASON_EXPIRED, name)
	}
	t.Logf("D reported expired %v after its hub was killed", time.Since(killed))

	for b.MoveCursor(ids[3], ids[3]) != nil {
		select {
		case <-ctx.Done():
			t.Fatal("B never reconnected after hub 1 was killed")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := c.WaitPresence(ctx, func(m map[crdt.ClientID]collabclient.PresenceView) bool {
		return m[idB].Cursor.Head == ids[3]
	}); err != nil {
		t.Fatalf("C never saw B's cursor after B reconnected: %v", err)
	}
	time.Sleep(ttl)
	for name, peer := range map[string]*collabclient.Client{"A'": a2, "C": c} {
		if _, ok := peer.Presence()[idB]; !ok {
			reason, _ := peer.LeftReason(idB)
			t.Fatalf("%s lost B, which reconnected, with reason %v", name, reason)
		}
	}
}

func waitPresent(t *testing.T, ctx context.Context, c *collabclient.Client, client crdt.ClientID, name string) {
	t.Helper()
	if err := c.WaitPresence(ctx, func(m map[crdt.ClientID]collabclient.PresenceView) bool {
		v, ok := m[client]
		return ok && v.Name == name
	}); err != nil {
		t.Fatalf("client %d never present as %q: %v", client, name, err)
	}
}

func waitLeft(t *testing.T, ctx context.Context, c *collabclient.Client, client crdt.ClientID, want collabv1.PresenceLeft_Reason, observer string) {
	t.Helper()
	if err := c.WaitPresence(ctx, func(m map[crdt.ClientID]collabclient.PresenceView) bool {
		_, ok := m[client]
		return !ok
	}); err != nil {
		t.Fatalf("%s still sees client %d: %v", observer, client, err)
	}
	if got, ok := c.LeftReason(client); !ok || got != want {
		t.Fatalf("%s saw client %d leave with %v, want %v", observer, client, got, want)
	}
}
