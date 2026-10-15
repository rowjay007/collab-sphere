package hub

import (
	"context"

	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/presence"
)

type PresenceSession = presence.Session

// Presence is where the hub writes presence. Its events come back through
// fanout.Sink.Presence, even to the hub that caused them. Move, Rename and
// Leave never block; Join and List are called off the hub loop.
type Presence interface {
	// Join upserts s with no cursor and returns everyone else's state and
	// the document's presence version.
	Join(ctx context.Context, docID string, s PresenceSession) ([]*presencev1.State, uint64, error)
	Move(docID string, s PresenceSession, anchor, head crdt.ID)
	Rename(docID string, s PresenceSession, name string)
	Leave(docID string, s PresenceSession)
	List(ctx context.Context, docID string) ([]*presencev1.State, uint64, error)
}
