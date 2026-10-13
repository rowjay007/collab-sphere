package crdt

import "fmt"

// ClientID identifies a replica. Zero is reserved for Root.
type ClientID uint64

// ID is a Lamport timestamp ordered by clock and then client.
type ID struct {
	Clock  uint64
	Client ClientID
}

// Root is the insertion tree root.
var Root = ID{}

// Less reports whether a precedes b in ascending (Clock, Client) order.
func (a ID) Less(b ID) bool {
	return a.Clock < b.Clock || a.Clock == b.Clock && a.Client < b.Client
}

// IsRoot reports whether the ID is Root.
func (a ID) IsRoot() bool {
	return a == Root
}

// String returns the ID as "client:clock".
func (a ID) String() string {
	return fmt.Sprintf("%d:%d", a.Client, a.Clock)
}

// VersionVector records the maximum clock received from each client.
type VersionVector map[ClientID]uint64

// Covers reports whether the vector includes id.
func (v VersionVector) Covers(id ID) bool {
	return id.IsRoot() || v[id.Client] >= id.Clock
}

// Clone returns an independent copy of the vector.
func (v VersionVector) Clone() VersionVector {
	clone := make(VersionVector, len(v))
	for client, clock := range v {
		clone[client] = clock
	}
	return clone
}
