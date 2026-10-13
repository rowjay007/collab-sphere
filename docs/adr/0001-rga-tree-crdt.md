# ADR 0001. The document is an RGA insertion tree with Lamport IDs

Status: accepted, Phase 1.

## Context

Two people typing at the same spot must end with the same text on every screen, in any arrival order, with no server lock per keystroke. The brief mandates an RGA where each character has a `(ClientID, Clock)` ID, siblings sort by `(Clock desc, ClientID desc)`, and deletes are tombstones.

## Decision

Each character is a node whose parent is the character it was typed after. The root is the zero ID. A node's children are sorted with the greater ID first. Document order is a pre-order walk: a node, then its children in order. Deletes add the delete's ID to the node's `deletedBy` list and never remove the node.

The state is two sets, inserts and deletes. Applying an op adds to a set, so merge is commutative, associative, and idempotent. The rendered text is a pure function of the two sets. An insert whose parent has not arrived waits in a pending map keyed by the parent. A delete whose target has not arrived waits the same way, so a delete always wins over its insert.

Clocks are Lamport clocks. A replica's clock is the largest clock it has applied, and a local op takes `clock + 1`. That puts a new character first among its parent's children, which is exactly "right after the character I typed after." `Apply` rejects an insert whose clock is not greater than its parent's.

Every op has one ID, inserts and deletes alike. Deletes need IDs so the version vector can say when a delete has been seen by everyone, which tombstone GC needs (ADR 0006).

## Alternatives rejected

- **A linked list in document order with the classic RGA skip scan.** It converges only if every client's clocks obey the Lamport rule. The tree converges for any set of valid ops, and the Lamport rule only affects where a character lands. A buggy client then breaks its own intent, not convergence.
- **Per-client counters without gaps instead of Lamport clocks.** A client with a low counter that inserts after a parent whose children have higher clocks would see its character land after them. That is the wrong place.
- **A separate op sequence number next to the Lamport node ID**, so version vectors track contiguous prefixes. Phase 1 has one hub and ordered sockets, so per-client delivery is already FIFO. A max-per-client vector is exact under FIFO.

## Consequences

Local insert at a visible index and text rendering both walk the tree, O(n). That is fine for Phase 1 document sizes. An order-statistic index can come later if a profile asks for it. The walk is iterative because typing one long run builds a chain as deep as the text is long.
