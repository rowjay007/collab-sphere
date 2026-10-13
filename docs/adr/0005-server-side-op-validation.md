# ADR 0005. The hub rejects ops it cannot integrate right away

Status: accepted, Phase 1.

## Context

The CRDT library accepts ops in any order. An insert whose parent is missing waits in a pending map. Replicas merging peer to peer need that. The hub is not a peer like the others, though. Every op a client can see came through the hub.

## Decision

For each op in a batch, the hub checks four things in order and stops at the first failure:

1. The op's ID carries the connection's client ID. Otherwise it sends `CODE_CLIENT_MISMATCH`.
2. The op's parent or target is already in the document. Otherwise it sends `CODE_MISSING_DEPENDENCY`.
3. `Apply` accepts it. Otherwise it sends `CODE_INVALID_OPERATION`.
4. An op the document already holds is skipped silently, so a client that resends is safe.

The connecting client picks its ID with the `client` query parameter. The hub refuses a second live connection with the same ID on the same document.

## Why

An honest client builds ops only on top of ops the server holds or its own earlier ops, and those arrive in order. A missing dependency therefore means a buggy or hostile client. Holding its op would grow server memory without bound. It would also show nobody the character, so the sender's view would silently drift from everyone else's.

## Consequences

Phase 1 has no authentication, so a client can claim any free client ID. The permission service in Phase 4 binds client IDs to authenticated sessions and checks a role on every message. Client IDs should be random 64-bit values so two devices never collide.
