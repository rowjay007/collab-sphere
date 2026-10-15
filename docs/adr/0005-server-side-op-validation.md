# ADR 0005. The hub rejects ops it cannot integrate right away

Status: accepted, Phase 1.

## Context

The CRDT library accepts ops in any order. An insert whose parent is missing waits in a pending map. Replicas merging peer to peer need that. The hub is not a peer like the others, though. Every op a client can see came through the hub.

## Decision

For each op in a batch, the hub checks five things in order and stops at the first failure:

1. The op's ID carries the connection's client ID. Otherwise it sends `CODE_CLIENT_MISMATCH`.
2. The op's clock is at most the document clock plus one. Otherwise it sends `CODE_INVALID_OPERATION`.
3. The op's parent or target is already in the document. Otherwise it sends `CODE_MISSING_DEPENDENCY`.
4. `Apply` accepts it. Otherwise it sends `CODE_INVALID_OPERATION`.
5. An op the document already holds is skipped silently, so a client that resends is safe.

The hub persists and broadcasts the valid prefix before the rejected op. It sends the error to the sender and then drops that peer. The client records the error and stops.

`CODE_UNAVAILABLE` is the only retryable error code. Every other error frame is terminal and the hub closes or refuses that connection. A cursor whose anchor or head is absent from the hub document is a benign race with the sender's in-flight ops, so the hub drops that cursor update silently.

The connecting client picks its ID with the `client` query parameter. The hub refuses a second live connection with the same ID on the same document.

## Why

An honest client builds ops only on top of ops the server holds or its own earlier ops, and those arrive in order. A missing dependency therefore means a buggy or hostile client. Holding its op would grow server memory without bound. It would also show nobody the character, so the sender's view would silently drift from everyone else's.

Every clock an honest client sends is at most the hub document clock plus one when the hub applies it. The client advances from the maximum clock it has seen. All clocks it sees came through this hub, and its own ops arrive in order. The bound prevents one invalid op from exhausting the clock space for every replica.

## Consequences

Phase 1 has no authentication, so a client can claim any free client ID. The permission service in Phase 4 binds client IDs to authenticated sessions and checks a role on every message. Client IDs should be random 64-bit values so two devices never collide.

Phase 2 multi-hub fan-out must revisit the clock bound. A hub may receive a valid op before it has observed the latest clock accepted by another hub.

## Amendment, Phase 2. The clock bound stays

With several hubs, a client's replica is `log[1..seq]` plus its own unacknowledged ops, where `seq` is the last seq it holds. Before admitting a resuming client, its hub reads the log until its own delivered seq is at least the client's. After that, every op and clock the client has seen is in the hub's document, and its own ops arrive in order. So the bound `clock ≤ doc.Clock()+1` holds across hubs, and the five checks stay as written. The hub implements this catch-up on every resuming join, before it answers with `MissedOps` or a `DocumentSync` (ADR 0007, Resume). A client that resumes from a seq above the log is refused with `CODE_INVALID_RESUME`.

Two other rejected options. A per-client `clock == vector[client]+1` check is wrong, because a Lamport clock legitimately jumps past ops seen from others. Removing the bound reopens the clock-exhaustion attack.

A second live connection with the same client ID is no longer refused. The newest one wins, as ADR 0007 describes. The old one gets `CODE_SUPERSEDED`, which is terminal, so two clients sharing an ID do not keep taking the slot back from each other.

**Conflicts across hubs.** The five checks run against the hub's own document, which can lack an op another hub logged. A client can then send an op whose `(client, clock)` the log holds with different content. All five checks pass, and `Append` refuses the group. document-service answers `AlreadyExists` with a `google.rpc.ErrorInfo` detail: reason `OP_CONFLICT`, metadata `client` and `clock`. The hub sends the peer with that client ID `CODE_INVALID_OPERATION` ("operation conflicts with the log") and closes it, which is terminal for that client. The group is already applied to the hub's document and cannot be un-applied. So the hub then fail-stops for the remaining peers with `CODE_UNAVAILABLE`. Honest peers reconnect and resend, and the log dedupes their ops. Any other `Append` error fail-stops the hub as before.

The residual risk is reconnect churn. Client IDs are unauthenticated. A malicious client that ignores the terminal error can reconnect and repeat the conflict, forcing every peer on one hub to reconnect each time. Closing that needs authenticated client IDs, a later phase.
