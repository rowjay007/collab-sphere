# ADR 0012. Comment anchors stay on character IDs

Status: accepted, Phase 3.

## Context

A comment has to stay on the same characters while other people type, delete, restore, and merge. Index ranges move. CRDT IDs do not. Tombstones keep a deleted character's place in the tree, and a restore re-creates text as a copy of those IDs. The same library function must answer "what does this comment cover now?" on the server and on every live replica.

Threads also cross documents. A comment created on branch B must stay invisible in D until a merge brings B's state in. Replies and resolve state belong to the thread, not to a per-document copy.

## Decision

An anchor is an inclusive range `[start, end]` of character IDs. comment-service checks both IDs against `LoadState` at creation. Both must be visible, and start must precede or equal end in document order. The thread stores `created_seq`, the home document's `op_seq` from that load.

Resolution is `(*Doc).ResolveRange(start, end)`. Each endpoint maps through `Resolve` to a visible copy, or stays at its tombstone. The range is every visible node from that start position to that end position, inclusive. Inserts inside the range join it. Text typed immediately before start, or as a child of end, stays outside. Zero visible characters is `DETACHED`. The stored IDs stay, so a restore or a merge that brings the text back re-attaches the comment.

A thread with home H and `created_seq` S is visible in document X if and only if `Frontier(X)[H] >= S`. That is one predicate. ListThreads asks Versions for Frontier(X), loads threads whose `home` is a key of that map (`WHERE home = ANY(...)`), and then applies the `created_seq` rule. It then resolves each visible anchor on X's state.

Mentions are parsed at write time with `(?:^|\s)@([a-z0-9][a-z0-9._-]{0,63})`, stored in first-appearance order, and de-duplicated. Notifications are rows in the same transaction as the comment or resolve write. That table is the outbox. ListNotifications pages it by a monotonic id. Phase 3 does not push comments over the WebSocket. Clients poll ListThreads and resolve anchors on the local replica.

`SetResolved` is last-writer-wins. A write that does not change the flag stores the request and sends no notification. A change by someone other than the thread author notifies that author.

Every write carries `request_id`. The store records a payload fingerprint. The same request returns the original row. A reused id with a different payload returns AlreadyExists.

Phase 5 must not collect a comment-anchor endpoint. Tombstones are what keep a detached range positioned.

### Version-service Frontier is bound

`versions.FrontierClient` is a gRPC client of `version.v1.VersionService/Frontier`. `VERSION_ADDR` is still required at process start. An Unavailable RPC error becomes `ErrUnavailable`. The client does not invent `{X: head}`. Unit tests still inject a fake Versions.

## Alternatives rejected

- **Index or UTF-16 offsets.** Concurrent inserts shift them. IDs do not.
- **Collapsing a deleted range to a caret.** The IDs would be gone, and a restore could not re-attach the comment.
- **Copying threads on merge.** That puts a cross-service write on the merge path. Replies would then diverge.
- **A Redis Stream for notifications.** There is no consumer this phase. A table is enough for cursor reads and later delivery.
- **Pushing comments on the WebSocket.** Phase 3 keeps that socket binary protobuf for ops and presence. Polling is enough until Phase 4 adds auth.

## Consequences

Clients highlight at keystroke latency because `ResolveAnchor` runs under the replica lock and does no I/O. The server resolution is the same function on `LoadState`, so a poll after settle matches every replica.

A B comment appears in D only after the merge that contains `B@created_seq`. Resolving it in D resolves it in B, because there is one thread row.

ListThreads returns Unavailable when version-service is down. Create, Reply, and SetResolved do not call Frontier.
