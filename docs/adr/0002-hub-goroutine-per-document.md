# ADR 0002. One hub goroutine owns each open document

Status: accepted, Phase 1.

## Context

Up to 100 editors send ops to the same document at once. The CRDT is a plain in-memory structure, and several goroutines touching it at once would need locks around every read and write.

## Decision

Each open document gets one hub goroutine. It alone holds the document's `*crdt.Doc`, the connected clients, and their last cursors. Connections talk to it over a channel of join, leave, op batch, and cursor events. Each connection has a read pump and a write pump, and only the pumps touch the socket. The write pump drains a bounded channel. When a client's channel fills, the hub drops that peer and closes its WebSocket rather than stall everyone else.

A registry maps document IDs to hubs and counts clients. The first join starts a hub, which loads state from document-service with a ten-second deadline before it serves events. Archived documents refuse joins. The last leave stops the hub.

Every WebSocket write has a deadline. The write pump sends a keepalive ping every 30 seconds and closes a connection whose ping fails. This releases client IDs held by half-open connections.

For each op batch the hub checks the ops, applies them, persists them through document-service, and only then broadcasts them. The broadcast goes to every client, the sender included. The sender's copy is its durable acknowledgement.

If persisting fails, the in-memory document is ahead of what the database holds. The hub tells every client, closes them all, and stops. The next join reloads from the database. This is fail-stop. Phase 1 clients have no offline buffer, so ops in that failed batch are lost to the sender. Phase 2 adds the buffer and replay.

## Alternatives rejected

- **A mutex around a shared document.** It works, but every read path then has to think about locking. With one owner, the type system and the channel enforce the rule.
- **Broadcast before persist.** Lower latency, but a crash can show other users ops that never reach the database.

## Consequences

One document's throughput is bounded by one goroutine plus one database round trip per batch. That is enough for 100 editors on one instance. Phase 2 spreads a document across instances with Redis fan-out.
