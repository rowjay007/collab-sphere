# ADR 0009. Presence lives in presence-service, ordered by per-document versions

Status: accepted, Phase 2.

## Context

Clients on different hubs need to see each other's cursors and names, learn who is already there when they join, and see someone vanish when that person's hub dies without saying goodbye. Phase 1 kept cursors in the hub's memory, which only covers one hub. Presence is ephemeral and lossy by nature, so it should not go through the op log, but it needs one place that decides who is present, hands out an order, and expires the dead.

## Decision

**One writer.** presence-service is the only process that writes presence keys in Redis. A hub calls it over gRPC (`Join`, `Apply`, `List`) and never touches those keys. presence-service publishes every change as a `presence.v1.Event` on `presence-events:{doc}`, which every hub already subscribes to through its fan-out connection (ADR 0008). The hub turns each event into a `PresenceBroadcast` for its own sockets, skipping the client the event is about.

**Atomic Lua.** Each write is one Lua script that changes the state, the member set and the expiry zset, bumps `presence-ver:{doc}`, and publishes the event, all inside one script. An observer can never see a change without its event or an event without its change. Every entry carries a per-connection session. A new session for the same client overwrites the old one, which is how rejoining on another hub works. `touch` and `remove` act only when the stored session matches, so a late Leave or heartbeat from a hub that lost the client does nothing. A touch distinguishes an absent entry from one owned by a newer session; the hub repairs only the absent entry and forgets a superseded session.

**Deadline sweep for expiry.** Every entry has a deadline from `redis.call('TIME')` in the global `presence-expiry` zset. A sweeper removes overdue entries every `PRESENCE_SWEEP`, and `ZREM` picks one winner when several presence-service replicas sweep at once. The winner publishes `PresenceLeft{REASON_EXPIRED}`. Hubs heartbeat their live sessions every `PRESENCE_HEARTBEAT`. A touch that reports the entry missing makes the hub upsert it again from what it holds.

**Versions and client merge rules.** Every change takes the next value of the document's counter, and states, events and syncs all carry it. Events and joins race each other across Redis, gRPC and the hub's socket queue, so the client merges by version instead of by arrival.

1. A broadcast applies only if its version is above the newest the client knows for that client.
2. A Left removes the entry and keeps its version as a tombstone, so an older broadcast arriving later cannot revive it.
3. An AwarenessSync applies its newer entries and drops entries absent from it whose version is at most the sync's version. Entries newer than the sync were changed after it was taken and stay.

**Writer in the hub.** `presence.Writer` coalesces moves and renames for 16ms and sends the latest state per session in one `Apply`, together with queued Leaves and due touches. It never blocks the hub loop. Failed batches retry with backoff from 50ms to 2s. A Join that fails leaves the session marked dirty, so it is upserted once presence-service is back. On shutdown the hub closes every session and the Writer flushes the Leaves for at most 1s, so a clean restart reports `REASON_LEAVE` instead of waiting for expiry.

**Join snapshot.** After the hub sends a joining client its first frame, a goroutine calls `Join`, which upserts the client without a cursor and returns everyone else. The hub sends that as the client's `AwarenessSync`, or an empty one if the call fails. A second connection for the same client waits for the first one's Join, so their order at presence-service matches their order at the hub.

**Canonical document IDs.** The hub parses the WebSocket path's document ID as a UUID and uses its canonical lowercase string for the registry, operation log, fan-out channels and presence calls. Different textual casing therefore cannot split one document across separate hubs or Redis channels.

**Local mode.** With `PRESENCE_SERVICE_ADDR` unset the hub uses `presence.Local`, which keeps the same sessions and per-document versions in memory and delivers events to the process's own hubs through `Bus.Loopback`. It has no TTL, because a process that dies takes its clients with it. A single hub without Redis needs nothing else, and hub and client tests run against it.

**Subscriber health.** Pub/Sub over a half-open TCP connection delivers nothing and reports no error, which would freeze presence and fan-out on that hub forever. The subscriber sends PING every 15s and expects the pong within 5s. When the pong is late it drops the connection, go-redis reconnects and resubscribes, and the already-live confirmation triggers the ordinary Resync. On Resync a hub also fetches `List` and sends every local client a fresh AwarenessSync, which the merge rules fold in.

## Alternatives rejected

- **Hubs write Redis directly through a shared library.** Every hub would carry the Lua and a sweeper, and there would be no single place to change the scheme.
- **Keyspace notifications for expiry.** They are lossy, reach only connected subscribers, and fire when Redis gets round to evicting the key rather than at the deadline. Expiry would be both unreliable and timing-flaky.
- **A gRPC Watch stream from presence-service to hubs.** Several presence-service replicas would need Pub/Sub among themselves anyway, adding a hop the hub's existing subscription avoids.
- **Ordering by arrival or by wall clock.** Arrival order differs per hub and per client, and clocks differ per machine. A Redis counter changed inside the same script as the state is the only order every observer can agree on.

## Consequences

A cursor costs one gRPC call, the coalescing window, one script and one Pub/Sub hop, about 16ms in the three-hub test against a 200ms target. A hub that dies is reported within TTL plus one sweep. Presence never affects document correctness: if presence-service is down, clients edit normally, joiners get an empty AwarenessSync, and their sessions appear once it recovers. A client keeps a tombstone for every client that left, which is bounded by the number of clients it has seen on that document.
