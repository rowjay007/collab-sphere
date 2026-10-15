# ADR 0008. Hubs share appends over Redis Pub/Sub, as a lossy hint

Status: accepted, Phase 2.

## Context

Several collab-hub instances serve one document. Each appends its clients' ops to document-service and delivers the log to its own sockets in seq order (ADR 0007). Without a bus, a hub learns about another hub's appends only when its own next append reveals a gap, so a reader on a quiet hub sees nothing. The hubs need a fast path for "these seqs now exist", and the log in Postgres must stay the only source of truth.

## Decision

**Publish after append.** When `Append` returns, the hub publishes one `fanout.v1.SequencedOps{origin, first_seq, ops}` on `doc:{id}`. It holds only the ops that Append newly stored, at seqs `first_new_seq..op_seq` in seq order. Ops the log already held are never published. The publish goes into a bounded queue of 1,024 that a separate goroutine drains. A full queue drops the batch, so a publish never blocks the hub loop and never fails the hub.

**Receive into the Sequencer.** Every hub subscribed to `doc:{id}` offers each received batch to its `fanout.Sequencer`, exactly as it offers its own appends. Its own echo is already delivered or buffered, so the Sequencer trims it. The Redis bus also drops batches by `origin`, but nothing depends on that. One shared subscriber connection reads every document's channels. It hands a batch to a hub with a non-blocking send into a 1,024-slot inbox and drops it when the inbox is full, so one slow hub never stalls the others.

**Subscribe before Load.** A hub joins the bus, waits up to 2s for Redis to confirm the subscription, then loads the document. A batch published after the load's snapshot therefore always reaches it. If the confirmation is late, the hub starts anyway and resyncs when it arrives. The hub also subscribes to `presence-events:{id}`, which a later unit acts on. When the last client leaves, or the hub fail-stops, the hub closes its subscription and the bus sends UNSUBSCRIBE.

**Three ways to heal.** Pub/Sub delivers at most once and drops messages while a subscriber is disconnected. Every loss is repaired from the log through the same fill path that ADR 0007 uses for gaps.

1. *Gap.* A later batch exposes the missing seqs, and the 30ms gap timer reads them.
2. *Idle.* When the hub has clients and `D` has not advanced for 1s, it reads the log past `D`. This covers a lost final message, which leaves no later batch to expose a gap, and a hub that crashed after its commit but before its publish on a document nobody else edits. Finding nothing is the normal outcome. A failed idle read retries on the next tick and does not count toward the gap timer's fail-stop.
3. *Resync.* go-redis resubscribes silently after a reconnect, and messages published in between are gone. The bus reads with `PubSub.Receive` rather than `Channel()`, so it sees each subscribe confirmation. A confirmation for a channel that was already live means a reconnect happened, and every hub on that document reads the log at once.

**Redis down.** Publishes fail and are logged at most once per second. The subscriber retries with backoff. Hubs keep serving their own clients and converge on other hubs' ops through gap and idle healing, one second behind at worst. When Redis returns, the resubscribe triggers a resync. The hub never fail-stops for Redis.

**Single instance.** With `REDIS_URL` unset, the hub uses `fanout.Memory`, an in-process bus. It delivers each batch back to its own publisher, where the Sequencer trims it. Tests run several registries on one `Memory` and use its hooks to drop, delay and reorder deliveries.

## Alternatives rejected

- **Redis Streams as the fan-out log.** Streams are durable and replayable, but they become a second log that competes with Postgres as the source of truth, and they need trimming and consumer bookkeeping. Postgres already hands out a seq that makes every loss detectable.
- **Publish before append.** It saves one round trip. But other users would see ops that might never become durable, and the message would carry no seq, so a lost one could not be detected. ADR 0002 rejected the single-hub version for the same reason.
- **Fail-stop when Redis is lost.** It would turn a latency dependency into an availability dependency. Every hub would drop every client during a Redis restart even though the log still serves them correctly.

## Consequences

Redis affects latency, not correctness. A lost message costs at most the gap grace plus one read when traffic continues, or the idle interval when it does not. Each hub with clients on a quiet document reads the log once a second. That read is an indexed empty range read, and it stops when the last client leaves. Pub/Sub ignores database numbers, so channel names carry the document ID and every hub on one Redis shares one namespace.
