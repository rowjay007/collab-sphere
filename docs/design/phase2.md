# CollabSphere Phase 2 design: multi-hub fan-out, replay, presence

Three independent designs were produced (Opus, GPT, Grok). All three agree on the core: the Postgres
seq is the only global order, Redis Pub/Sub is a lossy hint published after Append, hubs heal gaps
from document-service, a reconnecting client resumes from a seq, and presence-service is the only
writer of presence keys. This document is the Opus design, which won on ordering and validation,
with four changes from the synthesis.

## Synthesis changes (these override the sections below)

1. **No replay ring.** `replay.Window` is dropped. MissedOps always comes from
   `ReadOperations(after=clientSeq)`, bounded by `maxReplay` (20,000). It is an indexed range read,
   and a second in-memory copy of the log earns its place only if a measurement asks for it.
2. **Single-instance mode stays first-class.** With `REDIS_URL` unset the hub uses an in-process bus,
   and with `PRESENCE_SERVICE_ADDR` unset it uses an in-process presence that loops events back to
   local peers. Phase 1 tests and single-instance development run without Redis. (From Grok.)
3. **Newest connection wins per hub.** A second join for a client ID already connected to the same
   hub closes the old peer with reason `superseded` instead of rejecting the new one, so an
   auto-reconnect that lands on the same hub while the old socket is half-open succeeds. (From Grok.)
4. **Clock bound kept.** Rejected: a per-client `clock == vector[client]+1` check, since a Lamport
   clock legitimately jumps past ops seen from others. Rejected: removing the bound, which reopens
   the clock-exhaustion attack. §3 catch-up-on-join makes the existing bound sound across hubs.

## 0. Thesis

The Postgres op log's `seq` is the only order in the system. Every hub delivers ops to its sockets in
strict `seq` order, every client resumes from one number (the highest contiguous `seq` it holds), and
Redis Pub/Sub only makes it faster for a hub to learn a `seq` range exists. If Redis loses a message, or
goes down entirely, a gap in `seq` or a one-second idle poll lets the hub fill the hole from
document-service. Redis cuts latency. It is never what makes the result correct.

Verified Phase 1 facts this rests on: `Append` runs under `SELECT ... FOR UPDATE` on the document row, so
seqs are a linearization of every hub's appends. Ops are never deleted from `operations`, since
snapshots are a derived cache (ADR 0004), so a replay from any seq is always possible. `LoadState`
reads `op_seq`, the snapshot and the tail in one repeatable-read transaction.

## 1. Callers first

### 1.1 Client reconnect (collabclient)

```go
c, _ := collabclient.Dial(ctx, collabclient.Options{URLs: hubs, Name: "ann", AutoReconnect: true}, docID, 42)
c.Edit(fn)            // online or offline: apply locally, append to c.unacked, send if connected
c.Disconnect()        // test hook: drop the socket, stay usable offline
c.Reconnect(ctx, hubs[2])
// wire: GET /ws/{doc}?client=42&seq=<c.seq>   (seq absent on first connect)
// 1st frame: DocumentSync{seq} | MissedOps{first_seq, ops, seq}
//   -> apply, drop from unacked every op now held (MissedOps contents, or Doc.Contains for a sync)
//   -> resend the remaining unacked, in original order, in batches of at most 1024
// 2nd frame (any time later): AwarenessSync{states, version}
// then OperationBroadcast{first_seq, ops}; require first_seq == c.seq+1, else close and reconnect
```

Client invariant: **replica = log[1..seq] ∪ own unacked ops**. The client sends nothing until its first
frame arrives. Auto-reconnect rotates through `URLs` with jittered backoff (50ms doubling to 2s).

### 1.2 Hub to fan-out

```go
sub := bus.Join(docID, h)       // SUBSCRIBE doc:{id} and presence-events:{id}; h implements fanout.Sink
<-sub.Ready()                   // at most 2s, then continue degraded; Sink.Resync fires when it does subscribe
doc, seq := store.Load(...)     // only after subscribing, so no frame can fall between load and subscribe
seqs := store.Append(...)       // per-op seq (new or pre-existing)
bus.Publish(docID, fanout.Batch{FirstSeq: f, Ops: newOnes})   // async and best effort; never blocks the loop
// inbound (dispatcher goroutine -> hub events): RemoteOps(Batch), Presence(*presencev1.Event), Resync()
```

### 1.3 Hub to document-service

`LoadState` (existing, now read with `document.op_seq`), `AppendOperations` (now returns `seqs`), and
the new `ReadOperations(after_seq, limit)` for gap fill, join catch-up and replay past the window.

### 1.4 Hub to presence

```go
others, ver := pres.Join(ctx, docID, sess)   // from a goroutine after the peer is registered, not the hub loop
pres.Move(docID, sess, anchor, head)         // non-blocking, latest-wins coalescing, flushed every 16ms
pres.Leave(docID, sess)                      // non-blocking, flushed with the next batch
// heartbeats are the writer's own job: every PRESENCE_HEARTBEAT it touches every live session
// events come back ONLY through Redis presence-events:{id}, even to the hub where they started
```

## 2. Ordering and fan-out contract between hubs

- **Published on `doc:{id}`:** one `fanout.v1.SequencedOps{origin, first_seq, ops}` per Append, holding
  only the ops that Append newly sequenced. Those ops have contiguous seqs `first_seq..first_seq+n-1`.
  The publish happens after the commit. Ops that were duplicates are never published.
- **Order:** Redis keeps the order of messages from one publisher connection. Across hubs, publishes can
  arrive out of commit order (hub A commits 10–12, hub B commits 13–15, and B's PUBLISH lands first).
  Each hub runs a `fanout.Sequencer` keyed by seq. It holds `delivered` (D) plus a buffer of ranges
  above D.
- **Delivery:** when the range starting at D+1 is present, the hub applies each op to `doc`. `Apply` is
  idempotent, so the hub's own ops come back as Duplicate. It then sends `OperationBroadcast{first_seq,
  ops}` to every local peer, sender included, sets D to the range's end, and repeats.
- **The hub's own append:** `Append` returns the seqs. The hub publishes the batch and also `Offer`s it
  to its own Sequencer. Its own echo on `doc:{id}` is dropped because those seqs are already
  ≤ D or already buffered.
- **Detection and healing:**
  1. *Gap:* the buffer is non-empty and D+1 is missing. Arm `gapGrace` (30ms). When it fires with the
     gap still open, call `ReadOperations(after=D, limit=4096)` in the loop and deliver. Fill at once
     if more than 64 ranges are buffered.
  2. *Idle:* when the hub has peers and D has not moved for `idleSync` (1s), run the same
     ReadOperations. This covers a hub that crashed after commit but before publish on a document
     nobody else is editing, and loss with no later message to expose it.
  3. *Resubscribe:* the dispatcher reads `ps.Receive` and does not use `ps.Channel()`. go-redis
     resubscribes silently after a reconnect, but a `*redis.Subscription` arriving for a channel that is
     already active means messages may have been lost. It triggers `Resync` on that hub: an immediate
     ReadOperations plus a presence `List` (§6).
  4. *Duplicate signal:* if Append reports a pre-existing seq above D, the hub knows ops up to it exist.
     That opens a gap at once, which fills after the grace period.
- **What `doc` is.** Between events, `doc = log[1..D] ∪ G`, where G is the hub's own appended ops still
  in the Sequencer above D. Remote ops are never applied before their seq is delivered. Accepted but
  unappended ops exist only inside one group-commit step, because the loop is serial. Every op in `doc`
  between events is therefore durable. Live peers only ever see `log[1..D]`.
- **In-process loss:** the dispatcher does a non-blocking send to a 1024-slot hub inbox and drops on
  full; gap or idle healing covers it, so a slow hub never stalls the shared subscriber.

*Rejected: Redis Streams (XADD) as the fan-out log.* It is durable and replayable, but it becomes a
second log that competes with Postgres as source of truth and needs trimming and consumer bookkeeping.
Postgres already hands out a gap-detectable seq, and the brief names Pub/Sub. *Rejected: publish
before append* (lower latency, as Phase 1 ADR 0002 rejected for the same reason). Other users would see
ops that might never become durable, and the message would carry no seq, so loss could not be detected.
*Rejected: sticky routing of a document to one hub.* That violates "several instances serve one
document", and a crash still forces the same recovery.

## 3. Validation when a dependency was produced on another instance

Rule: **before admitting a resuming peer, the hub catches up to at least the client's `seq`.** If
`clientSeq > D`, the hub runs ReadOperations from D until `D ≥ clientSeq` and delivers to all existing
peers. If `clientSeq > op_seq`, it sends `CODE_INVALID_RESUME` and closes.

After that, an honest client's dependencies and clocks always lie in `doc`. Its replica is
`log[1..clientSeq] ∪ own ops`. The first part is in `doc` after catch-up. Its own ops arrive in FIFO
order and are applied before the next one is validated. Afterwards it sees only this hub's delivered
stream, which is in `doc`. So the Phase 1 checks stay exactly as they are: client match;
`clock ≤ doc.Clock()+1`; `Ready`; `Apply`. A failure still means a buggy or hostile client and drops the
peer. **The clock bound is kept, justified by catch-up-on-join.** ADR 0005 gets amended to say so. The
hub never waits on Redis before rejecting. The proof does not need Redis.

One more condition makes this hold. A `DocumentSync` must not expose G, since a client could build on an
op whose seq a different hub has not delivered. Before sending a DocumentSync, the hub forces a gap fill
until G is empty. Then the snapshot is exactly `log[1..D]` and `DocumentSync.seq = D`.

*Rejected: park ops with missing dependencies in the CRDT pending map until the dependency arrives
through fan-out.* Memory grows without bound for hostile clients, and the sender's view silently
diverges. It is also unnecessary once catch-up-on-join exists. *Rejected: a vector-based clock bound*
(`clock ≤ max over the vector + 1`). It is weaker and still needs catch-up to be sound.

## 4. Ack semantics

- **An op is acked when its sender sees it in a sequenced frame** (`OperationBroadcast` or `MissedOps`),
  or when a `DocumentSync` snapshot `Contains` it. Everything in a DocumentSync is durable (§2). Seeing
  the op means it is durable at a known seq. There is no separate Ack message.
- **Resend after reconnect:** the client resends unacked ops after its first frame, in order.
  document-service dedupes by exact bytes under UNIQUE(document_id, op_client, op_clock) and now returns
  the existing seq. If that seq is ≤ D, the op was in MissedOps and the client never resends it. If it
  is above D, the ack arrives when delivery reaches it, via the duplicate signal in §2 step 4. In both
  cases the log holds the op once and every replica applies it idempotently. That is "exactly once".
- **Hub-side duplicate of an op `doc` already holds:** skipped silently, as in Phase 1. It is either
  ≤ D, so the client already holds it, or in G, so the ack comes with delivery.
- **Append error:** fail-stop as in Phase 1, closing peers with `CODE_UNAVAILABLE`, which means
  retryable. Clients now buffer, so nothing is lost. A timed-out Append that actually committed is
  resolved by resend-and-dedupe. This removes the loss ADR 0002 accepted.

*Rejected: an explicit `Ack{op_ids}` message.* It would be a second ordering channel that has to be kept
consistent with the broadcast, and the echo already proves both durability and seq. *Rejected: echo the
sender's own batch at once, out of seq order* (saves one Redis hop under contention). The client's
resume token would then have to be a set rather than a number.

## 5. Missed-op replay

- **Client sends** `seq` as a query parameter: the highest contiguous seq it has applied. *Rejected: a
  version vector.* It is exact only under per-client FIFO, and the server can only diff it by scanning
  the whole log. A seq turns that into an indexed range read.
- **Server answers** in the hub loop after catch-up (§3):
  - `seq` absent → `DocumentSync{seq: D}` after forcing G empty.
  - `D - clientSeq ≤ maxReplay` (20,000) → `MissedOps{first_seq: clientSeq+1, ops, seq: D}`. The ops
    come from `replay.Window`, a ring of the last 8,192 sequenced ops, or else from
    `ReadOperations(after=clientSeq)`. MissedOps is sent even when empty. It is the "you are attached,
    resend now" signal.
  - Otherwise → `DocumentSync`. The client rebuilds from the snapshot, re-applies its unacked ops
    (their dependencies are in `log[1..clientSeq] ⊆` the snapshot, or are its own), drops the ones the
    snapshot holds, and resends the rest.
- **Exactly once for offline and unacked ops** follows from §4. Causal order of the log holds because a
  hub only appends an op whose dependencies are durable or earlier in the same batch, and a duplicate's
  existing seq is always below any seq newly assigned after it.
## 6. presence-service

**Shape: the hub calls presence-service over gRPC for every write and read. presence-service is the only
writer of presence keys and publishes events on Redis `presence-events:{docID}`, which the hub receives
on its existing fan-out subscriber.**

Why: writes need one authority, for TTL, session compare-and-delete, a version counter and the expiry
sweep. Fan-out to N hubs is what Redis Pub/Sub already does, and the hub already holds that subscription.
*Rejected: the hub writes Redis directly through a shared library.* The brief asks for a service, and
every hub would carry the Lua and a sweeper. *Rejected: a gRPC `Watch` stream to the hubs.*
presence-service replicas would need Pub/Sub among themselves anyway, a second hop. *Rejected: keyspace
notifications for expiry.* They are lossy, reach only connected subscribers, and fire lazily, so the TTL
test would be timing-flaky.

Redis keys. All deadlines come from `redis.call('TIME')` inside the scripts, so there is no skew between
replicas.

| Key | Type | Contents |
| --- | --- | --- |
| `presence:{docID}:{clientID}` | string, PX TTL | `presence.v1.State` (session, anchor, head, name, version) |
| `presence-members:{docID}` | set | clientIDs |
| `presence-expiry` | zset | `{docID}\|{clientID}`, scored by deadline in ms (global, so one sweep covers every document) |
| `presence-ver:{docID}` | counter | INCR per change. Stamps states, events and syncs |

Lua scripts. Each one is atomic and publishes from inside the script:

- `upsert(doc, client, session, state, ttl)`: SET PX, SADD, ZADD deadline, INCR version, PUBLISH
  `Event{state}`. A new session overwrites an old one for the same client. That handles rejoining on
  another hub.
- `touch(doc, client, session, ttl)`: if the key exists with this session, PEXPIRE and ZADD. Otherwise it
  returns `missing`, and the writer re-upserts from its last known state.
- `remove(doc, client, session, reason)`: only if the stored session matches: DEL, SREM, ZREM, INCR,
  PUBLISH `Event{left{session, reason}}`. When the hub that held a client's old session drops that
  session, its Leave does nothing because the new session no longer matches.
- `sweep(limit)`: ZRANGEBYSCORE up to now; for each member, `ZREM`, and only if that returned 1, DEL,
  SREM, INCR, PUBLISH `left{EXPIRED}`. ZREM picks one winner across presence replicas.
- `list(doc)`: SMEMBERS, MGET, GET version. Expired-but-unswept entries are filtered by deadline.

Cadence: `PRESENCE_TTL` defaults to 30s, `PRESENCE_SWEEP` to 1s, and the hub's `PRESENCE_HEARTBEAT` to
10s (TTL/3). An expiry removal reaches everyone within TTL + sweep + one Redis hop.

**AwarenessSync on join:** after the hub registers the peer and sends its first frame, a goroutine calls
`Join`. That upserts the client's state with no cursor and returns `List`. The goroutine then
`TrySend`s `AwarenessSync{states, version}`.

**Ordering races are harmless because of versions:** a client applies an event only if its version is
above the one it knows for that client, remembers a Left's version as a tombstone, and on an
AwarenessSync drops entries not in the sync whose version is ≤ the sync's version.

**Cursor path:** the hub validates a local `CursorMove` (anchor and head in `doc`, same as Phase 1),
then calls `pres.Move`. A `presence.v1.Event` from Redis becomes a `PresenceBroadcast{version}` sent to
local peers, skipping the client it is about. Remote cursors are not validated, since they are CRDT IDs
that stay meaningful. Clients store unknown anchors and resolve them once the character arrives.

**Latency budget:** gRPC (about 1ms) + 16ms coalescing + Lua (about 0.3ms) + Pub/Sub hop (under 1ms)
+ socket write. That is under 25ms against a 200ms target.

**Heal:** on `Resync` (resubscribe) the hub fetches `List` and sends every local peer a fresh
AwarenessSync, which the version rules merge correctly.

## 7. Proto changes

```proto
// collab/v1/ws.proto
message ClientMessage { oneof payload { OperationBatch operation_batch = 1; CursorMove cursor_move = 2;
  AwarenessUpdate awareness_update = 3; Ping ping = 4; } }          // the reserved 3 is now used
message AwarenessUpdate { string display_name = 1; }
message ServerMessage { oneof payload { OperationBroadcast operation_broadcast = 1;
  PresenceBroadcast presence_broadcast = 2; DocumentSync document_sync = 3; MissedOps missed_ops = 4;
  ErrorMessage error = 5; Pong pong = 6; AwarenessSync awareness_sync = 7; } }
message OperationBroadcast { repeated crdt.v1.Operation operations = 1; uint64 first_seq = 2; }
message DocumentSync { string document_id = 1; crdt.v1.Snapshot snapshot = 2; uint64 seq = 3; }
message MissedOps { string document_id = 1; uint64 first_seq = 2;
  repeated crdt.v1.Operation operations = 3; uint64 seq = 4; }      // seq = D, the last seq covered
message PresenceState { uint64 client = 1; CursorMove cursor = 2; string display_name = 3; uint64 version = 4; }
message AwarenessSync { repeated PresenceState states = 1; uint64 version = 2; }
message PresenceBroadcast { uint64 client = 1; oneof state { CursorMove cursor = 2; PresenceLeft left = 3; }
  uint64 version = 4; string display_name = 5; }
// ErrorMessage.Code: CODE_INVALID_RESUME = 6; CODE_UNAVAILABLE = 7 (retryable: reconnect, resend unacked)

// document/v1/document.proto
rpc ReadOperations(ReadOperationsRequest) returns (ReadOperationsResponse);
message AppendOperationsResponse { uint64 op_seq = 1; repeated uint64 seqs = 2; } // parallel to request ops
message ReadOperationsRequest { string id = 1; uint64 after_seq = 2; uint32 limit = 3; } // limit <= 10000
message ReadOperationsResponse { uint64 first_seq = 1; repeated crdt.v1.Operation operations = 2; uint64 op_seq = 3; }

// fanout/v1/fanout.proto (Redis payload only, never on the WebSocket)
message SequencedOps { string origin = 1; uint64 first_seq = 2; repeated crdt.v1.Operation operations = 3; }

// presence/v1/presence.proto
service PresenceService {
  rpc Join(JoinRequest) returns (ListResponse);        // upsert self (no cursor), return everyone else
  rpc Apply(ApplyRequest) returns (ApplyResponse);     // batched upserts, touches, leaves across documents
  rpc List(ListRequest) returns (ListResponse);
}
message State { string document_id = 1; uint64 client = 2; uint64 session = 3; crdt.v1.OpID anchor = 4;
  crdt.v1.OpID head = 5; string display_name = 6; uint64 version = 7; bool has_cursor = 8; }
message Change { oneof kind { State upsert = 1; Touch touch = 2; Leave leave = 3; } }
message Touch { string document_id = 1; uint64 client = 2; uint64 session = 3; }
message Leave { string document_id = 1; uint64 client = 2; uint64 session = 3; }
message ApplyRequest { repeated Change changes = 1; }
message ApplyResponse { repeated Touch missing = 1; }
message JoinRequest { State self = 1; }
message ListRequest { string document_id = 1; }
message ListResponse { repeated State states = 1; uint64 version = 2; }
message Event { string document_id = 1; uint64 client = 2; uint64 version = 3;
  oneof kind { State state = 4; Left left = 5; } }
message Left { uint64 session = 1; enum Reason { REASON_UNSPECIFIED = 0; REASON_LEAVE = 1; REASON_EXPIRED = 2; } Reason reason = 2; }
```

document-service: `existingOps` also selects `seq`; `Append` fills `seqs` (new, existing, or the first
occurrence's for in-batch repeats); `ReadOperations` is a primary-key range read `seq > $2 LIMIT $3`.

## 8. Go sketches

```go
// services/collab-hub/internal/hub/store.go (changed seam)
type Store interface {
	Load(ctx context.Context, docID string) (doc *crdt.Doc, seq uint64, err error)
	Append(ctx context.Context, docID string, ops []crdt.Op) (seqs []uint64, err error)
	ReadOperations(ctx context.Context, docID string, after uint64, limit int) (replay.Range, error)
}
type Presence interface {
	Join(ctx context.Context, docID string, s PresenceSession) ([]*presencev1.State, uint64, error)
	Move(docID string, s PresenceSession, anchor, head crdt.ID)
	Leave(docID string, s PresenceSession)
}
type PresenceSession struct{ Client crdt.ClientID; Session uint64; Name string }
func NewRegistry(store Store, bus fanout.Bus, pres Presence, cfg Config) *Registry
// new hub events: remoteOpsEvent{fanout.Batch}, presenceEvent{*presencev1.Event}, resyncEvent{}, timerEvent{gen uint64}
// joinEvent gains: resume bool, seq uint64, name string

// services/collab-hub/internal/fanout
package fanout
type Batch struct{ FirstSeq uint64; Ops []crdt.Op }
type Sink interface { RemoteOps(Batch); Presence(*presencev1.Event); Resync() }
type Subscription interface { Ready() <-chan struct{}; Close() }
type Bus interface {
	Join(docID string, sink Sink) Subscription
	Publish(docID string, b Batch)
}
type Redis struct{ /* rdb, ps *redis.PubSub, sinks map[string]Sink, pubq chan pub */ }
func NewRedis(rdb *redis.Client, origin string) *Redis                       { panic("not implemented") }
func (r *Redis) Run(ctx context.Context) error                               { panic("not implemented") } // Receive loop + publisher
func (r *Redis) Join(docID string, sink Sink) Subscription                   { panic("not implemented") }
func (r *Redis) Publish(docID string, b Batch)                               { panic("not implemented") }
type Memory struct{ /* in-process bus for multi-registry unit tests; can drop/reorder on demand */ }
func NewMemory() *Memory                                                     { panic("not implemented") }
func (m *Memory) Join(docID string, sink Sink) Subscription                  { panic("not implemented") }
func (m *Memory) Publish(docID string, b Batch)                              { panic("not implemented") }
func (m *Memory) SetLoss(drop func(docID string, b Batch) bool)              { panic("not implemented") }
type Sequencer struct{ delivered uint64; pending map[uint64]Batch }
func NewSequencer(delivered uint64) *Sequencer                               { panic("not implemented") }
func (s *Sequencer) Offer(b Batch) (ready []Batch)                           { panic("not implemented") } // trims overlap
func (s *Sequencer) Delivered() uint64                                       { panic("not implemented") }
func (s *Sequencer) Gap() (open bool, buffered int)                          { panic("not implemented") }
func DocChannel(docID string) string      { return "doc:" + docID }
func PresenceChannel(docID string) string { return "presence-events:" + docID }

// services/collab-hub/internal/replay
package replay
type Range struct{ FirstSeq uint64; Ops []crdt.Op; OpSeq uint64 }
type Window struct{ /* ring of (seq, op), capacity fixed */ }
func NewWindow(capacity int) *Window                                         { panic("not implemented") }
func (w *Window) Add(firstSeq uint64, ops []crdt.Op)                         { panic("not implemented") }
func (w *Window) Range(after, through uint64) ([]crdt.Op, bool)              { panic("not implemented") }
type Kind int
const ( KindSync Kind = iota; KindMissed )
type Plan struct{ Kind Kind; FirstSeq uint64; Ops []crdt.Op }
type Reader func(ctx context.Context, after uint64, limit int) (Range, error)
func Resolve(ctx context.Context, w *Window, read Reader, clientSeq, delivered uint64, maxOps int) (Plan, error) {
	panic("not implemented")
}

// services/collab-hub/internal/presence (client side: coalescing writer + heartbeat)
type Writer struct{ /* client presencev1.PresenceServiceClient, latest map[key]*Change, live map[key]*State */ }
func NewWriter(c presencev1.PresenceServiceClient, flush, heartbeat time.Duration) *Writer { panic("not implemented") }
func (w *Writer) Run(ctx context.Context) error                               { panic("not implemented") }
// implements hub.Presence

// services/presence-service/internal/store
type Store struct{ rdb *redis.Client; ttl time.Duration }
func New(rdb *redis.Client, ttl time.Duration) *Store                            { panic("not implemented") }
func (s *Store) Upsert(ctx context.Context, st *presencev1.State) error         { panic("not implemented") }
func (s *Store) Touch(ctx context.Context, t *presencev1.Touch) (bool, error)   { panic("not implemented") }
func (s *Store) Remove(ctx context.Context, l *presencev1.Leave) error          { panic("not implemented") }
func (s *Store) List(ctx context.Context, docID string) ([]*presencev1.State, uint64, error) { panic("not implemented") }
func (s *Store) Sweep(ctx context.Context, limit int) (int, error)              { panic("not implemented") }
// internal/grpc: Server{store}; cmd: REDIS_URL, GRPC_ADDR(:9091), PRESENCE_TTL, PRESENCE_SWEEP

// pkg/crdt: Contains reports whether an insert or delete with this ID is held or collected.
func (d *Doc) Contains(id ID) bool { panic("not implemented") }
```

Hub env: `REDIS_URL`, `PRESENCE_SERVICE_ADDR` (both required), `PRESENCE_HEARTBEAT` (10s), `HUB_ID`.
`hub.MemoryStore` gains a seq log, so unit tests run three Registries on one `fanout.Memory` under -race.

## 9. Hub lifecycle per instance

- **Start:** `bus.Join` → Ready (2s, then degraded) → `Load` → `Sequencer(D)`, dropping frames ≤ D.
- **Join:** catch up if `clientSeq > D` → `Resolve` → register → first frame → presence-join goroutine
  sends AwarenessSync.
- **Leave:** `pres.Leave`; removal reaches every peer via Redis. **Last leave:** stop, `sub.Close()`
  (UNSUBSCRIBE both channels); a new join reloads.
- **Eviction (Append failure):** `CODE_UNAVAILABLE` to every peer → `pres.Leave` for all → `sub.Close()`
  → `registry.evict`. Clients reconnect and resend.
- **Instance crash (SIGKILL):** sockets reset; clients auto-reconnect elsewhere with `seq` and resend.
  Ops the dead hub committed but never published are healed by the duplicate seq signal from those
  resends, the next append's gap, or the 1s idle poll. Its presence entries expire through the sweep
  unless their clients rejoin, which overwrites the session so no false removal fires.
- **SIGTERM:** `registry.Close` → peers get `CODE_UNAVAILABLE` (clients reconnect elsewhere) → the
  writer flushes Leaves (1s cap).
- **Redis outage:** publish errors are logged and dropped (the queue drops when full). Cross-hub
  delivery degrades to gap and idle polling: one second of latency, nothing lost, still converging.
  Presence calls fail; the writer retries with backoff and AwarenessSync is sent empty. On recovery,
  go-redis resubscribes, the `*redis.Subscription` triggers `Resync` per document, and ops and presence
  heal. The hub never fail-stops for Redis. *Rejected: fail-stop on Redis loss.* It would turn a
  latency dependency into an availability dependency.

## 10. Test harness and how each done run is driven

`tests/internal/stack`:

- It builds presence-service too. `New(t, opts...)` takes `WithHubs(n)` (default 1, so Phase 1 tests
  are untouched) and `WithPresenceTTL(d)` (sets `PRESENCE_SWEEP=TTL/10`, `PRESENCE_HEARTBEAT=TTL/4`).
- Redis comes from `COLLAB_TEST_REDIS_URL` (compose already has it on 56379); skip if unset. Pub/Sub
  ignores DB numbers, but names contain the per-test document UUID. The shared expiry zset is safe
  because its scores are absolute deadlines.
- New methods: `HubURL(i)`, `KillHub(i)` (SIGKILL), `StopHub(i)`, `StartHub(i)`, `Dial(ctx, doc, client, hubIdx)`.
- `Settle` also compares every snapshot with the `LoadState` snapshot (`proto.Equal`) and checks
  `Unacked()==0`.
- `AssertExactlyOnce(t, docs, doc, generated)` checks that `op_seq` equals the number of distinct op
  IDs the clients generated, and that `Contains` holds for every one of them.

| Run | How it is driven |
| --- | --- |
| 1 | `tests/load/hundred_test.go` gains `TestHundredEditorsThreeHubs`: three hubs, client *i* on hub *i%3*, 40 typist edits each, Settle, LoadState equal, -race. |
| 2 | `tests/e2e/reconnect_test.go`: three hubs, Y on hub 1. X on hub 0 edits, then `X.Disconnect()`, makes 50 offline edits (some on its own unacked chars), while Y makes 50. `X.Reconnect(HubURL(2))`. Assert `X.LastJoin()==KindMissed`, Settle, AssertExactlyOnce, every X op acked. A variant uses a `seq` older than the window to exercise the ReadOperations path. |
| 3 | `tests/e2e/failover_test.go`: three hubs, 12 auto-reconnect clients across them, editing continuously. At t=1s, `KillHub(1)`. Editors on hub 1 keep editing offline and auto-reconnect. Stop edits at 3s, then Settle and AssertExactlyOnce. |
| 4 | `tests/e2e/presence_test.go`, TTL 1.5s: A on hub 0 and B on hub 1. 20 cursor moves, each timed until `B.WaitPresence` matches; assert every one is under 200ms. C joins on hub 2 and gets one AwarenessSync holding A and B. `A.Close()` gives a Left at B and C. D on hub 1, then `KillHub(1)`, gives a Left (EXPIRED) at A and C within TTL+1s. |
| 5 | The unchanged Phase 1 suite: `make test-integration`, plus hub unit tests with the in-memory bus. |

## 11. Units to land, in order (each green alone, each pushable)

Unit 6 (presence-service) shares no files with units 1 to 5 except `go.mod`, so it runs in parallel
with them.

1. **document-service seqs:** `AppendOperationsResponse.seqs` and `ReadOperations` (proto, store, gRPC,
   tests, including duplicates returning their existing seq).
2. **Single-hub seq stream and replay:**
   - `pkg/crdt` `Contains`.
   - ws.proto: `first_seq`, `seq`, MissedOps, the two error codes.
   - `hub.Store` v2, `fanout.Sequencer`, `replay.Window`/`Resolve`.
   - Join with `seq`, catch-up, forced empty G before a sync.
   - ADR 0007 (seq-ordered delivery) and the ADR 0005 amendment.
   - Phase 1 tests still pass, with one hub and no bus. A local no-op bus is the single-instance mode,
     not a stub.
3. **collabclient resume:** the unacked queue, offline Edit, Disconnect/Reconnect/auto-reconnect, the
   contiguity check, `LastJoin`. An e2e test on one hub covers offline edits, reconnect and MissedOps.
4. **fanout over Redis:** `fanout.Redis`, `fanout.Memory`, publish after append, gap/idle/resync
   healing. Unit tests run three registries on Memory with forced drops and reordering. ADR 0008.
5. **N-hub harness:** stack `WithHubs`, `KillHub`. Runs 1, 2 and 3.
6. **presence-service:** proto, Lua store, sweeper, gRPC, integration tests against Redis (TTL 300ms).
   ADR 0009.
7. **Hub presence integration:** the Writer, AwarenessSync, AwarenessUpdate, cursors through presence
   events, `WaitPresence` in collabclient. Run 4. README Phase 2 section.

## 12. Risks

| Risk | Mitigation |
| --- | --- |
| Seq-ordered delivery makes an ack wait for gaps under cross-hub contention (Redis reorder: under 1ms; worst case gapGrace + one read, about 35ms). | Measure in run 1. Tune `gapGrace`. Fill at once when the buffer exceeds 64 ranges. |
| Per-document Postgres row lock bounds the write rate across all hubs. | It is unchanged from Phase 1. Group commit per hub keeps three hubs to three appends in flight. Phase 5 can measure. |
| Idle polling costs N hubs × open documents × 1 read/s. | Only when D is stale and the hub has peers. It is an indexed empty range read. |
| The hub loop blocks on catch-up and cold replay. | Bounded (limit and maxReplay). The window makes it rare. Presence calls never run in the loop. |
| The same client ID is live on two hubs during a reconnect race. | Ops are idempotent. Presence uses session compare-and-delete. The old socket's ops are still valid. |
| Phase 1 hub unit tests assert local cursor and Left frames. | They now arrive through the presence path. The in-memory Presence loops events back, and the tests wait for frames instead of asserting synchronously. |
| Pub/Sub ignores DB numbers. | UUID-scoped names. Documented in stack. |
| Timer-based presence assertions under -race on a loaded laptop. | Measure from send to receipt with a 200ms bound. Coalescing at 16ms leaves margin. |
| Snapshot fallback for a huge document is slow. | Only past maxReplay (20k ops). |

Rejected alternatives at a glance: Redis Streams log; publish before append; sticky document routing;
pending-dependency parking; vector-based clock bound; explicit Ack message; out-of-order own echo;
version-vector resume token; hub-direct Redis presence; gRPC Watch stream; keyspace-notification
expiry; fail-stop on Redis loss.
