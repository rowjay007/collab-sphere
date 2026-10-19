# CollabSphere Phase 3 design: version history and comments

Three independent designs were produced (Opus, GPT, Grok), each starting from a different answer to
op identity across branches: server-allocated replicas, rewriting IDs at merge, and per-document IDs
derived on the client. All three converged on the rest: a branch is a real document, merge is a plain
CRDT union of op sets with no ID rewriting (GPT proved a pure collision-free rewrite cannot exist in a
fixed-width ID space), checkpoint names and timestamps stay out of the hash, restore appends new ops
instead of rewinding, and diff matches characters by shared CRDT IDs. This document is the Opus design,
which won on merge-poisoning defense, the hand-specified hash format and comment survival across
restore, with three changes from the synthesis.

## Synthesis changes (these override the sections below)

1. **Replicas are claimed within a fork tree; registration is not mandatory.** `replicas` is keyed
   `(id, fork_root)` with the home `document_id`. A join with no `client` allocates a fresh ID from a
   sequence (skipping any ID already present). A join with an explicit `client` is accepted if that ID
   is homed at this document, or claimed for this document (one insert) if no document in the same fork
   tree homes it; otherwise the hub refuses with `CODE_UNKNOWN_REPLICA`. This keeps every guarantee the
   Opus rule bought (a device's D replica is refused on branch B, so nobody can mint B ops under a D
   replica and poison a later merge; every op ID has one author log in a tree; version vectors stay
   exact) while Phase 1 and 2 callers and tests that pass explicit IDs keep working unchanged. The
   migration backfills `(op_client, doc, doc)` for every existing document, each its own fork root.
   This replaces the `kind='legacy'` rows and the partial unique index. (From Grok's "check only what
   you can verify" stance and the cost of migrating every caller.)
2. **No allocation rate limit in Phase 3.** An explicit-ID claim already lets an unauthenticated client
   create replica rows, so a token bucket on allocation alone defends nothing. Row growth from abuse is
   recorded as a Phase 4 item, closed when replicas bind to authenticated users.
3. **Merge and restore are serialized per document by version-service's lock and recorded as
   persisted mutation plans exactly as below**; GPT's `ApplyTransition` with `expected_seq` inside
   document-service was rejected because the persisted plan plus Append's byte-exact dedupe already
   gives crash-safe, idempotent retries without a second write API on document-service. Restore and
   merge compute their plan against the head they read; ops that editors append concurrently are kept
   (CRDT intent), and the acceptance runs quiesce editors around restore exactly as the predicate says.

Kept from the others: GPT's argument against rewriting is recorded in ADR 0011; Grok's `ensure`-on-touch
naming for materialization; both agreed that comments do not ride the WebSocket in Phase 3.

## Done predicate

The six runs restated in §10 below (branch and merge with one device on both documents, live restore, diff oracle, 100-op versions with an independent hash check, comment anchors under concurrent edits and across merge, Phase 1 and 2 green), on real processes under `-race`.

---

## Problem

Phase 3 adds a Merkle DAG of versions (auto every 100 ops, checkpoints, branch, merge, diff,
restore) and CRDT-anchored comments. Four facts from the repo shape everything:

- The `operations` log is append-only and never truncated, so the state at any `(doc, seq)` is a
  pure function of the log. A version is a cached, hashed name for one log prefix.
- Op IDs are Lamport `(clock, client)`, and clients choose `client`. A device reusing its ID on D and
  branch B mints `(7, 500)` twice with different content, so merge hits `OP_CONFLICT` for good.
- `VersionVector` is max-clock-per-client. `Covers` is exact only if every document holds a
  *prefix* of each replica's op sequence (ADR 0001, ADR 0006). Branch and merge must keep that true.
- RGA has no undelete. Restore must re-create text under new IDs, and comments must follow it.

Constraints kept: document-service owns live state, merge stays a pure CRDT union, anchors are IDs,
the WebSocket stays binary protobuf, services talk gRPC, and storage is Postgres plus Redis only.

## Usage (caller's view)

A version-service user, over gRPC (the test is the first caller):

```go
v, _ := vs.Checkpoint(ctx, &versionv1.CheckpointRequest{DocId: d, Name: "draft-1", RequestId: rid()})
br, _ := vs.Branch(ctx, &versionv1.BranchRequest{VersionId: v.Id, Title: "alt ending", RequestId: rid()})
b := br.Document.Id                 // a real document: hubs and clients edit it unchanged
cB, _ := collabclient.DialWith(ctx, b, collabclient.Options{URLs: hubs}) // replica allocated on join
cB.InsertAt(0, 'x')
m, _ := vs.Merge(ctx, &versionv1.MergeRequest{SourceDocId: b, TargetDocId: d, RequestId: rid()})
// m.Version.Parents == [D's previous tip, B's head node]; m.Appended == ops new to D; m.Noop on re-run
r, _ := vs.Restore(ctx, &versionv1.RestoreRequest{DocId: d, VersionId: v.Id, RequestId: rid()})
// D's text == v's text; r.Version is a NEW node whose metadata says restored_from = v.Id
diff, _ := vs.Diff(ctx, &versionv1.DiffRequest{FromVersionId: v.Id, ToVersionId: m.Version.Id})
for _, s := range diff.Spans { fmt.Println(s.Kind, s.Text) } // RETAINED / INSERTED / DELETED runs
vers, _ := vs.ListVersions(ctx, &versionv1.ListVersionsRequest{DocId: d}) // autos at 100, 200, ...
```

A client device, through collabclient. The device holds one replica per document and persists it
with its offline state:

```go
c, _ := collabclient.DialWith(ctx, docID, collabclient.Options{URLs: hubs}) // no client ID: hub allocates
id := c.ID()                        // learned from DocumentSync.client; reused on every reconnect
c.Edit(...)                         // unchanged; offline buffering unchanged
r := c.ResolveAnchor(start, end)    // local, live: crdt.(*Doc).ResolveRange on the client's replica
```

A comment caller, over gRPC (a frontend gateway arrives with Phase 4 auth):

```go
t, _ := cs.CreateThread(ctx, &commentv1.CreateThreadRequest{DocId: d, Start: idA, End: idB,
    Author: "ann", Body: "tighten this @bob", RequestId: rid()})   // mentions parsed server-side
cs.Reply(ctx, &commentv1.ReplyRequest{ThreadId: t.Id, Author: "bob", Body: "done", RequestId: rid()})
cs.SetResolved(ctx, &commentv1.SetResolvedRequest{ThreadId: t.Id, Resolved: true, Actor: "ann"})
ls, _ := cs.ListThreads(ctx, &commentv1.ListThreadsRequest{DocId: d, Resolve: true}) // Range per thread
```

Phase 3 does not push comments over the WebSocket. Clients poll `ListThreads`, and anchors resolve
against the live local replica, so highlights follow concurrent edits at keystroke latency.

## Shape

### 1. Op identity: server-allocated, document-homed replicas (stance accepted, sharpened)

**Decision.** Every replica ID is allocated by document-service from one Postgres sequence. Each ID
is *homed* in exactly one document. A replica may author ops only in its home document. Ops reach
other documents only by fork (copying the log) or merge (copying ops). So each op ID has exactly one
author log, and its bytes are fixed there. The union of any set of documents is conflict-free.
Merge in either direction is `Append` of the source ops that the target lacks. There is no rewriting.

The home rule does more than the stance asked for. Global uniqueness alone stops honest collisions.
The home check also stops a hostile client from minting B ops under D's replica IDs to poison a later
merge, and it needs no Phase 4 auth: the hub refuses a replica not homed in the document it joins.
It also preserves the prefix invariant. A replica's ops enter its home log in clock order, and fork
and merge copy whole states, which are prefix-closed per replica. So every document holds a prefix of
every replica's sequence, and `VersionVector.Covers` stays exact. Containment
("does D hold all of B@hb?") is then an O(replicas) vector comparison.

**Protocol change (minimal).** The `client` query param becomes optional. Absent means allocate. The
hub's HTTP handler calls `AllocateReplica(doc, DEVICE)` before `NewPeer`. The new
`DocumentSync.client` field returns the ID. Present means verify: `GetReplica(id).document_id` must
equal the joined doc, or the hub answers with the new terminal code `CODE_UNKNOWN_REPLICA`. Hubs cache
positive checks for the hub's lifetime, because homes never change. The hub rejects `Insert.copy`
from sockets. Only version-service writes copies.

**Offline clients.** A device needs a `DocumentSync` before it can edit, and the ID rides on that frame.
After that, offline editing is unchanged: the client persists `(docID, replica, seq, unacked)`.

**Migration of client-chosen IDs.** Migration 0002 backfills
`replicas(id, document_id, kind='legacy')` from `SELECT DISTINCT document_id, op_client FROM
operations`. The PK is `(id, document_id)` because legacy IDs repeat across documents. A partial
unique index `UNIQUE (id) WHERE kind <> 'legacy'` makes every new ID globally unique. Allocation loops
on `nextval` until the value matches no existing row of any kind, so it skips legacy values. Legacy
duplicates live only in unrelated documents, and merge requires the same `fork_root`, so they never
meet. A branch inherits no replica rows, so a device reusing its legacy ID on B is refused and
allocates. The in-repo collabclient and every test migrate in the same unit
(migrate-callers-then-delete). Explicit IDs remain accepted only when registered.

**Abuse until Phase 4.** Allocation happens only inside a WebSocket join of an existing, active
document, and a per-remote-address token bucket on each hub limits it (10/s, burst 20). Rows are about
40 bytes. Rows are never pruned in Phase 3, because an offline device may hold an allocated ID with
unsynced ops for days. The residual risk is the pre-existing one: a client can claim another live
replica of the same document. Phase 4 adds `replicas.user_id` and binds a replica to a session.

**System replicas.** Restore ops come from one idempotently allocated `kind='system'` replica per
document. Restores are serialized per document (section 4), so its clocks only rise.

### 2. Merkle DAG

**A node is the state of one log prefix**, one node per `(doc, seq)`. Fields that are hashed are those
that are a pure function of the immutable log plus the recorded fork and merge events. Anyone can
recompute them, and creation is idempotent across retries and replicas.

```
content  = CanonicalSnapshot(state of log[1..seq])        // crdt.CanonicalBytes, below
content_hash = SHA-256(content)
node_id  = SHA-256( "CSV1" | doc_uuid[16] | uvarint(seq) | content_hash[32]
                    | uvarint(len(parents)) | parent_id[32]... )
parents  = []                       for D@0 (genesis of a non-forked doc)
           [base]                   for a fork's first node B@s; base = node(D@s)
           [prev]                   for any later node; prev = previous node of the same doc by seq
           [prev, source_head]      for a merge node
```

The node ID covers the location `(doc, seq)`. That ties the hash to the log, so verification can
rebuild content from the log, and every node has exactly one location. Metadata stays out of the hash:
kind labels (auto, checkpoint, merge, restore), checkpoint names, `restored_from`, author, and
`created_at`. Those vary with request time and would make an auto node non-reproducible. Parent order
is significant: the first parent is the document's own line.

**Canonical snapshot (one-way door, ADR 0010).** This is a hand-specified format, not protobuf. Go's
deterministic marshaling is explicitly not stable across library versions, and JSONB normalizes
bytes, so the hashed bytes live in `bytea`, not JSONB as the spec sketch suggests.

```
"CSS1"
uvarint n_nodes, then per node in document order (pre-order, siblings by ID descending):
  uvarint id.client, id.clock, parent.client, parent.clock, value, flags(bit0=copy)
  uvarint n_deleted, then (client, clock) ascending
uvarint n_vector,    (client, clock) ascending by client      // kept: survives Phase 5 GC
uvarint n_collected, (client, clock) ascending by client
```

All integers are minimal unsigned LEB128. A state with pending ops cannot be encoded and returns an
error, but log prefixes never hold pending ops because hubs reject missing dependencies and merges
append in dependency order. `FromCanonical` re-encodes and requires byte equality, which rejects
non-minimal varints. A 20k-rune document is about 300 KB.

**Storage.** Blobs are flat and content-addressed: `blobs(hash PK, bytes)`, inserted with
`ON CONFLICT DO NOTHING`. Identical states dedupe. B@s and D@s are separate nodes that share one blob.
The hash is defined over the flat bytes, so Phase 5 can store blobs as content-defined chunks under a
manifest without changing any ID.

**Refs.** There is no stored "head" ref. A document's live head is its `op_seq` in document-service,
and its DAG tip is `version_docs.tip`, both derived. Named checkpoints are immutable refs:
`checkpoints(doc, name) → node`. Recreating a name with the same node is a no-op; a different node
returns `AlreadyExists`. A branch is a document whose `fork_of/fork_seq` point at its base.

**Ownership.** version-service owns its tables in its own database. Each service has its own
`schema_migrations`, so they cannot share one database. document-service owns `replicas` and the fork
columns. comment-service owns its tables. No service reads another service's SQL.

### 3. Every-100-ops versions: derived, materialized on touch and by a sweeper

An auto node at `D@100k` is a pure function of the log, so nobody has to "create" it at the right
moment. `Materialize(doc, upTo)` takes the per-document lock (`version_docs` row `FOR UPDATE`), first
resumes any pending mutation (see 4), then loads the tip blob, pages `ReadOperations(after=tip.seq)`,
applies, and emits a node at each multiple of 100 ≤ `upTo`. Each insert uses `ON CONFLICT DO NOTHING`,
and the tip moves in the same transaction. A crash leaves a consistent tip, and rerunning converges
(make-operations-idempotent).

Two triggers call it. **On touch**: every version-service RPC on a document first materializes to
`op_seq`, so reads are exact. **Sweeper**: every 5s, `ListChangedDocuments(since − 10s)` on
document-service (an index on `documents.updated_at`, which every Append already bumps) returns active
documents. The overlap absorbs commit-order versus `now()` skew, and idempotence makes it harmless.

Rejected: a push from document-service, because version-service already calls document-service and
that makes a cycle. Also rejected: a Redis `doc:*` psubscribe, which receives every keystroke of
every document.

The first touch of a document derives its chain start from `Document.fork_of`. A non-fork starts with
genesis at seq 0. A fork starts with `B@fork_seq` and parent `node(fork_of@fork_seq)`, materialized on
the source first. This is why Branch needs no crash bookkeeping (derive, don't sync).

### 4. Branch, merge, restore: one per-document mutation state machine

**Branch.** document-service `ForkDocument(source, seq, title, request_id)` runs in one transaction. It
inserts the document row with `fork_of`, `fork_seq` and `fork_root` (the source's root, or the source
itself), copies op rows `1..seq` with their seqs (`INSERT … SELECT`), copies the latest snapshot at or
below `seq`, and sets `op_seq = seq`. It is idempotent on `request_id`. Copying keeps the invariant
"the `operations` rows are exactly the ops the document holds", which dedupe, `ReadOperations(0)`,
oracles and GC all rely on. A virtual log was rejected because it would touch every read path. Hubs
and clients see an ordinary document. version-service then touches B (section 3).

**Mutations.** Merge and restore write to document-service and then record a node in version-service,
which is two systems and so has a crash window. Each request becomes a row:

```sql
mutations(request_id PK, doc uuid, kind merge|restore, state planned|appended|done,
          source_node bytea, target_node bytea, plan bytea, result_node bytea)
UNIQUE (doc) WHERE state <> 'done'          -- at most one in flight per document
```

The plan (the exact ops to append) is persisted before any append. Every entry path that takes a
document lock first drives that document's pending mutation to `done`. Resuming re-Appends the plan in
chunks of at most 10,000 ops. `Append` dedupes identical bytes and returns existing seqs, so
`h = max(returned seqs)` locates the mutation exactly, whether or not the first attempt committed.
Then it materializes autos below `h`, inserts the node at `D@h`, and marks the row done. Locks are
taken in UUID order when a merge needs both documents.

**Merge B into D.** Lock B and D. Require the same `fork_root`. Materialize B to `hb`, so
`N_b = node(B@hb)`. If D's state covers `N_b`'s vector, return `noop` with the node that first
contained it. Otherwise:

```
plan = [op in FromCanonical(blob(N_b)).Ops() if !dState.Contains(op.ID)]   // canonical order = dependency order
```

Persist the plan and run it. The node at `D@h` has parents `[D tip, N_b]`. Live D editors keep typing:
the plan commits as at most a few Appends under the existing row lock, and hubs learn the new seqs
through the gap timer (30ms after their next append) or the 1s idle poll. Hubs, clients and the clock
bound are unchanged. A rerun is a noop by vector cover. Reversing direction is the same code.
Repeated merges append only the delta.

**Restore D to V.** Require `D ⊇ V` (vector cover). Otherwise return `FailedPrecondition`, because the
originals must be present to anchor copies. Restoring to a version taken on B after a merge into D
qualifies. Lock D, load head `H`, and run `plan = crdt.PlanRestore(H, V, systemReplica)`:

- `rep(c)` for each `c ∈ Vis(V)` is `H.Resolve(c)`, the first visible node in c's copy lineage
  (c, or a copy of c, recursively).
- If `c` has no representative, the plan inserts `Insert{Parent: c, Value: c.value, Copy: true}` with a
  clock above `H.Clock()`. That makes it c's first child, so it sits immediately after c's tombstone,
  in V's exact position. Relative RGA order never changes, so V's text re-forms in V's order.
- Every visible node of H that is not some chosen `rep` is deleted. That covers text added after V and
  extra copies left by a merge of two restores.

Then `Text(H ∪ plan) == Text(V)`, and planning against the result returns nothing (property test).
Editors who type during a restore keep their concurrent characters (CRDT intent). The done-predicate
run quiesces first. The node at `D@h` has metadata `restored_from = V`. Every node after V stays, and
nothing is destructive.

**Effect on comments.** Anchors on deleted characters resolve through `Resolve` to the copies, so a
restore re-attaches the comments on the text it brings back. The copy relation lives in the op itself
(`Insert.copy` plus its parent), so it survives merge and fork with no side table.

### 5. Diff

`crdt.Diff(a, b)` merges both states. They are disjoint-or-equal op sets in one ID space, so the union
is a plain CRDT merge. It then walks the union once in document order. Each *logical character* is the
lineage root of a node (a copy maps to its original), and its class is visible-in-a, visible-in-b, or
both. Nodes are emitted at the root's position, and same-kind runs are coalesced into
`Span{kind, text, first, last}`. Because lineage counts as identity, `Diff(V, restore-of-V)` is empty
rather than a delete and insert per character. The fast path is `content_hash` equality, which returns
nothing. The cost is O(|a| + |b|). Diff across a branch and its merge needs nothing special, because
the IDs are shared.

### 6. Comments

**Anchor.** An anchor is an inclusive range `[start, end]` of character IDs. Both must be visible and
acked at creation (comment-service checks via `LoadState`), and start precedes or equals end. It is
never collapsed at creation, and point comments are out of scope. Tombstones keep their position in
the tree, so both endpoints stay positioned forever.

**Resolution** is `(*Doc).ResolveRange(start, end)`. It takes `s = Resolve(start)` if one exists
(otherwise start's tombstone), likewise `e` for end, and returns every visible node in pre-order from
`pos(s)` to `pos(e)` inclusive. Inserts inside the range join it. Text typed after `end` is a child of
end and so lands after `pos(end)`. Text typed before `start` lands before it. So the range never grows
at its edges, which is the Docs behavior. If zero visible characters remain, the state is `DETACHED`.
The anchor is kept, so a restore or a merge that brings the text back re-attaches the comment. Clients
and comment-service run the same library function.

**Threads, mentions, resolve.** These are ordinary rows. A mention is `(?:^|\s)@([a-z0-9][a-z0-9._-]{0,63})`
parsed at write time and stored in `mentions`. Mentioned users and thread participants get a
`notifications` row in the same transaction as the comment. That table is the outbox. Phase 4/5
delivery (email, push) reads it by cursor. `ListNotifications` exposes it now. A Redis Stream relay
was rejected because it has no consumer this phase. `SetResolved` is idempotent and last-writer-wins,
and it records a notification for the thread author. Every write takes a `request_id`.

**Across branch and merge.** A thread belongs to its home document and stores `created_seq` (the home
`op_seq` at creation, at or above every anchor's seq). The thread is **visible in X iff X's head
contains `home@created_seq`**. The fork base and merge sources in X's DAG give that containment
exactly, through `VersionService.Frontier(X) → {doc: max seq}`. Frontier is a recursive query over
cross-document parent edges. So a comment made on B shows in D exactly when the merge brings B's state
in, never before. A branch inherits the comments that existed at its fork point. Replies and resolve
state are per thread, so they are shared wherever the thread is visible. A B comment resolved in D
re-verifies, because its anchor IDs are in D after the merge.

### 7. Type and signature sketches

```go
// pkg/crdt additions. Insert gains Copy; existing ops stay byte-identical (proto3 omits false).
type Insert struct { ID, Parent ID; Value rune; Copy bool } // Copy: re-creates Parent's character

func CanonicalBytes(d *Doc) ([]byte, error)          { panic("not implemented") } // ErrPending if any
func FromCanonical(b []byte) (*Doc, error)           { panic("not implemented") } // re-encodes, must match
func (v VersionVector) CoversAll(o VersionVector) bool { panic("not implemented") }
// Resolve returns id if visible, else the first visible node of its copy lineage in document order.
func (d *Doc) Resolve(id ID) (ID, bool)              { panic("not implemented") }
type Range struct { Detached bool; First, Last ID; Text string }
func (d *Doc) ResolveRange(start, end ID) (Range, error) { panic("not implemented") }
// PlanRestore returns ops authored by r that make head's visible text equal target's. head ⊇ target.
func PlanRestore(head, target *Doc, r ClientID) ([]Op, error) { panic("not implemented") }
type SpanKind uint8 // Retained, Inserted, Deleted
type Span struct { Kind SpanKind; Text string; First, Last ID }
func Diff(a, b *Doc) ([]Span, error)                 { panic("not implemented") }
```

```go
// services/version-service/internal/dag: pure node math, no I/O.
type NodeID [32]byte
type Hash [32]byte
type Node struct { Doc uuid.UUID; Seq uint64; Content Hash; Parents []NodeID } // len(Parents) <= 2
func (n Node) ID() NodeID                            { panic("not implemented") } // "CSV1" encoding
func ContentHash(canonical []byte) Hash              { return sha256.Sum256(canonical) }

// services/version-service/internal/store: Postgres; every method idempotent.
type Store struct{ pool *pgxpool.Pool }
// WithDoc locks version_docs rows in UUID order, finishes pending mutations, then runs fn.
func (s *Store) WithDoc(ctx context.Context, docs []uuid.UUID, fn func(tx pgx.Tx) error) error { panic("not implemented") }
func (s *Store) Tip(ctx context.Context, tx pgx.Tx, doc uuid.UUID) (Node, *crdt.Doc, error)   { panic("not implemented") }
func (s *Store) PutNode(ctx context.Context, tx pgx.Tx, n Node, canonical []byte, meta Meta) error { panic("not implemented") }

// services/version-service/internal/history: orchestration; the only caller of docsvc writes.
type Docs interface { // narrow port onto document-service
    Get(ctx context.Context, doc uuid.UUID) (Document, error)
    LoadState(ctx context.Context, doc uuid.UUID) (*crdt.Doc, uint64, error)
    Read(ctx context.Context, doc uuid.UUID, after uint64, limit int) ([]crdt.Op, uint64, error)
    Append(ctx context.Context, doc uuid.UUID, ops []crdt.Op) (maxSeq uint64, err error)
    Fork(ctx context.Context, src uuid.UUID, seq uint64, title, requestID string) (Document, error)
    SystemReplica(ctx context.Context, doc uuid.UUID) (crdt.ClientID, error)
}
type History struct { store *Store; docs Docs }
func (h *History) Materialize(ctx context.Context, doc uuid.UUID) (Node, error)           { panic("not implemented") }
func (h *History) Checkpoint(ctx context.Context, doc uuid.UUID, name, req string) (Node, error) { panic("not implemented") }
func (h *History) Branch(ctx context.Context, v NodeID, title, req string) (Document, error)    { panic("not implemented") }
func (h *History) Merge(ctx context.Context, src, dst uuid.UUID, req string) (MergeResult, error) { panic("not implemented") }
func (h *History) Restore(ctx context.Context, doc uuid.UUID, v NodeID, req string) (Node, error) { panic("not implemented") }
func (h *History) Diff(ctx context.Context, a, b NodeID) ([]crdt.Span, error)               { panic("not implemented") }
func (h *History) Frontier(ctx context.Context, doc uuid.UUID) (map[uuid.UUID]uint64, error) { panic("not implemented") }
type MergeResult struct { Node Node; Appended int; Noop bool }
```

```go
// services/comment-service/internal/comments
type Anchor struct { Start, End crdt.ID }            // inclusive, both visible at creation
type Thread struct { ID uuid.UUID; Home uuid.UUID; CreatedSeq uint64; Anchor Anchor
    Resolved bool; Comments []Comment }
type Comment struct { ID uuid.UUID; Author, Body string; Mentions []string; At time.Time }
func ParseMentions(body string) []string             { panic("not implemented") } // pure, deduped, ordered
type Service struct { db *pgxpool.Pool; docs Docs; versions Versions }
func (s *Service) Create(ctx context.Context, doc uuid.UUID, a Anchor, author, body, req string) (Thread, error) { panic("not implemented") }
func (s *Service) Reply(ctx context.Context, thread uuid.UUID, author, body, req string) (Comment, error)       { panic("not implemented") }
func (s *Service) SetResolved(ctx context.Context, thread uuid.UUID, resolved bool, actor string) (Thread, error) { panic("not implemented") }
func (s *Service) List(ctx context.Context, doc uuid.UUID, resolve bool) ([]ResolvedThread, error)             { panic("not implemented") }

// document-service store additions
func (s *Store) AllocateReplica(ctx context.Context, doc uuid.UUID, kind ReplicaKind) (uint64, error) { panic("not implemented") }
func (s *Store) ReplicaHome(ctx context.Context, id uint64, doc uuid.UUID) (bool, error)              { panic("not implemented") }
func (s *Store) Fork(ctx context.Context, src uuid.UUID, seq uint64, title, req string) (domain.Document, error) { panic("not implemented") }
func (s *Store) ListChanged(ctx context.Context, since time.Time, limit int) ([]domain.Document, error) { panic("not implemented") }

// collab-hub Store seam gains two methods; MemoryStore implements both.
//   Replica(ctx, docID string, requested crdt.ClientID) (crdt.ClientID, error) // 0 = allocate; ErrUnknownReplica
// collabclient: Options.Client crdt.ClientID (0 = allocate); (*Client).ID(); (*Client).ResolveAnchor(start, end)
//   (*Client).Authored() []crdt.Op  // every op this client minted, for test oracles
```

### 8. Proto and migration changes

```proto
// crdt/v1: Insert { bool copy = 3; }  Node { bool copy = 5; }
// collab/v1: DocumentSync { uint64 client = 4; }  ErrorMessage.Code CODE_UNKNOWN_REPLICA = 9; // terminal
// document/v1:
rpc AllocateReplica(AllocateReplicaRequest{string document_id; ReplicaKind kind;}) returns (Replica);
rpc GetReplica(GetReplicaRequest{uint64 id; string document_id;}) returns (Replica); // NotFound if not homed
rpc ForkDocument(ForkDocumentRequest{string source_id; uint64 seq; string title; string request_id;}) returns (Document);
rpc ListChangedDocuments(ListChangedRequest{Timestamp since; uint32 limit;}) returns (ListChangedResponse);
// Document gains: string fork_of = 9; uint64 fork_seq = 10; string fork_root = 11;
// version/v1 (new): ListVersions, GetVersion, GetBlob(hash) -> bytes, Checkpoint, Branch, Merge,
//   Restore, Diff, Frontier. Version{bytes id; string doc_id; uint64 seq; bytes content_hash;
//   repeated bytes parents; repeated string labels; bytes restored_from; Timestamp created_at}
// comment/v1 (new): CreateThread, Reply, SetResolved, ListThreads, ListNotifications.
```

document-service migration `0002_replicas_forks`: adds `replicas(id numeric, document_id, kind,
created_at, PK(id, document_id))`, the partial unique index, a `replica_ids` sequence, and the
backfill. It also adds `documents.fork_of/fork_seq/fork_root/fork_request_id UNIQUE` and an index on
`documents(updated_at)`. version-service `0001`: `version_docs(doc PK, tip)`,
`nodes(id PK, doc, seq, content_hash, meta jsonb, UNIQUE(doc, seq))`,
`node_parents(child, ord, parent, cross_doc)`, `blobs`, `checkpoints`, `mutations`. comment-service
`0001`: `threads`, `comments`, `mentions`, `notifications`, all keyed with `request_id` uniques for
idempotent writes.

### 9. GC contract for Phase 5

1. The `operations` table is never truncated. Fork, time-travel, materialization and oracles read it.
   GC applies only to in-memory and snapshot state.
2. The scope is the fork tree. A branch's replicas will deliver ops into every document of its
   `fork_root` through merge, so the stable vector for any document must be at most the pointwise
   minimum over FIFO reports from **every replica homed anywhere in the tree**. A per-document stable
   vector would let D collect a leaf that a B insert later re-parents to, and the merge's `Apply` would
   fail with "parent was collected".
3. Never collect a comment anchor endpoint. comment-service exports the set of anchor IDs of
   non-deleted threads as a pin set. Tombstones are what keep detached ranges positioned.
4. Restorable versions cap GC. Restore copies need the original present. Phase 5 must either cap
   stable at the vector of the oldest version still offered for restore or branch, or mark older
   versions view-only, which keeps diff and content from the blob. That is a product call.
5. Blobs referenced by any node are never deleted, because node IDs cover content hashes. Nodes are
   immutable. Copy originals are never leaves while a copy exists, so lineage survives GC by
   construction.

### 10. Test harness and how each done run is driven

`stack` gains `StartVersionService()` and `StartCommentService()`. Each service gets its own fresh
database, because of the `schema_migrations` name collision. It also gains
`stack.Dial(ctx, doc, hub)` without an ID (callers migrate) and gRPC clients `s.Versions`/`s.Comments`.
`collabclient.Authored()` feeds the oracles. `tests/internal/oracle` holds an **independent** canonical
encoder written from ADR 0010's byte spec, not from `pkg/crdt`, plus brute-force diff and range
oracles.

1. `TestBranchMergeLive` (2 hubs, Redis). Two clients edit D for 250 ops, then Checkpoint V and Branch
   B. Device X dials D and B and gets distinct IDs; dialing B with X's D ID gets `CODE_UNKNOWN_REPLICA`.
   D and B are edited concurrently, B quiesces, and Merge runs while D's editors type. After settling,
   the oracle `D@V ∪ Authored(D clients) ∪ Authored(B clients)` matches D's `LoadState` and every
   replica. A second Merge is `Noop` with `op_seq` unchanged. A node has parents `[D tip, node(B@hb)]`.
   Every node's `sha256(GetBlob)` and `Node.ID()` are recomputed from stored fields.
2. `TestRestoreLive`. Clients stay connected through Checkpoint V, edits that delete V text and add
   text, a quiesce, and Restore. All clients converge to `V.text`, every earlier node is still listed,
   the restore node's `restored_from` is V, and `Diff(V, restoreNode)` is empty.
3. `tests/crdt` checks `Diff` on 500 seeded histories of forks, edits, merges and restores against a
   brute-force oracle. Retained plus deleted spans give `a`'s text, retained plus inserted give `b`'s.
   The e2e run repeats this across a real branch and merge.
4. `TestAutoVersions` makes 1,050 ops and expects nodes at 100 to 1000. For each, the independent
   encoder over a replay of `ReadOperations(1..seq)` gives the stored `content_hash`.
5. `TestCommentsSurviveEdits` (3 clients) anchors `[i, j]` and runs concurrent inserts inside, partial
   deletes and a full delete (`DETACHED`). Server and client-local resolution both equal the oracle.
   It also covers `@bob` replies, `ListNotifications(bob)`, resolve and unresolve, and a B thread that
   is invisible in D before the merge, then visible and correctly resolved after it.
6. `make test-integration` runs the existing suites after the ID migration.

### 11. Units, in order (each green alone, each pushable to main)

1. **crdt additions** (`Copy`, canonical bytes, `Resolve`, `ResolveRange`, `PlanRestore`, `Diff`), with
   property tests and predicate 3 at library level. ADR 0010 fixes the canonical format.
2. **Homed replicas.** Migration, RPCs, hub allocation and checks, `CODE_UNKNOWN_REPLICA`, no `copy`
   from sockets, optional IDs in collabclient, and every test caller migrated. ADR 0011.
3. **Fork and the changed-docs feed** in document-service. A fork's `LoadState` equals the source's.
4. **version-service core.** Materialize, sweeper, list/get/blob/checkpoint, stack wiring. Predicate 4.
5. **Branch and the Diff RPC.** Predicate 3 end to end.
6. **Mutations and Merge.** Predicate 1, plus killing version-service between Append and node.
7. **Restore.** Predicate 2.
8. **comment-service and Frontier.** Predicate 5. ADR 0012 covers anchors and visibility.

## Tradeoffs accepted

- A replica row per (document, device install), never pruned in Phase 3, in exchange for
  collision-freedom by construction and a cross-document poisoning defense before auth.
- A branch copies its op rows (O(seq) once), in exchange for keeping the log invariant everywhere and
  zero read-path changes.
- Flat blobs (about 300 KB per version of a 20k-rune document; dedupe only of identical states), in
  exchange for a simple one-way-door hash. Chunking can come later without changing IDs.
- Merged-in ops reach live hubs in 30ms to 1s through existing healing, with no push. A merge is a
  user action, not a keystroke.
- If two branches restore the same text and are then merged, the text appears twice. That is honest
  union semantics, and the next restore removes the duplicate.
- No WebSocket push of comments; polling finds new threads. A push can follow Phase 4 permissions.
- Threads are shared across a merge, not copied. Resolving one in D resolves it in B too.

## Alternatives considered

- **Client-chosen random 64-bit IDs per (document, device).** Honest collisions are negligible and the
  protocol is unchanged. But a hostile client can choose D's live IDs on B and poison every future
  merge. Devices that reuse IDs still need a client change, so the work is spent anyway.
- **Rewrite B's IDs at merge.** This hides identity behind a persistent mapping that merge idempotency,
  repeated and reverse merges, diff and comment anchors would all need to consult. It is the shallowest
  interface with the most leakage.
- **Document-qualified IDs `(doc, client, clock)`.** This changes the ID type, wire format, storage and
  every comparison, and adds 16 bytes per reference, to get what a homed replica already gives.
- **No `(doc, seq)` in the node hash.** Nodes would dedupe across a fork prefix, but one node would
  have several locations, and the frontier and parent rules would become ambiguous.
- **A restore lineage side table instead of `Insert.copy`.** Every resolve would need an extra RPC,
  and merge would have to carry the table. A high-bit "system replica" convention also lost, because
  legacy random IDs already occupy that space.
- **Copying comment threads on merge.** It puts a cross-service write in the merge path, and replies
  diverge between the copies.

## Open questions and risks

- Should editors be allowed to emit `copy` inserts later, so that undo-of-delete keeps comments
  attached? The design permits it, but the hub forbids it for now.
- Is "merge requires the same `fork_root`" the right product rule?
- version-service holds a row lock across an Append. Does a 100k-op merge need a progress API?
- Phase 5 must choose between capping GC at restorable versions and making old versions view-only
  (GC contract 4).

## Next implementation step

Write `crdt.CanonicalBytes`/`FromCanonical` and the independent test encoder from the ADR 0010 byte
spec, and prove they agree on randomized documents. Every node hash depends on that format.
