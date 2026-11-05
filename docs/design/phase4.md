# CollabSphere Phase 4 design: identity, permissions, export, end-to-end encryption

Three designs were produced (Grok 4.7, Grok 4.6, Composer 2.5; Opus, GPT, Fable and Gemini were at
their usage limits). This document is the Grok 4.7 design. The original spec file is gone, so the
scope comes from its summary row: export-service (PDF, Markdown, DOCX via an async queue and S3),
permission-service (ACLs, signed share links), optional E2E (X25519, AES-256-GCM); a 100-page PDF in
under 10s; for an encrypted document the server stores only ciphertext.

## Synthesis decision

- **Revocation by a 500ms poll, not Redis invalidation** (Composer and Grok 4.6 both used a Redis
  publish). ADR 0008 treats pub/sub loss as normal, so a lost invalidation would leave a revoked
  editor writing past the 2s bound. The poll fails closed after 2s without a successful dump.
- **Ed25519 user tokens, not HS256** (Grok 4.6). With a shared HMAC secret every service could mint
  users. Services hold only the public key.
- **Encrypted ciphertext bound to the lineage root**, so server-side restore can copy ciphertext and
  clients still open it. Merge, branch and diff are refused for encrypted documents instead of
  half-translated.
- **Export queue on Postgres `SKIP LOCKED`** (agrees with Grok 4.6; Composer used Redis Streams). The
  job row the caller polls is the queue, so there is one source of truth.
- **CSS2/CSV2 for encrypted documents only** (all three agreed). CSS1 and CSV1 hashes are untouched.

## Units and throughput

Units follow §Units below. Parallel waves, each worker owning disjoint paths:

1. Wave 1, in parallel: unit 1 + 2 (`pkg/auth`, `cmd/devtoken`, permission-service); export
   renderers as a pure package (`pkg/render`, plus test parsers); CSS2 + encrypted value mode in
   `pkg/crdt` + `pkg/docrypt` + the independent CSS2 oracle.
2. Wave 2: unit 3 (auth and roles across every service, stack migration). Serial, it touches all
   services.
3. Wave 3, in parallel: unit 4 (share links on the stack) and unit 5 (export-service wiring, MinIO,
   crash and timing runs).
4. Wave 4: unit 6 wiring (encrypted documents end to end, storage scan).

---


Phase 4 makes every client call name a signed user, puts document roles in one service, and adds an export job plus an optional encrypted document mode. Plain documents keep today's RGA, CSS1 bytes, and CSV1 node ids. An encrypted document is a different mode from the first insert, not a wrapper around plaintext.

The reader of this design is the engineer who implements it. What changes for a caller is that a missing token no longer opens a socket or an RPC, a viewer can watch and cannot write, and an encrypted document never leaves plaintext in Postgres, Redis, or MinIO.

## Problem

Four facts in the repo decide the shape.

- Nothing authenticates. The hub accepts `GET /ws/{doc}` with `client`, `seq`, and `conn`. Every gRPC server calls `grpc.NewServer()` with no interceptor. `owner_id` and comment `author` are client-supplied strings.
- `replicas` is keyed by id and `fork_root` and homes one document. The hub caches each successful document and replica pair in an unbounded map for the process lifetime. Homes do not change. The user who claimed the id is not stored.
- A node value is a rune. `crdt.CanonicalBytes` writes CSS1, and version node ids are SHA-256 over the prefix `CSV1`. ADR 0010 says a new byte rule needs a new magic and a new node-id domain. Snapshots in document-service are protojson in `jsonb`. Operation bodies are protobuf `bytea`.
- Redis pub/sub carries sequenced ops and presence. It is a hint, not a log. ADR 0008 treats loss as normal. Postgres per service is the durable store. Compose today runs Postgres 17 and Redis 7 only.

Carried in from Phase 3, and closed here. Replica ids bind to the authenticated user. Every WebSocket message is checked against the current role. The positive replica cache has a fixed cap and stays right when a member is revoked.

## Done predicate

Same seven runs as the brief, on real processes under `-race`. The export timing run and the storage scan are specified in the tests section so the pass line is a measurement, not a feeling.

## Identity

There is no user directory and no password. A user id is the same handle comments already store. It matches `[a-z0-9][a-z0-9._-]{0,63}`. The signer says who you are. `pkg/auth` is the only parser.

**Token.** Ed25519 over a length-prefixed payload of subject, issued-at, and expiry, both unix seconds. The wire form is `v1.` plus base64url of the payload, plus `.`, plus base64url of the signature. Services hold `AUTH_PUBLIC_KEY`, the 32-byte public key in standard base64. Only tests and `cmd/devtoken` hold `AUTH_PRIVATE_KEY`. A service cannot mint a user.

Lifetime for tokens minted by `cmd/devtoken` is 15 minutes. Tests pass an explicit expiry and use one hour so a suite does not die on the clock. Verification allows 30 seconds of skew. Expired, truncated, and bit-flipped tokens fail closed. There is no denylist and no refresh token. A stolen user token works until it expires. Document revoke does not wait for that, because roles are not inside the token.

**Where it rides.** `Authorization: Bearer v1...` on the WebSocket upgrade and on user gRPC. The query string stays `client`, `seq`, and `conn`. A token in the query would land in access logs. Browser WebSocket clients cannot set headers. This repo has no browser client. ADR 0003 already defers that frontend. A subprotocol token can wait until a browser exists.

**Handshake failures.** Missing, expired, and tampered tokens get HTTP 401 and the hub does not call `websocket.Accept`. The hub calls `PolicyDump` before accept. A valid token with no grant gets HTTP 403 and no snapshot. If that call fails, the hub returns HTTP 503 and does not accept. gRPC returns `Unauthenticated` for a bad token and `PermissionDenied` for a real user with the wrong role.

**Rejected.** A JWT library, a uuid user table, a handle table beside mentions, a session denylist, and a role claim inside the token. The claim would stay true until expiry, which misses the 2 second revoke bound. HMAC with a shared user secret would let every service mint users.

## Permission service

New process `permission-service`, default `:9094`, own database. It owns grants, share links, and the key directory. document-service stores `owner_id` as the creator's handle. The grant row is the authority.

**Roles.** One enum, high to low: `owner`, `editor`, `commenter`, `viewer`. A document has one owner, the creator. Ownership transfer is out of scope. The owner cannot be revoked.

| Action | Minimum role |
| --- | --- |
| Receive ops, presence, cursors, list versions, get a blob, list comments, export | viewer |
| Create and reply to threads | commenter |
| Resolve a thread, send ops, checkpoint, restore | editor, or the thread author for resolve |
| Archive, grant, revoke, share links, publish key wraps | owner |

A branch copies grants onto the new document id. The caller must be an editor on the source. Share links are not copied. Fork of an encrypted document is refused, so this copy path is plain documents only.

**Schema.**

```sql
CREATE TABLE grants (
    doc_id     uuid NOT NULL,
    user_id    text NOT NULL,
    role       text NOT NULL CHECK (role IN ('owner', 'editor', 'commenter', 'viewer')),
    source_link uuid,
    PRIMARY KEY (doc_id, user_id)
);

CREATE TABLE share_links (
    id         uuid PRIMARY KEY,
    doc_id     uuid NOT NULL,
    role       text NOT NULL CHECK (role IN ('editor', 'commenter', 'viewer')),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    created_by text NOT NULL
);
```

`user_id` uses the handle grammar, checked in `pkg/auth` before the insert. The table does not re-encode that regex in a trigger.

**Check path.** The hub does not put a role in the socket and forget it. Every 500ms, while a document has peers, the hub calls `PolicyDump(doc)` and replaces that document's member map, write epoch, and epoch state. Every inbound frame reads that map inside the hub loop, not only in the HTTP reader, so an op already queued still sees a revoke that landed after the read.

A viewer or commenter who sends `OperationBatch` gets `CODE_FORBIDDEN` and the socket stays up. Broadcasts keep flowing, which is what "a viewer receives ops" requires. The batch is not appended. `CursorMove`, `AwarenessUpdate`, and `Ping` stay allowed for every role that is still connected. `CODE_FORBIDDEN` is the first non-terminal error in this protocol. ADR 0005 said every code except `CODE_UNAVAILABLE` closes the socket. This amends that for `CODE_FORBIDDEN` and `CODE_KEY_ROTATION` only. collabclient keeps reading after those two codes. It stops on every other error, as it does today.

Revoke, or a downgrade to no row, closes the live socket with terminal `CODE_REVOKED` even if the client is idle. Downgrade from editor to viewer leaves the socket open and refuses later batches. The poll period is 500ms, so the effect lands well inside 2 seconds when `PolicyDump` is healthy. If `PolicyDump` has not succeeded for 2 seconds, the hub closes that document's peers with `CODE_UNAVAILABLE` and stops applying ops. It does not keep the last map past the bound.

**Rejected.** A role claim in the token. A synchronous permission RPC on every keystroke. Redis pub/sub as the revoke signal. Pub/sub loss is an accepted property of ADR 0008, and a lost hint would leave a revoked editor in place. The poll meets the bound without a second channel.

**Create.** `CreateDocument` returns `InvalidArgument` when `owner_id` is non-empty and differs from the token subject. An empty `owner_id` is replaced with the token subject. document-service inserts the row, then calls `EnsureOwner`. If that call fails, it deletes the row. `EnsureOwner` is idempotent on the same owner. A second owner returns `AlreadyExists`.

**Authors.** comment-service writes `author` and `actor` from the token. A non-empty request field that differs is `InvalidArgument`. Mentions stay handles parsed from the body. They already match the user id grammar, so no join table appears.

**Share links.** permission-service holds `SHARE_PRIVATE_KEY`. The payload is the ASCII prefix `CSL1`, the link uuid, the doc uuid, one role byte, and the expiry as a big-endian uint64. The token is `s1.` plus base64url of that payload, plus `.`, plus base64url of the Ed25519 signature. Redeem checks the signature, loads the row, and refuses when the signature is bad, the row is missing, the signed fields differ from the row, `revoked_at` is set, or `expires_at` is in the past. A bad signature, a missing row, and a field mismatch use reason `LINK_BAD`. A revoked link uses `LINK_REVOKED`. An expired link uses `LINK_EXPIRED`. All three reasons are `FailedPrecondition`.

Expiry is required, at least one minute and at most 30 days. The link role is viewer, commenter, or editor, and not above the creator's role. Redeem inserts a grant of that role. If the user already has a higher role, the row stays and the RPC returns it unchanged. A link never demotes. Revoking a link blocks later redeems. It does not delete grants already issued. Removing the member does that.

**Rejected.** An opaque random URL with no signature. A signed link with no row, which cannot be revoked. Downgrade-on-redeem, which turns a viewer link into a way to strip the owner.

## Replica binding

Migration `0005_replica_user` adds `replicas.user_id text`. `kind = 'system'` keeps a null user. `kind = 'client'` may be null only for rows that already exist. A check forbids `system` with a user.

`ClaimReplica` stays a service RPC. The hub fills `user_id` from the token it already verified. Clients never call it.

- Allocate, client id zero. The new row stores the joiner.
- Explicit id with the same user and the same home. Success. Reconnect works.
- Explicit id owned by someone else. `FailedPrecondition` reason `REPLICA_OWNED`. The hub sends terminal `CODE_REPLICA_FORBIDDEN` and does not join.
- Explicit id homed on another document in the tree. Still `CODE_UNKNOWN_REPLICA`.
- Legacy row with null `user_id` and the matching home. One `UPDATE ... WHERE user_id IS NULL` binds the caller. The loser of a race gets `REPLICA_OWNED`. After that the user is immutable.

The residual is real. There is no production data, and the first authenticated claimer adopts a Phase 3 row. Tests that dial with a fixed id adopt it for the user the stack minted.

System replicas stay on the version-service internal token. A user token cannot claim `kind = 'system'`.

**Cache.** The process-lifetime map in `Registry.replicas` becomes an LRU of 4096 entries. The key is the document id plus the replica id. The value is the user id. Only a non-null user is inserted. A hit whose user differs from the joiner refuses without an RPC. A hit whose user matches still has to pass the role map. Eviction causes a `ClaimReplica` on the next join. It cannot authorize a write, because ownership never moves and the role map is a different structure. Revoke deletes nothing from the LRU. The role poll closes the socket. A second user still sees the cached owner and is refused. That is the revocation case the cache has to get right.

Negative results are not cached. A failed check can become valid when that user is the first to bind a legacy row.

## Service to service auth

User tokens and internal calls use different `Authorization` schemes. Internal calls send `Bearer internal.` plus 64 hex characters. Every backend process gets the same `INTERNAL_TOKEN` from the environment. The interceptor compares with `subtle.ConstantTimeCompare`. A user token on a service method is `Unauthenticated`, and so is an internal token on a user method.

One shared secret is enough. The attacker this phase closes is an anonymous client, not a sister process that already has the network. A compromised service can call the others. That limitation is accepted.

The hub, version-service, comment-service, and export-service present the internal token when they call document-service, presence-service, or `PolicyDump`. Presence RPCs are internal only. Clients never see them.

`GetBlob` and `GetDocument` accept either credential. A user must still be a viewer. A service call is how export and the hub read state.

| Caller facing | Methods |
| --- | --- |
| User token | document Create, Get, Archive. version List, Get, GetBlob, Head, Checkpoint, Branch, Merge, Diff, Restore. comment Create, Reply, SetResolved, List, ListNotifications. export Request and Get. permission Grant, Revoke, CreateLink, RevokeLink, Redeem, PutKey, ListKeys, PublishWraps, GetWraps |
| Internal token | document LoadState, Append, Read, ClaimReplica, Fork, ListChanged, SetEpoch. version Frontier. presence, all of it. permission PolicyDump and EnsureOwner |
| Either | document Get. version GetBlob and Head |

`Head` is new. It materializes to `op_seq` and returns the tip version. Export uses it when the request does not name a version.

## End to end encryption

Encryption is a document mode, chosen in `CreateDocument` and stored on `documents.encryption` as `plain` or `encrypted`. It never changes. Plain documents do not gain a cipher field on the wire in practice. proto3 leaves the new bytes empty, and CSS1 output is untouched.

**Value.** An insert carries either a rune or a ciphertext, never both and never neither. `pkg/crdt` rejects the illegal combination in `OpFromProto` and in `Apply`.

```go
type Mode uint8

const (
    Plain Mode = iota
    Encrypted
)

type Ciphertext struct {
    Epoch uint32
    Bytes []byte // AES-256-GCM output, tag included, nonce omitted. Length 17..20.
}

type Insert struct {
    ID, Parent ID
    Value  rune
    Cipher *Ciphertext
    Copy   bool
}
```

`Doc` stores `Mode`. `Text`, `CanonicalBytes`, and the text half of `Diff` return `ErrEncrypted` on an encrypted doc. `ResolveRange` still returns positions and `Detached`. It sets `Text` empty and `TextOmitted` true. The server has no key, so those functions must not guess.

**What a client seals.** One rune, as its UTF-8, under the document key for `epoch`. AES-256-GCM. The nonce is the first 12 bytes of SHA-256 over `doc_id || epoch_be32 || root_client_be64 || root_clock_be64`. The associated data is the ASCII prefix `CSAD1` followed by the same fields. The lineage root of a normal insert is its own id. The lineage root of a copy is the original character, which is the parent chain Phase 3 already walks. Nonce and associated data are not stored. Honest clients derive them. Two different roots under one document key do not share a nonce unless SHA-256 prefixes collide, which is the same assumption CSV1 already makes for node ids.

The server rejects a cipher whose length is outside 17..20. A rune's UTF-8 is 1 to 4 bytes and the tag is 16, so the marker string used in the scan cannot fit in one value. The server does not check the GCM tag. It does not have the key. A member who already holds the key can upload garbage. They can also read the document. The threat model is the server and non-members.

**Keys.** A document key is 32 random bytes the server never stores. Each member has an X25519 key from `crypto/ecdh`. `PutKey` stores the 32-byte public key in permission-service. Private keys stay in the test or the client.

A client that holds the document key builds a wrap as follows.

1. Generate an ephemeral X25519 key.
2. Derive `shared` with X25519 of the ephemeral private key and the member public key.
3. Derive `wrapKey` with HKDF-SHA256. The salt is `doc_id`. The info string is `collab-wrap-v1`, the epoch, and the user id. The length is 32.
4. Encrypt the document key with AES-256-GCM. The nonce is 12 random bytes and is stored with the wrap. Associated data is the doc id, the epoch, the user id, and the key id.
5. Discard the ephemeral private key. Store the ephemeral public key, the nonce, and the 48-byte sealed key.

`GetWraps` returns those blobs to the member. `pkg/docrypt` does seal, open, wrap, and unwrap. Services do not import it.

**Epoch state.** This is one row per epoch, not a bool beside a counter.

```sql
CREATE TABLE doc_epochs (
    doc_id uuid NOT NULL,
    epoch  int  NOT NULL,
    state  text NOT NULL CHECK (state IN ('pending', 'open')),
    PRIMARY KEY (doc_id, epoch)
);

CREATE TABLE doc_key_wraps (
    doc_id uuid NOT NULL,
    epoch  int  NOT NULL,
    user_id text NOT NULL,
    key_id uuid NOT NULL,
    eph_pub bytea NOT NULL,
    nonce bytea NOT NULL,
    ciphertext bytea NOT NULL,
    PRIMARY KEY (doc_id, epoch, user_id)
);
```

document-service stores the same `write_epoch` and `epoch_state` so `Append` can refuse without calling permission-service. permission-service is the place that decides. After its transaction commits, it calls `SetEpoch`. `SetEpoch` is idempotent for the same pair and rejects an older epoch. The hub's `PolicyDump` carries the same pair. A skew of one retry is fine. `Append` is the backstop, and the client resends.

Create of an encrypted doc starts epoch 1 in `pending`. The creator calls `PublishWraps` with a wrap for every current member, which at that moment is themselves. The server accepts the set only when the user ids equal the grant set, and only from the owner, and only while the epoch is `pending`. It then marks the epoch `open` and calls `SetEpoch`. A second publish for an open epoch returns the stored result when the request id matches, and `AlreadyExists` when it does not. The server cannot tell two document keys apart, so the first complete set wins.

**Add a member.** `Grant` on an encrypted doc includes one wrap per epoch from 1 through the current epoch, each for the new user. The RPC and the wraps commit together. The new member can then open every epoch. Clients keep every document key they have unwrapped. The owner is the member who can build those wraps. Any other member's client could build them too, but only the owner may call `Grant` and `PublishWraps`. An editor who could publish a garbage wrap would lock the group out of future writes, and the server could not repair it.

**Remove a member.** Delete their grant and every wrap row for them. Insert epoch `N+1` as `pending`, move the current epoch to it, and call `SetEpoch` so `Append` rejects epoch `N` and rejects epoch `N+1` until it is open. Live sockets see the new state on the next poll. Editors who are still members get `CODE_KEY_ROTATION`, non-terminal, until the owner publishes wraps for `N+1`. New inserts then use `N+1`.

What the removed member can still read. Every op whose epoch they already unwrapped, for as long as they kept that document key. Deleting the wrap stops them fetching it again. It does not erase a key in their process, and the server cannot re-encrypt history. Ops at epoch `N+1` and later are unreadable to them. If the owner is offline, new writes stay frozen and old text stays readable to remaining members who kept the old keys.

**CSS2.** `CanonicalBytes` on an encrypted doc returns `ErrEncrypted`. `CanonicalCSS2` writes the layout below. Version node ids for that blob use the prefix `CSV2` and are otherwise the Phase 3 formula. Plain nodes stay `CSV1` and CSS1, byte for byte. `FromCanonical` dispatches on the magic. It re-encodes and requires equality, as CSS1 does.

```
magic "CSS2"
n_nodes, then per node in document order:
  id, parent, epoch,
  n_bytes, bytes,
  flags with bit 0 meaning copy,
  n_deleted, then delete ids ascending
n_vector, then n_collected, same rules as CSS1
```

Empty encrypted state is the eight bytes `43 53 53 32 00 00 00`. A copy whose bytes or epoch differ from its parent is rejected. The independent encoder in `tests/internal/oracle` is written from this layout, not by calling `pkg/crdt`.

**Hub checks, in order, before the Phase 1 five.** The document mode matches the op shape. The epoch equals the open write epoch. Cipher length is 17..20. A copy's bytes equal the parent. Then the existing id, clock, dependency, and `Apply` checks run. `Apply` compares cipher bytes for copies instead of runes.

**What the server still does.** Checkpoint, list, get, and get-blob. The blob is CSS2. Restore stays server-side. `PlanRestore` copies cipher bytes onto new ids with `Copy` set. Clients open a copy with the lineage root, so the tag still verifies. Merge, branch, and `ForkDocument` return `FailedPrecondition` reason `ENCRYPTED`. A copied op is bound to the source doc id, and the server cannot re-seal it. `Diff` returns the same reason. A span of empty text would look like a successful diff that ate the characters. Clients diff after they open the blobs. `ListThreads` still returns anchors, attached or detached, and empty text with `text_omitted`. Create still checks that the ids are visible. It does not read values. Export returns `ENCRYPTED` and writes no job row.

Comment bodies stay plaintext handles and sentences. They are not the document. The storage scan types the marker only through inserts, and not into a title or a comment. Resolution text is the path that would have copied document plaintext into a response and then into a log. That path is closed.

**Rejected.** Per-run ciphertext, which gives up character ids. A single blob for the whole document, which gives up the CRDT. Reusing CSS1 with the rune field stuffed with cipher bytes, which changes every plain hash or lies about the type. Binding associated data to the copy's own op id, which makes server restore undecryptable. XChaCha, because the brief names AES-256-GCM. A tree-wide key so merge could work, which is a second design hiding inside this one.

## Export

New process `export-service`, default `:9095`, own database. The same binary serves gRPC and the claim loop. Compose adds MinIO on port 9000 with root user `collab` and the bucket `exports`. The service creates the bucket on startup if it is missing. The client is `aws-sdk-go-v2/service/s3` with path-style addressing and `S3_ENDPOINT`.

**Queue.** A job row is the queue. Redis streams would be a second source of truth next to the row the caller already polls. `SKIP LOCKED` on Postgres is the claim.

```sql
CREATE TABLE export_jobs (
    id uuid PRIMARY KEY,
    request_id text NOT NULL UNIQUE,
    doc_id uuid NOT NULL,
    version_id bytea NOT NULL,
    format text NOT NULL CHECK (format IN ('pdf', 'md', 'docx')),
    user_id text NOT NULL,
    state text NOT NULL CHECK (state IN ('queued', 'running', 'ready', 'failed')),
    lease_until timestamptz,
    attempts int NOT NULL DEFAULT 0,
    object_key text NOT NULL,
    error text
);
```

`RequestExport` checks the viewer role, rejects an encrypted doc, resolves head through `Head` or checks the named version, and inserts `queued` with `object_key = exports/{job_id}`. The same `request_id` returns the same row. The version id is pinned before the worker runs, so a retry renders the same blob after head has moved.

The claim is one statement. It sets `running`, sets `lease_until` to 30 seconds ahead, and increments `attempts`, for the oldest row whose state is `queued` or `running` and whose lease is null or in the past. Five attempts move the row to `failed`. A crash leaves `running` with an old lease. The next process claims that same row and the same object key.

Render writes a temp file, then one `PutObject`. No multipart upload, so a dead worker does not leave parts. `PutObject` to the same key overwrites. The ready update is `WHERE state = 'running'`. A second ready update changes nothing. The bucket ends with one object.

`GetExport` returns the state. On `ready` it presigns `GetObject` for 60 seconds. The URL is not stored. A member revoked after the URL was issued can fetch until it expires. That window is outside the 2 second socket bound, and it is the cost of not proxying the bytes.

**Render.** All three formats start from `crdt.VisibleText`, a single pass over CSS1. Nodes are already in document order. A node with `n_deleted = 0` contributes its rune. The function does not build a `Doc`. CSS2 returns an error, which export never feeds it because the RPC already refused.

- Markdown bytes are the rune text plus one trailing newline.
- DOCX is a zip of `[Content_Types].xml`, `_rels/.rels`, and `word/document.xml`. Paragraphs follow the same line breaks as the PDF wrap. `encoding/xml` escapes text. No extra dependency.
- PDF is hand-written, Helvetica, WinAnsi, 10pt, line height 14pt, letter page, 72pt margins. The content box is 468 by 648 points, which is 46 lines. Widths are the Adobe Helvetica AFM numbers for bytes 32 through 126, in 1000-unit ems. Greedy wrap. A rune outside that range fails the job with no object. The acceptance document is ASCII. Catalog, page tree, one content stream per page, and a classic xref are written in one shot.

**The 10 second budget.** This design does not claim a measured time. The run that may accept the design is five end-to-end exports under `-race`, after a checkpoint so the version blob already exists. The timer starts when `RequestExport` returns and stops when the presigned GET yields a PDF whose page count is 100 and whose extracted text contains the document text. Authoring is outside the timer. First-time materialize of a cold log is outside the timer. The pass line is a median under 10 seconds, every run under 12 seconds, and a spread under 2 seconds. A wider spread means the number is noise and the budget is not met.

The result that would change the design is a median over 10 seconds. The first place to look is `VisibleText` allocations and the content-stream buffer, then the MinIO put. That limiter is a guess from reading the code. It is not a profile. If the median exceeds 8 seconds, the test keeps a CPU profile of the worker with the number. Queue wait is not the expected limiter, because the worker is already up and the claim is one update.

The 100-page fixture is 4600 ASCII lines that each fit on one visual line at those widths. 46 lines times 100 pages. Extracted text and page count are the assertion. A parser counts page objects and reads `Tj` strings. DOCX is checked by reading `word/document.xml`. Markdown is compared as bytes.

**Crash test.** `EXPORT_CRASH_AFTER=upload` makes the process call `os.Exit(1)` after `PutObject` and before the ready update. The test starts a second process without that variable. It must reach `ready` and the bucket prefix for that job must list one object. `EXPORT_CRASH_AFTER=claimed` exits after the claim commit and before render, and the second process must still produce one object. Compose leaves the variable unset. A timing-based `SIGKILL` is not the pass line, because a fast render makes it flaky.

## Proto, ports, and env

New WebSocket codes. `CODE_FORBIDDEN = 10`, non-terminal. `CODE_REPLICA_FORBIDDEN = 11`, terminal. `CODE_REVOKED = 12`, terminal. `CODE_KEY_ROTATION = 13`, non-terminal.

`Insert` gains `bytes cipher = 4` and `uint32 epoch = 5`. `Node` gains the same at fields 6 and 7. `Document` gains `encryption`, `write_epoch`, and `epoch_state`. `Resolution` gains `bool text_omitted = 5`. `ClaimReplicaRequest` gains `user_id`. `CreateDocumentRequest` gains `bool encrypted` and `request_id`.

New protos, `permission/v1` and `export/v1`, cover the RPCs listed above. `version/v1` gains `Head`. `DiffResponse` does not grow a half-empty mode. Encrypted docs fail the RPC.

| Process | Listen | New env |
| --- | --- | --- |
| document-service | `:9090` | `AUTH_PUBLIC_KEY`, `INTERNAL_TOKEN`, `PERMISSION_ADDR` |
| presence-service | `:9091` | `INTERNAL_TOKEN` |
| comment-service | `:9092` | `AUTH_PUBLIC_KEY`, `INTERNAL_TOKEN`, `PERMISSION_ADDR` |
| version-service | compose sets `:9093` | `AUTH_PUBLIC_KEY`, `INTERNAL_TOKEN`, `PERMISSION_ADDR` |
| permission-service | `:9094` | `DATABASE_URL`, `AUTH_PUBLIC_KEY`, `SHARE_PRIVATE_KEY`, `INTERNAL_TOKEN`, `DOCUMENT_ADDR` |
| export-service | `:9095` | `DATABASE_URL`, `AUTH_PUBLIC_KEY`, `INTERNAL_TOKEN`, `DOCUMENT_ADDR`, `VERSION_ADDR`, `PERMISSION_ADDR`, `S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION` |
| collab-hub | `:8080` | `AUTH_PUBLIC_KEY`, `INTERNAL_TOKEN`, `PERMISSION_ADDR` |
| MinIO | `:9000` | root `collab` / `collabsecret`, bucket `exports` |

version-service and comment-service both default `GRPC_ADDR` to `:9092` in `main` today. Compose sets version to `:9093`. Tests already pass an address, so the binary default can stay until a later cleanup. Leaving both on 9092 makes a full stack fail at listen, which is why compose overrides it.

Each new service gets its own database, same as the existing split forced by `schema_migrations`.

## Tests

`stack` starts permission-service and export-service, mints one user token per subject through `pkg/auth`, and sets `INTERNAL_TOKEN` on every process. `collabclient.Options` gains `Token`. `Dial` sends the bearer header. Existing suites use a user who is the owner of each document they create. That is the Phase 1 to 3 migration.

New runs.

1. Authn. Drop the header, expire the token, and flip one signature byte. Hub answers 401. Each user-facing RPC answers `Unauthenticated`. An internal method with a user token does too.
2. Authz. A viewer receives another user's broadcast. Its batch gets `CODE_FORBIDDEN` and the socket is still open. The op is absent from the other replica. A commenter creates a thread and a batch is refused. An editor's batch is stored.
3. Revoke within 2 seconds, including an idle socket. The assertion fails if `CODE_REVOKED` or the refused batch is later than 2 seconds after `Revoke` returns. Downgrade to viewer refuses the next batch and keeps delivering.
4. Replica. User B dials A's id and gets `CODE_REPLICA_FORBIDDEN`. Fill the LRU past 4096 and confirm A's id still refuses B after eviction. Revoke A while the LRU holds A, and confirm the socket dies and B still cannot claim the id.
5. Share links. Tamper, expiry, and revoke each fail redeem. A clean redeem of a commenter link lets that user comment and not edit. An owner who redeems a viewer link is still owner.
6. Export. Markdown bytes, DOCX `document.xml` text, and PDF page count plus extracted text match the document. The crash variable yields one object. A non-member is `PermissionDenied`.
7. The 100-page timing run, reported with five samples, min, median, max, and the spread.
8. Encryption. Two clients seal and open the same text, including a restore. A third user without a grant is refused. After `Grant` with wraps, the new member reads the full text. After `Revoke`, a later insert is unreadable to the removed client and readable to the owner once they publish the next epoch. The removed client still opens an op from the previous epoch with the key it kept. Branch, merge, diff, and export of that doc return `ENCRYPTED`. ListThreads text is empty and `text_omitted` is set.
9. Storage scan, encrypted doc only. For every database the stack created, search each `text`, `jsonb`, and `bytea` value for the marker. Decode operation protobuf and snapshot protojson, including base64 fields, and search those bytes. `SCAN` every Redis value. Download every MinIO object. The marker is 48 ASCII bytes typed as inserts. The run fails if any value contains it. It also fails if an encrypted op used the rune field.

Phase 1 to 3 suites run in the same `make test-integration` after the token migration.

## Units

Each unit is green alone and pushable to main. Later units assume the earlier ones.

1. `pkg/auth` and `cmd/devtoken`. Mint, expiry, tamper, handle grammar. No service wired.
2. permission-service grants, links, and `PolicyDump`, with database tests. No hub calls it yet. Epoch tables land here too so encryption does not reopen the schema, but no RPC publishes wraps until unit 6.
3. Tokens and roles on document-service, the hub, version-service, comment-service, and presence-service. `EnsureOwner`, the 500ms poll, `CODE_FORBIDDEN`, `CODE_REVOKED`, replica `user_id`, the 4096 LRU. `stack` and every existing caller migrate in this unit. There is no build where a valid token is treated as owner of every document.
4. Share-link redeem on the real stack.
5. export-service, MinIO in compose, the three renderers, the crash exit, and the timing run.
6. CSS2, `pkg/docrypt`, wraps, epoch freeze, client seal and open, the refused RPCs, and the storage scan.

## Tradeoffs

- One internal token, in exchange for not building per-caller keys. A bad service is as powerful as the others.
- A 500ms poll, in exchange for a revoke bound that survives a lost Redis message. Active documents cost one `PolicyDump` per hub per half second.
- Legacy replicas bind to whoever authenticates first, in exchange for not breaking explicit ids in tests.
- Server restore of ciphertext, in exchange for associated data that names the lineage root rather than the copy id. Merge and branch are refused instead of given a half translation layer.
- A removed member keeps old epoch keys they already unwrapped, in exchange for the server never seeing plaintext and never rewriting history.
- PDF is WinAnsi ASCII, in exchange for a renderer with no font file and a 100-page path that is a linear scan plus a put.
- Presigned URLs live 60 seconds after revoke, in exchange for not streaming export bytes through the service.
- Comment bodies stay plaintext, in exchange for not inventing a second encrypted channel. The scan's marker never goes in a body, a title, or a display name.

## Rejected designs

- Bolt cipher bytes into the CSS1 rune field. That either changes plain node ids or stores ciphertext in a type the rest of the code treats as text.
- Check the role only at join. The brief requires every message, and a revoke would wait for reconnect.
- Cache replica checks for the process lifetime with no cap. That is the map Phase 3 shipped, and it grows without a bound.
- Push ACL invalidation on Redis and skip the poll. A lost message breaks the only bound that is in seconds.
- Encrypt the document as one blob. Collaboration is the product.
- Queue exports on Redis streams. The poll API already needs the job row, and a second queue can deliver twice or lose the pointer.

## Principles that changed a choice

- Laziness Protocol. One internal token, no user table, no Redis revoke bus, no PDF library, and the key directory sits in permission-service.
- Redesign from First Principles. Encrypted documents use a value mode, CSS2, and CSV2, so CSS1 hashes stay valid. Merge is refused rather than wrapped in a converter.
- Foundational Thinking. Handle, role, job state, document mode, and epoch rows are fixed before the handlers. `pkg/auth` lands before any service is wired.
- Model the Domain. A job is `queued`, `running`, `ready`, or `failed`. An epoch is `pending` or `open`. Those are columns with checks, not pairs of bools.
- Boundary Discipline. Tokens are parsed only in the interceptor and the hub handshake. `Append` rejects a bad op shape. Only permission-service signs links. Only `pkg/doccrypt` seals.
- Type System Discipline. An insert is a rune or a cipher. The two bearer prefixes cannot satisfy each other's methods. A share link cannot name the owner role.
- Make Operations Idempotent. The object key is chosen at insert, `PutObject` overwrites it, `request_id` is unique, and a crashed `running` row is claimed again by its lease.
- Sequence work into verifiable units. Auth, then roles and replicas, then links, then export, then encryption. Existing suites move with the token unit, not after a later unit.
- Outcome-Oriented Execution. Comment `author` comes from the token in the same unit that turns auth on. There is no release where any authenticated user owns every document.
- Explain the Number. The 10 second run names the timer, the sample count, the spread, and the result that would force a different renderer. No duration is claimed here.
- Experience First. A viewer keeps the socket and keeps receiving. `Diff` fails closed instead of returning empty text. Only the owner can publish wraps.
- Separate Before Serializing Shared State. The replica LRU stores ownership. The role map stores who may write. Revoke updates only the role map, so a cache hit cannot keep a revoked user writing.

## Next implementation step

Write `pkg/auth` and its tests. Mint a token, flip one byte, and show `Verify` rejecting it. Every later unit calls that function.
