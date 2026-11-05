# CollabSphere Phase 5 design: tombstone GC, observability, the 10k load proof, deploy

## Synthesis decision

Two designs were written independently against the same brief (Grok 4.7 and Composer 2.5). The Grok
design is the base and appears in full below. Reasons, each a point where the other design was weaker:

- The P99 proof runs on this laptop. The other design deferred the full 10,000-connection run to a
  Linux CI machine this repo does not have. The claim is scoped to what one laptop can support: echo
  and 20-way same-document fan-out while 10,000 sockets are held, not fan-out to 10,000 peers.
- Append stays on the hub loop as the ack. The other design moved persist to a background worker,
  which changes what an ack means for a load that is 50 appends per second. The real loop stalls are
  log reads and the policy wait, and those move off the loop.
- Offline replicas retire only when every unarchived document in the tree covers their clock. The
  other design let an owner abandon a replica by treating it as caught up, which can collect the
  parent of an op still sitting in an unmerged branch.

Grafted from the other design: a `collab_doc_gc_stable_age_seconds` gauge (age of the oldest report
holding the stable minimum), so a tree pinned by a missing device is visible on the dashboard.

Restore horizon is 30 days and forced resync is 14 days, as below. Nothing is deployed to a real
cluster in this phase. Helm and Argo CD are validated with `helm lint`, `helm template` and
kubeconform only.

## Units and waves

- Wave 1, in parallel on disjoint paths: unit 1 (`GCWithPins`), unit 4 (hub log reads and policy
  wait off the loop), unit 10 (Helm chart and Argo CD Application under `deploy/`, written against
  the ports and env names fixed in this document).
- Wave 2: unit 5 (metrics on every process, Prometheus, Grafana, dashboard test).
- Wave 3: unit 2 (reports and `documents.vector`), then unit 3 (hub report path), with unit 6
  (`AnchorPins`, `RestoreFloor`, horizon refusals) in parallel.
- Wave 4: unit 7 (GC worker and live snapshot), then unit 8 (lapse, resync, retirement).
- Wave 5: unit 9 (`TestTenThousand`, `make load-proof`). Then the cross-model review.

# Phase 5 design

HEAD a6c92c4. GC follows ADR 0006 and phase 3 §9. The operation log stays complete. Collected trees live in snapshots and in hub memory.

## Data shapes

### Replica report

A report is one row per replica, not an operation. `AppendOperations` never writes this table. The hub stamps the vector on its loop after that replica's preceding ops have been persisted, then a side goroutine inserts the row. A crash between the two leaves the previous row in place.

```sql
CREATE TABLE replica_reports (
    fork_root    uuid           NOT NULL,
    replica_id   numeric(20, 0) NOT NULL,
    vector       jsonb          NOT NULL,
    home_seq     bigint         NOT NULL CHECK (home_seq >= 0),
    reported_at  timestamptz    NOT NULL,
    released_at  timestamptz,
    PRIMARY KEY (fork_root, replica_id)
);
```

`vector` is a JSON object of decimal client id to clock. Zero clocks are omitted. `home_seq` is the home document's `op_seq` at the stamp. The replica's home stays on `replicas`. Rows are never deleted. `released_at` retires a device without freeing its id.

`documents.vector jsonb` is the committed version vector, updated in the same transaction as `AppendOperations` under the document row lock. A null vector is computed once from the compact snapshot plus later ops, then stored. GC skips a document whose vector is still null.

### Live snapshot

Compact snapshots stay immutable. ADR 0004's `ON CONFLICT DO NOTHING` path is unchanged. GC writes a different kind of row.

```sql
ALTER TABLE snapshots ADD COLUMN kind text NOT NULL DEFAULT 'compact';
ALTER TABLE snapshots ADD CONSTRAINT snapshots_kind_check
    CHECK (kind IN ('compact', 'live'));
-- primary key becomes (document_id, seq, kind)
CREATE UNIQUE INDEX snapshots_one_live ON snapshots (document_id) WHERE kind = 'live';
```

The live row is upserted. It holds a protojson `Snapshot` whose `collected` vector is the stable vector that `GCWithPins` just applied, and whose nodes are the tree after that call. `operations` is not updated. Fork copies `kind = 'compact'` only. Version blobs are not written by GC.

`LoadState` uses the live row when its `seq` is greater than or equal to `documents.snapshot_seq`. It then returns ops with `seq` above the live row. Otherwise it uses the compact row at `snapshot_seq`, as it does today.

### Pins, floor, stable

```go
type Report struct {
    Replica crdt.ClientID
    Vector  crdt.VersionVector
    At      time.Time
    Retired bool
}

type GCInput struct {
    Doc      crdt.VersionVector
    Reports  []Report
    Pins     map[crdt.ID]struct{}
    Floor    crdt.VersionVector
}

func Stable(in GCInput) crdt.VersionVector
```

`Stable` starts from `Doc`. For each report with `Retired == false` it takes the pointwise minimum. It then takes the pointwise minimum with `Floor`. A replica with no report contributes the zero vector. `Stable` does not look at pins. Pins are an exception list inside GC.

```go
func (d *Doc) GCWithPins(stable VersionVector, pins map[ID]struct{}) (int, error)
```

`GC` stays as it is and calls `GCWithPins` with an empty pin set. A node whose id is in `pins` is not eligible. Its ancestors stay because they still have a child. The rest of ADR 0006 is unchanged: deleted, no children, stable covers the node id and at least one delete id, and stable must be covered by the doc vector.

### Wire

`ClientMessage` gains `VectorReport`. It carries the sender's version vector. `DocumentSync` gains `bool resync`. `ErrorMessage.Code` gains `CODE_RESYNC = 14`, terminal, used only when a lapsed client sends an `OperationBatch` anyway. The normal path is the flag on `DocumentSync`, which is not an error.

`version.v1` gains internal `RestoreFloor(doc_id) -> vector, horizon_seq`. `comment.v1` gains internal `AnchorPins(fork_root) -> repeated OpID`. Both accept the internal token only.

`ReadOperationsResponse` gains the live snapshot's `seq` and `collected` vector so a hub can see a new live row without a second RPC.

### Metrics

Every process serves Prometheus text on `METRICS_ADDR`. Defaults: document `:9190`, presence `:9191`, comment `:9192`, version `:9193`, permission `:9194`, export `:9195`, hub `:9196`. Each registry includes the Go and process collectors.

| Name | Type | Where |
| --- | --- | --- |
| `collab_hub_apply_seconds` | histogram | hub loop, from taking an `opsEvent` through the sender `TrySend`, includes Append |
| `collab_hub_append_seconds` | histogram | the Append RPC inside `persist` |
| `collab_hub_deliver_seconds` | histogram | accepting hub's read-pump time, carried on the broadcast, to this hub's `TrySend` |
| `collab_hub_fanout_lag_seconds` | histogram | wall time from the publisher's `Publish` stamp to `Offer` on a receiver |
| `collab_hub_connections` | gauge | open sockets on this process |
| `collab_hub_policy_dump_seconds` | histogram | each `PolicyDump` |
| `collab_hub_policy_stale_documents` | gauge | open documents whose last good dump is older than 2s |
| `collab_doc_append_seconds` | histogram | document-service `AppendOperations` |
| `collab_doc_gc_pass_seconds` | histogram | one document's GC pass |
| `collab_doc_gc_nodes_removed_total` | counter | nodes removed |
| `collab_doc_gc_pinned_replicas` | gauge | replicas included in the last stable minimum |
| `collab_doc_gc_last_success_unixtime` | gauge | last pass that committed a live snapshot |
| `collab_export_jobs` | gauge, `state` label | `queued`, `running`, `ready`, `failed` |
| `collab_export_job_seconds` | histogram | insert to terminal state |
| `collab_grpc_server_seconds` | histogram, `method` and `code` | every gRPC service |

`collab_hub_deliver_seconds` is the server-observed end-to-end figure. The load test below is the client-observed figure. They are not the same number.

## GC

### Membership

The set is every `replicas` row whose `fork_root` is the document's `fork_root`, `kind` of `client` or `system`, archived homes included until the retirement rule below says otherwise. Connected sockets are not the set. ADR 0006 forbids that.

A fresh join (no `seq` query) has no unacked ops. After `DocumentSync` is queued, the loop stamps `doc.Vector()` for that replica. A resume does not get a server stamp. The client sends `VectorReport` after it has resent unacked ops, and every 30s while connected, and after each batch. An open socket therefore does not go quiet.

A system replica has no client buffer. `ClaimReplica` with `system = true` writes an initial report equal to `documents.vector` in that transaction. version-service calls `Report` with the loaded head's vector before it plans a mutation, and calls `Report` again with the post-append vector after `Append` returns. The first call is the floor for that in-flight restore or merge. GC cannot pass a parent the mutation already observed.

There is no production data, so the migration that adds `replica_reports` backfills each existing replica to its home document's vector.

### FIFO

The read pump is one goroutine. It writes `opsEvent` and `reportEvent` to the hub's event channel in socket order. `handleOpsGroup` persists queued ops before it returns a following `reportEvent`. The stamp's vector is the pointwise minimum of the client's vector and `doc.Vector()` after that persist. A clock the server has not stored is dropped. A clock below the stored report is rejected and the old row stands. The insert runs off the loop. A later append from the same replica can commit before the insert. The stored vector is then behind the log, which keeps stable lower. That is safe. The insert does not take the document lock.

### Stable inputs

1. Non-retired reports for the whole fork tree, including replicas with no open socket.
2. `RestoreFloor`, the pointwise minimum across unarchived documents in the tree.
3. The document vector. `Doc.GCWithPins` refuses a stable the doc does not cover.
4. `AnchorPins`, applied as pins, not as a vector.

`RestoreFloor` for one document is the version vector stored in the blob of the oldest `nodes` row with `created_at` inside the horizon. If every node is older than the horizon, the floor is the tip blob's vector, so the current text stays restorable. The horizon is `RESTORE_HORIZON`, default `720h`.

### Product call

Restore and Branch are offered for 30 days. An older version stays readable. `Restore` and `Branch` return `FailedPrecondition` with reason `RESTORE_HORIZON`. `Get`, `List`, `Diff`, and `GetBlob` stay. Blobs and the operation log are untouched, which is why diff and content still work.

Capping stable at the oldest version forever would keep every historical visible character in the head, and GC would not reclaim a document that is edited for months. A 24 hour horizon is shorter than the offline return ADR 0006 calls out. Thirty days leaves a month of undo and lets older leaf tombstones go.

Branch of an in-horizon version still copies operation rows and the compact snapshot. That path does not need the live tree. Branch of an older version is refused for the same reason restore is refused: a later merge would apply branch ops whose parents the head may already have collected. `Fork` uses the same horizon check, because `Branch` is `Fork` plus materialize.

### Pins

comment-service has no thread delete. Every thread row is non-deleted, resolved or not. Resolving does not unpin. `AnchorPins` returns `start` and `end` for every thread whose `home` is any document in the fork tree, including threads not yet visible on this document. Merge makes them visible later. Tombstones at those ids stay, which is what keeps a detached range positioned.

### Who runs GC

document-service runs one goroutine, `GC_INTERVAL` default `30s`. It does not run on the hub loop and it does not run inside `Compact`. Compact still writes `kind = 'compact'` with tombstones intact, so a historical snapshot and a version hash stay a pure replay of the log.

For each document whose `updated_at` is inside seven days, or whose live row is behind `op_seq`:

1. Load reports, call `RestoreFloor` and `AnchorPins`.
2. Compute `Stable`.
3. Build a doc from the compact snapshot plus ops, or from the current live row plus later ops.
4. Call `GCWithPins`.
5. Upsert the live snapshot at the `op_seq` the build covered.

One document at a time. A pass that cannot load pins or the floor skips that document and leaves the previous live row. Skipping lowers nothing.

The hub learns the new collected vector from `ReadOperations`. A goroutine builds a doc from `LoadState`. The loop swaps `h.doc` only when the built seq equals `delivered`, the sequencer has no gap, and this hub has no appended ops waiting above `D`. Otherwise it discards the build. The swap is a pointer write. GC does not walk the tree on the loop.

### Retention and return

`REPLICA_REPORT_TTL` defaults to `336h`. A replica whose `reported_at` is older than that, or whose `released_at` is set, is lapsed.

On join, a lapsed replica is sent `DocumentSync` with `resync = true` and is not sent `MissedOps`. collabclient replaces its doc from that snapshot and drops unacked ops. A batch that arrives anyway is discarded and the socket is closed with `CODE_RESYNC`. The replica id remains. The user keeps the device.

The last report stays in the stable minimum until retirement. Retirement requires the lapse, and either the home document is archived or every unarchived document in the tree has a vector that covers this replica's own client clock in the last report. Archiving a document refuses a later merge or fork from it. `ReleaseReplica` sets `released_at` and then uses the same rule. It does not delete the row.

That split is the whole trick. Forced resync drops unsent bytes that might parent a collected node. The report stays so ops already in a branch log still protect their parents until that branch is merged or archived. A tree with no branches retires a lapsed replica on the next GC pass, because the only unarchived document is the home and it already contains that replica's acked ops.

Fourteen days is the window because ADR 0006's 24 hour return must succeed without a resync, and a laptop left behind for a holiday should still come back inside the window. Unsent edits older than 14 days are dropped. Acked history remains in `operations`.

### Encrypted documents

GC does not look at plaintext. An encrypted node is removed under the same rules as a plain node. The live snapshot is CSS2-shaped protojson: remaining values stay ciphertext, `collected` advances, and no rune is written. Removed ciphertext stays in `operations`. Version CSV2 blobs are not rewritten, so node ids stay put. `PlanRestore` still copies cipher bytes, and the horizon keeps those source nodes in the head until the version ages out. The worker does not import `pkg/docrypt`. Export of an encrypted document stays refused. Comment pins are ids.

## Observability

`pkg/metrics` owns the HTTP server and a gRPC unary interceptor. Services pass a registerer into the packages that have the histograms above. The hub stamps `origin_unix_nano` on `fanout.v1.SequencedOps` when it calls `Publish`. Receivers record `collab_hub_fanout_lag_seconds`. The broadcast carries the accepting hub's read-pump unix nano so every hub can record `collab_hub_deliver_seconds` for its local sockets. A missing stamp records nothing. Clocks are wall clocks. On one machine that is the laptop clock. Across hosts the lag includes NTP skew, and the dashboard says so.

Export's gauge is a `count(*)` grouped by `state`, every 5s, from the job table. The histogram observes `ready` or `failed` minus insert time.

`deploy/grafana/collab.json` is one Grafana 11 dashboard. Datasource uid `prometheus`. Panels: hub connections, apply p50 and p99, deliver p50 and p99, fanout lag p99, append p99 on both hub and document-service, stale policy documents, GC nodes removed per second, GC last success, export jobs by state, gRPC error ratio. A Go test unmarshals the JSON and checks that every panel target is one of the names in the table above.

`deploy/prometheus/prometheus.yml` scrapes the seven metrics ports. Compose adds `prometheus` (`prom/prometheus`) and `grafana` (`grafana/grafana`) with the dashboard provisioned from the JSON. `make up` stays `postgres` and `redis` only, so integration tests do not boot Grafana. `docker compose up` starts the full file, including the app services that are missing from today's file: document-service, presence-service, comment-service, version-service, collab-hub, plus `comment-db` and `version-db`. comment-service and version-service keep separate databases because both use `schema_migrations`. version-service listens on `:9093`.

## Load

### What the number is

Two client-observed durations, both already implemented in `collabclient`.

Echo: `transmit`'s first `time.Now` for an op until `applySequenced` applies that op on the sender. That is the durable ack. ADR 0007 defines the ack as the sender's copy of the broadcast.

Fan-out: the same send time until `applySequenced` has applied that op on every other connection that is on the same document. The sample is the maximum of those receive times minus the send time. A missing receive time fails the test. It is not dropped.

The asserted number is both echo and same-document fan-out. It is not "last of 9,999 sockets".

### Mix

One test process, two hub processes, one document-service, one Postgres, one Redis.

10,000 connections: 500 documents, 20 connections each, striped by `doc index % 2` across the hubs. Documents are empty at create. Join snapshots stay small.

25 of those documents are active. Each of their 20 connections is a receiver. Two of the 20 are editors. The other 475 documents stay idle after `DocumentSync`. Idle hubs are part of the connection count and part of the noise.

Each editor sends 40 ops, one op per batch, with 200ms between sends. Cohort start is the moment editors are released. Samples whose send time is before cohort start plus 3s are not recorded. That leaves about 25 samples times 50 editors, about 1,250 samples in each histogram.

`HUB_IDLE_SYNC=10s` for this process only. The production default stays 1s. At 1s, 500 hub goroutines would each read the log once a second and the append tail would be the poll, not the ack.

### Machine

The proof machine is this laptop: macOS, 8 cores, Postgres and Redis in Docker Desktop, services and the test on the host.

The test reads `RLIMIT_NOFILE` and fails immediately when the soft limit is below 20,000. Each socket is one fd in the test and one fd in a hub. `ulimit -n 65536` is the setup. It also reads `net.inet.ip.portrange.first` and `.last` and fails when the range is narrower than 12,000. The default range is 49152 through 65535, which is 16,384 ports. 10,000 connections from one IP need 10,000 source ports. The test dials once and holds. It does not reconnect, because `TIME_WAIT` would then exhaust the range.

What is credible here: 10,000 parked sockets, 50 appends per second (25 documents, 2 editors, 200ms pacing), fan-out to 19 other sockets on that document, echo dominated by one Postgres commit. README puts Append p99 near 20ms on this shape of machine. A free loop plus a broadcast to 19 peers fits in the rest of a 50ms budget.

What is not credible here, and is not the claim: fan-out p99 under 50ms to 99 peers (README already measured 23ms to above 200ms), fan-out to 10,000 peers on one document (one hub goroutine, one Append, 10,000 `TrySend`s, and `sendBuf` of 4096 pointers per peer), or 1,000 appends per second against one Docker Postgres. Proving those needs the load generator on another machine, which this phase does not have.

### Histogram

`github.com/HdrHistogram/hdrhistogram-go`, lowest trackable value 1 microsecond, highest 60 seconds, 3 significant figures. One histogram for echo and one for fan-out. Record `Microseconds()`. p99 is `ValueAtPercentile(99.0)`. The claim is that both values are strictly below 50,000. A value of 50,000 or above refutes the claim for that run. One run is enough to refute. The test does not average runs and does not drop the worst samples beyond the 3s warm-up.

Build tag `loadproof`. `go test ./...` does not run it. `make load-proof` runs:

```sh
ulimit -n 65536
go test -tags loadproof -count=1 -timeout 20m -run TestTenThousand ./tests/load/
```

`make test-integration` does not pass the tag. A busy laptop that fails the assertion has refuted the claim for that run. The fix is a real stall, not a higher threshold.

### Loop stalls that would inflate the number

`PolicyDump` is already off the loop, on `pollMembers`. These calls are still on the loop and sit in front of the next op:

| Call | Where | What happens |
| --- | --- | --- |
| `store.Append` | `persist` | Stays. It is the ack. `collab_hub_append_seconds` times it. |
| `store.ReadOperations` | `fill`, reached from idle `poll` every `idleSync`, from gap `heal`, and from `handleJoin` | Moves off the loop. The goroutine reads and posts a batch. The loop only `Offer`s. |
| `store.ReadOperations` | `joinFrames`, MissedOps, up to five reads of 4,096 ops | Moves off the loop. The goroutine `TrySend`s frames, then posts `joinReady`. `Snapshot()` for a fresh join stays on the loop. It is CPU and the load documents are empty. |
| `waitPolicy` | `admitFresh` inside `handleJoin`, up to `policyStale` (2s) | Moves to the HTTP goroutine before `Join`. The loop calls `admit`, which only loads the pointer. A stale pointer refuses the join. |
| `pres.Move`, `Rename`, `Leave` | cursor, rename, leave | Stay. They take the writer mutex and do no RPC. |
| `bus.Publish` | `persist` | Stays. The Redis queue is non-blocking and drops when full. |
| `Load`, `subscribe`, `waitFirstPolicy` | before the select | Stay. They run once per hub start. |

Report inserts and live-snapshot builds are new work. Both run off the loop. The loop only stamps a vector or swaps a pointer. A regression test blocks `ReadOperations` for 500ms and requires an op submitted during that block to echo in under 100ms. That is the carried Phase 4 item, checked against the calls that replaced `PolicyDump`.

## Deploy

One chart, `deploy/helm/collab-sphere`. Templates for the seven processes, plus Postgres, Redis, and MinIO deployments and services. `postgresql.enabled`, `redis.enabled`, and `minio.enabled` default to true. When false, the chart uses `postgresql.host`, `redis.url`, and `minio.endpoint` and does not render the in-cluster objects. In-cluster databases are single-replica Deployments with a PVC. They are there so `helm template` renders a cluster that can boot. They are not a production operator.

Each app Deployment gets `DATABASE_URL` or the addresses it needs, `AUTH_PUBLIC_KEY`, `INTERNAL_TOKEN`, `METRICS_ADDR`, and the Phase 4 keys that service already requires. Secrets come from values. The committed values match the dev keys already in `docker-compose.yml`. A real cluster overrides them. The chart does not generate a production CA.

`deploy/argocd/collab-sphere.yaml` is one `Application`. Source path `deploy/helm/collab-sphere`, destination namespace `collab`, manual sync. Rendering the manifest does not sync it.

Validation is `make helm-check`:

1. `helm lint deploy/helm/collab-sphere`
2. `helm template` with in-cluster dependencies, and again with all three `enabled` flags false and external hosts set
3. `kubeconform -kubernetes-version 1.31 -summary` on both renders

No kind and no k3d. A local cluster would pull images and start a second Kubernetes, and this phase deploys nothing. The Application YAML is checked by a Go test for `kind`, the chart path, namespace `collab`, and a manual sync policy. Stock kubeconform schemas do not include the Argo CRD, so the Application is not fed to kubeconform.

## Done predicate

Each line fails on a non-zero exit.

1. `go test ./pkg/crdt/ ./services/document-service/...` includes GC pins, report monotonicity, live snapshot load, and an unchanged `operations` row count after GC.
2. `go test ./services/collab-hub/internal/hub/ -run TestReadDoesNotBlockApply` passes. A blocked `ReadOperations` does not hold an unrelated echo for 500ms.
3. `go test ./tests/internal/stack/ -run 'TestGC|TestResync|TestHorizon'` passes. The cases are listed in unit 8.
4. `go test ./deploy/grafana/ -run TestDashboardTargets` passes.
5. `make helm-check` passes.
6. `make load-proof` on this laptop, with the fd limit and the port range in range, prints echo p99 and fan-out p99, both below 50ms, and prints `connections=10000`. A p99 of 50ms or above refutes the claim.

`make test-integration` stays green without the load tag and without helm.

## Units

Each unit is one push to main. Later units compile against the earlier ones. A unit's tests pass before the next unit starts.

1. **`GCWithPins` in `pkg/crdt`.** Existing `GC` tests pass unchanged. New tests: a pinned leaf tombstone stays, its unpinned child leaf is removed, and a stable the doc does not cover is still refused.
2. **Reports and `documents.vector`.** Migration, stamp rules, monotonic upsert, backfill. No GC worker. A database test appends an op and a report for the same replica and shows `home_seq` equal to that op's seq when the stamp is taken after the append, and shows a backwards clock leaving the old row.
3. **Hub report path.** `VectorReport` from the client, stamp on the loop, insert off the loop. A fresh join stores `doc.Vector()`. A resume does not. The blocking-read regression test lands in this unit only if unit 4 is not yet done. Prefer unit 4 first if both are open. This unit must not call `ReadOperations` from a new place on the loop.
4. **Loop reads and policy wait move off the loop.** `TestReadDoesNotBlockApply`. Gap fill, idle fill, and MissedOps still deliver the same bytes. `admit` on the loop does not call `waitPolicy`.
5. **Metrics on all seven processes, Prometheus, Grafana, dashboard test.** Histograms that have no caller yet are registered and stay at zero. Compose scrapes them.
6. **`AnchorPins` and `RestoreFloor`.** comment-service returns every thread endpoint in the tree. version-service returns the horizon vector. `Restore` and `Branch` of an older version return `RESTORE_HORIZON`. `Diff` of that version still returns spans. Tests set `RESTORE_HORIZON` to one hour and move the clock.
7. **GC worker and live snapshot.** `Stable`, the worker, `LoadState`, fork still copying compact rows. Hub swap of a matching live doc. Encrypted live snapshot round-trips ciphertext and contains no rune.
8. **Lapse, resync, retirement.** Stack tests, with `REPLICA_REPORT_TTL` shortened:
   - Offline replica inside the TTL: its report holds stable, and a leaf it has not covered survives a pass.
   - Same replica past the TTL, no other document in the tree: the leaf is gone, `count(*)` of `operations` is unchanged, reconnect gets `resync = true`, and the unsent parented insert is absent from the log.
   - Replica homed on branch B: D does not collect a parent that B's report covers. Archiving B and retiring the replica lets the next pass collect it. Merge of an archived B is `FailedPrecondition`.
   - Pinned comment endpoint survives. An unpinned leaf tombstone in the same doc does not.
   - Encrypted document: the removed op's ciphertext is still in `operations`, and the live snapshot has no rune.
9. **`TestTenThousand` and `make load-proof`.** Depends on unit 4 and unit 5. The assertion is the done predicate's last line.
10. **Helm chart, Argo CD Application, `make helm-check`.** Depends on the metrics ports and the GC env names being fixed in unit 5 and unit 7.

## Tradeoffs

- One live snapshot row per document, upserted, in exchange for GC on a quiet document. Compact rows stay immutable so version hashes and forks stay a replay.
- A 30 day restore horizon, in exchange for GC that can finish. Older history remains in blobs and in the log.
- A 14 day forced resync, in exchange for dropping unsent ops. The report stays in the minimum so an unmerged branch does not lose parents.
- Report insert off the loop, in exchange for a stamp that can sit behind a later append. Stable moves late, not early.
- Hub GC by swapping a built doc, in exchange for not walking the tree on the loop. A hub that is mid-gap keeps tombstones until it is caught up.
- The 50ms claim is echo and 20-way fan-out on 25 active documents while 9,500 sockets are idle. A 10,000-way fan-out number is not claimed.
- `loadproof` is out of `make test-integration`, in exchange for a suite that does not flap when the laptop is busy. The proof command is explicit.
- In-chart Postgres, Redis, and MinIO are single pods, in exchange for `helm template` producing a bootable dev cluster without a dependency download.
- Manual Argo CD sync, in exchange for a manifest that does not deploy a cluster by being applied.

## Rejected designs

- Truncating `operations`, or deleting compact snapshots. Fork, materialize, and the version oracle read them. Phase 3 §9.1 forbids it.
- A stable vector per document, computed from that document's sockets. A branch replica can still parent a node on D. Phase 3 §9.2.
- Treating "no recent socket" as "exclude from the minimum" with no retirement check. Acked branch ops would then merge onto a collected parent.
- Refusing the replica id forever after the TTL. The id is the user's device. Forced resync drops the unsafe bytes and keeps the binding.
- Running `GC` inside `Compact` and overwriting the compact row. That changes the bytes a fork copies and the bytes a version hash replays.
- GC on the hub loop. The walk is proportional to the tombstone set and would sit on the ack path this phase is measuring.
- Putting the vector report in the operation log as a fake op. It would change seq, hashes, and the "operations are CRDT ops" rule. The FIFO proof only needs the stamp after persist.
- Unpinning resolved threads. The contract says non-deleted threads. Resolved threads are still listed and can be unresolved.
- Asserting p99 under 50ms for 10,000 receivers on one document, on this laptop. The hub is one goroutine, and the 100-editor fan-out tail is already over the line in `docs/perf`.
- Redis as the metrics bus, or a second log for lag. Prometheus scrapes the process. The fan-out stamp rides the existing hint message.
- Bitnami subcharts. `helm lint` would need a network fetch. Local templates do not.
- A kind install as the gate. This phase deploys nothing. kubeconform on `helm template` is the check.

## Risks

- A client that ignores `resync` and resends a parented insert is disconnected with `CODE_RESYNC`. A modified client that then reconnects without the flag and resends again is disconnected again. The op is never appended. A client that forges a high vector is clamped to `documents.vector`.
- Retirement uses version vectors of other documents. A bug that marks a branch covered before merge lets D collect a parent the branch still needs. The stack test that archives B before the leaf disappears is the guard. A pass that cannot load a sibling vector skips the document.
- `RestoreFloor` and `AnchorPins` are extra RPCs on the GC pass. If either is down, that document is skipped. Tombstones stay. They are not collected on a partial answer.
- The live snapshot and a concurrent append: the worker builds at `op_seq` N, an append commits N+1, the upsert is stored at N. `LoadState` replays the new op. The hub swap refuses the build because `delivered` has moved. The next pass rewrites the live row.
- System-replica `Report` before plan and after append is two RPCs. If the process dies between them, the floor report stands and GC waits. The mutation resume path already replays the append. It must also send the second report. Unit 8's crash is the version-service kill already used for restore, plus a GC pass that must not remove the restore's parent.
- 10,000 sockets with `sendBuf` 4096 allocate the channel arrays even when empty. That is on the order of a few hundred megabytes of pointers across two hubs. The test prints `runtime.MemStats.Alloc` for the hubs. It does not assert a memory ceiling in this phase. An alloc above 2 GiB on a hub is a reason to stop and shrink the idle fan-out buffers, not a reason to raise the latency threshold.
- Docker Desktop's Postgres p99 moves when the laptop is under other load. `make load-proof` is the claim. A red run refutes it for that boot. The histogram method does not give the run a second chance inside the same process.
- Wall-clock `collab_hub_fanout_lag_seconds` is wrong if the hub hosts' clocks diverge. The client-observed load numbers use one process's monotonic clock and are the proof. The Prometheus series is the dashboard.
