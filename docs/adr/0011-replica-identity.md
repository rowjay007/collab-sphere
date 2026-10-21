# ADR 0011. Replicas are homed in one document per fork tree

Status: accepted, Phase 3.

## Context

An operation ID is `(client, clock)`. A client that authors different operations with the same ID in two branches poisons a later merge. The operation log then has two byte representations for one ID, and the merge must reject one of them.

Version vectors also require each document to contain a prefix of every replica's operations. If one replica authors operations independently in two branches, a maximum clock no longer proves that all earlier operations are present.

## Decision

document-service stores each replica in `replicas`, keyed by `(id, fork_root)`. The row names one home `document_id`. A replica may author operations only through a connection to its home document. Fork and merge operations can copy those operations to other documents in the tree.

A WebSocket join may omit `client`. The hub calls `ClaimReplica` with client zero, and document-service allocates an ID from `replica_ids`. `DocumentSync.client` returns the allocated ID. collabclient keeps that ID and sends it on every reconnect.

An explicit ID claims an unused `(id, fork_root)` for the joined document. A later claim from the same document succeeds. A claim from another document in the tree fails with `REPLICA_HOMED_ELSEWHERE`, which the hub reports as `CODE_UNKNOWN_REPLICA`. Unrelated fork trees may use the same ID.

The claim rule gives each operation ID one author log within a fork tree. A merge can therefore use a plain CRDT union. Version vectors remain exact because every document contains a prefix of each replica's author log.

Registration is not a separate client step. Claiming on join preserves existing callers that send explicit IDs while enforcing the rule that matters at merge time. The database primary key resolves concurrent claims. One document wins, and the other claim reads that home and fails.

Hubs cache successful `(document, replica)` checks for the process lifetime. Replica homes never change. Failed checks are not cached.

## Alternatives rejected

- **Rewrite operation IDs during merge.** No pure collision-free function maps an unbounded set of document-qualified IDs into a fixed-width client ID. A stored mapping would leak into repeated merges, reverse merges, diffs, comments, and idempotency.
- **Use document-qualified operation IDs.** This changes the CRDT ID type, every wire format, storage, comparisons, and anchors. Homed replicas keep the existing fixed-width ID.
- **Require registration before join.** The extra lifecycle does not strengthen the fork-tree invariant. A claim on first use establishes the same home and keeps Phase 1 and Phase 2 callers working.
- **Make IDs globally unique.** Legacy client IDs can repeat in unrelated documents. Fork-tree scope prevents collisions where documents can merge without forcing an unrelated migration.

## Consequences

Phase 4 will bind replica rows to authenticated users. Until then, a caller can claim another live replica that is already homed in the same document. Authentication must close that gap.

Replica rows are not pruned because an offline client may return with unsynced operations. An unauthenticated caller can grow the table by claiming IDs. Phase 4 must add ownership and abuse controls for both explicit claims and allocation.

Every operation ID has one author log in a fork tree. Merge remains a byte-preserving union, and version-vector containment remains exact.
