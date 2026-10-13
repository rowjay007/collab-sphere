# ADR 0004. Postgres holds JSONB snapshots every 100 ops plus an idempotent op log

Status: accepted, Phase 1.

## Context

The brief mandates a full snapshot every 100 operations, stored as protobuf in Postgres JSONB, with later ops kept as a delta log. Restore is the snapshot plus a replay of the log.

## Decision

document-service owns three tables. `documents` holds metadata plus `op_seq`, the number of ops stored, and `snapshot_seq`, the op count the latest snapshot covers. `snapshots` holds `crdt.v1.Snapshot` as `protojson` in a JSONB column. `operations` holds each op as deterministic protobuf bytes, keyed by `(document_id, seq)` and unique on `(document_id, op_client, op_clock)`.

`AppendOperations` runs in one transaction. It locks the document row, drops ops already stored with identical bytes, rejects a held ID with different bytes, gives the new ops contiguous seqs, and bumps `op_seq`. When `op_seq - snapshot_seq` reaches 100, it rebuilds the document from the latest snapshot and the newer ops and writes a new snapshot at `op_seq`. A batch that crosses the boundary snapshots once, at its last seq.

`LoadState` reads the document, the snapshot at `snapshot_seq`, and every op after it, in one repeatable-read transaction. That way no append can slip in between the snapshot read and the log read.

The snapshot lists nodes in document order with parent IDs, so restore is one O(n) pass. The pass checks that each parent comes before its children and that sibling order holds.

## Alternatives rejected

- **Storing snapshots as protobuf bytes.** That is smaller than protojson, and Phase 1 never queries inside a snapshot. The brief asks for JSONB, so Phase 1 follows it. A 300k-character document is roughly tens of MB as protojson. If snapshot size shows up in measurements, switching the column to `bytea` is a one-migration change.
- **Ignoring duplicate ops with `ON CONFLICT DO NOTHING`.** That leaves gaps in `seq`, which would throw off the 100-op count.

## Consequences

The row lock serializes appends per document. That is the right order for a single hub. In Phase 2 it becomes the cross-instance write order. Old ops and snapshots are kept, and Phase 3's version history builds on them.
