# ADR 0003. The WebSocket protocol is binary protobuf with structured IDs

Status: accepted, Phase 1.

## Context

The brief mandates binary protobuf frames. Its sketch encodes a node ID as a string like `"7:42"`.

## Decision

Every frame is one binary WebSocket message holding a `collab.v1.ClientMessage` or `collab.v1.ServerMessage`. Text frames close the connection. IDs are `crdt.v1.OpID{client, clock}`. An operation is `Operation{id, oneof {Insert{parent, value}, Delete{target}}}`.

`crdt.OpFromProto` parses every op at the socket boundary and rejects zero IDs, missing fields, and invalid runes. Code past the boundary only sees `crdt.Insert` and `crdt.Delete`.

Phase 1 implements `OperationBatch`, `CursorMove`, and `Ping` from clients. It sends `DocumentSync`, `OperationBroadcast`, `PresenceBroadcast`, `ErrorMessage`, and `Pong` from the server. `AwarenessUpdate` and `MissedOps` keep their field numbers reserved for Phase 2.

Cursors are a pair of CRDT IDs, the anchor and the selection head. The cursor sits right after the anchor character. The hub rejects a cursor whose IDs it does not hold.

## Alternatives rejected

- **String IDs.** Every reader would parse them, and a bad parse is a runtime error the schema could have prevented.
- **A flat op message with a type enum and optional parent, target, and value fields.** A oneof makes a delete with a value unrepresentable.

## Consequences

Clients in other languages generate code from `proto/`. The brief also asks for a Yjs-compatible wire format for the frontend. That needs a translation layer when the frontend lands, and it is not part of Phase 1.
