# collab-sphere

Real-time collaborative text editing in Go. This repository holds Phase 1: a CRDT core and single-server collaboration. Two or more clients edit one document over WebSocket, and every replica converges to the same text.

## Layout

| Path | What it is |
| --- | --- |
| `pkg/crdt` | RGA sequence CRDT: insert, delete, merge, snapshot, tombstone GC |
| `pkg/collabclient` | Go WebSocket client used by the end-to-end and load tests |
| `services/document-service` | gRPC service that stores documents, the op log, and snapshots in PostgreSQL |
| `services/collab-hub` | WebSocket server with one goroutine per open document |
| `proto/` | Protobuf definitions for the CRDT snapshot, the WebSocket protocol, and the gRPC API |
| `gen/` | Generated Go code for `proto/`, committed |
| `tests/e2e` | Starts both binaries against a fresh database and drives real clients |
| `docs/adr` | Architecture decisions |
| `docs/perf` | Load-test measurements |

A client connects to `GET /ws/{documentID}?client={id}` on the hub. The hub loads the document from document-service, sends a `DocumentSync` with the full CRDT snapshot, and then relays binary protobuf `ClientMessage` and `ServerMessage` frames. The hub validates and applies each op, persists it through document-service, and only then broadcasts it. The broadcast doubles as the sender's acknowledgement.

## Requirements

- Go 1.26
- Docker, for PostgreSQL 17
- `protoc` with `protoc-gen-go` and `protoc-gen-go-grpc`, only if you change `proto/`

## Run

```sh
make up            # PostgreSQL on localhost:55432
make run-docsvc    # gRPC on :9090, applies migrations at startup
make run-hub       # HTTP and WebSocket on :8080
```

Configuration is by environment variable.

| Service | Variable | Default |
| --- | --- | --- |
| document-service | `DATABASE_URL` | required |
| document-service | `GRPC_ADDR` | `:9090` |
| collab-hub | `HTTP_ADDR` | `:8080` |
| collab-hub | `DOCUMENT_SERVICE_ADDR` | `localhost:9090` |

Create a document with the `CreateDocument` RPC, then connect clients to `/ws/{id}`. `/healthz` reports hub liveness.

## Test

```sh
make test               # unit tests with -race; database tests skip
make test-integration   # starts PostgreSQL and runs everything, including tests/e2e
```

The end-to-end suite builds both binaries, runs them as subprocesses on a database created for each test, and checks these things:

- Two clients making 700 concurrent edits converge to identical CRDT state, and document-service holds the same state.
- Both services restart and a new client receives the same text.
- 100 clients each make 40 edits concurrently and converge. The test logs op round-trip percentiles.

To reproduce the load numbers without the race detector:

```sh
COLLAB_TEST_DATABASE_URL='postgres://collab:collab@localhost:55432/collab?sslmode=disable' \
  go test -count=1 -v -run 'TestHundred|TestAppendLatency' ./tests/e2e/
```

## Performance

The 100-editor test starts from a 20,000-rune document. It reports sender echo latency and full fan-out latency. Full fan-out ends when the last of the other 99 clients receives an op. The target is full fan-out P99 under 50ms. The test logs the measurements but does not assert the target because shared laptop load is noisy.

Editors type at a cursor held as a CRDT ID, using `Replica.InsertAfter` and `Replica.Delete`. Index-based edits call `IDAt`, which walks the document. On a 20,000-rune document, that walk took more than half of the test process's CPU.

On one laptop running Postgres in Docker, both services and 100 full replicas, fan-out P50 is 7 to 11ms. Fan-out P99 ranges from 23ms to over 200ms between runs. The hub's Append call has a P50 of about 4.5ms and a P99 of about 20ms. Disabling snapshots or `synchronous_commit` does not remove the tail. `docs/perf/after-group-commit.txt` has the runs. Proving the target needs the load generator on separate hardware from the services.

## Phase 1 limits

- No authentication. Clients choose their own client ID, and the hub only rejects a second live connection with the same ID on one document.
- One hub process. There is no Redis fan-out and no cross-instance routing yet.
- A rejected operation disconnects its sender. The client must reconnect and replace local state from `DocumentSync`.
- The server pings each WebSocket every 30 seconds. Failed keepalives and slow consumers release their client IDs.
- Archived documents refuse collaboration joins.
- `crdt.Doc.GC` exists and is tested, but no service calls it. Tombstones accumulate.
- Snapshots are protojson in JSONB, as the brief asks. ADR 0004 covers the `bytea` alternative.
