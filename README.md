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

With 100 editors on one laptop, op round trip from send to echo has a P50 of about 6.5ms. P99 is 16 to 30ms in 7 of 8 runs. The eighth run reached 71ms because of a single 228ms stall. The target is P99 under 50ms. `docs/perf/after-group-commit.txt` has the raw runs and the path from the 13.5s baseline.

## Phase 1 limits

- No authentication. Clients choose their own client ID, and the hub only rejects a second live connection with the same ID on one document.
- One hub process. There is no Redis fan-out and no cross-instance routing yet.
- `crdt.Doc.GC` exists and is tested, but no service calls it. Tombstones accumulate.
- Snapshots are protojson in JSONB, as the brief asks. ADR 0004 covers the `bytea` alternative.
