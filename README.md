# collab-sphere

collab-sphere is a real-time collaborative text editor backend written in Go. Clients edit a shared document over WebSocket, and every replica converges to the same text. Several hubs can serve one document at once. Users sign in with tokens, roles control who can read, comment, and edit, and documents keep a version history that you can restore, branch, merge, and export.

## How it fits together

A client opens a WebSocket on `collab-hub` at `GET /ws/{documentID}`. The hub checks the user's token and role, loads the document from `document-service`, and sends a `DocumentSync` with the full CRDT snapshot. It then relays binary protobuf `ClientMessage` and `ServerMessage` frames.

The hub validates each operation, appends it through `document-service`, and broadcasts it only after the append commits. The broadcast doubles as the sender's acknowledgement. `document-service` gives every operation a sequence number, and each hub delivers operations to its sockets in that order. Hubs publish appends on Redis Pub/Sub as a hint and repair anything they missed from the log.

| Service | Owns |
| --- | --- |
| `collab-hub` | WebSocket sessions, one goroutine per open document, fan-out between hubs |
| `document-service` | Documents, the operation log, snapshots, replica reports, and tombstone garbage collection, in PostgreSQL |
| `presence-service` | Cursors and display names in Redis. It expires sessions whose hub died. |
| `permission-service` | Owners, grants, and share links. Roles are `owner`, `editor`, `commenter`, and `viewer`. |
| `comment-service` | Comment threads anchored to text, which survive edits |
| `version-service` | The version graph: checkpoints, restore, branch, merge, and diff |
| `export-service` | Export jobs that render a version and store the file in S3-compatible storage |

Shared packages live in `pkg/`:

- `crdt` is the RGA sequence CRDT, with insert, delete, merge, snapshot, and garbage collection.
- `auth` mints and verifies Ed25519 user tokens and the internal service token.
- `access` is the `permission-service` client that the other services use to check roles.
- `docrypt` seals document text and wraps document keys for end-to-end encrypted documents. Clients use it. Services never import it, so they store only ciphertext.
- `render` turns visible document text into Markdown, DOCX, and PDF.
- `metrics` serves the Prometheus registry.
- `collabclient` is the Go WebSocket client that the tests use.

`proto/` holds the protobuf definitions. `gen/` holds the generated Go code, which is committed. The reasoning behind each design choice is in [`docs/adr`](docs/adr).

## Requirements

- Go 1.26
- Docker with Compose
- `protoc` with `protoc-gen-go` and `protoc-gen-go-grpc`, only if you change `proto/`
- `helm` and `kubeconform`, only if you change `deploy/helm`

## Run the stack

To start every service with its databases, Redis, MinIO, Prometheus, and Grafana, run:

```sh
docker compose up --build
```

The hub listens on `localhost:8080`. Grafana is on `localhost:3000`, and Prometheus is on `localhost:9098`. `docker-compose.yml` sets development keys and passwords. Never reuse them outside your machine.

To run one service from source against the Compose databases, start Compose and use the matching target, for example `make run-docsvc`, `make run-hub`, `make run-presence`, `make run-permission`, or `make run-export`.

## Connect a client

1. Create a document with the `CreateDocument` RPC on `document-service`.
2. Mint a user token with `auth.Mint` and the private key that matches `AUTH_PUBLIC_KEY`.
3. Open `ws://localhost:8080/ws/{documentID}` with the header `Authorization: Bearer {token}`.

The hub answers `401` for a bad token and `403` when the user has no role on the document. `GET /healthz` reports hub liveness.

These query parameters are optional:

| Parameter | Meaning |
| --- | --- |
| `client` | The replica ID to reuse. The server assigns one when it is absent. |
| `seq` | Resume after this sequence number. The hub then sends only the operations you missed. |
| `conn` | The connection epoch. A newer connection for the same replica replaces an older one. |

If a replica has been offline longer than `REPLICA_REPORT_TTL`, or was released, the hub sends a `DocumentSync` with `resync` set. The client then replaces its local state and drops its unsent operations.

## Configure the services

Each service reads its configuration from environment variables. Every backend service also needs `INTERNAL_TOKEN`, and every service except `presence-service` needs `AUTH_PUBLIC_KEY`.

| Service | Variable | Default |
| --- | --- | --- |
| all | `METRICS_ADDR` | `:9190` to `:9196` in the order of the table above. Empty turns metrics off. |
| `collab-hub` | `HTTP_ADDR` | `:8080` |
| `collab-hub` | `DOCUMENT_SERVICE_ADDR` | `localhost:9090` |
| `collab-hub` | `PERMISSION_ADDR` | required |
| `collab-hub` | `REDIS_URL` | unset, which means in-process fan-out and a single hub |
| `collab-hub` | `HUB_ID` | `{hostname}-{pid}` |
| `collab-hub` | `PRESENCE_SERVICE_ADDR` | unset, which means in-process presence and a single hub |
| `collab-hub` | `PRESENCE_HEARTBEAT` | `10s` |
| `collab-hub` | `HUB_IDLE_SYNC` | `1s` |
| `document-service` | `DATABASE_URL` | required |
| `document-service` | `GRPC_ADDR` | `:9090` |
| `document-service` | `PERMISSION_ADDR`, `VERSION_ADDR`, `COMMENT_ADDR` | required |
| `document-service` | `GC_INTERVAL` | `30s`. `0` or empty turns garbage collection off. |
| `document-service` | `REPLICA_REPORT_TTL` | `336h` |
| `presence-service` | `REDIS_URL` | required |
| `presence-service` | `GRPC_ADDR` | `:9091` |
| `presence-service` | `PRESENCE_TTL` | `30s` |
| `presence-service` | `PRESENCE_SWEEP` | `1s` |
| `comment-service` | `DATABASE_URL`, `DOCUMENT_ADDR`, `VERSION_ADDR`, `PERMISSION_ADDR` | required |
| `comment-service` | `GRPC_ADDR` | `:9092` |
| `version-service` | `DATABASE_URL`, `DOCUMENT_ADDR`, `PERMISSION_ADDR` | required |
| `version-service` | `GRPC_ADDR` | `:9093` |
| `version-service` | `SWEEP_INTERVAL` | `5s` |
| `version-service` | `RESTORE_HORIZON` | `720h` |
| `permission-service` | `DATABASE_URL`, `DOCUMENT_ADDR`, `SHARE_PRIVATE_KEY` | required |
| `permission-service` | `GRPC_ADDR` | `:9094` |
| `export-service` | `DATABASE_URL`, `DOCUMENT_ADDR`, `VERSION_ADDR`, `PERMISSION_ADDR` | required |
| `export-service` | `S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION` | required |
| `export-service` | `GRPC_ADDR` | `:9095` |

Versions older than `RESTORE_HORIZON` stay readable and diffable, but `Restore` and `Branch` refuse them with `RESTORE_HORIZON`.

## Test

```sh
make test               # unit tests with -race. Database tests skip.
make test-integration   # starts PostgreSQL and Redis, then runs every test, including end-to-end and load tests
make load-proof         # 10,000 WebSocket connections. Needs COLLAB_TEST_DATABASE_URL and COLLAB_TEST_REDIS_URL.
make helm-check         # renders the chart and validates it with kubeconform
```

Integration tests read `COLLAB_TEST_DATABASE_URL`, `COLLAB_TEST_REDIS_URL`, and `COLLAB_TEST_S3_URL`. `tests/internal/stack` builds the service binaries once and starts them on a fresh database for each test. `tests/e2e` covers editing across three hubs, a killed hub, a Redis outage, and presence. `tests/crdt` checks merge correctness in every arrival order.

## Deploy

`deploy/helm/collab-sphere` is a Helm chart for every service, with in-cluster PostgreSQL, Redis, and MinIO that you can swap for external ones. `deploy/argocd/collab-sphere.yaml` is an Argo CD `Application` with manual sync. `deploy/prometheus` and `deploy/grafana` hold the scrape config and the dashboard.

To build images and install the chart on a local cluster, run:

```sh
make images IMAGE_TAG=dev
helm upgrade --install collab deploy/helm/collab-sphere \
	--namespace collab-sphere --create-namespace \
	--set images.document.tag=dev --set images.presence.tag=dev \
	--set images.comment.tag=dev --set images.version.tag=dev \
	--set images.permission.tag=dev --set images.export.tag=dev \
	--set images.hub.tag=dev
```

The chart's default secrets are for development only. Override `secrets.*` on any shared cluster.

## Performance

The target is a 99th-percentile echo and fan-out latency under 50ms with 10,000 open connections. `make load-proof` asserts it. On one laptop that runs the services, Redis, PostgreSQL, and the load generator together, the result varies between runs. With PostgreSQL in Docker, the 99th percentile measured 53 to 114ms. With PostgreSQL running natively, it measured 42 to 162ms, and half the runs passed. A reliable result needs the load generator on a separate machine. `docs/perf` has earlier measurements.

## Known limits

- Garbage collection is conservative across branches. While any branch of a document stays open, tombstones from clients that branch has not seen are kept. Nothing is lost, but storage grows on documents with long-lived branches.
- There is no user directory. Another system issues tokens, and the services only verify them against `AUTH_PUBLIC_KEY`.
- A rejected operation disconnects its sender. The client reconnects and replaces local state from `DocumentSync`.
- Snapshots are stored as protojson in JSONB. ADR 0004 covers the `bytea` alternative.
