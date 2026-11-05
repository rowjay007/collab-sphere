# ADR 0015. Export jobs are rows

Status: accepted, Phase 4.

## Context

A viewer needs a PDF, a Markdown file, or a DOCX of one pinned version. The bytes are large, and the caller polls until they exist. The worker can die after it has uploaded the file and before it records success. A second process has to finish that same job and leave one object.

The caller already polls a job row. A Redis stream beside that row would be a second source of truth. The version blob already exists after checkpoint, so the export worker should not materialize the log again.

## Decision

`export-service` owns `export_jobs` and runs the gRPC server and the claim loop in one process. `RequestExport` requires a viewer, refuses a CSS2 blob with `FailedPrecondition` reason `ENCRYPTED`, and inserts no row in that case. An empty version id resolves through `Head`. A 32-byte version id is loaded with `GetVersion` and must belong to the document. The same `request_id` returns the stored row.

`version_id` stores the 32-byte node id followed by the 32-byte content hash. `GetVersion` accepts a user token only. The worker calls `GetBlob` with the internal token, including after a restart, so the hash has to sit in the row. The RPC returns the node id.

The claim is one `UPDATE` with `FOR UPDATE SKIP LOCKED`. It takes the oldest queued row, or a running row whose lease is null or due, sets `running`, sets `lease_until` 30 seconds ahead, and increments `attempts`. A row whose `attempts` is already 5 is marked `failed` instead of being rendered. `EXPORT_CRASH_AFTER=claimed` exits after that update commits. `EXPORT_CRASH_AFTER=upload` exits after `PutObject` and before the ready update. Compose leaves the variable unset.

The object key is `exports/{job_id}` in the bucket named by `S3_BUCKET`. The service creates that bucket on startup when it is missing. Render writes a temp file and uploads it with one `PutObject`. There is no multipart upload. A retry uses the same key, so the second put overwrites the first. The ready update is `WHERE state = 'running'`. A second ready update changes nothing. The bucket then holds one object for that job.

`GetExport` requires a viewer of the job's document. When the state is `ready` it presigns `GetObject` for 60 seconds. The URL is not stored. A member who is revoked after the URL was issued can fetch the object until the signature expires.

The worker reads the canonical blob with `GetBlob` and calls `render.Text`. Markdown is that text plus one trailing newline. DOCX and PDF use `render.DOCX` and `render.PDF`. A rune outside WinAnsi fails the job and writes no object.

## Alternatives rejected

- **A Redis stream as the queue.** The caller already polls the row. The stream can disagree with it.
- **Multipart upload.** A dead worker would leave parts. One `PutObject` of a finished temp file does not.
- **Proxying the file through export-service.** The presign window is the cost of handing the client a URL. The window is 60 seconds.
- **Rendering whatever `Head` is at worker time.** The version is pinned at request time so a retry matches the blob the caller asked for after head has moved.

## Consequences

A crash waits out the remainder of the 30 second lease before another process claims the row. Five render attempts that die are followed by a failed row. The presigned URL outlives a revoke by up to 60 seconds. `GetVersion` staying user-only is why the row stores the content hash next to the node id. Unit 6's encrypted documents hit the CSS2 refusal and produce no job.
