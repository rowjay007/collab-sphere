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
