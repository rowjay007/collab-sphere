ALTER TABLE documents ADD COLUMN fork_of uuid NULL REFERENCES documents (id);
ALTER TABLE documents ADD COLUMN fork_seq bigint NULL CHECK (fork_seq IS NULL OR fork_seq >= 0);
ALTER TABLE documents ADD COLUMN fork_request_id text NULL UNIQUE;
ALTER TABLE documents ADD CONSTRAINT documents_fork_origin_chk CHECK (
    (fork_of IS NULL AND fork_seq IS NULL AND fork_request_id IS NULL)
    OR
    (fork_of IS NOT NULL AND fork_seq IS NOT NULL AND fork_request_id IS NOT NULL)
);
CREATE INDEX documents_updated_at_idx ON documents (updated_at);
