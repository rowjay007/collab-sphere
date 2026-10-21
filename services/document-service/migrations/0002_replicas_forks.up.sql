ALTER TABLE documents ADD COLUMN fork_root uuid;
UPDATE documents SET fork_root = id;
ALTER TABLE documents ALTER COLUMN fork_root SET NOT NULL;

CREATE SEQUENCE replica_ids AS bigint MINVALUE 1;

CREATE TABLE replicas (
    id          numeric(20, 0) NOT NULL,
    fork_root   uuid           NOT NULL,
    document_id uuid           NOT NULL REFERENCES documents (id) ON DELETE CASCADE,
    created_at  timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (id, fork_root)
);

INSERT INTO replicas (id, fork_root, document_id)
SELECT DISTINCT o.op_client, d.fork_root, o.document_id
FROM operations o
JOIN documents d ON d.id = o.document_id;
