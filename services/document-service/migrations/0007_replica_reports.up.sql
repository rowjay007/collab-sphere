ALTER TABLE documents ADD COLUMN vector jsonb;

CREATE TABLE replica_reports (
    fork_root    uuid           NOT NULL,
    replica_id   numeric(20, 0) NOT NULL,
    vector       jsonb          NOT NULL,
    home_seq     bigint         NOT NULL CHECK (home_seq >= 0),
    reported_at  timestamptz    NOT NULL,
    released_at  timestamptz,
    PRIMARY KEY (fork_root, replica_id)
);
