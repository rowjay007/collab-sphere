CREATE TABLE documents (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    title        text        NOT NULL,
    owner_id     text        NOT NULL,
    status       text        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'archived')),
    op_seq       bigint      NOT NULL DEFAULT 0 CHECK (op_seq >= 0),
    snapshot_seq bigint      NOT NULL DEFAULT 0 CHECK (snapshot_seq >= 0 AND snapshot_seq <= op_seq),
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE snapshots (
    document_id uuid        NOT NULL REFERENCES documents (id) ON DELETE CASCADE,
    seq         bigint      NOT NULL,
    state       jsonb       NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (document_id, seq)
);

CREATE TABLE operations (
    document_id uuid    NOT NULL REFERENCES documents (id) ON DELETE CASCADE,
    seq         bigint  NOT NULL,
    op_client   numeric NOT NULL,
    op_clock    numeric NOT NULL,
    op          bytea   NOT NULL,
    PRIMARY KEY (document_id, seq),
    UNIQUE (document_id, op_client, op_clock)
);
