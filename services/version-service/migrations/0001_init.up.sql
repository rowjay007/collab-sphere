CREATE TABLE blobs (
    hash  bytea PRIMARY KEY CHECK (length(hash) = 32),
    bytes bytea NOT NULL
);

CREATE TABLE nodes (
    id           bytea PRIMARY KEY CHECK (length(id) = 32),
    doc          uuid NOT NULL,
    seq          bigint NOT NULL CHECK (seq >= 0),
    content_hash bytea NOT NULL REFERENCES blobs (hash),
    meta         jsonb NOT NULL DEFAULT '{}',
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (doc, seq)
);

CREATE TABLE node_parents (
    child      bytea NOT NULL REFERENCES nodes (id),
    ord        smallint NOT NULL CHECK (ord >= 0),
    parent     bytea NOT NULL REFERENCES nodes (id),
    parent_doc uuid NOT NULL,
    parent_seq bigint NOT NULL,
    PRIMARY KEY (child, ord)
);

CREATE TABLE version_docs (
    doc     uuid PRIMARY KEY,
    tip_seq bigint NULL,
    tip     bytea NULL REFERENCES nodes (id),
    CHECK ((tip IS NULL) = (tip_seq IS NULL))
);

CREATE TABLE checkpoints (
    doc        uuid NOT NULL,
    name       text NOT NULL,
    node       bytea NOT NULL REFERENCES nodes (id),
    request_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (doc, name)
);

CREATE TABLE mutations (
    request_id  text PRIMARY KEY,
    doc         uuid NOT NULL,
    kind        text NOT NULL CHECK (kind IN ('merge', 'restore')),
    state       text NOT NULL CHECK (state IN ('planned', 'appended', 'done')),
    source_node bytea NULL REFERENCES nodes (id),
    target_node bytea NULL REFERENCES nodes (id),
    plan        bytea NULL,
    result_node bytea NULL REFERENCES nodes (id),
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX mutations_one_in_flight_idx ON mutations (doc) WHERE state <> 'done';
