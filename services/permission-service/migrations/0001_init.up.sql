CREATE TABLE grants (
    doc_id      uuid NOT NULL,
    user_id     text NOT NULL,
    role        text NOT NULL CHECK (role IN ('owner', 'editor', 'commenter', 'viewer')),
    source_link uuid,
    PRIMARY KEY (doc_id, user_id)
);

CREATE UNIQUE INDEX grants_one_owner ON grants (doc_id) WHERE role = 'owner';

CREATE TABLE share_links (
    id         uuid PRIMARY KEY,
    doc_id     uuid NOT NULL,
    role       text NOT NULL CHECK (role IN ('editor', 'commenter', 'viewer')),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    created_by text NOT NULL
);

CREATE TABLE doc_epochs (
    doc_id uuid NOT NULL,
    epoch  int  NOT NULL,
    state  text NOT NULL CHECK (state IN ('pending', 'open')),
    PRIMARY KEY (doc_id, epoch)
);

CREATE TABLE doc_key_wraps (
    doc_id     uuid NOT NULL,
    epoch      int  NOT NULL,
    user_id    text NOT NULL,
    key_id     uuid NOT NULL,
    eph_pub    bytea NOT NULL,
    nonce      bytea NOT NULL,
    ciphertext bytea NOT NULL,
    PRIMARY KEY (doc_id, epoch, user_id)
);
