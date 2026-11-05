CREATE TABLE user_keys (
    key_id     uuid PRIMARY KEY,
    user_id    text NOT NULL,
    public_key bytea NOT NULL,
    UNIQUE (user_id, public_key)
);

CREATE TABLE wrap_publishes (
    doc_id     uuid NOT NULL,
    request_id text NOT NULL,
    epoch      int  NOT NULL,
    PRIMARY KEY (doc_id, request_id)
);
