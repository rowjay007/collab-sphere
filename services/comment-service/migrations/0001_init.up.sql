CREATE TABLE threads (
    id           uuid        PRIMARY KEY,
    home         uuid        NOT NULL,
    created_seq  bigint      NOT NULL CHECK (created_seq >= 0),
    start_client numeric     NOT NULL,
    start_clock  numeric     NOT NULL,
    end_client   numeric     NOT NULL,
    end_clock    numeric     NOT NULL,
    resolved     boolean     NOT NULL DEFAULT false,
    author       text        NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX threads_home_idx ON threads (home);

CREATE TABLE comments (
    id         uuid        PRIMARY KEY,
    thread_id  uuid        NOT NULL REFERENCES threads (id) ON DELETE CASCADE,
    author     text        NOT NULL,
    body       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX comments_thread_idx ON comments (thread_id, created_at, id);

CREATE TABLE mentions (
    comment_id uuid    NOT NULL REFERENCES comments (id) ON DELETE CASCADE,
    username   text    NOT NULL,
    ord        integer NOT NULL,
    PRIMARY KEY (comment_id, ord)
);

CREATE TABLE notifications (
    id         bigserial   PRIMARY KEY,
    username   text        NOT NULL,
    thread_id  uuid        NOT NULL REFERENCES threads (id) ON DELETE CASCADE,
    comment_id uuid        REFERENCES comments (id) ON DELETE CASCADE,
    kind       text        NOT NULL CHECK (kind IN ('mention', 'reply', 'resolved', 'unresolved')),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX notifications_user_idx ON notifications (username, id);

CREATE TABLE requests (
    request_id   text        PRIMARY KEY,
    kind         text        NOT NULL CHECK (kind IN ('create', 'reply', 'set_resolved')),
    fingerprint  text        NOT NULL,
    thread_id    uuid        REFERENCES threads (id) ON DELETE CASCADE,
    comment_id   uuid        REFERENCES comments (id) ON DELETE CASCADE,
    resolved     boolean,
    created_at   timestamptz NOT NULL DEFAULT now()
);
