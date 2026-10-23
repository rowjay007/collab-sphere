ALTER TABLE version_docs ADD COLUMN system_replica bigint;

ALTER TABLE mutations
    ADD COLUMN source_doc uuid,
    ADD COLUMN appended integer NOT NULL DEFAULT 0,
    ADD COLUMN noop boolean NOT NULL DEFAULT false;
