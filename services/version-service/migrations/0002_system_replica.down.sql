ALTER TABLE mutations
    DROP COLUMN source_doc,
    DROP COLUMN appended,
    DROP COLUMN noop;

ALTER TABLE version_docs DROP COLUMN system_replica;
