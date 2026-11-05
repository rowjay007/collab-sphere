ALTER TABLE documents
    ADD COLUMN encryption text NOT NULL DEFAULT 'plain' CHECK (encryption IN ('plain', 'encrypted')),
    ADD COLUMN write_epoch integer NOT NULL DEFAULT 0 CHECK (write_epoch >= 0),
    ADD COLUMN epoch_state text NOT NULL DEFAULT '' CHECK (epoch_state IN ('', 'pending', 'open'));
