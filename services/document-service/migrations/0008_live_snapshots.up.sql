ALTER TABLE snapshots ADD COLUMN kind text NOT NULL DEFAULT 'compact';
ALTER TABLE snapshots ADD CONSTRAINT snapshots_kind_check CHECK (kind IN ('compact', 'live'));
ALTER TABLE snapshots DROP CONSTRAINT snapshots_pkey;
ALTER TABLE snapshots ADD PRIMARY KEY (document_id, seq, kind);
CREATE UNIQUE INDEX snapshots_one_live ON snapshots (document_id) WHERE kind = 'live';
