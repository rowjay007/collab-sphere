DROP INDEX snapshots_one_live;
ALTER TABLE snapshots DROP CONSTRAINT snapshots_pkey;
ALTER TABLE snapshots ADD PRIMARY KEY (document_id, seq);
ALTER TABLE snapshots DROP CONSTRAINT snapshots_kind_check;
ALTER TABLE snapshots DROP COLUMN kind;
