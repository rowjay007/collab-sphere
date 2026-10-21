DROP INDEX documents_updated_at_idx;
ALTER TABLE documents DROP CONSTRAINT documents_fork_origin_chk;
ALTER TABLE documents DROP COLUMN fork_request_id;
ALTER TABLE documents DROP COLUMN fork_seq;
ALTER TABLE documents DROP COLUMN fork_of;
