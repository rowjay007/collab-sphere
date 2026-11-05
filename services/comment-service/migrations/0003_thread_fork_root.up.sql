ALTER TABLE threads ADD COLUMN fork_root uuid;
CREATE INDEX threads_fork_root ON threads (fork_root);
