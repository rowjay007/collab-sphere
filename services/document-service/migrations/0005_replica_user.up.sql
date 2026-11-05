ALTER TABLE replicas ADD COLUMN user_id text;
ALTER TABLE replicas ADD CONSTRAINT replicas_system_user CHECK (kind <> 'system' OR user_id IS NULL);
