ALTER TABLE replicas ADD COLUMN kind text NOT NULL DEFAULT 'client';
ALTER TABLE replicas ADD CONSTRAINT replicas_kind_check CHECK (kind IN ('client', 'system'));
