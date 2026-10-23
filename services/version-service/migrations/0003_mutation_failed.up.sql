ALTER TABLE mutations DROP CONSTRAINT mutations_state_check;
ALTER TABLE mutations ADD CONSTRAINT mutations_state_check CHECK (state IN ('planned', 'appended', 'done', 'failed'));
DROP INDEX mutations_one_in_flight_idx;
CREATE UNIQUE INDEX mutations_one_in_flight_idx ON mutations (doc) WHERE state IN ('planned', 'appended');
