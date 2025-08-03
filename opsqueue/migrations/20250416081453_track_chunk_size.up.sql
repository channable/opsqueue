ALTER TABLE submissions ADD COLUMN chunk_size INTEGER;
ALTER TABLE submissions_completed ADD COLUMN chunk_size INTEGER;
ALTER TABLE submissions_failed ADD COLUMN chunk_size INTEGER;
