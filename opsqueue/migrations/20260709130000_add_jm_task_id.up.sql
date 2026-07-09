ALTER TABLE submissions ADD COLUMN jm_task_id TEXT;
ALTER TABLE submissions_completed ADD COLUMN jm_task_id TEXT;
ALTER TABLE submissions_failed ADD COLUMN jm_task_id TEXT;
ALTER TABLE submissions_cancelled ADD COLUMN jm_task_id TEXT;
