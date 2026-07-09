CREATE INDEX submissions_by_jm_task_id ON submissions (jm_task_id) WHERE jm_task_id IS NOT NULL;
CREATE INDEX submissions_completed_by_jm_task_id ON submissions_completed (jm_task_id) WHERE jm_task_id IS NOT NULL;
CREATE INDEX submissions_failed_by_jm_task_id ON submissions_failed (jm_task_id) WHERE jm_task_id IS NOT NULL;
CREATE INDEX submissions_cancelled_by_jm_task_id ON submissions_cancelled (jm_task_id) WHERE jm_task_id IS NOT NULL;
