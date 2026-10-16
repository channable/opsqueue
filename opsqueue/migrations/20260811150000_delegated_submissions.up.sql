CREATE TABLE delegated_submissions
(
    submission_id    BIGINT NOT NULL UNIQUE,
    external_task_id TEXT   NOT NULL UNIQUE,
    last_status_sent TEXT
);
