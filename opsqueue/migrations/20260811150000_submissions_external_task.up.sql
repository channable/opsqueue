CREATE TABLE submissions_external_task
(
    submission_id    BIGINT NOT NULL UNIQUE,
    task_id          TEXT   NOT NULL UNIQUE,
    last_status_sent TEXT
);
