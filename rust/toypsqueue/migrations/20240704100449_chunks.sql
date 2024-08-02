-- Add migration script here
CREATE TABLE chunks
(
    submission_id INTEGER NOT NULL,
    id INTEGER NOT NULL,
    uri BLOB NOT NULL DEFAULT "",

    PRIMARY KEY (submission_id, id)
) STRICT;
