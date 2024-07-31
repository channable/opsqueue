CREATE TABLE chunks
(
    submission_id INTEGER NOT NULL,
    id INTEGER NOT NULL,
    input_content BLOB NOT NULL,
    retries INTEGER NOT NULL DEFAULT 0,

    PRIMARY KEY (submission_id, id)
) STRICT;

CREATE TABLE chunks_completed
(
    submission_id INTEGER NOT NULL,
    id INTEGER NOT NULL,
    output_content BLOB NOT NULL,
    completed_at DATETIME NOT NULL, -- Unix Timestamp

    PRIMARY KEY (submission_id, id)
);


CREATE TABLE chunks_failed
(
    submission_id INTEGER NOT NULL,
    id INTEGER NOT NULL,
    input_content BLOB NOT NULL,
    failure BLOB NOT NULL DEFAULT '',
    failed_at DATETIME NOT NULL, -- Unix Timestamp
    skipped BOOL NOT NULL DEFAULT false,

    PRIMARY KEY (submission_id, id)
);
