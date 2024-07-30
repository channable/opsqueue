CREATE TABLE chunks
(
    submission_id INTEGER NOT NULL,
    id INTEGER NOT NULL,
    input_content BLOB NOT NULL,
    retries INTEGER NOT NULL,

    PRIMARY KEY (submission_id, id)
) STRICT;

CREATE TABLE chunks_completed
(
    submission_id INTEGER NOT NULL,
    id INTEGER NOT NULL,
    output_content BLOB NOT NULL,
    completed_at INTEGER NOT NULL, -- Unix Timestamp

    PRIMARY KEY (submission_id, id)
) STRICT;


CREATE TABLE chunks_failed
(
    submission_id INTEGER NOT NULL,
    id INTEGER NOT NULL,
    input_content BLOB NOT NULL,
    failure BLOB NOT NULL,
    failed_at INTEGER NOT NULL, -- Unix Timestamp

    PRIMARY KEY (submission_id, id)
) STRICT;
