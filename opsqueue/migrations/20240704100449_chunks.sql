CREATE TABLE chunks
(
    submission_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    input_content BLOB NULL, -- Unset iff only (submission_id, id) + submission's metadata is enough to figure out content of Chunk from object_storage
    retries INTEGER NOT NULL DEFAULT 0,

    PRIMARY KEY (submission_id, chunk_index)
) WITHOUT ROWID, STRICT;

CREATE TABLE chunks_completed
(
    submission_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    output_content BLOB NULL, -- Unset iff only (submission_id, id) + submission's metadata is enough to figure out content of Chunk from object_storage
    completed_at DATETIME NOT NULL, -- Unix Timestamp

    PRIMARY KEY (submission_id, chunk_index)
) WITHOUT ROWID;


CREATE TABLE chunks_failed
(
    submission_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    input_content BLOB NULL,
    failure BLOB NOT NULL DEFAULT '',
    failed_at DATETIME NOT NULL, -- Unix Timestamp
    skipped BOOL NOT NULL DEFAULT false,

    PRIMARY KEY (submission_id, chunk_index)
) WITHOUT ROWID;
