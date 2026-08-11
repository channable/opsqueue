CREATE TABLE submissions_paused
(
    id BIGINT PRIMARY KEY NOT NULL,
    prefix TEXT,
    chunks_total INTEGER NOT NULL DEFAULT 0,
    chunks_done INTEGER NOT NULL DEFAULT 0,
    metadata BLOB,
    otel_trace_carrier TEXT NOT NULL DEFAULT '{}',
    chunk_size INTEGER
);

CREATE INDEX submissions_paused_prefix ON submissions_paused (prefix, id);

CREATE TABLE chunks_paused
(
    submission_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    input_content BLOB NULL,
    retries INTEGER NOT NULL DEFAULT 0,

    PRIMARY KEY (submission_id, chunk_index)
) WITHOUT ROWID, STRICT;
