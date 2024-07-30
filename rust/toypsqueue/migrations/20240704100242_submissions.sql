-- Add migration script here
CREATE TABLE submissions
(
    id INTEGER PRIMARY KEY NOT NULL,
    chunks_total INTEGER NOT NULL DEFAULT 0,
    chunks_done INTEGER NOT NULL DEFAULT 0,
    metadata BLOB
) STRICT;

CREATE TABLE submissions_completed
(
    id INTEGER PRIMARY KEY NOT NULL,
    chunks_total INTEGER NOT NULL DEFAULT 0,
    metadata BLOB,
    completed_at INTEGER NOT NULL -- Unix Timestamp
) STRICT;

CREATE TABLE submissions_failed
(
    id INTEGER PRIMARY KEY NOT NULL,
    chunks_total INTEGER NOT NULL DEFAULT 0,
    metadata BLOB,
    failed_chunk_id INTEGER NOT NULL, -- NOTE: There might in rare cases be multiple concurrent failures, but only one of them will be 'first' (ordered by who gets write access to the submissions table lock first), which is the one stored here.
    failed_at INTEGER NOT NULL -- Unix Timestamp
) STRICT;
