CREATE TABLE submissions_cancelled
(
    id BIGINT PRIMARY KEY NOT NULL,
    prefix TEXT,
    chunks_total INTEGER NOT NULL DEFAULT 0,
    chunks_done INTEGER NOT NULL DEFAULT 0,
    metadata BLOB,
    cancelled_at DATETIME NOT NULL -- Unix Timestamp
);
