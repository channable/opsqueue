-- Add migration script here
PRAGMA journal_mode="WAL";
PRAGMA synchronous="normal";

CREATE TABLE submissions
(
    id INTEGER PRIMARY KEY NOT NULL,
    chunks_total INTEGER NOT NULL DEFAULT 0,
    chunks_done INTEGER NOT NULL DEFAULT 0,
    metadata BLOB
) STRICT;
