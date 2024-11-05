-- The 16-bit fibonacci hash of `submission_id + chunk_index`.
-- https://en.wikipedia.org/wiki/Hash_function#Fibonacci_hashing
--
-- Since SQLite doesn't do wrapping arithmetic and all integers are i64s,
-- we use 16 bit modular arithmetic,
-- which ensures we never overflow the 63 bit range with the multiplication
--
-- This is a virtual column which means it is generated on the fly when looking at the table itself
-- (and it occupies no space in the table).
--
-- But we create an index on it, where it _will_ be stored, to allow fast ORDER BY and binary search.
ALTER TABLE chunks ADD COLUMN random_order INTEGER NOT NULL GENERATED ALWAYS AS (
    (((submission_id + chunk_index) % 65536) * 40503) % 65536
    ) VIRTUAL;

CREATE INDEX random_chunks_order ON chunks (
      random_order
    , submission_id
    , chunk_index
);
