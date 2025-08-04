-- 1. Drop all indexes and finally the column itself
DROP INDEX random_chunks_order;
DROP INDEX random_chunks_metadata_order;
DROP INDEX random_chunks_metadata_order2;
ALTER TABLE chunks DROP COLUMN random_order;
ALTER TABLE chunks_metadata DROP COLUMN random_order;

-- 2. Recreate the column with its new, proper definition
--
-- Compared to the OG definition, we ensure that the top 42 bits of `submission_id`
-- which contain the timestamp part of the snowflake,
-- always participate in the `random_order`,
-- since the lower 22 bits are likely to be `0` except when under peak load.
ALTER TABLE chunks ADD COLUMN random_order INTEGER NOT NULL GENERATED ALWAYS AS (
    (((submission_id + (submission_id >> 22) + chunk_index) % 65536) * 40503) % 65536
    ) VIRTUAL;
ALTER TABLE chunks_metadata ADD COLUMN random_order INTEGER NOT NULL GENERATED ALWAYS AS (
    (((submission_id + (submission_id >> 22) + chunk_index) % 65536) * 40503) % 65536
) VIRTUAL;


-- 3. Recreate all dropped indexes
CREATE INDEX random_chunks_order ON chunks (
      random_order
    , submission_id
    , chunk_index
);

CREATE INDEX random_chunks_metadata_order ON chunks_metadata (
      metadata_key
    , metadata_value
    , random_order
    , submission_id
    , chunk_index
);

CREATE INDEX random_chunks_metadata_order2 ON chunks_metadata (
      metadata_key
    , random_order
    , metadata_value
    , submission_id
    , chunk_index
);
