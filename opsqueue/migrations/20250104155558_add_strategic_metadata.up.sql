-- Keep track of metadata at submission level:
CREATE TABLE submissions_metadata
(
    submission_id INTEGER NOT NULL,
    metadata_key TEXT NOT NULL,

    -- We only support 64-bit integers as values,
    -- for efficiency and reduced storage.
    --
    -- Since we do not depend on this value for ordering (Ord) but only for filtering (Eq),
    -- it's okay to transform a u64 into a i64 before saving,
    -- so we can truly accept the full 64 bit range here.
    metadata_value INTEGER NOT NULL,

    -- NOTE: We don't set a foreign key here
    -- because the submission_id might point to
    -- a record in one of multiple tables
    -- (and we want to keep the metadata available also for completed/failed submissions)

    -- Quickly select all metadata of a particular submission
    PRIMARY KEY (submission_id, metadata_key, metadata_value)
) WITHOUT ROWID, STRICT;

-- Quickly select all submissions matching certain metadata
CREATE INDEX lookup_submission_by_metadata ON submissions_metadata (
      metadata_key
    , metadata_value
    , submission_id
);

-- Keep track of strategic metadata at the chunk level.
--
-- In essence this is a manually-maintained index
-- of the cartesian product `submissions_metadata` × `chunks`
--
-- The goal of this index is to support fast random order indexing
-- i.e. within a given `metadata_key/metadata_val`
-- select chunks (of all matching submissions) in random order
CREATE TABLE chunks_metadata
(
    submission_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    metadata_key TEXT NOT NULL,

    metadata_value INTEGER NOT NULL,

    -- auto-delete when chunk completes or fails:
    FOREIGN KEY (submission_id, chunk_index) REFERENCES chunks ON DELETE CASCADE,

    PRIMARY KEY (submission_id, chunk_index, metadata_key, metadata_value)
) WITHOUT ROWID, STRICT;

ALTER TABLE chunks_metadata ADD COLUMN random_order INTEGER NOT NULL GENERATED ALWAYS AS (
    (((submission_id + chunk_index) % 65536) * 40503) % 65536
) VIRTUAL;

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
