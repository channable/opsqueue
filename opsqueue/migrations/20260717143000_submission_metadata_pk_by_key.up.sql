CREATE TABLE submissions_metadata_new
(
    submission_id INTEGER NOT NULL,
    metadata_key TEXT NOT NULL,
    metadata_value INTEGER NOT NULL,

    -- Align with StrategicMetadataMap: one value per key per submission.
    PRIMARY KEY (submission_id, metadata_key)
) WITHOUT ROWID, STRICT;

-- If legacy duplicates exist for the same (submission_id, metadata_key),
-- keep one deterministic value.
INSERT INTO submissions_metadata_new (submission_id, metadata_key, metadata_value)
SELECT
      submission_id
    , metadata_key
    , MAX(metadata_value) AS metadata_value
FROM submissions_metadata
GROUP BY submission_id, metadata_key;

DROP INDEX lookup_submission_by_metadata;
DROP TABLE submissions_metadata;
ALTER TABLE submissions_metadata_new RENAME TO submissions_metadata;

CREATE INDEX lookup_submission_by_metadata ON submissions_metadata (
      metadata_key
    , metadata_value
    , submission_id
);
