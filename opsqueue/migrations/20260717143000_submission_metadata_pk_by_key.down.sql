CREATE TABLE submissions_metadata_old
(
    submission_id INTEGER NOT NULL,
    metadata_key TEXT NOT NULL,
    metadata_value INTEGER NOT NULL,

    PRIMARY KEY (submission_id, metadata_key, metadata_value)
) WITHOUT ROWID, STRICT;

INSERT INTO submissions_metadata_old (submission_id, metadata_key, metadata_value)
SELECT
      submission_id
    , metadata_key
    , metadata_value
FROM submissions_metadata;

DROP INDEX lookup_submission_by_metadata;
DROP TABLE submissions_metadata;
ALTER TABLE submissions_metadata_old RENAME TO submissions_metadata;

CREATE INDEX lookup_submission_by_metadata ON submissions_metadata (
      metadata_key
    , metadata_value
    , submission_id
);
