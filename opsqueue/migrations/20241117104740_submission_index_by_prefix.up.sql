CREATE INDEX submissions_prefix ON submissions (prefix, id);
CREATE INDEX submissions_completed_prefix ON submissions_completed (prefix, id);
CREATE INDEX submissions_failed_prefix ON submissions_failed (prefix, id);
