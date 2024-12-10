ALTER TABLE submissions ADD COLUMN otel_trace_carrier TEXT NOT NULL DEFAULT "{}";
ALTER TABLE submissions_completed ADD COLUMN otel_trace_carrier TEXT NOT NULL DEFAULT "{}";
ALTER TABLE submissions_failed ADD COLUMN otel_trace_carrier TEXT NOT NULL DEFAULT "{}";
