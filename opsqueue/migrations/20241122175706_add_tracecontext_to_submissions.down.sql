ALTER TABLE submissions DROP COLUMN otel_trace_carrier;
ALTER TABLE submissions_completed DROP COLUMN otel_trace_carrier;
ALTER TABLE submissions_failed DROP COLUMN otel_trace_carrier;
