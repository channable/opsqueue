CREATE SCHEMA IF NOT EXISTS opsqueue;

CREATE TABLE opsqueue.submissions (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    prefix text,
    chunks_total bigint NOT NULL CHECK (chunks_total >= 0),
    chunks_done bigint NOT NULL DEFAULT 0 CHECK (chunks_done >= 0),
    chunk_size bigint NOT NULL DEFAULT 1 CHECK (chunk_size > 0),
    metadata bytea,
    strategic_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL CHECK (status IN ('in_progress', 'paused', 'completed', 'failed', 'cancelled')),
    failed_chunk_index bigint,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    failed_at timestamptz,
    cancelled_at timestamptz
);

CREATE INDEX submissions_prefix_id ON opsqueue.submissions (prefix, id);
CREATE INDEX submissions_status_id ON opsqueue.submissions (status, id);

CREATE TABLE opsqueue.chunks (
    submission_id bigint NOT NULL REFERENCES opsqueue.submissions(id) ON DELETE CASCADE,
    chunk_index bigint NOT NULL CHECK (chunk_index >= 0),
    input_content bytea,
    random_order integer GENERATED ALWAYS AS (
        (((((submission_id % 65536) + ((submission_id >> 22) % 65536)
            + (chunk_index % 65536)) % 65536) * 40503) % 65536)
    ) STORED,
    output_content bytea,
    retries integer NOT NULL DEFAULT 0 CHECK (retries >= 0),
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'reserved', 'completed', 'failed', 'skipped')),
    reserved_by uuid,
    reserved_until timestamptz,
    failure text,
    completed_at timestamptz,
    failed_at timestamptz,
    PRIMARY KEY (submission_id, chunk_index),
    CHECK (
        (status = 'reserved' AND reserved_by IS NOT NULL AND reserved_until IS NOT NULL)
        OR (status <> 'reserved' AND reserved_by IS NULL AND reserved_until IS NULL)
    )
);

CREATE INDEX chunks_pending_order ON opsqueue.chunks (submission_id, chunk_index) WHERE status = 'pending';
CREATE INDEX chunks_expired_reservations ON opsqueue.chunks (reserved_until) WHERE status = 'reserved';
CREATE INDEX chunks_random_order ON opsqueue.chunks (random_order, submission_id, chunk_index);

CREATE TABLE opsqueue.chunk_failures (
    submission_id bigint NOT NULL,
    chunk_index bigint NOT NULL,
    retry_number integer NOT NULL,
    failure text NOT NULL,
    failed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (submission_id, chunk_index, retry_number),
    FOREIGN KEY (submission_id, chunk_index)
        REFERENCES opsqueue.chunks(submission_id, chunk_index) ON DELETE CASCADE
);

CREATE FUNCTION opsqueue.submit(
    p_chunk_count bigint,
    p_prefix text DEFAULT NULL,
    p_chunk_size bigint DEFAULT 1,
    p_metadata bytea DEFAULT NULL,
    p_strategic_metadata jsonb DEFAULT '{}'::jsonb,
    p_initial_status text DEFAULT 'in_progress',
    p_chunk_contents bytea[] DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    v_id bigint;
    v_status text;
BEGIN
    IF p_chunk_count IS NULL OR p_chunk_size IS NULL OR p_initial_status IS NULL
       OR p_chunk_count < 0 OR p_chunk_size <= 0 THEN
        RAISE EXCEPTION 'chunk_count must be non-negative and chunk_size must be positive';
    END IF;
    IF p_initial_status NOT IN ('in_progress', 'paused') THEN
        RAISE EXCEPTION 'initial_status must be in_progress or paused';
    END IF;
    IF p_chunk_contents IS NOT NULL AND cardinality(p_chunk_contents) <> p_chunk_count THEN
        RAISE EXCEPTION 'chunk_contents length must match chunk_count';
    END IF;

    v_status := CASE
        WHEN p_chunk_count = 0 THEN 'completed'
        WHEN p_initial_status = 'paused' THEN 'paused'
        ELSE 'in_progress'
    END;

    INSERT INTO opsqueue.submissions (
        prefix, chunks_total, chunk_size, metadata, strategic_metadata, status, completed_at
    ) VALUES (
        p_prefix, p_chunk_count, p_chunk_size, p_metadata, coalesce(p_strategic_metadata, '{}'::jsonb),
        v_status, CASE WHEN v_status = 'completed' THEN clock_timestamp() END
    ) RETURNING id INTO v_id;

    IF p_chunk_count > 0 THEN
        INSERT INTO opsqueue.chunks (submission_id, chunk_index, input_content)
        SELECT v_id, generated.chunk_index, p_chunk_contents[generated.chunk_index + 1]
        FROM generate_series(0, p_chunk_count - 1) AS generated(chunk_index);
    END IF;

    RETURN v_id;
END;
$$;

CREATE FUNCTION opsqueue._reserve_random_window(
    p_worker_id uuid,
    p_limit integer,
    p_lease interval,
    p_random_from integer,
    p_random_to integer
) RETURNS TABLE (
    submission_id bigint,
    chunk_index bigint,
    input_content bytea,
    retries integer,
    prefix text,
    chunk_size bigint,
    metadata bytea,
    strategic_metadata jsonb
)
LANGUAGE sql
AS $$
    WITH candidates AS MATERIALIZED (
        SELECT c.submission_id, c.chunk_index, s.prefix, s.chunk_size, s.metadata,
               s.strategic_metadata, c.input_content
        FROM opsqueue.chunks AS c
        JOIN opsqueue.submissions AS s ON s.id = c.submission_id
        WHERE s.status = 'in_progress'
          AND (c.status = 'pending' OR (c.status = 'reserved' AND c.reserved_until <= clock_timestamp()))
          AND c.random_order >= p_random_from AND c.random_order < p_random_to
        ORDER BY c.random_order, c.submission_id, c.chunk_index
        LIMIT p_limit
        FOR UPDATE OF c SKIP LOCKED
    ), reserved AS (
        UPDATE opsqueue.chunks AS c
        SET status = 'reserved', reserved_by = p_worker_id,
            reserved_until = clock_timestamp() + p_lease
        FROM candidates AS x
        WHERE c.submission_id = x.submission_id AND c.chunk_index = x.chunk_index
          AND (c.status = 'pending' OR (c.status = 'reserved' AND c.reserved_until <= clock_timestamp()))
          AND EXISTS (
              SELECT 1 FROM opsqueue.submissions AS s
              WHERE s.id = c.submission_id AND s.status = 'in_progress'
          )
        RETURNING c.submission_id, c.chunk_index, c.input_content, c.retries
    )
    SELECT r.submission_id, r.chunk_index, r.input_content, r.retries,
           x.prefix, x.chunk_size, x.metadata, x.strategic_metadata
    FROM reserved AS r
    JOIN candidates AS x USING (submission_id, chunk_index)
$$;

CREATE FUNCTION opsqueue.reserve(
    p_worker_id uuid,
    p_limit integer,
    p_lease interval DEFAULT interval '30 seconds',
    p_strategy text DEFAULT 'oldest',
    p_prefer_distinct_keys text[] DEFAULT NULL,
    p_underlying_strategy text DEFAULT 'oldest'
) RETURNS TABLE (
    submission_id bigint,
    chunk_index bigint,
    input_content bytea,
    retries integer,
    prefix text,
    chunk_size bigint,
    metadata bytea,
    strategic_metadata jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_random_offset integer;
    v_reserved integer;
BEGIN
    IF p_worker_id IS NULL OR p_limit IS NULL OR p_lease IS NULL
       OR p_limit <= 0 OR p_lease <= interval '0 seconds' THEN
        RAISE EXCEPTION 'worker_id, positive limit, and positive lease are required';
    END IF;
    IF p_strategy IS NULL OR p_strategy NOT IN ('oldest', 'newest', 'random', 'prefer_distinct') THEN
        RAISE EXCEPTION 'strategy must be oldest, newest, random, or prefer_distinct';
    END IF;
    IF p_underlying_strategy IS NULL OR p_underlying_strategy NOT IN ('oldest', 'newest', 'random') THEN
        RAISE EXCEPTION 'underlying_strategy must be oldest, newest, or random';
    END IF;
    IF p_strategy = 'prefer_distinct'
       AND (p_prefer_distinct_keys IS NULL OR cardinality(p_prefer_distinct_keys) = 0) THEN
        RAISE EXCEPTION 'prefer_distinct requires at least one metadata key';
    END IF;

    IF p_strategy = 'random' THEN
        v_random_offset := floor(random() * 65536)::integer;
        RETURN QUERY
        SELECT * FROM opsqueue._reserve_random_window(
            p_worker_id, p_limit, p_lease, v_random_offset, 65536
        );
        GET DIAGNOSTICS v_reserved = ROW_COUNT;
        IF v_reserved < p_limit THEN
            RETURN QUERY
            SELECT * FROM opsqueue._reserve_random_window(
                p_worker_id, p_limit - v_reserved, p_lease, 0, v_random_offset
            );
        END IF;
        RETURN;
    END IF;

    RETURN QUERY
    WITH candidates AS MATERIALIZED (
        SELECT c.submission_id, c.chunk_index, s.prefix, s.chunk_size, s.metadata,
               s.strategic_metadata, c.input_content
        FROM opsqueue.chunks AS c
        JOIN opsqueue.submissions AS s ON s.id = c.submission_id
        WHERE s.status = 'in_progress'
          AND (c.status = 'pending' OR (c.status = 'reserved' AND c.reserved_until <= clock_timestamp()))
        ORDER BY
            CASE WHEN p_strategy = 'prefer_distinct' THEN ARRAY(
                SELECT CASE WHEN s.strategic_metadata ? metadata_key
                    THEN (
                        SELECT count(*)
                        FROM opsqueue.chunks AS active_chunk
                        JOIN opsqueue.submissions AS active_submission
                          ON active_submission.id = active_chunk.submission_id
                        WHERE active_chunk.status = 'reserved'
                          AND active_chunk.reserved_until > clock_timestamp()
                          AND active_submission.strategic_metadata ? metadata_key
                          AND active_submission.strategic_metadata -> metadata_key
                              = s.strategic_metadata -> metadata_key
                    )
                    ELSE -1
                END
                FROM unnest(p_prefer_distinct_keys) WITH ORDINALITY AS keys(metadata_key, key_order)
                ORDER BY key_order
            ) END ASC,
            CASE WHEN p_strategy = 'newest'
                OR (p_strategy = 'prefer_distinct' AND p_underlying_strategy = 'newest')
                THEN c.submission_id END DESC,
            CASE WHEN p_strategy = 'oldest'
                OR (p_strategy = 'prefer_distinct' AND p_underlying_strategy = 'oldest')
                THEN c.submission_id END ASC,
            CASE WHEN p_strategy = 'prefer_distinct' AND p_underlying_strategy = 'random'
                THEN c.random_order END,
            c.chunk_index
        LIMIT p_limit
        FOR UPDATE OF c SKIP LOCKED
    ), reserved AS (
        UPDATE opsqueue.chunks AS c
        SET status = 'reserved', reserved_by = p_worker_id,
            reserved_until = clock_timestamp() + p_lease
        FROM candidates AS x
        WHERE c.submission_id = x.submission_id AND c.chunk_index = x.chunk_index
          AND (c.status = 'pending' OR (c.status = 'reserved' AND c.reserved_until <= clock_timestamp()))
          AND EXISTS (
              SELECT 1 FROM opsqueue.submissions AS s
              WHERE s.id = c.submission_id AND s.status = 'in_progress'
          )
        RETURNING c.submission_id, c.chunk_index, c.input_content, c.retries
    )
    SELECT r.submission_id, r.chunk_index, r.input_content, r.retries,
           x.prefix, x.chunk_size, x.metadata, x.strategic_metadata
    FROM reserved AS r
    JOIN candidates AS x USING (submission_id, chunk_index);
END;
$$;

CREATE FUNCTION opsqueue.renew(
    p_worker_id uuid,
    p_submission_id bigint,
    p_chunk_index bigint,
    p_lease interval DEFAULT interval '30 seconds'
) RETURNS boolean
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE opsqueue.chunks
    SET reserved_until = clock_timestamp() + p_lease
    WHERE submission_id = p_submission_id AND chunk_index = p_chunk_index
      AND status = 'reserved' AND reserved_by = p_worker_id
      AND reserved_until > clock_timestamp()
      AND p_lease > interval '0 seconds';
    RETURN FOUND;
END;
$$;

CREATE FUNCTION opsqueue.complete(
    p_worker_id uuid,
    p_submission_id bigint,
    p_chunk_index bigint,
    p_output_content bytea DEFAULT NULL
) RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    v_total bigint;
    v_done bigint;
BEGIN
    SELECT chunks_total, chunks_done INTO v_total, v_done
    FROM opsqueue.submissions
    WHERE id = p_submission_id AND status = 'in_progress'
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    UPDATE opsqueue.chunks
    SET status = 'completed', output_content = p_output_content,
        completed_at = clock_timestamp(), reserved_by = NULL, reserved_until = NULL
    WHERE submission_id = p_submission_id AND chunk_index = p_chunk_index
      AND status = 'reserved' AND reserved_by = p_worker_id
      AND reserved_until > clock_timestamp();
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    v_done := v_done + 1;
    UPDATE opsqueue.submissions
    SET chunks_done = v_done,
        status = CASE WHEN v_done = v_total THEN 'completed' ELSE status END,
        completed_at = CASE WHEN v_done = v_total THEN clock_timestamp() ELSE completed_at END
    WHERE id = p_submission_id;
    RETURN true;
END;
$$;

CREATE FUNCTION opsqueue.fail(
    p_worker_id uuid,
    p_submission_id bigint,
    p_chunk_index bigint,
    p_failure text,
    p_max_retries integer DEFAULT 10
) RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    v_retries integer;
BEGIN
    IF p_max_retries IS NULL OR p_max_retries <= 0 THEN
        RAISE EXCEPTION 'max_retries must be positive';
    END IF;

    PERFORM 1 FROM opsqueue.submissions
    WHERE id = p_submission_id AND status = 'in_progress'
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    UPDATE opsqueue.chunks
    SET retries = retries + 1, failure = p_failure,
        status = CASE WHEN retries + 1 >= p_max_retries THEN 'failed' ELSE 'pending' END,
        failed_at = CASE WHEN retries + 1 >= p_max_retries THEN clock_timestamp() ELSE failed_at END,
        reserved_by = NULL, reserved_until = NULL
    WHERE submission_id = p_submission_id AND chunk_index = p_chunk_index
      AND status = 'reserved' AND reserved_by = p_worker_id
      AND reserved_until > clock_timestamp()
    RETURNING retries INTO v_retries;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    INSERT INTO opsqueue.chunk_failures (submission_id, chunk_index, retry_number, failure)
    VALUES (p_submission_id, p_chunk_index, v_retries, coalesce(p_failure, ''));

    IF v_retries >= p_max_retries THEN
        UPDATE opsqueue.submissions
        SET status = 'failed', failed_chunk_index = p_chunk_index, failed_at = clock_timestamp()
        WHERE id = p_submission_id;
        UPDATE opsqueue.chunks
        SET status = 'skipped', reserved_by = NULL, reserved_until = NULL
        WHERE submission_id = p_submission_id AND status IN ('pending', 'reserved')
          AND chunk_index <> p_chunk_index;
    END IF;
    RETURN true;
END;
$$;

CREATE FUNCTION opsqueue.cancel(p_submission_id bigint) RETURNS boolean
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE opsqueue.submissions
    SET status = 'cancelled', cancelled_at = clock_timestamp()
    WHERE id = p_submission_id AND status IN ('in_progress', 'paused');
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    UPDATE opsqueue.chunks
    SET status = 'skipped', reserved_by = NULL, reserved_until = NULL
    WHERE submission_id = p_submission_id AND status IN ('pending', 'reserved');
    RETURN true;
END;
$$;

CREATE FUNCTION opsqueue.unpause(p_submission_id bigint) RETURNS boolean
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE opsqueue.submissions
    SET status = CASE WHEN chunks_total = 0 THEN 'completed' ELSE 'in_progress' END,
        completed_at = CASE WHEN chunks_total = 0 THEN clock_timestamp() ELSE completed_at END
    WHERE id = p_submission_id AND status = 'paused';
    RETURN FOUND;
END;
$$;

CREATE FUNCTION opsqueue.status(p_submission_id bigint)
RETURNS TABLE (
    id bigint,
    prefix text,
    chunks_total bigint,
    chunks_done bigint,
    chunk_size bigint,
    metadata bytea,
    strategic_metadata jsonb,
    status text,
    failed_chunk_index bigint,
    created_at timestamptz,
    completed_at timestamptz,
    failed_at timestamptz,
    cancelled_at timestamptz
)
LANGUAGE sql STABLE
AS $$
    SELECT s.id, s.prefix, s.chunks_total, s.chunks_done, s.chunk_size, s.metadata,
           s.strategic_metadata, s.status, s.failed_chunk_index, s.created_at,
           s.completed_at, s.failed_at, s.cancelled_at
    FROM opsqueue.submissions AS s
    WHERE s.id = p_submission_id
$$;

CREATE FUNCTION opsqueue.results(p_submission_id bigint)
RETURNS TABLE (chunk_index bigint, output_content bytea)
LANGUAGE sql STABLE
AS $$
    SELECT c.chunk_index, c.output_content
    FROM opsqueue.chunks AS c
    WHERE c.submission_id = p_submission_id AND c.status = 'completed'
    ORDER BY c.chunk_index
$$;

CREATE FUNCTION opsqueue.failure_history(p_submission_id bigint)
RETURNS TABLE (chunk_index bigint, retry_number integer, failure text, failed_at timestamptz)
LANGUAGE sql STABLE
AS $$
    SELECT f.chunk_index, f.retry_number, f.failure, f.failed_at
    FROM opsqueue.chunk_failures AS f
    WHERE f.submission_id = p_submission_id
    ORDER BY f.chunk_index, f.retry_number
$$;

CREATE FUNCTION opsqueue.lookup_by_prefix(p_prefix text) RETURNS bigint
LANGUAGE sql STABLE
AS $$
    SELECT id FROM opsqueue.submissions WHERE prefix = p_prefix ORDER BY id DESC LIMIT 1
$$;

CREATE FUNCTION opsqueue.lookup_by_metadata(p_metadata jsonb, p_limit integer DEFAULT 1000)
RETURNS SETOF bigint
LANGUAGE sql STABLE
AS $$
    SELECT id FROM opsqueue.submissions
    WHERE strategic_metadata @> coalesce(p_metadata, '{}'::jsonb)
    ORDER BY id DESC
    LIMIT greatest(p_limit, 0)
$$;

CREATE FUNCTION opsqueue.count_in_progress() RETURNS bigint
LANGUAGE sql STABLE
AS $$
    SELECT count(*) FROM opsqueue.submissions WHERE status = 'in_progress'
$$;

CREATE FUNCTION opsqueue.count_completed() RETURNS bigint
LANGUAGE sql STABLE
AS $$
    SELECT count(*) FROM opsqueue.submissions WHERE status = 'completed'
$$;
