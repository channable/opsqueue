from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from opsqueue.postgres import (
    PostgresConsumerClient,
    PostgresProducerClient,
    PostgresSubmissionFailedError,
)

from opsqueue import common


@pytest.fixture
def postgres_database() -> Iterator[tuple[str, list[int]]]:
    dsn = os.environ.get("OPSQUEUE_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip(
            "set OPSQUEUE_TEST_POSTGRES_DSN to run PostgreSQL integration tests"
        )

    psycopg = pytest.importorskip("psycopg")
    connection = psycopg.connect(dsn, autocommit=True)
    submission_ids: list[int] = []
    try:
        connection.execute("SELECT opsqueue.count_in_progress()")
        has_submissions = connection.execute(
            "SELECT EXISTS (SELECT 1 FROM opsqueue.submissions)"
        ).fetchone()[0]
        if has_submissions:
            pytest.fail(
                "The PostgreSQL integration-test database must be empty; "
                "use a dedicated database for these tests"
            )
        yield dsn, submission_ids
    finally:
        if submission_ids:
            connection.execute(
                "DELETE FROM opsqueue.submissions WHERE id = ANY(%s)",
                (submission_ids,),
            )
        connection.close()


def test_producer_submission_state_and_metadata(
    postgres_database: tuple[str, list[int]],
) -> None:
    dsn, submission_ids = postgres_database
    with PostgresProducerClient(dsn) as producer:
        submission_id = producer.insert_submission(
            range(4),
            chunk_size=2,
            metadata=b"test-metadata",
            strategic_metadata={"tenant": 73},
            initial_status="paused",
        )
        submission_ids.append(submission_id.id)

        status = producer.get_submission_status(submission_id)
        assert status is not None
        assert status["status"] == "paused"
        assert status["chunks_total"] == 2
        assert status["metadata"] == b"test-metadata"
        assert status["strategic_metadata"] == {"tenant": 73}

        assert producer.unpause_submission(submission_id)
        assert submission_id in producer.lookup_submission_ids_by_strategic_metadata(
            {"tenant": 73}
        )
        assert producer.cancel_submission(submission_id)
        status = producer.get_submission_status(submission_id)
        assert status is not None and status["status"] == "cancelled"


def test_producer_and_consumer_round_trip(
    postgres_database: tuple[str, list[int]],
) -> None:
    dsn, submission_ids = postgres_database
    with PostgresProducerClient(dsn) as producer:
        submission_id = producer.insert_submission(range(6), chunk_size=2)
        submission_ids.append(submission_id.id)

        with PostgresConsumerClient(dsn, lease_seconds=60) as consumer:
            chunks = consumer.reserve_chunks(max=3, strategy="oldest")
            assert len(chunks) == 3
            assert all(chunk.submission_id == submission_id for chunk in chunks)

            for chunk in chunks:
                operations = common.decode_chunk(
                    chunk.input_content, common.DEFAULT_SERIALIZATION_FORMAT
                )
                if chunk.chunk_index.id == 0:
                    assert consumer.renew_chunk(submission_id, chunk.chunk_index)
                output = common.encode_chunk(
                    [value + 1 for value in operations],
                    common.DEFAULT_SERIALIZATION_FORMAT,
                )
                consumer.complete_chunk(
                    chunk.submission_id,
                    chunk.submission_prefix,
                    chunk.chunk_index,
                    output,
                )

        status = producer.get_submission_status(submission_id)
        assert status is not None and status["status"] == "completed"
        results = producer.try_stream_completed_submission_chunks(submission_id)
        decoded = [
            value
            for encoded_chunk in results
            for value in common.decode_chunk(
                encoded_chunk, common.DEFAULT_SERIALIZATION_FORMAT
            )
        ]
        assert decoded == [1, 2, 3, 4, 5, 6]


def test_consumer_failure_is_reported_by_producer(
    postgres_database: tuple[str, list[int]],
) -> None:
    dsn, submission_ids = postgres_database
    with PostgresProducerClient(dsn) as producer:
        submission_id = producer.insert_submission_chunks([b"input"])
        submission_ids.append(submission_id.id)

        with PostgresConsumerClient(dsn, max_retries=1) as consumer:
            [chunk] = consumer.reserve_chunks(strategy="oldest")
            consumer.fail_chunk(
                chunk.submission_id,
                chunk.submission_prefix,
                chunk.chunk_index,
                "intentional test failure",
            )

        with pytest.raises(PostgresSubmissionFailedError) as error:
            list(producer.try_stream_completed_submission_chunks(submission_id))

        assert error.value.submission_id == submission_id
        assert error.value.chunk_index == 0
        assert error.value.failure == "intentional test failure"
