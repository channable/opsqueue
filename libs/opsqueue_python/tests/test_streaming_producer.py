import asyncio

import pytest
from conftest import OpsqueueProcess, background_process
from opsqueue.common import DEFAULT_SERIALIZATION_FORMAT, decode_chunk
from opsqueue.consumer import ConsumerClient
from opsqueue.producer import (
    InitialSubmissionStatus,
    StreamingProducerClient,
    SubmissionFailedError,
    SubmissionId,
    SubmissionNotFoundError,
    SubmissionStatus,
)

SUBMISSION_COMPLETED_TIMEOUT = 10.0


def test_async_stream_completed_submission_group(
    opsqueue: OpsqueueProcess,
) -> None:
    url = f"localhost:{opsqueue.port}"
    object_store_url = (
        "file:///tmp/opsqueue/test_async_stream_completed_submission_group"
    )
    producer_client = StreamingProducerClient(url, object_store_url)
    submission_ids = [
        producer_client.insert_submission([1, 2], chunk_size=1),
        producer_client.insert_submission([3, 4], chunk_size=1),
    ]

    consumer_client = ConsumerClient(url, object_store_url)
    expected_submission_ids = [submission_ids[1]] * 2 + [submission_ids[0]] * 2
    for expected_submission_id in expected_submission_ids:
        [chunk] = consumer_client.reserve_chunks()
        assert chunk.submission_id == expected_submission_id
        consumer_client.complete_chunk(
            chunk.submission_id,
            chunk.submission_prefix,
            chunk.chunk_index.id,
            chunk.input_content,
        )

    async def stream_group(ids: list[SubmissionId]) -> list[int]:
        chunks = await producer_client.async_stream_completed_submission_group(ids)
        output: list[int] = []
        async for chunk in chunks:
            output.extend(decode_chunk(chunk, DEFAULT_SERIALIZATION_FORMAT))
        return output

    assert asyncio.run(stream_group(submission_ids)) == [1, 2, 3, 4]
    assert asyncio.run(stream_group([])) == []
    with pytest.raises(SubmissionNotFoundError):
        asyncio.run(stream_group([SubmissionId(0)]))


def test_blocking_stream_completed_submission_group(
    opsqueue: OpsqueueProcess,
) -> None:
    url = f"localhost:{opsqueue.port}"
    object_store_url = (
        "file:///tmp/opsqueue/test_blocking_stream_completed_submission_group"
    )
    producer_client = StreamingProducerClient(url, object_store_url)
    submission_ids = [
        producer_client.insert_submission([1, 2], chunk_size=1),
        producer_client.insert_submission([3, 4], chunk_size=1),
    ]

    consumer_client = ConsumerClient(url, object_store_url)
    expected_submission_ids = [submission_ids[1]] * 2 + [submission_ids[0]] * 2
    for expected_submission_id in expected_submission_ids:
        [chunk] = consumer_client.reserve_chunks()
        assert chunk.submission_id == expected_submission_id
        consumer_client.complete_chunk(
            chunk.submission_id,
            chunk.submission_prefix,
            chunk.chunk_index.id,
            chunk.input_content,
        )

    def stream_group(ids: list[SubmissionId]) -> list[int]:
        output: list[int] = []
        for chunk in producer_client.blocking_stream_completed_submission_group(ids):
            output.extend(decode_chunk(chunk, DEFAULT_SERIALIZATION_FORMAT))
        return output

    assert stream_group(submission_ids) == [1, 2, 3, 4]
    assert stream_group([]) == []
    with pytest.raises(SubmissionNotFoundError):
        stream_group([SubmissionId(0)])


def test_insert_submission_group_creates_all_batches_before_return(
    opsqueue: OpsqueueProcess,
) -> None:
    url = f"localhost:{opsqueue.port}"
    object_store_url = "file:///tmp/opsqueue/test_insert_submission_group"
    producer_client = StreamingProducerClient(url, object_store_url)
    submission_ids = producer_client.insert_submission_group(
        iter([1, 2, 3]),
        submission_size=2,
        chunk_size=1,
        metadata=b"group metadata",
        strategic_metadata={"company_id": 42},
    )

    assert producer_client.count_submissions() == 2
    first_submission_id, second_submission_id = submission_ids
    first_status = producer_client.get_submission_status(first_submission_id)
    assert isinstance(first_status, SubmissionStatus.InProgress)
    assert first_status.submission.chunks_total == 2
    assert first_status.submission.metadata == b"group metadata"
    assert first_status.submission.strategic_metadata == {"company_id": 42}

    second_status = producer_client.get_submission_status(second_submission_id)
    assert isinstance(second_status, SubmissionStatus.InProgress)
    assert second_status.submission.chunks_total == 1
    assert second_status.submission.metadata == b"group metadata"
    assert second_status.submission.strategic_metadata == {"company_id": 42}
    assert (
        producer_client.insert_submission_group([], submission_size=2, chunk_size=1)
        == []
    )
    with pytest.raises(ValueError, match="submission_size"):
        producer_client.insert_submission_group([], submission_size=0, chunk_size=1)

    async def insert_async_group() -> list[SubmissionId]:
        async_submission_ids = await producer_client.async_insert_submission_group(
            [4, 5, 6], submission_size=2, chunk_size=1
        )
        assert producer_client.count_submissions() == 4
        first_id = await anext(async_submission_ids)
        assert producer_client.count_submissions() == 4
        remaining_ids = [submission_id async for submission_id in async_submission_ids]
        return [first_id, *remaining_ids]

    assert len(asyncio.run(insert_async_group())) == 2
    assert producer_client.count_submissions() == 4


def test_async_count_completed_chunks(opsqueue: OpsqueueProcess) -> None:
    url = f"localhost:{opsqueue.port}"
    object_store_url = "file:///tmp/opsqueue/test_async_count_completed_chunks"
    producer_client = StreamingProducerClient(url, object_store_url)
    consumer_client = ConsumerClient(url, object_store_url)
    submission_id = producer_client.insert_submission([1, 2], chunk_size=1)

    [chunk] = consumer_client.reserve_chunks()
    consumer_client.complete_chunk(
        chunk.submission_id,
        chunk.submission_prefix,
        chunk.chunk_index.id,
        chunk.input_content,
    )
    assert (
        asyncio.run(
            producer_client.async_count_completed_chunks([submission_id, submission_id])
        )
        == 1
    )

    [chunk] = consumer_client.reserve_chunks()
    consumer_client.complete_chunk(
        chunk.submission_id,
        chunk.submission_prefix,
        chunk.chunk_index.id,
        chunk.input_content,
    )
    assert (
        asyncio.run(producer_client.async_count_completed_chunks([submission_id])) == 2
    )

    cancelled_submission_id = producer_client.insert_submission([3, 4], chunk_size=1)
    [chunk] = consumer_client.reserve_chunks()
    consumer_client.complete_chunk(
        chunk.submission_id,
        chunk.submission_prefix,
        chunk.chunk_index.id,
        chunk.input_content,
    )
    producer_client.cancel_submission(cancelled_submission_id)
    assert isinstance(
        producer_client.get_submission_status(cancelled_submission_id),
        SubmissionStatus.Cancelled,
    )

    paused_submission_id = producer_client.insert_submission(
        [5], chunk_size=1, initial_status=InitialSubmissionStatus.Paused
    )
    assert isinstance(
        producer_client.get_submission_status(paused_submission_id),
        SubmissionStatus.Paused,
    )
    assert (
        asyncio.run(
            producer_client.async_count_completed_chunks(
                [submission_id, cancelled_submission_id, paused_submission_id]
            )
        )
        == 3
    )

    with pytest.raises(SubmissionNotFoundError):
        asyncio.run(producer_client.async_count_completed_chunks([SubmissionId(0)]))


def test_async_stream_failed_submission(opsqueue: OpsqueueProcess) -> None:
    url = f"localhost:{opsqueue.port}"
    object_store_url = "file:///tmp/opsqueue/test_async_stream_failed_submission"
    producer_client = StreamingProducerClient(url, object_store_url)
    submission_id = producer_client.insert_submission(range(100), chunk_size=20)

    def run_consumer() -> None:
        consumer_client = ConsumerClient(url, object_store_url)

        def fail_operation(value: int) -> int:
            raise ValueError(f"cannot process {value}")

        consumer_client.run_each_op(fail_operation)

    async def stream_failed_submission() -> list[bytes]:
        chunks = await producer_client.async_stream_completed_submission_group(
            [submission_id]
        )
        return [chunk async for chunk in chunks]

    with background_process(run_consumer):
        with pytest.raises(SubmissionFailedError):
            asyncio.run(
                asyncio.wait_for(
                    stream_failed_submission(),
                    timeout=SUBMISSION_COMPLETED_TIMEOUT,
                )
            )

        status = producer_client.get_submission_status(submission_id)
        assert isinstance(status, SubmissionStatus.Failed)
        assert asyncio.run(
            producer_client.async_count_completed_chunks([submission_id])
        ) == (status.submission.chunks_done or 0)
