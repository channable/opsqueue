# If you need to debug tests:
# - use pytest's `--log-cli-level=info` (or `=debug`) argument to get more detailed logs from the producer/consumer clients
# - use `RUST_LOG="opsqueue=info"` (or `opsqueue=debug` or `debug` for even more verbosity), together with to the pytest option `-s` AKA `--capture=no`, to debug the opsqueue binary itself.

from collections.abc import Iterator, Sequence
from opsqueue.producer import (
    SubmissionId,
    ProducerClient,
    SubmissionCompleted,
    SubmissionFailed,
    ChunkFailed,
    SubmissionStatus,
    SubmissionFailedError,
    SubmissionNotFoundError,
    SubmissionNotCancellable,
    SubmissionNotCancellableError,
    TooManyMatchingSubmissionsError,
)
from opsqueue.consumer import ConsumerClient, Chunk
from opsqueue.common import SerializationFormat
from conftest import (
    background_process,
    multiple_background_processes,
    OpsqueueProcess,
    opsqueue_service,
    StrategyDescription,
    strategy_from_description,
)
import logging
import time
import pytest

SUBMISSION_COMPLETED_TIMEOUT = 10.0


def increment(data: int) -> int:
    return data + 1


def test_roundtrip(
    opsqueue: OpsqueueProcess, any_consumer_strategy: StrategyDescription
) -> None:
    """
    A most basic test that round-trips all three components.
    If this fails, something is very wrong.
    """
    producer_client = ProducerClient(
        f"localhost:{opsqueue.port}", "file:///tmp/opsqueue/test_roundtrip"
    )

    def run_consumer() -> None:
        consumer_client = ConsumerClient(
            f"localhost:{opsqueue.port}", "file:///tmp/opsqueue/test_roundtrip"
        )
        strategy = strategy_from_description(any_consumer_strategy)
        consumer_client.run_each_op(increment, strategy=strategy)

    with background_process(run_consumer) as _consumer:
        input_iter = range(0, 100)

        output_iter: Iterator[int] = producer_client.run_submission(
            input_iter,
            chunk_size=20,
            strategic_metadata={"id": 42},
            timeout=SUBMISSION_COMPLETED_TIMEOUT,
        )
        res = sum(output_iter)

        assert res == sum(range(1, 101))


def test_complete_then_fail_chunks(
    opsqueue: OpsqueueProcess, any_consumer_strategy: StrategyDescription
) -> None:
    """
    A most test that round-trips all chunks as completed and failed.
    This simulates the consumer completing chunks while the server released the reservation.

    Each consumer completes 1/n_consumers chunks before the reservation expired
    Each consumer receives a reservation expiration for each chunk
    Each consumer completes 1/n_consumers chunks after the reservation expired

    Registration expirations are looped via the client to allow in-flight completions to still be registered
    """
    producer_client = ProducerClient(
        f"localhost:{opsqueue.port}",
        "file:///tmp/opsqueue/test_complete_then_fail_chunks",
    )
    n_ops = 100
    chunk_size = 10
    n_chunks = n_ops // chunk_size
    n_consumers = 3
    assert n_chunks // n_consumers > 1, (
        "Need more than one chunk per consumer for this test"
    )

    def run_consumer(_consumer_id: int) -> None:
        consumer_client = ConsumerClient(
            f"localhost:{opsqueue.port}",
            "file:///tmp/opsqueue/test_complete_then_fail_chunks",
        )
        strategy = strategy_from_description(any_consumer_strategy)

        for i in range(n_chunks):
            [chunk] = consumer_client.reserve_chunks(strategy=strategy)
            if i % n_consumers == 0:
                # Each consumers completes 1/n_consumers chunks before the reservation expired.
                consumer_client.complete_chunk(
                    chunk.submission_id,
                    chunk.submission_prefix,
                    chunk.chunk_index,
                    chunk.input_content,
                )
            # Simulate the reservation expiring while the consumer's completion wasn't registered.
            consumer_client.fail_chunk(
                chunk.submission_id,
                chunk.submission_prefix,
                chunk.chunk_index,
                "Simulated failure",
            )
            if i % n_consumers == 1:
                # Each consumers completes 1/n_consumers chunks after its reservation expired.
                consumer_client.complete_chunk(
                    chunk.submission_id,
                    chunk.submission_prefix,
                    chunk.chunk_index,
                    chunk.input_content,
                )

    with multiple_background_processes(run_consumer, n_consumers) as _consumers:
        input_iter = range(0, n_ops)

        output_iter: Iterator[int] = producer_client.run_submission(
            input_iter,
            chunk_size=chunk_size,
            strategic_metadata={"id": 42, "second_id": 69},
            timeout=SUBMISSION_COMPLETED_TIMEOUT,
        )
        res = sum(output_iter)

        assert res == sum(range(0, n_ops))


def test_empty_submission(opsqueue: OpsqueueProcess) -> None:
    """
    Empty submissions ought to be supported without problems.
    Opsqueue should immediately consider these 'completed'
    and no errors should be thrown.
    """
    producer_client = ProducerClient(
        f"localhost:{opsqueue.port}", "file:///tmp/opsqueue/test_empty_submission"
    )

    input_iter: list[int] = []
    output_iter: Iterator[int] = producer_client.run_submission(
        input_iter,
        chunk_size=20,
        timeout=SUBMISSION_COMPLETED_TIMEOUT,
    )
    res = sum(output_iter)
    assert res == 0


def test_roundtrip_explicit_serialization_format(
    opsqueue: OpsqueueProcess,
    any_consumer_strategy: StrategyDescription,
    serialization_format: SerializationFormat,
) -> None:
    """
    A most basic test that round-trips all three components,
    but this time with explicitly specified serialization format.

    Tests whether various serialization formats work with the interface correctly.
    """
    producer_client = ProducerClient(
        f"localhost:{opsqueue.port}", "file:///tmp/opsqueue/test_roundtrip"
    )

    def run_consumer() -> None:
        consumer_client = ConsumerClient(
            f"localhost:{opsqueue.port}", "file:///tmp/opsqueue/test_roundtrip"
        )
        strategy = strategy_from_description(any_consumer_strategy)
        consumer_client.run_each_op(
            increment,
            strategy=strategy,
            serialization_format=serialization_format,
        )

    with background_process(run_consumer) as _consumer:
        input_iter = range(0, 100)

        output_iter: Iterator[int] = producer_client.run_submission(
            input_iter,
            chunk_size=20,
            serialization_format=serialization_format,
            timeout=SUBMISSION_COMPLETED_TIMEOUT,
        )
        res = sum(output_iter)

        assert res == sum(range(1, 101))


def test_submission_failure_exception(opsqueue: OpsqueueProcess) -> None:
    """
    Ensure that if a chunk keeps on failing, the producer will raise a SubmissionFailedError.

    (If this test hangs, it may be that the consumer crashed early.
    Check by calling `run_consumer()` directly)
    """

    producer_client = ProducerClient(
        f"localhost:{opsqueue.port}",
        "file:///tmp/opsqueue/test_submission_failure_exception",
    )

    def run_consumer() -> None:
        log_level = logging.root.level
        logging.basicConfig(
            format="Consumer - %(levelname)s: %(message)s",
            level=log_level,
            force=True,
        )

        def broken_increment(input: int) -> float:
            return input / 0

        consumer_client = ConsumerClient(
            f"localhost:{opsqueue.port}",
            "file:///tmp/opsqueue/test_submission_failure_exception",
        )
        consumer_client.run_each_op(broken_increment)

    with background_process(run_consumer) as consumer:
        logging.error(f"Opsqueue: {opsqueue}")
        logging.error(f"Consumer: {consumer}")
        input_iter = range(0, 100)

        with pytest.raises(SubmissionFailedError) as exc_info:
            producer_client.run_submission(
                input_iter,
                chunk_size=20,
                timeout=SUBMISSION_COMPLETED_TIMEOUT,
            )

        # We expect the intended attributes to be there:
        assert isinstance(exc_info.value.failure, str)
        assert isinstance(exc_info.value.submission, SubmissionFailed)
        assert isinstance(exc_info.value.chunk, ChunkFailed)

        # And the result should contain info about the original exception:
        assert "ZeroDivisionError" in exc_info.value.failure


def test_chunk_roundtrip(
    opsqueue: OpsqueueProcess, basic_consumer_strategy: StrategyDescription
) -> None:
    """
    Tests whether everything still works well
    if we're directly reading/writing chunks as bytes
    rather than relying on opsqueue.common.encode_chunk / opsqueue.common.decode_chunk
    """
    import cbor2

    producer_client = ProducerClient(
        f"localhost:{opsqueue.port}", "file:///tmp/opsqueue/test_chunk_roundtrip"
    )

    def run_consumer() -> None:
        def increment_list(ints: Sequence[int], _chunk: Chunk) -> Sequence[int]:
            return [increment(i) for i in ints]

        consumer_client = ConsumerClient(
            f"localhost:{opsqueue.port}",
            "file:///tmp/opsqueue/test_chunk_roundtrip",
        )
        strategy = strategy_from_description(basic_consumer_strategy)
        consumer_client.run_each_chunk(increment_list, strategy=strategy)

    with background_process(run_consumer) as _consumer:
        input_iter = map(lambda i: cbor2.dumps([i, i, i]), range(0, 10))
        output_iter: Iterator[list[int]] = map(
            lambda c: cbor2.loads(c),
            producer_client.run_submission_chunks(
                input_iter,
                timeout=SUBMISSION_COMPLETED_TIMEOUT,
            ),
        )
        import itertools

        res = sum(itertools.chain.from_iterable(output_iter))

        assert res == 165


def test_many_consumers(
    opsqueue: OpsqueueProcess, any_consumer_strategy: StrategyDescription
) -> None:
    """
    Ensure the system still works if we have many consumers concurrently working
    on the same submission
    """
    producer_client = ProducerClient(
        f"localhost:{opsqueue.port}", "file:///tmp/opsqueue/test_many_consumers"
    )

    def run_consumer(consumer_id: int) -> None:
        # Log inside the consumers when we log on the outside:
        log_level = logging.root.level
        logging.basicConfig(
            format=f"Consumer {consumer_id} - %(levelname)s: %(message)s",
            level=log_level,
            force=True,
        )

        consumer_client = ConsumerClient(
            f"localhost:{opsqueue.port}", "file:///tmp/opsqueue/test_many_consumers"
        )
        strategy = strategy_from_description(any_consumer_strategy)
        consumer_client.run_each_op(increment, strategy=strategy)

    n_consumers = 16
    with multiple_background_processes(run_consumer, n_consumers) as _consumers:
        input_iter = range(0, 1000)
        output_iter: Iterator[int] = producer_client.run_submission(
            input_iter,
            chunk_size=100,
            timeout=SUBMISSION_COMPLETED_TIMEOUT,
        )
        res = sum(output_iter)

        assert res == sum(range(1, 1001))


def test_async_producer(opsqueue: OpsqueueProcess) -> None:
    """
    A simple sanity check to ensure the async API does its basic job
    """
    import asyncio

    def run_consumer() -> None:
        def increment_list(ints: Sequence[int], _chunk: Chunk) -> Sequence[int]:
            return [increment(i) for i in ints]

        consumer_client = ConsumerClient(
            f"localhost:{opsqueue.port}",
            "file:///tmp/opsqueue/test_async_producer",
        )
        consumer_client.run_each_op(increment)

    producer_client = ProducerClient(
        f"localhost:{opsqueue.port}", "file:///tmp/opsqueue/test_async_producer"
    )

    async def run_one_submission(top: int) -> int:
        logging.debug(f"Running submission {top}")
        input_iter = range(0, top)
        output_iter = await producer_client.async_run_submission(
            input_iter, chunk_size=1000
        )
        logging.debug(f"Submission for {top} done!")

        res = 0
        async for x in output_iter:
            res += x

        logging.debug(f"Finished summing {top}: {res}")
        return res

    async def run_many_submissions() -> None:
        res = await asyncio.gather(*[run_one_submission(top) for top in range(1, 10)])
        assert res == [1, 3, 6, 10, 15, 21, 28, 36, 45]

    with background_process(run_consumer) as _consumer:
        asyncio.run(run_many_submissions())


def test_metadata_in_submission_complete(
    opsqueue: OpsqueueProcess, any_consumer_strategy: StrategyDescription
) -> None:
    """SubmissionCompleted should include the submission's metadata and
    strategic metadata.

    """
    metadata = b"1234"
    strategic_metadata = {"6": 7}
    url = "file:///tmp/opsqueue/test_metadata_in_submission_complete"
    producer_client = ProducerClient(f"localhost:{opsqueue.port}", url)
    submission_id = producer_client.insert_submission(
        (1, 2, 3),
        chunk_size=1,
        metadata=metadata,
        strategic_metadata=strategic_metadata,
    )

    def run_consumer() -> None:
        consumer_client = ConsumerClient(f"localhost:{opsqueue.port}", url)
        strategy = strategy_from_description(any_consumer_strategy)
        consumer_client.run_each_op(lambda x: x, strategy=strategy)

    with background_process(run_consumer):
        # Wait for the submission to complete.
        producer_client.blocking_stream_completed_submission(
            submission_id,
            timeout=SUBMISSION_COMPLETED_TIMEOUT,
        )
        submission = producer_client.get_submission_status(submission_id)
        assert submission is not None
        assert isinstance(submission.submission, SubmissionCompleted)
        assert submission.submission.metadata == metadata
        assert submission.submission.strategic_metadata == strategic_metadata


def test_metadata_in_submission_failed(
    opsqueue: OpsqueueProcess, any_consumer_strategy: StrategyDescription
) -> None:
    """SubmissionFailed should include the submission's metadata and
    strategic metadata.

    """
    metadata = b"I am meta"
    strategic_metadata = {"6": 7}
    url = "file:///tmp/opsqueue/test_metadata_in_submission_failed"
    producer_client = ProducerClient(f"localhost:{opsqueue.port}", url)
    submission_id = producer_client.insert_submission(
        (1, 2, 3),
        chunk_size=1,
        metadata=metadata,
        strategic_metadata=strategic_metadata,
    )

    def run_consumer() -> None:
        consumer_client = ConsumerClient(f"localhost:{opsqueue.port}", url)

        def consume(x: int) -> None:
            raise ValueError(f"Couldn't consume {x}")

        strategy = strategy_from_description(any_consumer_strategy)
        consumer_client.run_each_op(consume, strategy=strategy)

    with background_process(run_consumer):

        def assert_submission_failed_has_metadata(x: SubmissionFailed) -> None:
            assert isinstance(x, SubmissionFailed)
            assert x.metadata == metadata
            assert x.strategic_metadata == strategic_metadata

        with pytest.raises(SubmissionFailedError) as exc_info:
            # Wait for the submission to fail.
            producer_client.blocking_stream_completed_submission(
                submission_id,
                timeout=SUBMISSION_COMPLETED_TIMEOUT,
            )
        assert_submission_failed_has_metadata(exc_info.value.submission)

        submission = producer_client.get_submission_status(submission_id)
        assert submission is not None
        assert_submission_failed_has_metadata(submission.submission)


def test_cancel_submission_not_found(
    opsqueue: OpsqueueProcess,
) -> None:
    """Attempting to cancel a submission that doesn't exist raises a
    SubmissionNotFoundError.

    """
    url = "file:///tmp/opsqueue/test_cancel_submission_not_found"
    producer_client = ProducerClient(f"localhost:{opsqueue.port}", url)
    submission_id = 0
    with pytest.raises(SubmissionNotFoundError) as exc_info:
        producer_client.cancel_submission(SubmissionId(0))
    assert exc_info.value.submission_id == submission_id


def test_cancel_in_progress(
    opsqueue: OpsqueueProcess,
) -> None:
    """Cancelling a submission that is in progress succeeds and returns none.
    Attempting to cancel a submission that is already cancelled should raise a
    SubmissionNotCancellableError.

    """

    url = "file:///tmp/opsqueue/test_cancel_in_progress"
    producer_client = ProducerClient(f"localhost:{opsqueue.port}", url)
    submission_id = producer_client.insert_submission((1, 2, 3), chunk_size=1)
    status = producer_client.get_submission_status(submission_id)
    # Sanity check submission is in progress before proceeding to cancel.
    assert isinstance(status, SubmissionStatus.InProgress)
    # Cancelling an in progress submission should change submission status to
    # cancelled.
    producer_client.cancel_submission(submission_id)
    assert isinstance(
        producer_client.get_submission_status(submission_id), SubmissionStatus.Cancelled
    )


def test_cancel_already_cancelled(
    opsqueue: OpsqueueProcess,
) -> None:
    """Attempting to cancel a submission that is already cancelled should raise
    a SubmissionNotCancellableError.

    """

    url = "file:///tmp/opsqueue/test_cancel_already_cancelled"
    producer_client = ProducerClient(f"localhost:{opsqueue.port}", url)
    submission_id = producer_client.insert_submission((1, 2, 3), chunk_size=1)
    # Cancelling an in progress submission should change submission status to
    # cancelled.
    producer_client.cancel_submission(submission_id)
    # Confirm test pre-requisite, that the submission is indeed cancelled.
    assert isinstance(
        producer_client.get_submission_status(submission_id), SubmissionStatus.Cancelled
    )
    # Cancelling an already cancelled submission should raise an exception.
    with pytest.raises(SubmissionNotCancellableError) as exc_info:
        producer_client.cancel_submission(submission_id)
    assert isinstance(exc_info.value.submission, SubmissionNotCancellable.Cancelled)


def test_cancel_complete_submission(
    opsqueue: OpsqueueProcess, any_consumer_strategy: StrategyDescription
) -> None:
    """Attempting to cancel a submission that has already completed should raise
    a SubmissionNotCancellableError.

    """
    url = "file:///tmp/opsqueue/test_cancel_complete_submission"
    producer_client = ProducerClient(f"localhost:{opsqueue.port}", url)
    submission_id = producer_client.insert_submission((1, 2, 3), chunk_size=1)

    def run_consumer() -> None:
        consumer_client = ConsumerClient(f"localhost:{opsqueue.port}", url)
        strategy = strategy_from_description(any_consumer_strategy)
        consumer_client.run_each_op(lambda x: x, strategy=strategy)

    with background_process(run_consumer):
        # Wait for the submission to complete.
        producer_client.blocking_stream_completed_submission(
            submission_id,
            timeout=SUBMISSION_COMPLETED_TIMEOUT,
        )
        submission = producer_client.get_submission_status(submission_id)
        assert submission is not None
        assert isinstance(submission.submission, SubmissionCompleted)
        # Cancelling the already completed submission should fail.
        with pytest.raises(SubmissionNotCancellableError) as exc_info:
            producer_client.cancel_submission(submission_id)
        assert isinstance(exc_info.value.submission, SubmissionNotCancellable.Completed)
        assert exc_info.value.chunk is None


def test_cancel_failed_submission(
    opsqueue: OpsqueueProcess, any_consumer_strategy: StrategyDescription
) -> None:
    """Attempting to cancel a submission that has failed should raise a
    SubmissionNotCancellableError.

    """
    url = "file:///tmp/opsqueue/test_cancel_failed_submission"
    producer_client = ProducerClient(f"localhost:{opsqueue.port}", url)
    submission_id = producer_client.insert_submission((1, 2, 3), chunk_size=1)

    def run_consumer() -> None:
        consumer_client = ConsumerClient(f"localhost:{opsqueue.port}", url)

        def consume(x: int) -> None:
            raise ValueError(f"Couldn't consume {x}")

        strategy = strategy_from_description(any_consumer_strategy)
        consumer_client.run_each_op(consume, strategy=strategy)

    with background_process(run_consumer):
        with pytest.raises(SubmissionFailedError):
            producer_client.blocking_stream_completed_submission(
                submission_id,
                timeout=SUBMISSION_COMPLETED_TIMEOUT,
            )
        # Cancelling the failed submission should fail.
        with pytest.raises(SubmissionNotCancellableError) as exc_info:
            producer_client.cancel_submission(submission_id)
        assert isinstance(exc_info.value.submission, SubmissionNotCancellable.Failed)
        assert isinstance(exc_info.value.chunk, ChunkFailed)


def test_failed_submission_includes_chunks_done(opsqueue: OpsqueueProcess) -> None:
    """The SubmissionFailed (in a SubmissionFailedError) includes the count of
    chunks done.

    """
    url = "file:///tmp/opsqueue/test_failed_submission_includes_chunks_done"
    producer_client = ProducerClient(f"localhost:{opsqueue.port}", url)
    chunks = [1, 2, 3]
    submission_id = producer_client.insert_submission(chunks, chunk_size=1)

    def run_consumer() -> None:
        consumer_client = ConsumerClient(f"localhost:{opsqueue.port}", url)

        def consume(x: int) -> int | None:
            if x == chunks[-1]:
                raise ValueError(f"Couldn't consume {x}")
            return x

        consumer_client.run_each_op(
            consume, strategy=strategy_from_description("Oldest")
        )

    with background_process(run_consumer):
        with pytest.raises(SubmissionFailedError) as exc_info:
            producer_client.blocking_stream_completed_submission(
                submission_id,
                timeout=SUBMISSION_COMPLETED_TIMEOUT,
            )
        assert exc_info.value.submission.chunks_done == len(chunks) - 1


def test_lookup_submission_ids_by_strategic_metadata(opsqueue: OpsqueueProcess) -> None:
    """Lookup of submission IDs should only match in progress submissions with
    all pieces of strategic metadata.

    """
    url = "file:///tmp/opsqueue/test_lookup_submission_ids_by_strategic_metadata"
    producer_client = ProducerClient(f"localhost:{opsqueue.port}", url)
    id_1 = producer_client.insert_submission(
        [1], chunk_size=1, strategic_metadata={"foo": 1, "bar": 2, "wow": 3}
    )
    id_2 = producer_client.insert_submission(
        [1], chunk_size=1, strategic_metadata={"foo": 1, "bar": 2, "moo": 3}
    )
    # Inserting some similar data to that above, which shouldn't get matched.
    producer_client.insert_submission(
        [1], chunk_size=1, strategic_metadata={"foo": 2, "bar": 1}
    )

    def test_lookup(
        strategic_metadata: dict[str, int], expected_ids: list[int]
    ) -> None:
        found_ids = producer_client.lookup_submission_ids_by_strategic_metadata(
            strategic_metadata
        )
        assert isinstance(found_ids, list)
        assert all(map(lambda x: isinstance(x, SubmissionId), found_ids))
        assert found_ids == expected_ids

    test_lookup({"foo": 1}, [id_1, id_2])
    test_lookup({"foo": 1, "bar": 2}, [id_1, id_2])
    test_lookup({"foo": 1, "MISS": 2}, [])
    test_lookup({"wow": 3}, [id_1])

    # Should only match in-progress submission.
    producer_client.cancel_submission(id_1)
    test_lookup({"foo": 1}, [id_2])


def test_lookup_submission_ids_by_empty_strategic_metadata(
    opsqueue: OpsqueueProcess,
) -> None:
    """Lookup of submission IDs with empty strategic_metadata should NOT raise
    an exception.

    """
    url = "file:///tmp/opsqueue/test_lookup_submission_ids_by_empty_strategic_metadata"
    producer_client = ProducerClient(f"localhost:{opsqueue.port}", url)
    count = 6
    for _ in range(count):
        producer_client.insert_submission([1], chunk_size=1)
    assert len(producer_client.lookup_submission_ids_by_strategic_metadata({})) == count


def test_lookup_too_many_submission_ids_by_strategic_metadata() -> None:
    """Lookup of too many submission IDs beyond the configured limit raises
    TooManyMatchingSubmissionsError.

    """
    max_ = 2
    # We didn't request the OpsQueueProcess as a parameter so an instance isn't
    # started, instead we start one here with custom args.
    with opsqueue_service(
        command_args=["--max-submissions-returned", str(max_)]
    ) as opsqueue:
        url = "file:///tmp/opsqueue/test_lookup_too_many_matching_submissions"
        producer_client = ProducerClient(f"localhost:{opsqueue.port}", url)
        inserted: list[SubmissionId] = []
        strategic_metadata = {"k": 1}
        for _ in range(max_ + 1):
            assert (
                inserted
                == producer_client.lookup_submission_ids_by_strategic_metadata(
                    strategic_metadata
                )
            )
            inserted.append(
                producer_client.insert_submission(
                    [1], chunk_size=1, strategic_metadata=strategic_metadata
                )
            )
        with pytest.raises(TooManyMatchingSubmissionsError) as exc:
            assert len(inserted) == max_ + 1
            producer_client.lookup_submission_ids_by_strategic_metadata(
                strategic_metadata
            )
        assert exc.type is TooManyMatchingSubmissionsError
        assert exc.value.max_submissions == max_


def test_run_submission_timeout(opsqueue: OpsqueueProcess) -> None:
    url = "file:///tmp/opsqueue/test_run_submission_timeout"
    producer_client = ProducerClient(f"localhost:{opsqueue.port}", url)

    def run_consumer() -> None:
        consumer_client = ConsumerClient(f"localhost:{opsqueue.port}", url)

        def process_op(x: int) -> int:
            time.sleep(2.0)
            return x

        consumer_client.run_each_op(process_op)

    with background_process(run_consumer) as _consumer:
        with pytest.raises(TimeoutError):
            producer_client.run_submission(
                [1],
                chunk_size=1,
                timeout=0.1,
            )


def test_unpause_and_complete(opsqueue: OpsqueueProcess) -> None:
    """Unpausing a paused submission makes it available to consumers again,
    and it can be completed normally afterwards."""
    url = "file:///tmp/opsqueue/test_unpause_and_complete"
    producer_client = ProducerClient(f"localhost:{opsqueue.port}", url)
    submission_id = producer_client.insert_submission(
        (1, 2, 3), chunk_size=1, paused=True
    )

    assert isinstance(
        producer_client.get_submission_status(submission_id), SubmissionStatus.Paused
    )

    producer_client.unpause_submission(submission_id)
    assert isinstance(
        producer_client.get_submission_status(submission_id),
        SubmissionStatus.InProgress,
    )

    def run_consumer() -> None:
        consumer_client = ConsumerClient(f"localhost:{opsqueue.port}", url)
        consumer_client.run_each_op(lambda x: x)

    with background_process(run_consumer):
        producer_client.blocking_stream_completed_submission(submission_id)
        assert isinstance(
            producer_client.get_submission_status(submission_id),
            SubmissionStatus.Completed,
        )


def test_unpause_not_found(opsqueue: OpsqueueProcess) -> None:
    """Unpausing a submission that is not paused (e.g. in-progress) raises
    SubmissionNotFoundError."""
    url = "file:///tmp/opsqueue/test_unpause_not_found"
    producer_client = ProducerClient(f"localhost:{opsqueue.port}", url)
    submission_id = producer_client.insert_submission(
        (1, 2, 3), chunk_size=1, paused=False
    )
    assert isinstance(
        producer_client.get_submission_status(submission_id),
        SubmissionStatus.InProgress,
    )
    with pytest.raises(SubmissionNotFoundError):
        producer_client.unpause_submission(submission_id)


def test_cancel_paused(opsqueue: OpsqueueProcess) -> None:
    """A paused submission can be cancelled; its status becomes Cancelled."""
    url = "file:///tmp/opsqueue/test_cancel_paused"
    producer_client = ProducerClient(f"localhost:{opsqueue.port}", url)
    submission_id = producer_client.insert_submission(
        (1, 2, 3), chunk_size=1, paused=True
    )

    assert isinstance(
        producer_client.get_submission_status(submission_id), SubmissionStatus.Paused
    )

    producer_client.cancel_submission(submission_id)
    assert isinstance(
        producer_client.get_submission_status(submission_id), SubmissionStatus.Cancelled
    )
