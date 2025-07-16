# If you need to debug tests:
# - use pytest's `--log-cli-level=info` (or `=debug`) argument to get more detailed logs from the producer/consumer clients
# - use `RUST_LOG="opsqueue=info"` (or `opsqueue=debug` or `debug` for even more verbosity), together with to the pytest option `-s` AKA `--capture=no`, to debug the opsqueue binary itself.

from collections.abc import Iterator, Sequence
from opsqueue.producer import (
    ProducerClient,
    SubmissionFailed,
    ChunkFailed,
    SubmissionFailedError,
)
from opsqueue.consumer import ConsumerClient, Strategy, Chunk
from opsqueue.common import SerializationFormat
from conftest import background_process, multiple_background_processes, OpsqueueProcess
import logging

import pytest


def increment(data: int) -> int:
    return data + 1


def test_roundtrip(opsqueue: OpsqueueProcess, any_consumer_strategy: Strategy) -> None:
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
        consumer_client.run_each_op(increment, strategy=any_consumer_strategy)

    with background_process(run_consumer) as _consumer:
        input_iter = range(0, 100)

        output_iter: Iterator[int] = producer_client.run_submission(
            input_iter, chunk_size=20, strategic_metadata={"id": 42}
        )
        res = sum(output_iter)

        assert res == sum(range(1, 101))


def test_empty_submission(
    opsqueue: OpsqueueProcess, any_consumer_strategy: Strategy
) -> None:
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
        input_iter, chunk_size=20
    )
    res = sum(output_iter)
    assert res == 0


def test_roundtrip_explicit_serialization_format(
    opsqueue: OpsqueueProcess,
    any_consumer_strategy: Strategy,
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
        consumer_client.run_each_op(
            increment,
            strategy=any_consumer_strategy,
            serialization_format=serialization_format,
        )

    with background_process(run_consumer) as _consumer:
        input_iter = range(0, 100)

        output_iter: Iterator[int] = producer_client.run_submission(
            input_iter, chunk_size=20, serialization_format=serialization_format
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
            producer_client.run_submission(input_iter, chunk_size=20)

        # We expect the intended attributes to be there:
        assert isinstance(exc_info.value.failure, str)
        assert isinstance(exc_info.value.submission, SubmissionFailed)
        assert isinstance(exc_info.value.chunk, ChunkFailed)

        # And the result should contain info about the original exception:
        assert "ZeroDivisionError" in exc_info.value.failure


def test_chunk_roundtrip(
    opsqueue: OpsqueueProcess, basic_consumer_strategy: Strategy
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
        consumer_client.run_each_chunk(increment_list, strategy=basic_consumer_strategy)

    with background_process(run_consumer) as _consumer:
        input_iter = map(lambda i: cbor2.dumps([i, i, i]), range(0, 10))
        output_iter: Iterator[list[int]] = map(
            lambda c: cbor2.loads(c),
            producer_client.run_submission_chunks(input_iter),
        )
        import itertools

        res = sum(itertools.chain.from_iterable(output_iter))

        assert res == 165


def test_many_consumers(
    opsqueue: OpsqueueProcess, any_consumer_strategy: Strategy
) -> None:
    """
    Ensure the system still works if we have many consumers concurrently
    working on thes same submission
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
        consumer_client.run_each_op(increment, strategy=any_consumer_strategy)

    n_consumers = 16
    with multiple_background_processes(run_consumer, n_consumers) as _consumers:
        input_iter = range(0, 1000)
        output_iter: Iterator[int] = producer_client.run_submission(
            input_iter, chunk_size=100
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
