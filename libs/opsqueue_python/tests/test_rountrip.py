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
from conftest import background_process, multiple_background_processes, OpsqueueProcess
import logging

import pytest


def increment(data: int) -> int:
    return data + 1


def test_roundtrip(
    opsqueue: OpsqueueProcess, basic_consumer_strategy: Strategy
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
        consumer_client.run_each_op(increment, strategy=basic_consumer_strategy)

    with background_process(run_consumer) as _consumer:
        input_iter = range(0, 100)

        output_iter: Iterator[int] = producer_client.run_submission(
            input_iter, chunk_size=20
        )
        res = sum(output_iter)

        assert res == sum(range(1, 101))


def test_submission_failure_exception(opsqueue: OpsqueueProcess):
    producer_client = ProducerClient(
        f"localhost:{opsqueue.port}",
        "file:///tmp/opsqueue/test_submission_failure_exception",
    )

    def run_consumer() -> None:
        def broken_increment(input: int) -> int:
            return input / 0

        log_level = logging.root.level
        logging.basicConfig(
            format="Consumer - %(levelname)s: %(message)s",
            level=log_level,
            force=True,
        )

        consumer_client = ConsumerClient(
            f"localhost:{opsqueue.port}",
            "file:///tmp/opsqueue/test_submission_failure_exception",
        )
        consumer_client.run_each_op(broken_increment)

    # run_consumer()

    with background_process(run_consumer) as _consumer:
        input_iter = range(0, 100)

        with pytest.raises(SubmissionFailedError) as exc_info:
            producer_client.run_submission(input_iter, chunk_size=20)

        # We expect the intended attributes to be there:
        assert exc_info.value.failure is str
        assert exc_info.value.submission is SubmissionFailed
        assert exc_info.value.chunk is ChunkFailed

        # And the result should contain info about the original exception:
        assert "ZeroDivisionError" in exc_info.value.failure


def test_chunk_roundtrip(
    opsqueue: OpsqueueProcess, basic_consumer_strategy: Strategy
) -> None:
    import json

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
        input_iter = map(lambda i: json.dumps([i, i, i]).encode(), range(0, 10))
        output_iter: Iterator[list[int]] = map(
            lambda c: json.loads(c),
            producer_client.run_submission_chunks(input_iter),
        )
        import itertools

        res = sum(itertools.chain.from_iterable(output_iter))

        assert res == 165


def test_many_consumers(
    opsqueue: OpsqueueProcess, basic_consumer_strategy: Strategy
) -> None:
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
        consumer_client.run_each_op(increment, strategy=basic_consumer_strategy)

    n_consumers = 16
    with multiple_background_processes(run_consumer, n_consumers) as _consumers:
        input_iter = range(0, 1000)
        output_iter: Iterator[int] = producer_client.run_submission(
            input_iter, chunk_size=100
        )
        res = sum(output_iter)

        assert res == sum(range(1, 1001))
