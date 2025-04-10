from __future__ import annotations
from collections.abc import Sequence
import typing
from typing import Any, Callable, cast

import opentelemetry
import opentelemetry.trace
import opentelemetry.baggage
import opentelemetry.util.types

from . import opsqueue_internal
from .opsqueue_internal import Chunk, Strategy, SubmissionId  # type: ignore[import-not-found]
from . import tracing
from . import common

DEFAULT_STRATEGY = Strategy.Random()

__all__ = ["ConsumerClient", "Strategy", "Chunk", "SubmissionId"]


class ConsumerClient:
    """
    Opsqueue consumer client. Allows working on individual (chunks of) operations.
    """

    __slots__ = "inner"

    def __init__(
        self,
        opsqueue_url: str,
        object_store_url: str,
        *,
        object_store_options: list[tuple[str, str]] = [],
    ):
        """
        Creates a new consumer client.

        - opsqueue_url: URL at which the opsqueue binary can be reached.

        - object_store_url: URL to reach the object store in which chunks are stored.
            Use `file:///some/local/path` for local testing.
            Use `gs://bucket/path` for a GCS bucket.
            See https://docs.rs/object_store/0.11.1/object_store/enum.ObjectStoreScheme.html for details.

        The persistent connection to opsqueue is established lazily on first use.

        Raises `NewObjectStoreClientError` when the given `object_store_url` is incorrect.
        """
        self.inner = opsqueue_internal.ConsumerClient(
            opsqueue_url, object_store_url, object_store_options
        )

    def __repr__(self) -> str:
        return cast(str, self.inner.__repr__())

    def run_each_op(
        self,
        op_callback: Callable[[Any], Any],
        *,
        strategy: Strategy = DEFAULT_STRATEGY,
        serialization_format: common.SerializationFormat = common.DEFAULT_SERIALIZATION_FORMAT,
    ) -> None:
        """
        Runs the given `op_callback` for each reservable operation in a loop.

        This function blocks 'forever', except when 'special' exceptions like KeyboardInterrupt are raised.
        Specifically, normal exceptions (inheriting from `Exception`) will be caught and cause
        `fail_chunk` to be called, with the loop afterwards continuing.
        Exceptions inheriting only from `BaseException` will cause the loop to terminate.
        """

        def chunk_callback(chunk_ops: Sequence[Any], _chunk: Chunk) -> Any:
            return [op_callback(op) for op in chunk_ops]

        self.run_each_chunk(
            chunk_callback, strategy=strategy, serialization_format=serialization_format
        )

    def run_each_chunk(
        self,
        chunk_callback: Callable[[Sequence[Any], Chunk], Sequence[Any]],
        *,
        strategy: Strategy = DEFAULT_STRATEGY,
        serialization_format: common.SerializationFormat = common.DEFAULT_SERIALIZATION_FORMAT,
    ) -> None:
        def raw_chunk_callback(chunk: Chunk) -> bytes:
            ctx = _trace_context_from_chunk(chunk)
            with opentelemetry.trace.get_tracer(
                "opsqueue.consumer"
            ).start_as_current_span(
                "run_chunk", context=ctx, kind=opentelemetry.trace.SpanKind.CONSUMER
            ) as span:
                span.set_attribute("submission_id", chunk.submission_id.id)
                span.set_attribute("chunk_index", chunk.chunk_index.id)
                for k, v in opentelemetry.baggage.get_all(ctx).items():
                    safe_v = typing.cast(opentelemetry.util.types.AttributeValue, v)
                    span.set_attribute(k, safe_v)

                chunk_contents = common.decode_chunk(
                    chunk.input_content, serialization_format
                )
                chunk_result_contents = chunk_callback(chunk_contents, chunk)
                return common.encode_chunk(chunk_result_contents, serialization_format)

        self.run_each_chunk_raw(raw_chunk_callback, strategy=strategy)

    def run_each_chunk_raw(
        self,
        chunk_callback: Callable[[Chunk], bytes],
        *,
        strategy: Strategy = DEFAULT_STRATEGY,
    ) -> None:
        """
        Runs the given `chunk_callback` for each chunk the consumer can reserve in a loop.
        This expects encoding/decoding of the chunk contents from/to bytes to be done manually by you.

        This function blocks 'forever', except when 'fatal' exceptions like KeyboardInterrupt are raised.
        Specifically, normal exceptions (inheriting from `Exception`) will be caught and cause
        `fail_chunk` to be called, with the loop afterwards continuing.
        Only Exceptions not inheriting from `Exception` will cause the loop to terminate.
        """
        self.inner.run_per_chunk(strategy, chunk_callback)

    def reserve_chunks(
        self, max: int = 1, strategy: Strategy = Strategy.Newest
    ) -> list[Chunk]:
        """
        Low-level function to manually reserve one or more chunks for processing.

        Reserved chunks need to individually be marked as completed or failed
        using `complete_chunk` resp. `fail_chunk`.

        If your code crashes, all reserved chunks will automatically be marked as failed.

        Raises:
        - IncorrectUsageError when the `max` parameter is not a positive integer.
        - InternalConsumerClientError if there is a low-level internal error
        """
        return self.inner.reserve_chunks(max, strategy)  # type: ignore[no-any-return]

    def complete_chunk(
        self,
        submission_id: SubmissionId,
        submission_prefix: str,
        chunk_index: int,
        output_content: bytes,
    ) -> None:
        """
        Low-level function to manually mark a chunk as completed,
        passing the output content bytes.

        The submission_id, submission_prefix and chunk_index can be found
        on the `Chunk` type originally received as part of `reserve_chunks`.

        Raises:
        - InternalConsumerClientError if there is a low-level internal error
        """
        self.inner.complete_chunk(
            submission_id, submission_prefix, chunk_index, output_content
        )

    def fail_chunk(
        self,
        submission_id: SubmissionId,
        submission_prefix: str,
        chunk_index: int,
        failure: str,
    ) -> None:
        """
        Low-level function to manually mark a chunk as completed,
        passing the failure message as a string.

        This failure message is meant for developer eyes, i.e. it should be a
        pretty-printed exception message and possibly its stack trace.

        The submission_id, submission_prefix and chunk_index can be found
        on the `Chunk` type originally received as part of `reserve_chunks`.

        Raises:
        - InternalConsumerClientError if there is a low-level internal error
        """
        self.inner.fail_chunk(submission_id, submission_prefix, chunk_index, failure)


def _trace_context_from_chunk(chunk: Chunk) -> opentelemetry.context.Context:
    return tracing.carrier_to_opentelemetry_tracecontext(
        chunk.submission_otel_trace_carrier
    )
