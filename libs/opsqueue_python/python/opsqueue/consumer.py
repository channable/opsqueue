from __future__ import annotations
from collections.abc import Sequence
from typing import Any, Callable

from . import opsqueue_internal
from .opsqueue_internal import Chunk, Strategy, SubmissionId  # type: ignore[import-not-found]
from .common import (
    SerializationFormat,
    encode_chunk,
    decode_chunk,
    DEFAULT_SERIALIZATION_FORMAT,
)

DEFAULT_STRATEGY = Strategy.Newest


class ConsumerClient:
    """
    Opsqueue consumer client. Allows working on individual (chunks of) operations.
    """

    __slots__ = "inner"

    def __init__(self, opsqueue_url: str, object_store_url: str):
        self.inner = opsqueue_internal.ConsumerClient(opsqueue_url, object_store_url)

    def run_each_op(
        self,
        op_callback: Callable[[Any], Any],
        *,
        strategy: Strategy = DEFAULT_STRATEGY,
        serialization_format: SerializationFormat = DEFAULT_SERIALIZATION_FORMAT,
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
        serialization_format: SerializationFormat = DEFAULT_SERIALIZATION_FORMAT,
    ) -> None:
        def raw_chunk_callback(chunk: Chunk) -> bytes:
            chunk_contents = decode_chunk(chunk.input_content, serialization_format)
            chunk_result_contents = chunk_callback(chunk_contents, chunk)
            return encode_chunk(chunk_result_contents, serialization_format)

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

        This function blocks 'forever', except when 'special' exceptions like KeyboardInterrupt are raised.
        Specifically, normal exceptions (inheriting from `Exception`) will be caught and cause
        `fail_chunk` to be called, with the loop afterwards continuing.
        Exceptions inheriting only from `BaseException` will cause the loop to terminate.
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
        """
        self.inner.fail_chunk(submission_id, submission_prefix, chunk_index, failure)
