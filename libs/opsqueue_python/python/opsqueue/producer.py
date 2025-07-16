from __future__ import annotations
from collections.abc import Iterable, Iterator, AsyncIterator
from typing import Any, cast

import itertools

from opentelemetry import trace

from opsqueue.common import (
    SerializationFormat,
    encode_chunk,
    decode_chunk,
    DEFAULT_SERIALIZATION_FORMAT,
)
from . import opsqueue_internal
from . import tracing
from opsqueue.exceptions import SubmissionFailedError
from .opsqueue_internal import (  # type: ignore[import-not-found]
    SubmissionId,
    SubmissionStatus,
    SubmissionFailed,
    ChunkFailed,
)

__all__ = [
    "ProducerClient",
    "SubmissionId",
    "SubmissionStatus",
    "SubmissionFailedError",
    "SubmissionFailed",
    "ChunkFailed",
]


class ProducerClient:
    """
    Opsqueue producer client. Allows sending of large collections of operations ('submissions')
    and waiting for them to be completed.

    Can also be used for basic introspection/debugging/maintenance of an opsqueue queue.
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
        Creates a new producer client.

        - opsqueue_url: URL at which the opsqueue binary can be reached.

        - object_store_url: URL to reach the object store in which chunks are stored.
            Use `file:///some/local/path` for local testing.
            Use `gs://bucket/path` for a GCS bucket.
            See https://docs.rs/object_store/0.11.1/object_store/enum.ObjectStoreScheme.html for details.

        Raises `NewObjectStoreClientError` when the given `object_store_url` is incorrect.
        """
        self.inner = opsqueue_internal.ProducerClient(
            opsqueue_url, object_store_url, object_store_options
        )

    def __repr__(self) -> str:
        return cast(str, self.inner.__repr__())

    def server_version(self) -> str:
        """
        Ask the Opsqueue server/service to return its version information as a string.

        This is mainly useful for debugging
        """
        return cast(str, self.inner.server_version())

    def run_submission(
        self,
        ops: Iterable[Any],
        *,
        chunk_size: int,
        serialization_format: SerializationFormat = DEFAULT_SERIALIZATION_FORMAT,
        metadata: None | bytes = None,
        strategic_metadata: None | dict[str, str | int] = None,
    ) -> Iterator[Any]:
        """
        Inserts a submission into the queue, and blocks until it is completed.

        Chunking is done automatically, based on the provided chunk size.

        If the submission fails, an exception will be raised.
        (If opsqueue or the object storage cannot be reached, exceptions will also be raised).

        Raises:
        - `ChunkSizeIsZeroError` if passing an incorrect chunk size of zero;
        - `InternalProducerClientError` if there is a low-level internal error.
        """
        tracer = trace.get_tracer("opsqueue.producer")
        with tracer.start_as_current_span("run_submission"):
            results_iter = self.run_submission_chunks(
                _chunk_iterator(ops, chunk_size, serialization_format),
                metadata=metadata,
                strategic_metadata=strategic_metadata,
                chunk_size=chunk_size,
            )
            return _unchunk_iterator(results_iter, serialization_format)

    async def async_run_submission(
        self,
        ops: Iterable[Any],
        *,
        chunk_size: int,
        serialization_format: SerializationFormat = DEFAULT_SERIALIZATION_FORMAT,
        metadata: None | bytes = None,
        strategic_metadata: None | dict[str, str | int] = None,
    ) -> AsyncIterator[Any]:
        tracer = trace.get_tracer("opsqueue.producer")
        with tracer.start_as_current_span("run_submission"):
            results_iter = await self.async_run_submission_chunks(
                _chunk_iterator(ops, chunk_size, serialization_format),
                metadata=metadata,
                strategic_metadata=strategic_metadata,
                chunk_size=chunk_size,
            )
            return _async_unchunk_iterator(results_iter, serialization_format)

    def insert_submission(
        self,
        ops: Iterable[Any],
        *,
        chunk_size: int,
        serialization_format: SerializationFormat = DEFAULT_SERIALIZATION_FORMAT,
        metadata: None | bytes = None,
    ) -> SubmissionId:
        """
        Inserts a submission into the queue,
        returning an ID you can use to track the submission's progress afterwards.

        Chunking is done automatically, based on the provided chunk size.

        Raises:
        - `ChunkSizeIsZeroError` if passing an incorrect chunk size of zero;
        - `InternalProducerClientError` if there is a low-level internal error.
        """
        return self.insert_submission_chunks(
            _chunk_iterator(ops, chunk_size, serialization_format),
            metadata=metadata,
            chunk_size=chunk_size,
        )

    def blocking_stream_completed_submission(
        self,
        submission_id: SubmissionId,
        *,
        serialization_format: SerializationFormat = DEFAULT_SERIALIZATION_FORMAT,
    ) -> Iterator[Any]:
        """
        Blocks until the submission is completed.
        Then, returns the operation-results, as an iterator that lazily
        looks up each of the chunk-results one by one from the object storage.

        Raises:
        - `InternalProducerClientError` if there is a low-level internal error.
        - `SubmissionFailedError` if the submission failed permanently
          (after retrying a consumer kept failing on one of the chunks)
        """
        return _unchunk_iterator(
            self.blocking_stream_completed_submission_chunks(submission_id),
            serialization_format,
        )

    def try_stream_completed_submission(
        self,
        submission_id: SubmissionId,
        *,
        serialization_format: SerializationFormat = DEFAULT_SERIALIZATION_FORMAT,
    ) -> Iterator[Any]:
        """
        Returns the operation-results of a completed submission, as an iterator that lazily
        looks up each of the chunk-results one by one from the object storage.

        Raises:
        - `SubmissionNotCompletedYet` if the submission you want to stream is not in the completed state.
        - `InternalProducerClientError` if there is a low-level internal error.
        """
        return _unchunk_iterator(
            self.try_stream_completed_submission_chunks(submission_id),
            serialization_format,
        )

    def run_submission_chunks(
        self,
        chunk_contents: Iterable[bytes],
        *,
        metadata: None | bytes = None,
        strategic_metadata: None | dict[str, str | int] = None,
        chunk_size: None | int = None,
    ) -> Iterator[bytes]:
        """
        Inserts an already-chunked submission into the queue, and blocks until it is completed.

        If the submission fails, an exception will be raised.
        (If opsqueue or the object storage cannot be reached, exceptions will also be raised).

        Raises:
        - `InternalProducerClientError` if there is a low-level internal error.
        - `SubmissionFailedError` if the submission failed permanently
          (after retrying a consumer kept failing on one of the chunks)
        """
        submission_id = self.insert_submission_chunks(
            chunk_contents,
            metadata=metadata,
            strategic_metadata=strategic_metadata,
            chunk_size=chunk_size,
        )
        return self.blocking_stream_completed_submission_chunks(submission_id)

    async def async_run_submission_chunks(
        self,
        chunk_contents: Iterable[bytes],
        *,
        metadata: None | bytes = None,
        strategic_metadata: None | dict[str, str | int] = None,
        chunk_size: None | int = None,
    ) -> AsyncIterator[bytes]:
        # NOTE: the insertion is not currently async.
        # Why? Simplicity. This is unlikely to be the bottleneck
        # for most async apps.
        # If it does cause a problem in the future this can be revisited
        submission_id = self.insert_submission_chunks(
            chunk_contents,
            metadata=metadata,
            strategic_metadata=strategic_metadata,
            chunk_size=chunk_size,
        )

        return await self.async_stream_completed_submission_chunks(submission_id)

    def insert_submission_chunks(
        self,
        chunk_contents: Iterable[bytes],
        *,
        metadata: None | bytes = None,
        strategic_metadata: None | dict[str, str | int] = None,
        chunk_size: None | int = None,
    ) -> SubmissionId:
        """
        Inserts an already-chunked submission into the queue,
        returning an ID you can use to track the submission's progress afterwards.

        Raises:
        - `InternalProducerClientError` if there is a low-level internal error.
        """
        otel_trace_carrier = tracing.current_opentelemetry_tracecontext_to_carrier()

        return self.inner.insert_submission_chunks(
            iter(chunk_contents),
            metadata=metadata,
            strategic_metadata=strategic_metadata,
            chunk_size=chunk_size,
            otel_trace_carrier=otel_trace_carrier,
        )

    def blocking_stream_completed_submission_chunks(
        self, submission_id: SubmissionId
    ) -> Iterator[bytes]:
        """
        Blocks until the submission is completed, and returns an iterator that lazily
        looks up the chunk-results one by one from the object storage.

        Raises:
        - `InternalProducerClientError` if there is a low-level internal error.
        - `SubmissionFailedError` if the submission failed permanently
          (after retrying a consumer kept failing on one of the chunks)
        """
        return self.inner.blocking_stream_completed_submission_chunks(submission_id)  # type: ignore[no-any-return]

    async def async_stream_completed_submission_chunks(
        self, submission_id: SubmissionId
    ) -> AsyncIterator[bytes]:
        return await self.inner.async_stream_completed_submission_chunks(submission_id)  # type: ignore[no-any-return]

    def try_stream_completed_submission_chunks(
        self, submission_id: SubmissionId
    ) -> Iterator[bytes]:
        """
        Non-blocking version of `blocking_stream_completed_submission_chunks`.
        Will fail with a `SubmissionNotCompletedYet` exception
        if called before the submission is completed.

        Returns the chunk-results of a completed submission, as an iterator that lazily
        looks up the chunk-results one by one from the object storage.

        Raises:
        - `SubmissionNotCompletedYet` if the submission you want to stream is not in the completed state.
        - `InternalProducerClientError` if there is a low-level internal error.
        """
        return self.inner.try_stream_completed_submission_chunks(submission_id)  # type: ignore[no-any-return]

    def count_submissions(self) -> int:
        """
        Returns the number of active submissions in the queue.

        (This does not include completed or failed submissions.)

        Raises:
        - `InternalProducerClientError` if there is a low-level internal error.
        """
        return self.inner.count_submissions()  # type: ignore[no-any-return]

    def get_submission_status(
        self, submission_id: SubmissionId
    ) -> SubmissionStatus | None:
        """
        Retrieve the status (in progress, completed, or failed) of a specific submission.

        Returns `None` if no submission for the given ID can be found.

        The returned SubmissionStatus object also includes the number of chunks finished so far,
        timestamps indicating when the submission was started/completed/failed,
        and the metadata submitted earlier.

        This call does not on its own fetch the results of a (completed) submission.

        Raises:
        - `InternalProducerClientError` if there is a low-level internal error.
        """
        return self.inner.get_submission_status(submission_id)

    def lookup_submission_id_by_prefix(self, prefix: str) -> SubmissionId | None:
        """
        Attempts to find the submission ID if only the prefix of the submission
        (AKA the path at which the submision's chunks are stored in the object store)
        is known.

        Returns `None` if no submission could be found.

        Raises:
        - `InternalProducerClientError` if there is a low-level internal error.
        """
        return self.inner.lookup_submission_id_by_prefix(prefix)

    def is_completed(self, submission_id: SubmissionId) -> bool:
        raise NotImplementedError


def _chunk_iterator(
    iter: Iterable[Any], chunk_size: int, serialization_format: SerializationFormat
) -> Iterator[bytes]:
    if chunk_size <= 0:
        raise ChunkSizeIsZeroError
    return map(
        lambda c: encode_chunk(c, serialization_format),
        itertools.batched(iter, chunk_size),
    )


def _unchunk_iterator(
    encoded_chunks_iter: Iterable[bytes], serialization_format: SerializationFormat
) -> Iterator[Any]:
    for chunk in encoded_chunks_iter:
        ops = decode_chunk(chunk, serialization_format)
        for op in ops:
            yield op


async def _async_unchunk_iterator(
    encoded_chunks_iter: AsyncIterator[bytes], serialization_format: SerializationFormat
) -> AsyncIterator[Any]:
    async for chunk in encoded_chunks_iter:
        ops = decode_chunk(chunk, serialization_format)
        for op in ops:
            yield op


class ChunkSizeIsZeroError(Exception):
    def __str__(self) -> str:
        return "Chunk size must be a positive integer"
