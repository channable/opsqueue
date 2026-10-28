from __future__ import annotations

import itertools
from collections.abc import AsyncIterator, Iterable, Iterator
from typing import Any, cast

from opentelemetry import trace

from opsqueue.common import (
    DEFAULT_SERIALIZATION_FORMAT,
    SerializationFormat,
    decode_chunk,
    encode_chunk,
)
from opsqueue.exceptions import (
    SubmissionFailedError,
    SubmissionNotCancellableError,
    SubmissionNotFoundError,
    TooManyMatchingSubmissionsError,
)

from . import opsqueue_internal, tracing
from .opsqueue_internal import (  # type: ignore[import-not-found]
    ChunkFailed,
    InitialSubmissionStatus,
    SubmissionCompleted,
    SubmissionFailed,
    SubmissionId,
    SubmissionNotCancellable,
    SubmissionPaused,
    SubmissionStatus,
)

__all__ = [
    "ChunkFailed",
    "InitialSubmissionStatus",
    "ProducerClient",
    "StreamingProducerClient",
    "SubmissionCompleted",
    "SubmissionFailed",
    "SubmissionFailedError",
    "SubmissionId",
    "SubmissionNotCancellable",
    "SubmissionNotCancellableError",
    "SubmissionNotFoundError",
    "SubmissionPaused",
    "SubmissionStatus",
    "TooManyMatchingSubmissionsError",
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
        strategic_metadata: None | dict[str, int] = None,
        timeout: float | None = None,
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
                timeout=timeout,
            )
            return _unchunk_iterator(results_iter, serialization_format)

    async def async_run_submission(
        self,
        ops: Iterable[Any],
        *,
        chunk_size: int,
        serialization_format: SerializationFormat = DEFAULT_SERIALIZATION_FORMAT,
        metadata: None | bytes = None,
        strategic_metadata: None | dict[str, int] = None,
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
        strategic_metadata: None | dict[str, int] = None,
        initial_status: InitialSubmissionStatus = InitialSubmissionStatus.InProgress,
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
            strategic_metadata=strategic_metadata,
            chunk_size=chunk_size,
            initial_status=initial_status,
        )

    def blocking_stream_completed_submission(
        self,
        submission_id: SubmissionId,
        *,
        serialization_format: SerializationFormat = DEFAULT_SERIALIZATION_FORMAT,
        timeout: float | None = None,
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
            self.blocking_stream_completed_submission_chunks(submission_id, timeout),
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
        strategic_metadata: None | dict[str, int] = None,
        chunk_size: None | int = None,
        timeout: float | None = None,
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
        return self.blocking_stream_completed_submission_chunks(submission_id, timeout)

    async def async_run_submission_chunks(
        self,
        chunk_contents: Iterable[bytes],
        *,
        metadata: None | bytes = None,
        strategic_metadata: None | dict[str, int] = None,
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
        strategic_metadata: None | dict[str, int] = None,
        chunk_size: None | int = None,
        initial_status: InitialSubmissionStatus = InitialSubmissionStatus.InProgress,
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
            initial_status=initial_status,
        )

    def blocking_stream_completed_submission_chunks(
        self,
        submission_id: SubmissionId,
        timeout: float | None = None,
    ) -> Iterator[bytes]:
        """
        Blocks until the submission is completed, and returns an iterator that lazily
        looks up the chunk-results one by one from the object storage.

        Raises:
        - `InternalProducerClientError` if there is a low-level internal error.
        - `SubmissionFailedError` if the submission failed permanently
          (after retrying a consumer kept failing on one of the chunks)
        """
        return self.inner.blocking_stream_completed_submission_chunks(  # type: ignore[no-any-return]
            submission_id, timeout
        )

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

    def cancel_submission(self, submission_id: SubmissionId) -> None:
        """
        Cancel a specific submission that is in progress or paused.

        Returns None if the submission was successfully cancelled.

        Raises:
        - `SubmissionNotCancellableError` if the submission could not be
          cancelled because it was already completed, failed or cancelled.
        - `SubmissionNotFoundError` if the submission could not be found.
        """
        self.inner.cancel_submission(submission_id)

    def unpause_submission(self, submission_id: SubmissionId) -> None:
        """
        Unpause a specific submission that is currently paused,
        making it available to consumers.

        Returns None if the submission was successfully unpaused.

        Raises:
        - `SubmissionNotFoundError` if the submission is not currently paused.
        - `InternalProducerClientError` if there is a low-level internal error.
        """
        self.inner.unpause_submission(submission_id)

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
        (AKA the path at which the submission's chunks are stored in the object store)
        is known.

        Returns `None` if no submission could be found.

        Raises:
        - `InternalProducerClientError` if there is a low-level internal error.
        """
        return self.inner.lookup_submission_id_by_prefix(prefix)

    def lookup_submission_ids_by_strategic_metadata(
        self, strategic_metadata: dict[str, int]
    ) -> list[SubmissionId]:
        """Attempts to find in-progress submissions where the strategic metadata
        of that submission includes all of the key-value pairs of the given
        'strategic_metadata'. A matching submission must include all of the
        given key-value pairs, but it may also contain other key-value pairs.

        Raises:
        - `TooManyMatchingSubmissionsError` if the lookup matches more
          submissions than the server's configured maximum. Narrow the query
          with more specific strategic metadata.
        - `InternalProducerClientError` if there is a low-level internal error.

        """
        return self.inner.lookup_submission_ids_by_strategic_metadata(  # type: ignore[no-any-return]
            strategic_metadata
        )

    def is_completed(self, submission_id: SubmissionId) -> bool:
        raise NotImplementedError


class StreamingProducerClient(ProducerClient):
    """
    Producer client helpers for streaming work across independent submissions.
    """

    __slots__ = ()

    def insert_submission_group(
        self,
        ops: Iterable[Any],
        *,
        submission_size: int,
        chunk_size: int,
        serialization_format: SerializationFormat = DEFAULT_SERIALIZATION_FORMAT,
        metadata: None | bytes = None,
        strategic_metadata: None | dict[str, int] = None,
        initial_status: InitialSubmissionStatus = InitialSubmissionStatus.InProgress,
    ) -> list[SubmissionId]:
        """
        Insert all operations as independent submissions before returning.

        Each submission contains at most `submission_size` operations, which
        are chunked using `chunk_size`. Each batch is an independent
        submission. The final submission may be smaller; empty input yields an
        empty list. All IDs are returned only after every batch is inserted.
        """
        if submission_size <= 0:
            raise ValueError("submission_size must be a positive integer")

        submission_ids = []
        for batch in itertools.batched(ops, submission_size):
            submission_ids.append(
                self.insert_submission(
                    batch,
                    chunk_size=chunk_size,
                    serialization_format=serialization_format,
                    metadata=metadata,
                    strategic_metadata=strategic_metadata,
                    initial_status=initial_status,
                )
            )

        return submission_ids

    async def async_insert_submission_group(
        self,
        ops: Iterable[Any],
        *,
        submission_size: int,
        chunk_size: int,
        serialization_format: SerializationFormat = DEFAULT_SERIALIZATION_FORMAT,
        metadata: None | bytes = None,
        strategic_metadata: None | dict[str, int] = None,
        initial_status: InitialSubmissionStatus = InitialSubmissionStatus.InProgress,
    ) -> AsyncIterator[SubmissionId]:
        """
        Async-iterator version of `insert_submission_group`.

        All batches are inserted during this call, before the async iterator is
        returned. The underlying insertion calls remain synchronous.
        """
        submission_ids = self.insert_submission_group(
            ops,
            submission_size=submission_size,
            chunk_size=chunk_size,
            serialization_format=serialization_format,
            metadata=metadata,
            strategic_metadata=strategic_metadata,
            initial_status=initial_status,
        )

        async def async_submission_ids() -> AsyncIterator[SubmissionId]:
            for submission_id in submission_ids:
                yield submission_id

        return async_submission_ids()

    async def async_stream_completed_submission_group(
        self, submission_ids: Iterable[SubmissionId]
    ) -> AsyncIterator[bytes]:
        """
        Stream completed submissions in the supplied ID order.

        Each submission is awaited before the next one is streamed. Chunk order
        within each submission is preserved by
        `async_stream_completed_submission_chunks`.
        """

        async def stream_group() -> AsyncIterator[bytes]:
            for submission_id in submission_ids:
                chunks = await self.async_stream_completed_submission_chunks(
                    submission_id
                )
                async for chunk in chunks:
                    yield chunk

        return stream_group()

    async def async_count_completed_chunks(
        self, submission_ids: Iterable[SubmissionId]
    ) -> int:
        """
        Count successfully completed chunks across distinct submissions.

        Partial progress is included for in-progress, paused, cancelled, and
        failed submissions. A failed submission with an unknown completed
        chunk count contributes zero.

        Raises `SubmissionNotFoundError` if any ID cannot be found.
        """
        completed_chunks = 0
        seen_submission_ids: set[SubmissionId] = set()
        for submission_id in submission_ids:
            if submission_id in seen_submission_ids:
                continue
            seen_submission_ids.add(submission_id)

            status = self.get_submission_status(submission_id)
            if status is None:
                raise SubmissionNotFoundError(submission_id.id)

            if isinstance(status, SubmissionStatus.Completed):
                completed_chunks += status.submission.chunks_total
            elif isinstance(status, SubmissionStatus.Failed):
                completed_chunks += status.submission.chunks_done or 0
            else:
                completed_chunks += status.submission.chunks_done

        return completed_chunks


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
