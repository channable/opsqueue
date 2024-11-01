from __future__ import annotations
from collections.abc import Iterable, Iterator
from typing import Any

import itertools

from opsqueue.common import (
    SerializationFormat,
    encode_chunk,
    decode_chunk,
    DEFAULT_SERIALIZATION_FORMAT,
)
from . import opsqueue_internal
from .opsqueue_internal import SubmissionId, SubmissionStatus  # type: ignore[import-not-found]


class ProducerClient:
    """
    Opsqueue producer client. Allows sending of large collections of operations ('submissions')
    and waiting for all of them to be done.
    """

    __slots__ = "inner"

    def __init__(self, opsqueue_url: str, object_store_url: str):
        """
        Creates a new producer client.

        Raises `NewObjectStoreClientError` when the given `object_store_url` is incorrect.
        """
        self.inner = opsqueue_internal.ProducerClient(opsqueue_url, object_store_url)

    def run_submission(
        self,
        ops: Iterable[Any],
        *,
        chunk_size: int,
        serialization_format: SerializationFormat = DEFAULT_SERIALIZATION_FORMAT,
        metadata: None | bytes = None,
    ) -> Iterator[bytes]:
        """
        Inserts a submission into the queue, and blocks until it is completed.

        Chunking is done automatically, based on the provided chunk size.

        If the submission fails, an exception will be raised.
        (If opsqueue or the object storage cannot be reached, exceptions will also be raised).

        Raises:
        - `ChunkSizeIsZeroError` if passing an incorrect chunk size of zero;
        - `InternalProducerClientError` if there is a low-level internal error.
        """
        results_iter = self.run_submission_chunks(
            _chunk_iterator(ops, chunk_size, serialization_format), metadata=metadata
        )
        return _unchunk_iterator(results_iter, serialization_format)

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
        - `ChunkCountIsZeroError` if passing an empty list of operations;
        - `InternalProducerClientError` if there is a low-level internal error.
        """
        return self.insert_submission_chunks(
            _chunk_iterator(ops, chunk_size, serialization_format), metadata=metadata
        )

    def stream_completed_submission(
        self,
        submission_id: SubmissionId,
        *,
        serialization_format: SerializationFormat = DEFAULT_SERIALIZATION_FORMAT,
    ) -> Iterator[Any]:
        """
        Returns the operation-results of a completed submission, as an iterator that lazily
        looks up each of the chunk-results one by one from the object storage.

        Raises:
        - TODO `SubmissionNotCompletedYet` if the submission you want to stream is not in the completed state.
        - `InternalProducerClientError` if there is a low-level internal error.
        """
        return _unchunk_iterator(
            self.inner.stream_completed_submission(submission_id), serialization_format
        )

    def run_submission_chunks(
        self, chunk_contents: Iterable[bytes], *, metadata: None | bytes = None
    ) -> Iterator[bytes]:
        """
        Inserts an already-chunked submission into the queue, and blocks until it is completed.

        If the submission fails, an exception will be raised.
        (If opsqueue or the object storage cannot be reached, exceptions will also be raised).

        Raises:
        - `ChunkCountIsZeroError` if passing an empty list of operations;
        - `InternalProducerClientError` if there is a low-level internal error.
        - TODO special exception for when the submission fails.
        """
        return self.inner.run_submission_chunks(chunk_contents, metadata=metadata)  # type: ignore[no-any-return]

    def insert_submission_chunks(
        self, chunk_contents: Iterable[bytes], *, metadata: None | bytes = None
    ) -> SubmissionId:
        """
        Inserts an already-chunked submission into the queue,
        returning an ID you can use to track the submission's progress afterwards.

        Raises:
        - `ChunkCountIsZeroError` if passing an empty list of operations;
        - `InternalProducerClientError` if there is a low-level internal error.
        - TODO special exception for when the submission fails.
        """
        return self.inner.insert_submission_chunks(
            iter(chunk_contents), metadata=metadata
        )

    def stream_completed_submission_chunks(
        self, submission_id: SubmissionId
    ) -> Iterator[bytes]:
        """
        Returns the chunk-results of a completed submission, as an iterator that lazily
        looks up the chunk-results one by one from the object storage.

        Raises:
        - TODO `SubmissionNotCompletedYet` if the submission you want to stream is not in the completed state.
        - `InternalProducerClientError` if there is a low-level internal error.
        """
        return self.inner.stream_completed_submission(submission_id)  # type: ignore[no-any-return]

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
    return _flatten_iterator(
        map(lambda c: decode_chunk(c, serialization_format), encoded_chunks_iter)
    )


def _flatten_iterator(iter_of_iters: Iterable[Iterable[bytes]]) -> Iterator[bytes]:
    "Flatten one level of nesting."
    return itertools.chain.from_iterable(iter_of_iters)


class ChunkSizeIsZeroError(Exception):
    def __str__(self) -> str:
        return "Chunk size must be a positive integer"
