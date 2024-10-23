from typing import Any, Iterable, Iterator
import itertools

import json

import opsqueue_producer.opsqueue_producer_internal
from opsqueue_producer.opsqueue_producer_internal import SubmissionId
from opsqueue_producer.opsqueue_producer_internal import SubmissionStatus

class Client():
    """
    Opsqueue producer client. Allows sending of large collections of operations ('submissions')
    and waiting for all of them to be done.
    """
    __slots__ = 'inner'
    def __init__(self, opsqueue_url: str, object_store_url: str):
        self.inner = opsqueue_producer_internal.Client(opsqueue_url, object_store_url)

    # TODO: Make serialization format customizable
    def run_submission(self, ops: Iterable[Any], chunk_size: int, metadata: None | bytes = None) -> Iterator[bytes]:
        """
        Inserts a submission into the queue, and blocks until it is completed.

        Chunking is done automatically, based on the provided chunk size.

        If the submission fails, an exception will be raised.
        (If opsqueue or the object storage cannot be reached, exceptions will also be raised).
        """
        results_iter = self.run_submission_chunks(_chunk_iterator(ops, chunk_size), metadata)
        return _unchunk_iterator(results_iter)

    # TODO: Make serialization format customizable
    def insert_submission(self, ops: Iterable[Any], chunk_size: int, metadata: None | bytes = None) -> SubmissionId:
        """
        Inserts a submission into the queue, 
        returning an ID you can use to track the submission's progress afterwards.

        Chunking is done automatically, based on the provided chunk size.
        """
        return self.insert_submission_chunks(_chunk_iterator(ops, chunk_size), metadata)


    def stream_completed_submission(self, submission_id: SubmissionId) -> Iterator[Any]:
        """
        Returns the operation-results of a completed submission, as an iterator that lazily 
        looks up each of the chunk-results one by one from the object storage.

        Will raise an exception if the submission was not completed yet.
        (use `get_submission_status` or `is_completed` to check for this.)
        """
        return _unchunk_iterator(self.inner.stream_completed_submission(submission_id))


    def run_submission_chunks(self, chunk_contents: Iterable[bytes], metadata: None | bytes = None) -> Iterator[bytes]:
        """
        Inserts an already-chunked submission into the queue, and blocks until it is completed.

        If the submission fails, an exception will be raised.
        (If opsqueue or the object storage cannot be reached, exceptions will also be raised).
        """
        return self.inner.run_submission_chunks(chunk_contents, metadata = metadata)

    def insert_submission_chunks(self, chunk_contents: Iterable[bytes], metadata: None | bytes = None) -> SubmissionId:
        """
        Inserts an already-chunked submission into the queue, 
        returning an ID you can use to track the submission's progress afterwards.
        """
        return self.inner.insert_submission_chunks(iter(chunk_contents), metadata = metadata)

    def stream_completed_submission_chunks(self, submission_id: SubmissionId) -> Iterator[bytes]:
        """
        Returns the chunk-results of a completed submission, as an iterator that lazily 
        looks up the chunk-results one by one from the object storage.

        Will raise an exception if the submission was not completed yet.
        (use `get_submission_status` or `is_completed` to check for this.)
        """
        return self.inner.stream_completed_submission(submission_id)

    def count_submissions(self) -> int:
        """
        Returns the number of active submissions in the queue.

        (This does not include completed or failed submissions.)
        """
        return self.inner.count_submissions()

    def get_submission_status(self, submission_id: SubmissionId) -> SubmissionStatus | None:
        """
        Retrieve the status (in progress, completed, or failed) of a specific submission.

        The returned SubmissionStatus object also includes the number of chunks finished so far,
        timestamps indicating when the submission was started/completed/failed,
        and the metadata submitted earlier.

        This call does not on its own fetch the results of aa (completed) submission.
        """
        return self.inner.get_submission_status(submission_id)

    def is_completed(self, submission_id: SubmissionId) -> bool:
        raise NotImplementedError


def _chunk_iterator(iter: Iterable[Any], chunk_size: int) -> Iterator[bytes]:
    return map(_encode_chunk, itertools.batched(iter, chunk_size))

def _unchunk_iterator(encoded_chunks_iter: Iterable[bytes]) -> Iterator[Any]:
    return _flatten_iterator(map(_decode_chunk, encoded_chunks_iter))

    chunks_iterator = map(_encode_chunk, itertools.batched(ops, chunk_size))
    encoded_chunk_results_iterator = self.run_submission_chunks(chunks_iterator, metadata)
    chunk_results_iterator = map(_decode_chunk, encoded_chunk_results_iterator)


def _encode_chunk(chunk: tuple[Any]) -> bytes:
    return json.dumps(chunk).encode()

def _decode_chunk(chunk: bytes) -> tuple[Any]:
    return json.loads(chunk.decode())


def _flatten_iterator(iter_of_iters: Iterable[Iterable[bytes]]) -> Iterator[bytes]:
    "Flatten one level of nesting."
    return itertools.chain.from_iterable(iter_of_iters)
