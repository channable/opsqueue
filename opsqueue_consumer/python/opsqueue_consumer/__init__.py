from __future__ import annotations
from collections.abc import Iterable, Iterator, Sequence
from typing import Any, Protocol, Callable

import json

from opsqueue_consumer.opsqueue_consumer_internal import Chunk, Strategy, SubmissionId  # type: ignore[import-not-found]


class json_as_bytes:
    """
    JSON encoding as per the `json` module,
    but making sure that the output type is `bytes` rather than `str`.
    """

    @classmethod
    def dumps(cls, obj: Any) -> bytes:
        return json.dumps(obj).encode()

    @classmethod
    def loads(cls, data: bytes) -> Any:
        return json.loads(data.decode())


DEFAULT_SERIALIZATION_FORMAT: SerializationFormat = json_as_bytes


class Client:
    """
    Opsqueue consumer client. Allows working on individual (chunks of) operations.
    """

    __slots__ = "inner"

    def __init__(self, opsqueue_url: str, object_store_url: str):
        self.inner = opsqueue_consumer_internal.Client(opsqueue_url, object_store_url)  # type: ignore[name-defined] # noqa: F821

    def reserve_chunks(self, max: int, strategy: Strategy) -> list[Chunk]:
        self.inner.reserve_chunks(max, strategy)  # type: ignore[no-any-return]

    def complete_chunk(
        self,
        submission_id: SubmissionId,
        submission_prefix: str,
        chunk_index: int,
        output_content: bytes,
    ):
        self.inner.complete_chunk(
            submission_id, submission_prefix, chunk_index, output_content
        )  # type: ignore[no-any-return]

    def fail_chunk(
        self,
        submission_id: SubmissionId,
        submission_prefix: str,
        chunk_index: int,
        failure: str,
    ):
        self.inner.fail_chunk(submission_id, submission_prefix, chunk_index, failure)  # type: ignore[no-any-return]

    def run_each_op(
        self,
        strategy: Strategy,
        op_callback: Callable[[Chunk], bytes],
        *,
        serialization_format: SerializationFormat = DEFAULT_SERIALIZATION_FORMAT,
    ):
        def chunk_callback(c: Chunk) -> bytes:
            ops = _decode_chunk(c.input_content, serialization_format)
            ops_results = [op_callback(op) for op in ops]
            return _encode_chunk(ops_results, serialization_format)

        self.run_each_chunk_raw(strategy, chunk_callback)

    # TODO: run_each_chunk

    def run_each_chunk_raw(
        self,
        strategy: Strategy,
        chunk_callback: Callable[[Chunk], bytes],
        *,
        serialization_format: SerializationFormat = DEFAULT_SERIALIZATION_FORMAT,
    ):
        self.inner.run_per_chunk(strategy, chunk_callback)  # type: ignore[no-any-return]


def _encode_chunk(
    chunk: Sequence[Any], serialization_format: SerializationFormat
) -> bytes:
    return serialization_format.dumps(chunk)


def _decode_chunk(
    chunk: bytes, serialization_format: SerializationFormat
) -> Sequence[Any]:
    res = serialization_format.loads(chunk)
    assert isinstance(
        res, Sequence
    ), f"Decoding a chunk should always return a sequence, got unexpected type {type(res)}"
    return res


class SerializationFormat(Protocol):
    def dumps(self, obj: Any) -> bytes: ...
    def loads(self, data: bytes) -> Any: ...
