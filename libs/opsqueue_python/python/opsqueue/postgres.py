from __future__ import annotations

import json
import time
import traceback
import uuid
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import opentelemetry.trace

from . import common, tracing
from .opsqueue_internal import ChunkIndex, InitialSubmissionStatus, SubmissionId


class PostgresSubmissionFailedError(RuntimeError):
    def __init__(self, submission_id: SubmissionId, chunk_index: int, failure: str):
        self.submission_id = submission_id
        self.chunk_index = chunk_index
        self.failure = failure
        super().__init__(
            f"Submission {submission_id.id} failed on chunk {chunk_index}: {failure}"
        )


@dataclass(frozen=True, slots=True)
class PostgresChunk:
    submission_id: SubmissionId
    chunk_index: ChunkIndex
    input_content: bytes
    retries: int
    submission_prefix: str | None
    submission_metadata: bytes | None
    submission_otel_trace_carrier: dict[str, str]


def _connect(dsn: str) -> Any:
    try:
        import psycopg
    except ImportError as error:
        raise ImportError(
            "PostgreSQL clients require Psycopg 3; install opsqueue[postgres]"
        ) from error
    return psycopg.connect(dsn, autocommit=True)


def _jsonb(value: dict[str, int]) -> Any:
    from psycopg.types.json import Jsonb

    return Jsonb(value)


def _initial_status(value: InitialSubmissionStatus | str) -> str:
    if value == InitialSubmissionStatus.Paused or value == "paused":
        return "paused"
    if value == InitialSubmissionStatus.InProgress or value == "in_progress":
        return "in_progress"
    raise ValueError("initial_status must be 'in_progress' or 'paused'")


class PostgresProducerClient:
    """Producer backed directly by the Opsqueue PostgreSQL extension."""

    def __init__(self, dsn: str, *, poll_interval: float = 0.5):
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        self._connection = _connect(dsn)
        self._poll_interval = poll_interval

    def __repr__(self) -> str:
        return "<opsqueue.PostgresProducerClient>"

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> PostgresProducerClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def insert_submission_chunks(
        self,
        chunk_contents: Iterable[bytes],
        *,
        metadata: bytes | None = None,
        strategic_metadata: dict[str, int] | None = None,
        chunk_size: int | None = None,
        initial_status: InitialSubmissionStatus | str = "in_progress",
    ) -> SubmissionId:
        chunks = list(chunk_contents)
        if any(not isinstance(chunk, bytes) for chunk in chunks):
            raise TypeError("chunk_contents must contain bytes")
        with self._connection.cursor() as cursor:
            cursor.execute(
                """SELECT opsqueue.submit(
                    %s, p_chunk_size => %s, p_metadata => %s,
                    p_strategic_metadata => %s::jsonb,
                    p_initial_status => %s, p_chunk_contents => %s::bytea[],
                    p_trace_context => %s
                )""",
                (
                    len(chunks),
                    1 if chunk_size is None else chunk_size,
                    metadata,
                    _jsonb(strategic_metadata or {}),
                    _initial_status(initial_status),
                    chunks,
                    json.dumps(tracing.current_opentelemetry_tracecontext_to_carrier()),
                ),
            )
            row = cursor.fetchone()
        return SubmissionId(row[0])

    def insert_submission(
        self,
        ops: Iterable[Any],
        *,
        chunk_size: int,
        serialization_format: common.SerializationFormat = common.DEFAULT_SERIALIZATION_FORMAT,
        metadata: bytes | None = None,
        strategic_metadata: dict[str, int] | None = None,
        initial_status: InitialSubmissionStatus | str = "in_progress",
    ) -> SubmissionId:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        chunks = (
            common.encode_chunk(batch, serialization_format)
            for batch in _batches(ops, chunk_size)
        )
        return self.insert_submission_chunks(
            chunks,
            metadata=metadata,
            strategic_metadata=strategic_metadata,
            chunk_size=chunk_size,
            initial_status=initial_status,
        )

    def get_submission_status(
        self, submission_id: SubmissionId
    ) -> dict[str, Any] | None:
        columns = (
            "id",
            "prefix",
            "chunks_total",
            "chunks_done",
            "chunks_ready",
            "chunk_size",
            "metadata",
            "strategic_metadata",
            "otel_trace_carrier",
            "status",
            "failed_chunk_index",
            "created_at",
            "completed_at",
            "failed_at",
            "cancelled_at",
        )
        with self._connection.cursor() as cursor:
            cursor.execute("SELECT * FROM opsqueue.status(%s)", (submission_id.id,))
            row = cursor.fetchone()
        return None if row is None else dict(zip(columns, row, strict=True))

    def try_stream_completed_submission_chunks(
        self, submission_id: SubmissionId
    ) -> Iterator[bytes]:
        status = self.get_submission_status(submission_id)
        if status is None:
            raise LookupError(f"Submission {submission_id.id} was not found")
        if status["status"] == "failed":
            raise self._submission_failed(submission_id, status)
        if status["status"] != "completed":
            raise RuntimeError(f"Submission {submission_id.id} is not completed")
        with self._connection.cursor() as cursor:
            cursor.execute(
                "SELECT output_content FROM opsqueue.results(%s) ORDER BY chunk_index",
                (submission_id.id,),
            )
            results = cursor.fetchall()
        for (output_content,) in results:
            if output_content is None:
                raise RuntimeError("Completed chunk has no output_content")
            yield output_content

    def blocking_stream_completed_submission_chunks(
        self, submission_id: SubmissionId, timeout: float | None = None
    ) -> Iterator[bytes]:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            status = self.get_submission_status(submission_id)
            if status is None:
                raise LookupError(f"Submission {submission_id.id} was not found")
            if status["status"] == "completed":
                return self.try_stream_completed_submission_chunks(submission_id)
            if status["status"] == "failed":
                raise self._submission_failed(submission_id, status)
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Submission {submission_id.id} did not complete in time"
                )
            time.sleep(self._poll_interval)

    def run_submission_chunks(
        self,
        chunk_contents: Iterable[bytes],
        *,
        metadata: bytes | None = None,
        strategic_metadata: dict[str, int] | None = None,
        chunk_size: int | None = None,
        timeout: float | None = None,
    ) -> Iterator[bytes]:
        submission_id = self.insert_submission_chunks(
            chunk_contents,
            metadata=metadata,
            strategic_metadata=strategic_metadata,
            chunk_size=chunk_size,
        )
        return self.blocking_stream_completed_submission_chunks(submission_id, timeout)

    def run_submission(
        self,
        ops: Iterable[Any],
        *,
        chunk_size: int,
        serialization_format: common.SerializationFormat = common.DEFAULT_SERIALIZATION_FORMAT,
        metadata: bytes | None = None,
        strategic_metadata: dict[str, int] | None = None,
        timeout: float | None = None,
    ) -> Iterator[Any]:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        encoded_results = self.run_submission_chunks(
            (
                common.encode_chunk(batch, serialization_format)
                for batch in _batches(ops, chunk_size)
            ),
            metadata=metadata,
            strategic_metadata=strategic_metadata,
            chunk_size=chunk_size,
            timeout=timeout,
        )
        return _unchunk_results(encoded_results, serialization_format)

    def count_submissions(self) -> int:
        return self._scalar("SELECT opsqueue.count_in_progress()")

    def cancel_submission(self, submission_id: SubmissionId) -> bool:
        return self._scalar("SELECT opsqueue.cancel(%s)", (submission_id.id,))

    def unpause_submission(self, submission_id: SubmissionId) -> bool:
        return self._scalar("SELECT opsqueue.unpause(%s)", (submission_id.id,))

    def lookup_submission_id_by_prefix(self, prefix: str) -> SubmissionId | None:
        value = self._scalar("SELECT opsqueue.lookup_by_prefix(%s)", (prefix,))
        return None if value is None else SubmissionId(value)

    def lookup_submission_ids_by_strategic_metadata(
        self, strategic_metadata: dict[str, int], *, max_results: int = 100_000
    ) -> list[SubmissionId]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                "SELECT opsqueue.lookup_by_metadata(%s::jsonb, %s)",
                (_jsonb(strategic_metadata), max_results),
            )
            return [SubmissionId(row[0]) for row in cursor.fetchall()]

    def is_completed(self, submission_id: SubmissionId) -> bool:
        status = self.get_submission_status(submission_id)
        return status is not None and status["status"] == "completed"

    def _submission_failed(
        self, submission_id: SubmissionId, status: dict[str, Any]
    ) -> PostgresSubmissionFailedError:
        chunk_index = status["failed_chunk_index"]
        with self._connection.cursor() as cursor:
            cursor.execute(
                """SELECT failure FROM opsqueue.failure_history(%s)
                   WHERE chunk_index = %s ORDER BY retry_number DESC LIMIT 1""",
                (submission_id.id, chunk_index),
            )
            row = cursor.fetchone()
        return PostgresSubmissionFailedError(
            submission_id, chunk_index, "unknown failure" if row is None else row[0]
        )

    def _scalar(self, query: str, params: tuple[Any, ...] = ()) -> Any:
        with self._connection.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchone()[0]


class PostgresConsumerClient:
    """Consumer backed directly by the Opsqueue PostgreSQL extension."""

    def __init__(
        self,
        dsn: str,
        *,
        worker_id: uuid.UUID | str | None = None,
        lease_seconds: float = 600,
        max_retries: int = 10,
        poll_interval: float = 0.5,
    ):
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if max_retries <= 0:
            raise ValueError("max_retries must be positive")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        self._connection = _connect(dsn)
        self.worker_id = (
            uuid.UUID(str(worker_id)) if worker_id is not None else uuid.uuid4()
        )
        self.lease = timedelta(seconds=lease_seconds)
        self.max_retries = max_retries
        self._poll_interval = poll_interval

    def __repr__(self) -> str:
        return f"<opsqueue.PostgresConsumerClient(worker_id={self.worker_id})>"

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> PostgresConsumerClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def reserve_chunks(
        self,
        max: int = 1,
        strategy: str = "random",
        *,
        prefer_distinct_keys: Sequence[str] | None = None,
        underlying_strategy: str = "oldest",
    ) -> list[PostgresChunk]:
        if max <= 0:
            raise ValueError("max must be positive")
        with self._connection.cursor() as cursor:
            cursor.execute(
                """SELECT * FROM opsqueue.reserve(
                    %s, %s, %s, %s, %s, %s, %s
                )""",
                (
                    self.worker_id,
                    max,
                    self.lease,
                    strategy,
                    None
                    if prefer_distinct_keys is None
                    else list(prefer_distinct_keys),
                    underlying_strategy,
                    self.max_retries,
                ),
            )
            rows = cursor.fetchall()
        return [self._chunk_from_row(row) for row in rows]

    def complete_chunk(
        self,
        submission_id: SubmissionId,
        submission_prefix: str | None,
        chunk_index: ChunkIndex | int,
        output_content: bytes,
    ) -> None:
        del submission_prefix
        index = chunk_index.id if isinstance(chunk_index, ChunkIndex) else chunk_index
        completed = self._scalar(
            "SELECT opsqueue.complete(%s, %s, %s, %s)",
            (self.worker_id, submission_id.id, index, output_content),
        )
        if not completed:
            raise RuntimeError(
                "Chunk reservation expired or is no longer owned by this worker"
            )

    def fail_chunk(
        self,
        submission_id: SubmissionId,
        submission_prefix: str | None,
        chunk_index: ChunkIndex | int,
        failure: str,
    ) -> None:
        del submission_prefix
        index = chunk_index.id if isinstance(chunk_index, ChunkIndex) else chunk_index
        failed = self._scalar(
            "SELECT opsqueue.fail(%s, %s, %s, %s, %s)",
            (self.worker_id, submission_id.id, index, failure, self.max_retries),
        )
        if not failed:
            raise RuntimeError(
                "Chunk reservation expired or is no longer owned by this worker"
            )

    def renew_chunk(
        self, submission_id: SubmissionId, chunk_index: ChunkIndex | int
    ) -> bool:
        index = chunk_index.id if isinstance(chunk_index, ChunkIndex) else chunk_index
        return self._scalar(
            "SELECT opsqueue.renew(%s, %s, %s, %s)",
            (self.worker_id, submission_id.id, index, self.lease),
        )

    def run_each_chunk_raw(
        self,
        chunk_callback: Callable[[PostgresChunk], bytes],
        *,
        strategy: str = "random",
        prefer_distinct_keys: Sequence[str] | None = None,
        underlying_strategy: str = "oldest",
    ) -> None:
        while True:
            chunks = self.reserve_chunks(
                strategy=strategy,
                prefer_distinct_keys=prefer_distinct_keys,
                underlying_strategy=underlying_strategy,
            )
            if not chunks:
                time.sleep(self._poll_interval)
            for chunk in chunks:
                try:
                    output_content = chunk_callback(chunk)
                except Exception:
                    self.fail_chunk(
                        chunk.submission_id,
                        chunk.submission_prefix,
                        chunk.chunk_index,
                        traceback.format_exc(),
                    )
                else:
                    self.complete_chunk(
                        chunk.submission_id,
                        chunk.submission_prefix,
                        chunk.chunk_index,
                        output_content,
                    )

    def run_each_chunk(
        self,
        chunk_callback: Callable[[Sequence[Any], PostgresChunk], Sequence[Any]],
        *,
        strategy: str = "random",
        prefer_distinct_keys: Sequence[str] | None = None,
        underlying_strategy: str = "oldest",
        serialization_format: common.SerializationFormat = common.DEFAULT_SERIALIZATION_FORMAT,
    ) -> None:
        def process(chunk: PostgresChunk) -> bytes:
            context = tracing.carrier_to_opentelemetry_tracecontext(
                chunk.submission_otel_trace_carrier
            )
            with opentelemetry.trace.get_tracer(
                "opsqueue.consumer"
            ).start_as_current_span(
                "run_chunk", context=context, kind=opentelemetry.trace.SpanKind.CONSUMER
            ):
                operations = common.decode_chunk(
                    chunk.input_content, serialization_format
                )
                return common.encode_chunk(
                    chunk_callback(operations, chunk), serialization_format
                )

        self.run_each_chunk_raw(
            process,
            strategy=strategy,
            prefer_distinct_keys=prefer_distinct_keys,
            underlying_strategy=underlying_strategy,
        )

    def run_each_op(
        self,
        op_callback: Callable[[Any], Any],
        *,
        strategy: str = "random",
        prefer_distinct_keys: Sequence[str] | None = None,
        underlying_strategy: str = "oldest",
        serialization_format: common.SerializationFormat = common.DEFAULT_SERIALIZATION_FORMAT,
    ) -> None:
        self.run_each_chunk(
            lambda operations, _chunk: [
                op_callback(operation) for operation in operations
            ],
            strategy=strategy,
            prefer_distinct_keys=prefer_distinct_keys,
            underlying_strategy=underlying_strategy,
            serialization_format=serialization_format,
        )

    def _chunk_from_row(self, row: tuple[Any, ...]) -> PostgresChunk:
        (
            submission_id,
            chunk_index,
            input_content,
            retries,
            prefix,
            _chunk_size,
            metadata,
            _strategic_metadata,
            trace_carrier,
        ) = row
        if input_content is None:
            raise RuntimeError(
                "This client requires chunk payloads stored in PostgreSQL bytea columns"
            )
        carrier = json.loads(trace_carrier or "{}")
        return PostgresChunk(
            SubmissionId(submission_id),
            ChunkIndex(chunk_index),
            input_content,
            retries,
            prefix,
            metadata,
            carrier,
        )

    def _scalar(self, query: str, params: tuple[Any, ...]) -> Any:
        with self._connection.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchone()[0]


def _batches(values: Iterable[Any], size: int) -> Iterator[list[Any]]:
    batch: list[Any] = []
    for value in values:
        batch.append(value)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def _unchunk_results(
    encoded_chunks: Iterable[bytes], serialization_format: common.SerializationFormat
) -> Iterator[Any]:
    for encoded_chunk in encoded_chunks:
        yield from common.decode_chunk(encoded_chunk, serialization_format)


__all__ = [
    "PostgresChunk",
    "PostgresConsumerClient",
    "PostgresProducerClient",
    "PostgresSubmissionFailedError",
]
