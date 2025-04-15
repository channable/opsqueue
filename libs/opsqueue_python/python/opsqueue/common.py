from __future__ import annotations
from collections.abc import Sequence
from typing import Any, Protocol

import json
import cbor2


class SerializationFormat(Protocol):
    def dumps(self, obj: Any) -> bytes: ...
    def loads(self, data: bytes) -> Any: ...


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


DEFAULT_SERIALIZATION_FORMAT: SerializationFormat = cbor2


def encode_chunk(
    chunk: Sequence[Any], serialization_format: SerializationFormat
) -> bytes:
    return serialization_format.dumps(chunk)


def decode_chunk(
    chunk: bytes, serialization_format: SerializationFormat
) -> Sequence[Any]:
    res = serialization_format.loads(chunk)
    assert isinstance(res, Sequence), (
        f"Decoding a chunk should always return a sequence, got unexpected type {type(res)}"
    )
    return res
