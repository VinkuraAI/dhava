"""Serialization and binary framing routines for DDIL protocol packets."""

from __future__ import annotations

import hashlib
import json
import struct
from enum import Enum
from typing import Any

import msgpack

# Wire Framing Format:
# [4 bytes MAGIC: b"DDIL"]
# [1 byte VERSION: 0x01]
# [4 bytes PAYLOAD_LENGTH (uint32 big-endian)]
# [32 bytes SHA256 DIGEST of raw payload]
# [N bytes PAYLOAD]

FRAME_MAGIC = b"DDIL"
FRAME_VERSION = 1
FRAME_HEADER_FORMAT = "!4sB I 32s"
FRAME_HEADER_SIZE = struct.calcsize(FRAME_HEADER_FORMAT)  # 4 + 1 + 4 + 32 = 41 bytes


def _default_encoder(obj: Any) -> Any:
    if isinstance(obj, Enum):
        return obj.value
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    if isinstance(obj, (set, frozenset)):
        return list(obj)
    if isinstance(obj, bytes):
        return obj
    raise TypeError(f"Cannot serialize object of type {type(obj)} to msgpack")


def pack_msgpack(data: Any) -> bytes:
    """Serialize Python object to MessagePack bytes."""
    return msgpack.packb(data, default=_default_encoder, use_bin_type=True)


def unpack_msgpack(data: bytes) -> Any:
    """Deserialize MessagePack bytes to Python object."""
    return msgpack.unpackb(data, raw=False)


def frame_payload(payload: bytes) -> bytes:
    """
    Wrap payload in binary envelope with length header and SHA-256 verification digest.
    """
    length = len(payload)
    digest = hashlib.sha256(payload).digest()
    header = struct.pack(FRAME_HEADER_FORMAT, FRAME_MAGIC, FRAME_VERSION, length, digest)
    return header + payload


def unframe_payload(data: bytes) -> tuple[bytes, bytes]:
    """
    Extract a single framed payload from byte stream.
    Returns: (extracted_payload, remaining_bytes)
    Raises ValueError on incomplete data, bad magic, or checksum mismatch.
    """
    if len(data) < FRAME_HEADER_SIZE:
        raise ValueError(f"Incomplete frame header (have {len(data)}, need {FRAME_HEADER_SIZE})")

    magic, version, length, expected_digest = struct.unpack(
        FRAME_HEADER_FORMAT, data[:FRAME_HEADER_SIZE]
    )

    if magic != FRAME_MAGIC:
        raise ValueError(f"Invalid frame magic: {magic!r}")
    if version != FRAME_VERSION:
        raise ValueError(f"Unsupported frame version: {version}")

    total_frame_size = FRAME_HEADER_SIZE + length
    if len(data) < total_frame_size:
        raise ValueError(
            f"Incomplete payload (have {len(data) - FRAME_HEADER_SIZE}, need {length})"
        )

    payload = data[FRAME_HEADER_SIZE:total_frame_size]
    actual_digest = hashlib.sha256(payload).digest()

    if actual_digest != expected_digest:
        raise ValueError("Payload SHA-256 digest mismatch (data corrupted or tampered)")

    remaining = data[total_frame_size:]
    return payload, remaining


def json_dumps(data: Any) -> str:
    """Deterministic JSON string serializer with ISO formatting."""
    return json.dumps(data, sort_keys=True, default=_default_encoder)


def json_loads(data: str) -> Any:
    """JSON string deserializer."""
    return json.loads(data)
