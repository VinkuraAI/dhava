"""Protocol message definitions and serialization for peer sync cycles."""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from utils.serialization import pack_msgpack, unpack_msgpack

PROTOCOL_VERSION = "1.0"


@dataclass
class SyncPushRequest:
    """Push payload transmitted from edge node to peer or server."""

    node_id: str
    session_id: str
    operation_count: int
    payload_size_bytes: int
    payload_hash: str
    compression: str
    encryption: str
    sender_vector_clock: dict[str, int]
    encrypted_payload: bytes
    protocol_version: str = PROTOCOL_VERSION
    timestamp: float = field(default_factory=time.time)

    @classmethod
    def create(
        cls,
        node_id: str,
        sender_vector_clock: dict[str, int],
        encrypted_payload: bytes,
        raw_payload: bytes,
        operation_count: int,
        compression: str = "zstd",
        encryption: str = "aes-256-gcm",
        session_id: str | None = None,
    ) -> SyncPushRequest:
        payload_hash = hashlib.sha256(raw_payload).hexdigest()
        return cls(
            node_id=node_id,
            session_id=session_id or str(uuid.uuid4()),
            operation_count=operation_count,
            payload_size_bytes=len(encrypted_payload),
            payload_hash=payload_hash,
            compression=compression,
            encryption=encryption,
            sender_vector_clock=sender_vector_clock,
            encrypted_payload=encrypted_payload,
            timestamp=time.time(),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "protocol_version": self.protocol_version,
            "node_id": self.node_id,
            "session_id": self.session_id,
            "timestamp": self.timestamp,
            "operation_count": self.operation_count,
            "payload_size_bytes": self.payload_size_bytes,
            "payload_hash": self.payload_hash,
            "compression": self.compression,
            "encryption": self.encryption,
            "sender_vector_clock": self.sender_vector_clock,
            "encrypted_payload": self.encrypted_payload,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SyncPushRequest:
        return cls(
            protocol_version=data.get("protocol_version", PROTOCOL_VERSION),
            node_id=data["node_id"],
            session_id=data["session_id"],
            timestamp=float(data.get("timestamp", time.time())),
            operation_count=int(data["operation_count"]),
            payload_size_bytes=int(data["payload_size_bytes"]),
            payload_hash=data["payload_hash"],
            compression=data.get("compression", "zstd"),
            encryption=data.get("encryption", "aes-256-gcm"),
            sender_vector_clock=dict(data.get("sender_vector_clock", {})),
            encrypted_payload=bytes(data["encrypted_payload"]),
        )

    def serialize(self) -> bytes:
        return pack_msgpack(self.to_dict())

    @classmethod
    def deserialize(cls, raw: bytes) -> SyncPushRequest:
        data = unpack_msgpack(raw)
        return cls.from_dict(data)


@dataclass
class SyncPullResponse:
    """Pull response transmitted back from peer or server to edge node."""

    node_id: str
    session_id: str
    status: str
    operation_count: int
    payload_size_bytes: int
    payload_hash: str
    compression: str
    encryption: str
    acked_op_ids: list[str]
    rejected_op_ids: dict[str, str]
    encrypted_payload: bytes
    error: str | None = None
    protocol_version: str = PROTOCOL_VERSION
    timestamp: float = field(default_factory=time.time)

    @classmethod
    def create(
        cls,
        node_id: str,
        session_id: str,
        status: str,
        acked_op_ids: list[str],
        rejected_op_ids: dict[str, str] | None = None,
        encrypted_payload: bytes = b"",
        raw_payload: bytes = b"",
        operation_count: int = 0,
        compression: str = "zstd",
        encryption: str = "aes-256-gcm",
        error: str | None = None,
    ) -> SyncPullResponse:
        payload_hash = hashlib.sha256(raw_payload).hexdigest() if raw_payload else ""
        return cls(
            node_id=node_id,
            session_id=session_id,
            status=status,
            error=error,
            operation_count=operation_count,
            payload_size_bytes=len(encrypted_payload),
            payload_hash=payload_hash,
            compression=compression,
            encryption=encryption,
            acked_op_ids=acked_op_ids,
            rejected_op_ids=rejected_op_ids or {},
            encrypted_payload=encrypted_payload,
            timestamp=time.time(),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "protocol_version": self.protocol_version,
            "node_id": self.node_id,
            "session_id": self.session_id,
            "timestamp": self.timestamp,
            "status": self.status,
            "error": self.error,
            "operation_count": self.operation_count,
            "payload_size_bytes": self.payload_size_bytes,
            "payload_hash": self.payload_hash,
            "compression": self.compression,
            "encryption": self.encryption,
            "acked_op_ids": self.acked_op_ids,
            "rejected_op_ids": self.rejected_op_ids,
            "encrypted_payload": self.encrypted_payload,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SyncPullResponse:
        return cls(
            protocol_version=data.get("protocol_version", PROTOCOL_VERSION),
            node_id=data["node_id"],
            session_id=data["session_id"],
            timestamp=float(data.get("timestamp", time.time())),
            status=data["status"],
            error=data.get("error"),
            operation_count=int(data.get("operation_count", 0)),
            payload_size_bytes=int(data.get("payload_size_bytes", 0)),
            payload_hash=data.get("payload_hash", ""),
            compression=data.get("compression", "zstd"),
            encryption=data.get("encryption", "aes-256-gcm"),
            acked_op_ids=list(data.get("acked_op_ids", [])),
            rejected_op_ids=dict(data.get("rejected_op_ids", {})),
            encrypted_payload=bytes(data.get("encrypted_payload", b"")),
        )

    def serialize(self) -> bytes:
        return pack_msgpack(self.to_dict())

    @classmethod
    def deserialize(cls, raw: bytes) -> SyncPullResponse:
        data = unpack_msgpack(raw)
        return cls.from_dict(data)
