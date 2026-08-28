"""Core data models for DDIL Sync Engine."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Priority(str, Enum):
    """
    Operation priority levels for bandwidth-constrained environments.
    Higher importance corresponds to lower numeric ranking (P0 is highest).
    """
    P0 = "P0"  # CRITICAL: Life-safety, security alerts, directives
    P1 = "P1"  # HIGH: Operational events, duty rosters, workflow updates
    P2 = "P2"  # NORMAL: Routine records, entity updates, audit entries
    P3 = "P3"  # LOW: Media thumbnails, non-urgent telemetry
    P4 = "P4"  # BULK: Full media files, large attachments, archives

    @property
    def level(self) -> int:
        levels = {"P0": 0, "P1": 1, "P2": 2, "P3": 3, "P4": 4}
        return levels[self.value]

    def __lt__(self, other: Priority) -> bool:
        if not isinstance(other, Priority):
            return NotImplemented
        return self.level < other.level

    def __le__(self, other: Priority) -> bool:
        if not isinstance(other, Priority):
            return NotImplemented
        return self.level <= other.level

    def __gt__(self, other: Priority) -> bool:
        if not isinstance(other, Priority):
            return NotImplemented
        return self.level > other.level

    def __ge__(self, other: Priority) -> bool:
        if not isinstance(other, Priority):
            return NotImplemented
        return self.level >= other.level


class OperationType(str, Enum):
    CREATE = "create"  # New record
    UPDATE = "update"  # Full replacement of existing record
    DELETE = "delete"  # Soft delete (tombstone)
    MERGE = "merge"    # Partial update of specified fields


class SyncState(str, Enum):
    PENDING = "pending"        # In outbox, awaiting transmission
    SYNCED = "synced"          # Successfully acknowledged by remote peer
    FAILED = "failed"          # Transmission failed, pending retry
    CONFLICTED = "conflicted"  # Concurrent modification detected


@dataclass
class Operation:
    """The atomic unit of state change captured in the outbox."""

    op_id: str
    node_id: str
    op_type: OperationType
    collection: str
    record_id: str
    payload: dict[str, Any]
    timestamp: float
    vector_clock: dict[str, int]
    lamport_timestamp: int
    priority: Priority = Priority.P2
    user_id: str | None = None
    authority: str | None = None
    source_module: str = "app"
    sync_state: SyncState = SyncState.PENDING
    synced_at: float | None = None
    sync_attempts: int = 0
    last_error: str | None = None
    enqueued_at: float = field(default_factory=time.time)

    @classmethod
    def create(
        cls,
        node_id: str,
        op_type: OperationType,
        collection: str,
        record_id: str,
        payload: dict[str, Any],
        vector_clock: dict[str, int],
        lamport_timestamp: int,
        priority: Priority = Priority.P2,
        user_id: str | None = None,
        authority: str | None = None,
        source_module: str = "app",
        timestamp: float | None = None,
    ) -> Operation:
        return cls(
            op_id=str(uuid.uuid4()),
            node_id=node_id,
            op_type=op_type,
            collection=collection,
            record_id=record_id,
            payload=payload,
            timestamp=timestamp if timestamp is not None else time.time(),
            vector_clock=vector_clock,
            lamport_timestamp=lamport_timestamp,
            priority=priority,
            user_id=user_id,
            authority=authority,
            source_module=source_module,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "op_id": self.op_id,
            "node_id": self.node_id,
            "op_type": self.op_type.value if isinstance(self.op_type, OperationType) else self.op_type,
            "collection": self.collection,
            "record_id": self.record_id,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "vector_clock": self.vector_clock,
            "lamport_timestamp": self.lamport_timestamp,
            "priority": self.priority.value if isinstance(self.priority, Priority) else self.priority,
            "user_id": self.user_id,
            "authority": self.authority,
            "source_module": self.source_module,
            "sync_state": self.sync_state.value if isinstance(self.sync_state, SyncState) else self.sync_state,
            "synced_at": self.synced_at,
            "sync_attempts": self.sync_attempts,
            "last_error": self.last_error,
            "enqueued_at": self.enqueued_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Operation:
        return cls(
            op_id=data["op_id"],
            node_id=data["node_id"],
            op_type=OperationType(data["op_type"]),
            collection=data["collection"],
            record_id=data["record_id"],
            payload=data.get("payload", {}),
            timestamp=float(data["timestamp"]),
            vector_clock=dict(data.get("vector_clock", {})),
            lamport_timestamp=int(data["lamport_timestamp"]),
            priority=Priority(data.get("priority", Priority.P2.value)),
            user_id=data.get("user_id"),
            authority=data.get("authority"),
            source_module=data.get("source_module", "app"),
            sync_state=SyncState(data.get("sync_state", SyncState.PENDING.value)),
            synced_at=float(data["synced_at"]) if data.get("synced_at") is not None else None,
            sync_attempts=int(data.get("sync_attempts", 0)),
            last_error=data.get("last_error"),
            enqueued_at=float(data.get("enqueued_at", data["timestamp"])),
        )


@dataclass
class Record:
    """Application record residing in local store."""

    record_id: str
    collection: str
    data: dict[str, Any]
    version: int
    vector_clock: dict[str, int]
    last_modified: float
    last_modified_by: str
    deleted: bool = False
    deleted_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_id": self.record_id,
            "collection": self.collection,
            "data": self.data,
            "version": self.version,
            "vector_clock": self.vector_clock,
            "last_modified": self.last_modified,
            "last_modified_by": self.last_modified_by,
            "deleted": self.deleted,
            "deleted_at": self.deleted_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Record:
        return cls(
            record_id=data["record_id"],
            collection=data["collection"],
            data=data.get("data", {}),
            version=int(data["version"]),
            vector_clock=dict(data.get("vector_clock", {})),
            last_modified=float(data["last_modified"]),
            last_modified_by=data["last_modified_by"],
            deleted=bool(data.get("deleted", False)),
            deleted_at=float(data["deleted_at"]) if data.get("deleted_at") is not None else None,
        )


@dataclass
class AuditEntry:
    """Immutable audit trail record."""

    audit_id: str
    timestamp: float
    action_type: str
    node_id: str
    details: dict[str, Any]
    user_id: str | None = None
    authority: str | None = None

    @classmethod
    def create(
        cls,
        action_type: str,
        node_id: str,
        details: dict[str, Any],
        user_id: str | None = None,
        authority: str | None = None,
        timestamp: float | None = None,
    ) -> AuditEntry:
        return cls(
            audit_id=str(uuid.uuid4()),
            timestamp=timestamp if timestamp is not None else time.time(),
            action_type=action_type,
            node_id=node_id,
            details=details,
            user_id=user_id,
            authority=authority,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "audit_id": self.audit_id,
            "timestamp": self.timestamp,
            "action_type": self.action_type,
            "node_id": self.node_id,
            "details": self.details,
            "user_id": self.user_id,
            "authority": self.authority,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AuditEntry:
        return cls(
            audit_id=data["audit_id"],
            timestamp=float(data["timestamp"]),
            action_type=data["action_type"],
            node_id=data["node_id"],
            details=data.get("details", {}),
            user_id=data.get("user_id"),
            authority=data.get("authority"),
        )


@dataclass
class SyncSession:
    """Tracks performance and outcomes of an atomic push-pull sync session."""

    session_id: str
    peer_node_id: str
    transport: str
    started_at: float
    ended_at: float | None = None
    ops_pushed: int = 0
    bytes_pushed: int = 0
    push_duration: float = 0.0
    ops_pulled: int = 0
    bytes_pulled: int = 0
    pull_duration: float = 0.0
    conflicts_detected: int = 0
    conflicts_resolved: int = 0
    status: str = "in_progress"  # in_progress, completed, failed, partial
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "peer_node_id": self.peer_node_id,
            "transport": self.transport,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "ops_pushed": self.ops_pushed,
            "bytes_pushed": self.bytes_pushed,
            "push_duration": self.push_duration,
            "ops_pulled": self.ops_pulled,
            "bytes_pulled": self.bytes_pulled,
            "pull_duration": self.pull_duration,
            "conflicts_detected": self.conflicts_detected,
            "conflicts_resolved": self.conflicts_resolved,
            "status": self.status,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SyncSession:
        return cls(
            session_id=data["session_id"],
            peer_node_id=data["peer_node_id"],
            transport=data["transport"],
            started_at=float(data["started_at"]),
            ended_at=float(data["ended_at"]) if data.get("ended_at") is not None else None,
            ops_pushed=int(data.get("ops_pushed", 0)),
            bytes_pushed=int(data.get("bytes_pushed", 0)),
            push_duration=float(data.get("push_duration", 0.0)),
            ops_pulled=int(data.get("ops_pulled", 0)),
            bytes_pulled=int(data.get("bytes_pulled", 0)),
            pull_duration=float(data.get("pull_duration", 0.0)),
            conflicts_detected=int(data.get("conflicts_detected", 0)),
            conflicts_resolved=int(data.get("conflicts_resolved", 0)),
            status=data.get("status", "in_progress"),
            error=data.get("error"),
        )


@dataclass
class MediaReference:
    """Metadata handle for media items stored on-disk rather than synced raw."""

    media_id: str
    media_hash: str
    media_size_bytes: int
    media_type: str
    thumbnail_hash: str | None = None
    thumbnail_size_bytes: int = 0
    local_path: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "media_id": self.media_id,
            "media_hash": self.media_hash,
            "media_size_bytes": self.media_size_bytes,
            "media_type": self.media_type,
            "thumbnail_hash": self.thumbnail_hash,
            "thumbnail_size_bytes": self.thumbnail_size_bytes,
            "local_path": self.local_path,
        }


@dataclass
class NodeKeyMaterial:
    """Cryptographic identities for authentication, transport encryption, and storage."""

    node_id: str
    signing_key: bytes          # Ed25519 private key (32 bytes)
    signing_public_key: bytes   # Ed25519 public key (32 bytes)
    encryption_key: bytes       # AES-256 master key (32 bytes)
    store_key: bytes = b""      # Derived key for local store
    outbox_key: bytes = b""     # Derived key for outbox
    audit_key: bytes = b""      # Derived key for audit
    media_key: bytes = b""      # Derived key for media files
