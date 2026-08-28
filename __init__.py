"""
Dhava — Sovereign offline-first data synchronization engine
for Denied, Disrupted, Intermittent, and Limited bandwidth environments.
"""

from audit import AuditLogger
from backends.base import StorageBackend
from backends.inmemory import InMemoryBackend
from backends.sqlite import SQLiteBackend
from conflict import ConflictResolver, Resolution, ResolutionAction
from crypto import CryptoLayer
from engine import DDILSyncEngine, EngineConfig, EngineState, EngineStatus
from models import (
    AuditEntry,
    MediaReference,
    NodeKeyMaterial,
    Operation,
    OperationType,
    Priority,
    Record,
    SyncSession,
    SyncState,
)
from outbox import OutboxQueue
from protocol import SyncPullResponse, SyncPushRequest
from server import DDILSyncServer, NodeInfo, ServerConfig
from store import LocalStore
from transport import (
    FileTransport,
    HTTPTransport,
    LoopbackTransport,
    SerialTransport,
    TCPTransport,
    Transport,
    TransportManager,
    TransportResult,
    TransportStatus,
)
from vector_clock import VectorClock

__version__ = "0.1.0"
__all__ = [
    "DDILSyncEngine",
    "EngineConfig",
    "EngineStatus",
    "EngineState",
    "LocalStore",
    "StorageBackend",
    "SQLiteBackend",
    "InMemoryBackend",
    "OutboxQueue",
    "ConflictResolver",
    "Resolution",
    "ResolutionAction",
    "TransportManager",
    "Transport",
    "TransportStatus",
    "TransportResult",
    "HTTPTransport",
    "TCPTransport",
    "FileTransport",
    "SerialTransport",
    "LoopbackTransport",
    "CryptoLayer",
    "AuditLogger",
    "Operation",
    "OperationType",
    "SyncState",
    "Record",
    "AuditEntry",
    "SyncSession",
    "Priority",
    "MediaReference",
    "NodeKeyMaterial",
    "VectorClock",
    "SyncPushRequest",
    "SyncPullResponse",
    "DDILSyncServer",
    "ServerConfig",
    "NodeInfo",
]
