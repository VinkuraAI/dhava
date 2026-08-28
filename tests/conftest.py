"""Pytest fixtures for DDIL Sync Engine test suite."""

from __future__ import annotations

import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest

from audit import AuditLogger
from backends.inmemory import InMemoryBackend
from conflict import ConflictResolver
from crypto import CryptoLayer
from engine import DDILSyncEngine, EngineConfig
from outbox import OutboxQueue
from server import DDILSyncServer
from store import LocalStore
from transport import (
    LoopbackTransport,
)


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    with tempfile.TemporaryDirectory() as td:
        yield Path(td)


@pytest.fixture
def crypto_key() -> bytes:
    return CryptoLayer.generate_key()


@pytest.fixture
def crypto_layer(crypto_key: bytes) -> CryptoLayer:
    return CryptoLayer(encryption_key=crypto_key, compression="zstd")


@pytest.fixture
def in_memory_backend() -> InMemoryBackend:
    return InMemoryBackend()


@pytest.fixture
def local_store(in_memory_backend: InMemoryBackend) -> LocalStore:
    return LocalStore(in_memory_backend, node_id="test-node-01")


@pytest.fixture
def outbox_queue(in_memory_backend: InMemoryBackend) -> OutboxQueue:
    return OutboxQueue(in_memory_backend)


@pytest.fixture
def conflict_resolver() -> ConflictResolver:
    return ConflictResolver(node_id="test-node-01", clock_skew_tolerance=1.0)


@pytest.fixture
def audit_logger(in_memory_backend: InMemoryBackend) -> AuditLogger:
    return AuditLogger(in_memory_backend, node_id="test-node-01")


@pytest.fixture
def server_and_client(crypto_key: bytes) -> tuple[DDILSyncServer, DDILSyncEngine, LoopbackTransport]:
    server = DDILSyncServer.create(
        server_node_id="hq-server",
        encryption_key=crypto_key,
    )
    transport = LoopbackTransport(server=server)
    client = DDILSyncEngine.create(
        node_id="edge-node-01",
        transports=[transport],
        encryption_key=crypto_key,
        config=EngineConfig(sync_interval_seconds=0.1),
    )
    return server, client, transport
