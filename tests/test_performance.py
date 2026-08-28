"""Performance and stress benchmarks (high volume throughput, compression, queries)."""

from __future__ import annotations

import time

from engine import DDILSyncEngine, EngineConfig
from models import Priority
from server import DDILSyncServer
from transports.loopback import LoopbackTransport


def test_high_volume_sync_performance(crypto_key: bytes) -> None:
    server = DDILSyncServer.create(server_node_id="hq-perf", encryption_key=crypto_key)
    transport = LoopbackTransport(server=server)
    engine = DDILSyncEngine.create(
        node_id="edge-perf",
        transports=[transport],
        encryption_key=crypto_key,
        config=EngineConfig(batch_size=1000),
    )

    # 1. Enqueue 1,000 operations rapidly
    start = time.perf_counter()
    count = 1000
    for i in range(count):
        engine.create(
            collection="sensor_telemetry",
            record_id=f"rec-{i}",
            data={
                "id": i,
                "lat": 28.6139 + (i * 0.0001),
                "lon": 77.2090 + (i * 0.0001),
                "reading": 42.5,
                "sector": "Sector-B",
            },
            priority=Priority.P1 if i % 10 == 0 else Priority.P2,
        )
    write_duration = time.perf_counter() - start
    assert write_duration < 5.0, f"Local write took too long: {write_duration:.2f}s"
    assert engine.outbox.pending_count() == count

    # 2. Sync operations
    sync_start = time.perf_counter()
    session = engine.sync_now()
    sync_duration = time.perf_counter() - sync_start

    assert session.status == "completed"
    assert session.ops_pushed == count
    assert sync_duration < 5.0, f"Sync took too long: {sync_duration:.2f}s"
    assert server.local_store.count("sensor_telemetry") == count

    # 3. Fast filtered query performance
    query_start = time.perf_counter()
    results = server.local_store.query("sensor_telemetry", limit=100)
    query_duration = time.perf_counter() - query_start
    assert len(results) == 100
    assert query_duration < 0.1, f"Query took too long: {query_duration:.4f}s"
