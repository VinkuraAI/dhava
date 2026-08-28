"""Integration test: Crash resilience, SQLite WAL durability, and startup recovery."""

from __future__ import annotations

from pathlib import Path

from engine import DDILSyncEngine
from models import Priority


def test_crash_recovery_and_persistence(temp_dir: Path, crypto_key: bytes) -> None:
    db_file = temp_dir / "crash_test.db"

    # Step 1: Initialize engine, write 10 records
    engine1 = DDILSyncEngine.create(
        node_id="node-crash-test",
        db_path=db_file,
        encryption_key=crypto_key,
    )
    for i in range(10):
        engine1.create(
            "incidents",
            f"inc-{i}",
            {"detail": f"critical incident {i}"},
            priority=Priority.P0 if i == 0 else Priority.P2,
        )

    assert engine1.outbox.pending_count() == 10
    assert engine1.count("incidents") == 10

    # Step 2: Simulate ungraceful process crash (close backend abruptly without sync)
    engine1.local_store.backend.close()

    # Step 3: Start brand new engine instance pointing to the same persistent DB file
    engine2 = DDILSyncEngine.create(
        node_id="node-crash-test",
        db_path=db_file,
        encryption_key=crypto_key,
    )

    # Step 4: Verify outbox queue and local store survived intact
    assert engine2.count("incidents") == 10
    assert engine2.outbox.pending_count() == 10

    p0_records = engine2.outbox.get_pending(priority=Priority.P0)
    assert len(p0_records) == 1
    assert p0_records[0].record_id == "inc-0"

    rec = engine2.get("incidents", "inc-0")
    assert rec is not None
    assert rec.data["detail"] == "critical incident 0"
