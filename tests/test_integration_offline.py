"""Integration test: Offline blackout mode and reconnection catch-up."""

from __future__ import annotations

from engine import DDILSyncEngine
from models import Priority
from server import DDILSyncServer


def test_offline_mode_and_reconnection_catchup(
    server_and_client: tuple[DDILSyncServer, DDILSyncEngine, object],
) -> None:
    server, client, transport = server_and_client

    # 1. Initial sync
    client.create("telemetry", "dev-1", {"status": "nominal"})
    client.sync_now()
    assert server.local_store.count("telemetry") == 1

    # 2. Network blackout (transport unavailable)
    transport.connected = False  # type: ignore

    # 3. Create 10 operations offline on client
    for i in range(10):
        client.create(
            "telemetry",
            f"dev-{i + 2}",
            {"status": f"offline_reading_{i}"},
            priority=Priority.P0 if i == 0 else Priority.P2,
        )

    # Client reads locally offline with zero issues
    assert client.count("telemetry") == 11
    assert client.outbox.pending_count() == 10

    # Sync fails gracefully while offline without losing queue
    failed_session = client.sync_now()
    assert failed_session.status == "failed"
    assert client.outbox.pending_count() == 10

    # 4. Connectivity restored
    transport.connected = True  # type: ignore

    # 5. Catch-up sync
    catchup = client.sync_now()
    assert catchup.status == "completed"
    assert catchup.ops_pushed == 10
    assert client.outbox.pending_count() == 0
    assert server.local_store.count("telemetry") == 11
