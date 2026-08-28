"""Integration test: Two-node bidirectional synchronization."""

from __future__ import annotations

from engine import DDILSyncEngine
from models import Priority
from server import DDILSyncServer


def test_two_node_bidirectional_sync(
    server_and_client: tuple[DDILSyncServer, DDILSyncEngine, object],
) -> None:
    server, client, transport = server_and_client

    # Edge node creates 5 records
    for i in range(5):
        client.create(
            collection="patrols",
            record_id=f"p-{i}",
            data={"sector": "North", "id": i},
            priority=Priority.P1,
        )

    assert client.outbox.pending_count() == 5
    assert server.local_store.count("patrols") == 0

    # Sync
    session = client.sync_now()
    assert session.status == "completed"
    assert session.ops_pushed == 5
    assert client.outbox.pending_count() == 0
    assert server.local_store.count("patrols") == 5

    # Server now creates 2 records independently
    server.local_store.apply_operation(
        client.outbox._row_to_operation({
            "op_id": "srv-op-1",
            "node_id": "hq-server",
            "op_type": "create",
            "collection": "patrols",
            "record_id": "p-hq-1",
            "payload": '{"sector": "HQ", "directive": "standby"}',
            "timestamp": 200.0,
            "vector_clock": '{"hq-server": 1}',
            "lamport_timestamp": 10,
            "priority": "P0",
            "user_id": "hq_commander",
            "authority": "hq",
            "source_module": "hq",
            "sync_state": "synced",
            "synced_at": 200.0,
            "sync_attempts": 0,
            "last_error": None,
            "enqueued_at": 200.0,
        })
    )

    # Next sync should pull the HQ record to edge node
    session2 = client.sync_now()
    assert session2.status == "completed"
    assert session2.ops_pulled == 1

    pulled_rec = client.get("patrols", "p-hq-1")
    assert pulled_rec is not None
    assert pulled_rec.data["directive"] == "standby"
