"""Integration test: Multi-node concurrent edits and conflict resolution."""

from __future__ import annotations

from engine import DDILSyncEngine
from models import Priority
from server import DDILSyncServer
from transports.loopback import LoopbackTransport


def test_multi_node_concurrent_conflict_resolution(crypto_key: bytes) -> None:
    # Set up HQ server and two edge nodes (Node A and Node B)
    server = DDILSyncServer.create(server_node_id="hq-main", encryption_key=crypto_key)

    transport_a = LoopbackTransport(server=server, name="trans_a")
    node_a = DDILSyncEngine.create(node_id="node_a", transports=[transport_a], encryption_key=crypto_key)

    transport_b = LoopbackTransport(server=server, name="trans_b")
    node_b = DDILSyncEngine.create(node_id="node_b", transports=[transport_b], encryption_key=crypto_key)

    # 1. Node A creates initial record X
    node_a.create("cases", "case-100", {"status": "open", "lead": "Sharma"})
    node_a.sync_now()

    # 2. Node B syncs and gets record X
    node_b.sync_now()
    rec_b = node_b.get("cases", "case-100")
    assert rec_b is not None
    assert rec_b.data["status"] == "open"

    # 3. Both nodes go offline (disconnect)
    transport_a.connected = False
    transport_b.connected = False

    # 4. Node A updates record X at t=100
    node_a.update(
        "cases",
        "case-100",
        {"status": "investigating", "lead": "Sharma"},
        priority=Priority.P1,
    )

    # 5. Node B updates record X concurrently at t=105 (later timestamp)
    node_b.update(
        "cases",
        "case-100",
        {"status": "closed", "lead": "Verma"},
        priority=Priority.P0,
    )

    # 6. Node A comes online and syncs to HQ
    transport_a.connected = True
    node_a.sync_now()
    hq_rec1 = server.local_store.get("cases", "case-100")
    assert hq_rec1.data["status"] == "investigating"

    # 7. Node B comes online and syncs to HQ (Conflict detected!)
    transport_b.connected = True
    session_b = node_b.sync_now()
    assert session_b.status == "completed"

    # Node B's version wins Last-Write-Wins (due to later timestamp)
    hq_rec2 = server.local_store.get("cases", "case-100")
    assert hq_rec2.data["status"] == "closed"
    assert hq_rec2.data["lead"] == "Verma"

    # 8. Node A syncs again and pulls the winning resolved record
    node_a.sync_now()
    rec_a_final = node_a.get("cases", "case-100")
    assert rec_a_final.data["status"] == "closed"
