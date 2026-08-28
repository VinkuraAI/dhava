"""Unit tests for DDILSyncServer (node registration, request handling, idempotency)."""

from __future__ import annotations

from crypto import CryptoLayer
from protocol import SyncPushRequest
from server import DDILSyncServer
from utils.serialization import pack_msgpack


def test_server_node_registration_and_status(crypto_key: bytes) -> None:
    server = DDILSyncServer.create(server_node_id="hq-main", encryption_key=crypto_key)

    server.register_node("node-01", metadata={"location": "Sector B", "type": "outpost"})
    nodes = server.list_nodes()
    assert len(nodes) == 1
    assert nodes[0].node_id == "node-01"
    assert nodes[0].metadata["location"] == "Sector B"


def test_server_sync_and_idempotency(crypto_key: bytes) -> None:
    server = DDILSyncServer.create(server_node_id="hq-main", encryption_key=crypto_key)
    crypto = CryptoLayer(encryption_key=crypto_key)

    op_payload = [
        {
            "op_id": "op-test-1",
            "node_id": "edge-01",
            "op_type": "create",
            "collection": "events",
            "record_id": "evt-1",
            "payload": {"status": "detected"},
            "timestamp": 100.0,
            "vector_clock": {"edge-01": 1},
            "lamport_timestamp": 1,
            "priority": "P1",
        }
    ]
    raw_ops = pack_msgpack(op_payload)
    enc_payload = crypto.pack(raw_ops)

    req = SyncPushRequest.create(
        node_id="edge-01",
        sender_vector_clock={"edge-01": 1},
        encrypted_payload=enc_payload,
        raw_payload=raw_ops,
        operation_count=1,
    )

    # First sync
    resp1 = server.handle_sync_request(req)
    assert resp1.status == "ok"
    assert resp1.acked_op_ids == ["op-test-1"]
    assert server.local_store.count("events") == 1

    # Second sync with exact same request (simulating network ack loss / retry) -> idempotent
    resp2 = server.handle_sync_request(req)
    assert resp2.status == "ok"
    assert resp2.acked_op_ids == ["op-test-1"]
    assert server.local_store.count("events") == 1
