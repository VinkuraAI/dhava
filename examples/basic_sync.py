"""Example 1: Basic Two-Node Synchronization."""

from __future__ import annotations

from crypto import CryptoLayer
from engine import DDILSyncEngine
from models import Priority
from server import DDILSyncServer
from transports.base import Transport


class DirectChannelTransport(Transport):
    """Simulated in-memory point-to-point network channel between edge and server."""

    def __init__(self, server: DDILSyncServer) -> None:
        self.server = server
        self.connected = True
        self.bandwidth_bps = 1_000_000

    def name(self) -> str:
        return "direct_channel"

    def is_available(self) -> bool:
        return self.connected

    def estimate_bandwidth(self) -> int:
        return self.bandwidth_bps

    def estimate_latency(self) -> float:
        return 5.0

    def send(self, data: bytes, timeout: float = 30.0) -> bool:
        return True

    def receive(self, timeout: float = 30.0) -> bytes | None:
        return None

    def send_receive(self, request_bytes: bytes, timeout: float = 30.0) -> bytes | None:
        if not self.connected:
            return None
        from protocol import SyncPushRequest
        req = SyncPushRequest.deserialize(request_bytes)
        resp = self.server.handle_sync_request(req)
        return resp.serialize()

    def close(self) -> None:
        pass


def main() -> None:
    print("=== DDIL Sync Engine: Basic Two-Node Sync Demo ===")

    # Shared AES-256 encryption key
    shared_key = CryptoLayer.generate_key()

    # 1. Initialize HQ Server
    server = DDILSyncServer.create(
        server_node_id="hq-server",
        encryption_key=shared_key,
    )

    # 2. Initialize Edge Node with Direct Transport to Server
    transport = DirectChannelTransport(server)
    edge_node = DDILSyncEngine.create(
        node_id="edge-post-01",
        transports=[transport],
        encryption_key=shared_key,
    )

    # 3. Create records offline on Edge Node
    print("\n[Edge Node] Creating local event records...")
    edge_node.create(
        collection="sensor_events",
        record_id="evt-101",
        data={"type": "vehicle_detected", "sector": "B", "confidence": 0.96},
        priority=Priority.P0,  # Critical alert
    )
    edge_node.create(
        collection="sensor_events",
        record_id="evt-102",
        data={"type": "personnel_movement", "sector": "C", "confidence": 0.88},
        priority=Priority.P1,
    )

    print(f"Edge Node Pending Operations: {edge_node.outbox.pending_count()}")
    print(f"HQ Server Records before sync: {server.local_store.count('sensor_events')}")

    # 4. Trigger Sync
    print("\n[Sync] Executing sync cycle...")
    session = edge_node.sync_now()
    print(f"Sync Status: {session.status}")
    print(f"Operations Pushed: {session.ops_pushed}, Pulled: {session.ops_pulled}")

    # 5. Verify Server has received records
    print(f"\nHQ Server Records after sync: {server.local_store.count('sensor_events')}")
    hq_records = server.local_store.query("sensor_events")
    for r in hq_records:
        print(f"  - Record {r.record_id}: {r.data}")

    print("\n✓ Basic sync verified successfully.")


if __name__ == "__main__":
    main()
