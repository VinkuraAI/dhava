"""Example 2: Offline Disconnection, Local Operations, and Reconnection Catch-Up."""

from __future__ import annotations

from crypto import CryptoLayer
from engine import DDILSyncEngine
from models import Priority
from protocol import SyncPushRequest
from server import DDILSyncServer
from transports.base import Transport


class MockChannelTransport(Transport):
    def __init__(self, server: DDILSyncServer) -> None:
        self.server = server
        self.online = True

    def name(self) -> str:
        return "mock_radio"

    def is_available(self) -> bool:
        return self.online

    def estimate_bandwidth(self) -> int:
        return 256_000

    def estimate_latency(self) -> float:
        return 30.0

    def send(self, data: bytes, timeout: float = 30.0) -> bool:
        return self.online

    def receive(self, timeout: float = 30.0) -> bytes | None:
        return None

    def send_receive(self, request_bytes: bytes, timeout: float = 30.0) -> bytes | None:
        if not self.online:
            return None
        req = SyncPushRequest.deserialize(request_bytes)
        resp = self.server.handle_sync_request(req)
        return resp.serialize()

    def close(self) -> None:
        pass


def main() -> None:
    print("=== DDIL Sync Engine: Offline Disconnection & Reconnection Catch-Up ===")

    shared_key = CryptoLayer.generate_key()
    server = DDILSyncServer.create(server_node_id="hq-main", encryption_key=shared_key)
    transport = MockChannelTransport(server)
    edge = DDILSyncEngine.create(
        node_id="border-post-07", transports=[transport], encryption_key=shared_key
    )

    # Step 1: Initial sync while online
    print("\n1. Initial state: Both nodes connected")
    edge.create("patrols", "patrol-1", {"status": "dispatched", "officer": "Rathore"})
    edge.sync_now()
    print(f"Server records: {server.local_store.count('patrols')}")

    # Step 2: Simulate Network Blackout (DENIED environment)
    print("\n2. Network Blackout: Radio link drops (Offline Mode)")
    transport.online = False
    print(f"Transport available? {transport.is_available()}")

    # Step 3: Edge node continues full local operations offline
    print("\n3. Performing local writes while offline...")
    edge.create(
        "patrols",
        "patrol-2",
        {"status": "checkpoint_reached", "officer": "Sharma"},
        priority=Priority.P1,
    )
    edge.update(
        "patrols",
        "patrol-1",
        {"status": "sector_cleared", "officer": "Rathore"},
        priority=Priority.P0,
    )

    print(f"Edge pending outbox queue: {edge.outbox.pending_count()} operations queued.")

    # Attempt sync during blackout (should gracefully fail without losing queue)
    failed_session = edge.sync_now()
    print(f"Sync attempt during blackout: status='{failed_session.status}', queue intact={edge.outbox.pending_count()}")

    # Step 4: Network connectivity restored
    print("\n4. Network Restored: Reconnecting link and executing catch-up sync...")
    transport.online = True

    catchup_session = edge.sync_now()
    print(f"Catch-up Sync Status: {catchup_session.status}")
    print(f"Operations pushed to HQ: {catchup_session.ops_pushed}")
    print(f"Pending operations remaining on edge: {edge.outbox.pending_count()}")

    # Verify server records
    p1 = server.local_store.get("patrols", "patrol-1")
    p2 = server.local_store.get("patrols", "patrol-2")
    print(f"\nServer State for patrol-1: {p1.data if p1 else None}")
    print(f"Server State for patrol-2: {p2.data if p2 else None}")

    print("\n✓ Offline-then-reconnect catch-up completed successfully.")


if __name__ == "__main__":
    main()
