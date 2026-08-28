"""In-memory loopback transport for unit testing and local node pairing."""

from __future__ import annotations

from typing import TYPE_CHECKING

from transports.base import Transport

if TYPE_CHECKING:
    from server import DDILSyncServer


class LoopbackTransport(Transport):
    """
    Direct in-memory transport channel connecting an engine directly to a server instance.
    Ideal for unit testing, test fixtures, and in-process simulation.
    """

    def __init__(
        self,
        server: DDILSyncServer | None = None,
        name: str = "loopback",
        bandwidth_bps: int = 10_000_000,
        latency_ms: float = 1.0,
    ) -> None:
        self.server = server
        self._name = name
        self.connected = True
        self.bandwidth_bps = bandwidth_bps
        self.latency_ms = latency_ms

    def name(self) -> str:
        return self._name

    def is_available(self) -> bool:
        return self.connected and self.server is not None

    def estimate_bandwidth(self) -> int:
        return self.bandwidth_bps

    def estimate_latency(self) -> float:
        return self.latency_ms

    def send(self, data: bytes, timeout: float = 30.0) -> bool:
        return self.is_available()

    def receive(self, timeout: float = 30.0) -> bytes | None:
        return None

    def send_receive(self, request_bytes: bytes, timeout: float = 30.0) -> bytes | None:
        if not self.is_available() or self.server is None:
            return None
        from protocol import SyncPushRequest

        req = SyncPushRequest.deserialize(request_bytes)
        resp = self.server.handle_sync_request(req)
        return resp.serialize()

    def close(self) -> None:
        pass
