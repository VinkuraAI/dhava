"""Raw TCP transport for mesh radio networks and direct device-to-device sync."""

from __future__ import annotations

import socket
import time

from transports.base import Transport
from utils.serialization import FRAME_HEADER_SIZE, frame_payload, unframe_payload


class TCPTransport(Transport):
    """
    Direct TCP client transport. Uses binary length-prefixed framing with SHA-256 verification.
    """

    def __init__(
        self,
        host: str,
        port: int,
        timeout: float = 30.0,
        transport_name: str = "tcp",
        default_bandwidth_bps: int = 500_000,
    ) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self._name = transport_name
        self._bandwidth_bps = default_bandwidth_bps
        self._latency_ms = 20.0
        self._last_received_bytes: bytes | None = None

    def name(self) -> str:
        return self._name

    def is_available(self) -> bool:
        try:
            start = time.perf_counter()
            with socket.create_connection((self.host, self.port), timeout=2.0):
                self._latency_ms = max(1.0, (time.perf_counter() - start) * 1000.0)
                return True
        except Exception:
            return False

    def estimate_bandwidth(self) -> int:
        return self._bandwidth_bps

    def estimate_latency(self) -> float:
        return self._latency_ms

    def send_receive(self, request_bytes: bytes, timeout: float = 30.0) -> bytes | None:
        try:
            start = time.perf_counter()
            with socket.create_connection((self.host, self.port), timeout=timeout) as sock:
                sock.settimeout(timeout)
                framed = frame_payload(request_bytes)
                sock.sendall(framed)

                # Read response frame header first
                header_data = b""
                while len(header_data) < FRAME_HEADER_SIZE:
                    chunk = sock.recv(FRAME_HEADER_SIZE - len(header_data))
                    if not chunk:
                        return None
                    header_data += chunk

                import struct
                _, _, length, _ = struct.unpack("!4sB I 32s", header_data)

                # Read exact payload
                payload_data = b""
                while len(payload_data) < length:
                    chunk = sock.recv(length - len(payload_data))
                    if not chunk:
                        return None
                    payload_data += chunk

                duration = max(0.001, time.perf_counter() - start)
                extracted_payload, _ = unframe_payload(header_data + payload_data)

                total_bytes = len(framed) + len(header_data + payload_data)
                self._bandwidth_bps = max(1000, int((total_bytes * 8) / duration))
                self._latency_ms = duration * 500.0
                return extracted_payload
        except Exception:
            return None

    def send(self, data: bytes, timeout: float = 30.0) -> bool:
        resp = self.send_receive(data, timeout=timeout)
        if resp is not None:
            self._last_received_bytes = resp
            return True
        self._last_received_bytes = None
        return False

    def receive(self, timeout: float = 30.0) -> bytes | None:
        data = self._last_received_bytes
        self._last_received_bytes = None
        return data

    def close(self) -> None:
        pass
