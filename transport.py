"""Transport manager coordinating heterogeneous networks with preference ranking and failover."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

from transports.base import Transport, TransportResult, TransportStatus
from transports.file import FileTransport
from transports.http import HTTPTransport
from transports.loopback import LoopbackTransport
from transports.serial import SerialTransport
from transports.tcp import TCPTransport


class TransportManager:
    """
    Manages multiple heterogeneous transport links.
    Automatically identifies the best available link and seamlessly fails over.
    """

    def __init__(
        self,
        transports: list[Transport] | None = None,
        preference_order: list[str] | None = None,
    ) -> None:
        self.transports: dict[str, Transport] = {t.name(): t for t in (transports or [])}
        self.preference_order: list[str] = preference_order or list(self.transports.keys())
        self._callbacks: list[Callable[[TransportStatus], None]] = []
        self._lock = threading.RLock()
        self._last_active_name: str | None = None

    def add_transport(self, transport: Transport, preference_index: int | None = None) -> None:
        """Register a new transport."""
        with self._lock:
            name = transport.name()
            self.transports[name] = transport
            if name not in self.preference_order:
                if preference_index is not None:
                    self.preference_order.insert(preference_index, name)
                else:
                    self.preference_order.append(name)

    def get_active_transport(self) -> Transport | None:
        """Return highest preference available transport, or None if completely disconnected."""
        with self._lock:
            for name in self.preference_order:
                transport = self.transports.get(name)
                if transport and transport.is_available():
                    if self._last_active_name != name:
                        self._last_active_name = name
                        self._notify_status(
                            TransportStatus(
                                name=name,
                                available=True,
                                estimated_bandwidth=transport.estimate_bandwidth(),
                                latency_ms=transport.estimate_latency(),
                                last_checked=time.time(),
                            )
                        )
                    return transport
            self._last_active_name = None
            return None

    def list_available(self) -> list[TransportStatus]:
        """Query availability status and diagnostics for all registered transports."""
        statuses: list[TransportStatus] = []
        with self._lock:
            for name in self.preference_order:
                transport = self.transports.get(name)
                if transport:
                    avail = transport.is_available()
                    bw = transport.estimate_bandwidth() if avail else 0
                    lat = transport.estimate_latency() if avail else 0.0
                    statuses.append(
                        TransportStatus(
                            name=name,
                            available=avail,
                            estimated_bandwidth=bw,
                            latency_ms=lat,
                            last_checked=time.time(),
                        )
                    )
        return statuses

    def send(self, data: bytes, timeout: float = 30.0) -> TransportResult:
        """Send data over the current active transport."""
        transport = self.get_active_transport()
        if not transport:
            return TransportResult(
                success=False,
                bytes_sent=0,
                duration_seconds=0.0,
                transport_used="none",
                error="No transport currently available (network denied)",
            )

        start = time.perf_counter()
        try:
            ok = transport.send(data, timeout=timeout)
            duration = max(0.001, time.perf_counter() - start)
            return TransportResult(
                success=ok,
                bytes_sent=len(data) if ok else 0,
                duration_seconds=duration,
                transport_used=transport.name(),
                error=None if ok else "Transport send failed",
            )
        except Exception as exc:
            duration = max(0.001, time.perf_counter() - start)
            return TransportResult(
                success=False,
                bytes_sent=0,
                duration_seconds=duration,
                transport_used=transport.name(),
                error=str(exc),
            )

    def receive(self, timeout: float = 30.0) -> bytes | None:
        """Receive data from active transport."""
        transport = self.get_active_transport()
        if not transport:
            return None
        try:
            return transport.receive(timeout=timeout)
        except Exception:
            return None

    def exchange(
        self, request_bytes: bytes, timeout: float = 30.0
    ) -> tuple[bytes | None, TransportResult]:
        """Execute an atomic push-pull exchange over the active transport."""
        transport = self.get_active_transport()
        if not transport:
            res = TransportResult(
                success=False,
                bytes_sent=0,
                duration_seconds=0.0,
                transport_used="none",
                error="No transport available",
            )
            return None, res

        start = time.perf_counter()
        try:
            resp = transport.send_receive(request_bytes, timeout=timeout)
            duration = max(0.001, time.perf_counter() - start)
            if resp is not None:
                res = TransportResult(
                    success=True,
                    bytes_sent=len(request_bytes),
                    duration_seconds=duration,
                    transport_used=transport.name(),
                    error=None,
                )
                return resp, res
            else:
                res = TransportResult(
                    success=False,
                    bytes_sent=len(request_bytes),
                    duration_seconds=duration,
                    transport_used=transport.name(),
                    error="No response received from remote peer",
                )
                return None, res
        except Exception as exc:
            duration = max(0.001, time.perf_counter() - start)
            res = TransportResult(
                success=False,
                bytes_sent=0,
                duration_seconds=duration,
                transport_used=transport.name(),
                error=str(exc),
            )
            return None, res

    def register_status_callback(self, callback: Callable[[TransportStatus], None]) -> None:
        with self._lock:
            self._callbacks.append(callback)

    def _notify_status(self, status: TransportStatus) -> None:
        for cb in self._callbacks:
            try:
                cb(status)
            except Exception:
                pass

    def close_all(self) -> None:
        with self._lock:
            for transport in self.transports.values():
                try:
                    transport.close()
                except Exception:
                    pass


__all__ = [
    "TransportManager",
    "Transport",
    "TransportStatus",
    "TransportResult",
    "HTTPTransport",
    "TCPTransport",
    "FileTransport",
    "SerialTransport",
    "LoopbackTransport",
]
