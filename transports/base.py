"""Abstract base class and status models for network transports."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class TransportStatus:
    """Current health, connectivity, and telemetry of a transport interface."""

    name: str
    available: bool
    estimated_bandwidth: int  # in bits per second (bps)
    latency_ms: float         # in milliseconds
    last_checked: float = field(default_factory=time.time)
    error: str | None = None


@dataclass
class TransportResult:
    """Diagnostic outcome of a transport transmission."""

    success: bool
    bytes_sent: int
    duration_seconds: float
    transport_used: str
    error: str | None = None


class Transport(ABC):
    """Abstract network transport interface."""

    @abstractmethod
    def name(self) -> str:
        """Transport identifier (e.g. 'lte', 'wifi', 'satcom', 'mesh', 'serial', 'file')."""
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Probe if the physical link or remote endpoint is reachable."""
        pass

    @abstractmethod
    def estimate_bandwidth(self) -> int:
        """Estimated available bandwidth in bits per second (bps)."""
        pass

    @abstractmethod
    def estimate_latency(self) -> float:
        """Estimated round-trip latency in milliseconds."""
        pass

    @abstractmethod
    def send(self, data: bytes, timeout: float = 30.0) -> bool:
        """Send raw data buffer over transport."""
        pass

    @abstractmethod
    def receive(self, timeout: float = 30.0) -> bytes | None:
        """Receive raw data buffer from transport."""
        pass

    def send_receive(self, request_bytes: bytes, timeout: float = 30.0) -> bytes | None:
        """
        Execute an atomic request-response transaction over the transport.
        Default implementation calls send() then receive(). Subclasses may optimize.
        """
        if not self.send(request_bytes, timeout=timeout):
            return None
        return self.receive(timeout=timeout)

    @abstractmethod
    def close(self) -> None:
        """Release any open sockets, file descriptors, or serial handles."""
        pass
