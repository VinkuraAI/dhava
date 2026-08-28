"""Unit tests for transports (file sneakernet, transport manager failover)."""

from __future__ import annotations

from pathlib import Path

from transport import FileTransport, TransportManager
from transports.base import Transport


class DummyTransport(Transport):
    def __init__(self, name: str, available: bool = True, bw: int = 1000) -> None:
        self._name = name
        self._available = available
        self._bw = bw

    def name(self) -> str:
        return self._name

    def is_available(self) -> bool:
        return self._available

    def estimate_bandwidth(self) -> int:
        return self._bw

    def estimate_latency(self) -> float:
        return 10.0

    def send(self, data: bytes, timeout: float = 30.0) -> bool:
        return self._available

    def receive(self, timeout: float = 30.0) -> bytes | None:
        return b"response" if self._available else None

    def close(self) -> None:
        pass


def test_transport_manager_preference_and_failover() -> None:
    t_wifi = DummyTransport("wifi", available=False, bw=10_000_000)
    t_lte = DummyTransport("lte", available=True, bw=1_000_000)
    t_satcom = DummyTransport("satcom", available=True, bw=64_000)

    tm = TransportManager(
        transports=[t_wifi, t_lte, t_satcom],
        preference_order=["wifi", "lte", "satcom"],
    )

    # Active transport should be LTE since Wi-Fi is unavailable
    active = tm.get_active_transport()
    assert active is not None
    assert active.name() == "lte"

    # When LTE fails, failover to Satcom
    t_lte._available = False
    active2 = tm.get_active_transport()
    assert active2 is not None
    assert active2.name() == "satcom"

    # When all down, active is None
    t_satcom._available = False
    assert tm.get_active_transport() is None


def test_file_sneakernet_transport(temp_dir: Path) -> None:
    export_dir = temp_dir / "usb_export"
    import_dir = temp_dir / "usb_import"

    t1 = FileTransport(export_dir=export_dir, import_dir=import_dir)
    assert t1.is_available() is True

    # Send payload
    data = b"Air-gapped sync bundle payload"
    assert t1.send(data) is True

    # File should exist in export dir
    bundles = list(export_dir.glob("ddil_push_*.bundle"))
    assert len(bundles) == 1

    # Simulate physically carrying the file from export to import
    carried_file = import_dir / bundles[0].name
    carried_file.write_bytes(bundles[0].read_bytes())

    # Receiver reads bundle
    received = t1.receive()
    assert received == data
