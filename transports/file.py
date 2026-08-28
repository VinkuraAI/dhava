"""File-based Sneakernet transport for air-gapped and zero-connectivity physical transfers."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

from transports.base import Transport
from utils.serialization import frame_payload, unframe_payload


class FileTransport(Transport):
    """
    Air-gapped sync transport writing encrypted binary bundles to physical media (e.g., USB drives).
    """

    def __init__(
        self,
        export_dir: str | Path,
        import_dir: str | Path | None = None,
        transport_name: str = "file",
        default_bandwidth_bps: int = 100_000_000,  # High speed local USB bus
    ) -> None:
        self.export_dir = Path(export_dir)
        self.import_dir = Path(import_dir) if import_dir else self.export_dir
        self._name = transport_name
        self._bandwidth_bps = default_bandwidth_bps
        self._latency_ms = 10.0
        self._last_received_bytes: bytes | None = None

        self.export_dir.mkdir(parents=True, exist_ok=True)
        self.import_dir.mkdir(parents=True, exist_ok=True)

    def name(self) -> str:
        return self._name

    def is_available(self) -> bool:
        """Available if target directories exist and are writable."""
        return self.export_dir.exists() and os_is_writable(self.export_dir)

    def estimate_bandwidth(self) -> int:
        return self._bandwidth_bps

    def estimate_latency(self) -> float:
        return self._latency_ms

    def send(self, data: bytes, timeout: float = 30.0) -> bool:
        """Write framed payload to export directory."""
        try:
            self.export_dir.mkdir(parents=True, exist_ok=True)
            bundle_id = str(uuid.uuid4())[:8]
            timestamp = int(time.time())
            filename = f"ddil_push_{timestamp}_{bundle_id}.bundle"
            filepath = self.export_dir / filename
            temp_path = self.export_dir / f"{filename}.tmp"

            framed = frame_payload(data)
            temp_path.write_bytes(framed)
            temp_path.replace(filepath)
            return True
        except Exception:
            return False

    def receive(self, timeout: float = 30.0) -> bytes | None:
        """Read oldest unprocessed response bundle from import directory."""
        try:
            if not self.import_dir.exists():
                return None

            bundles = sorted(
                list(self.import_dir.glob("ddil_pull_*.bundle"))
                + list(self.import_dir.glob("ddil_push_*.bundle")),
                key=lambda p: p.stat().st_mtime,
            )

            for bundle in bundles:
                try:
                    raw = bundle.read_bytes()
                    payload, _ = unframe_payload(raw)
                    # Mark processed
                    processed_dir = self.import_dir / ".processed"
                    processed_dir.mkdir(parents=True, exist_ok=True)
                    bundle.replace(processed_dir / bundle.name)
                    return payload
                except Exception:
                    continue
            return None
        except Exception:
            return None

    def send_receive(self, request_bytes: bytes, timeout: float = 30.0) -> bytes | None:
        if self.send(request_bytes, timeout=timeout):
            return self.receive(timeout=timeout)
        return None

    def close(self) -> None:
        pass


def os_is_writable(path: Path) -> bool:
    try:
        test_file = path / f".write_test_{uuid.uuid4().hex[:6]}"
        test_file.write_text("test")
        test_file.unlink(missing_ok=True)
        return True
    except Exception:
        return False
