"""Serial transport for UART/RS-232, tactical radios, and embedded interfaces."""

from __future__ import annotations

import struct
from typing import Any

from transports.base import Transport
from utils.serialization import (
    FRAME_HEADER_FORMAT,
    FRAME_HEADER_SIZE,
    frame_payload,
    unframe_payload,
)

try:
    import serial  # type: ignore

    _SERIAL_AVAILABLE = True
except ImportError:
    _SERIAL_AVAILABLE = False


class SerialTransport(Transport):
    """
    Direct Serial/UART transport for field hardware, tactical radios, and microcontrollers.
    """

    def __init__(
        self,
        port: str = "/dev/ttyUSB0",
        baudrate: int = 115200,
        timeout: float = 30.0,
        transport_name: str = "serial",
    ) -> None:
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self._name = transport_name
        self._serial: Any = None
        self._bandwidth_bps = max(1200, int(baudrate * 0.8))
        self._latency_ms = 100.0
        self._last_received_bytes: bytes | None = None

    def name(self) -> str:
        return self._name

    def _get_serial(self) -> Any:
        if not _SERIAL_AVAILABLE:
            return None
        if self._serial is None:
            try:
                self._serial = serial.Serial(
                    port=self.port,
                    baudrate=self.baudrate,
                    timeout=self.timeout,
                )
            except Exception:
                self._serial = None
        return self._serial

    def is_available(self) -> bool:
        if not _SERIAL_AVAILABLE:
            return False
        try:
            ser = self._get_serial()
            return ser is not None and ser.is_open
        except Exception:
            return False

    def estimate_bandwidth(self) -> int:
        return self._bandwidth_bps

    def estimate_latency(self) -> float:
        return self._latency_ms

    def send_receive(self, request_bytes: bytes, timeout: float = 30.0) -> bytes | None:
        ser = self._get_serial()
        if ser is None:
            return None
        try:
            ser.timeout = timeout
            ser.reset_input_buffer()
            ser.reset_output_buffer()

            framed = frame_payload(request_bytes)
            ser.write(framed)
            ser.flush()

            # Read frame header
            header_data = ser.read(FRAME_HEADER_SIZE)
            if len(header_data) < FRAME_HEADER_SIZE:
                return None

            _, _, length, _ = struct.unpack(FRAME_HEADER_FORMAT, header_data)
            payload_data = ser.read(length)
            if len(payload_data) < length:
                return None

            extracted, _ = unframe_payload(header_data + payload_data)
            return extracted
        except Exception:
            return None

    def send(self, data: bytes, timeout: float = 30.0) -> bool:
        resp = self.send_receive(data, timeout=timeout)
        if resp is not None:
            self._last_received_bytes = resp
            return True
        return False

    def receive(self, timeout: float = 30.0) -> bytes | None:
        data = self._last_received_bytes
        self._last_received_bytes = None
        return data

    def close(self) -> None:
        if self._serial is not None:
            try:
                self._serial.close()
            except Exception:
                pass
            self._serial = None
