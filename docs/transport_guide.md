# Transport Guide & Extensibility

## 1. The Transport Abstraction

All communication in DDIL Sync Engine is mediated through the `Transport` base interface:

```python
from transport import Transport

class MyCustomSatelliteTransport(Transport):
    def name(self) -> str:
        return "iridium_sbd"

    def is_available(self) -> bool:
        # Check satellite modem signal
        return True

    def estimate_bandwidth(self) -> int:
        return 2400 # 2.4 Kbps

    def estimate_latency(self) -> float:
        return 1200.0 # 1.2s round-trip latency

    def send(self, data: bytes, timeout: float = 30.0) -> bool:
        ...

    def receive(self, timeout: float = 30.0) -> bytes | None:
        ...

    def send_receive(self, request_bytes: bytes, timeout: float = 30.0) -> bytes | None:
        ...

    def close(self) -> None:
        ...
```

---

## 2. Built-In Transports

1. **HTTPTransport**: Ideal for LTE, 5G, Wi-Fi, and broadband links. Supports Bearer token authentication and custom TLS certificates.
2. **TCPTransport**: High-performance socket transport with binary length-prefixed framing and SHA-256 validation for peer-to-peer mesh radio networks.
3. **FileTransport (Sneakernet)**: Writes encrypted `.bundle` files to physical USB storage for air-gapped field posts.
4. **SerialTransport**: Direct RS-232 / UART serial communication for field hardware, tactical military radios, and microcontrollers.
5. **LoopbackTransport**: Zero-overhead in-memory transport for automated unit testing and simulation.

---

## 3. Dynamic Failover & TransportManager

The `TransportManager` maintains a prioritized preference order (e.g. `["wifi", "lte", "satcom", "mesh"]`). When the primary link drops, it fails over to the next available interface without interrupting outbox queuing or application execution.
