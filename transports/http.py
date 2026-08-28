"""HTTP/HTTPS transport for LTE, Wi-Fi, and broadband connections."""

from __future__ import annotations

import time

import httpx

from transports.base import Transport


class HTTPTransport(Transport):
    """
    HTTP/HTTPS client transport for syncing with an HQ sync server.
    """

    def __init__(
        self,
        server_url: str,
        auth_token: str | None = None,
        client_cert_path: str | None = None,
        timeout: float = 30.0,
        retry_count: int = 3,
        transport_name: str = "http",
        default_bandwidth_bps: int = 1_000_000,
    ) -> None:
        self.server_url = server_url.rstrip("/")
        self.auth_token = auth_token
        self.client_cert_path = client_cert_path
        self.timeout = timeout
        self.retry_count = retry_count
        self._name = transport_name
        self._bandwidth_bps = default_bandwidth_bps
        self._latency_ms = 50.0
        self._last_received_bytes: bytes | None = None

        headers: dict[str, str] = {
            "Content-Type": "application/octet-stream",
            "User-Agent": "DDIL-Sync-Client/1.0",
        }
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        self._client = httpx.Client(
            headers=headers,
            timeout=httpx.Timeout(timeout),
            cert=client_cert_path,
            follow_redirects=True,
        )

    def name(self) -> str:
        return self._name

    def is_available(self) -> bool:
        """Health check endpoint or HEAD request."""
        try:
            # Probe health endpoint with short timeout
            url = f"{self.server_url}/health" if not self.server_url.endswith("/health") else self.server_url
            start = time.perf_counter()
            resp = self._client.get(url, timeout=3.0)
            self._latency_ms = max(1.0, (time.perf_counter() - start) * 1000.0)
            return resp.status_code in (200, 204)
        except Exception:
            return False

    def estimate_bandwidth(self) -> int:
        return self._bandwidth_bps

    def estimate_latency(self) -> float:
        return self._latency_ms

    def send(self, data: bytes, timeout: float = 30.0) -> bool:
        """Send sync payload and buffer response."""
        try:
            start = time.perf_counter()
            resp = self._client.post(
                self.server_url,
                content=data,
                timeout=timeout,
            )
            duration = max(0.001, time.perf_counter() - start)
            if resp.status_code == 200:
                self._last_received_bytes = resp.content
                total_bytes = len(data) + len(resp.content)
                self._bandwidth_bps = int((total_bytes * 8) / duration)
                self._latency_ms = duration * 500.0
                return True
            self._last_received_bytes = None
            return False
        except Exception:
            self._last_received_bytes = None
            return False

    def receive(self, timeout: float = 30.0) -> bytes | None:
        data = self._last_received_bytes
        self._last_received_bytes = None
        return data

    def send_receive(self, request_bytes: bytes, timeout: float = 30.0) -> bytes | None:
        for _ in range(max(1, self.retry_count)):
            try:
                start = time.perf_counter()
                resp = self._client.post(
                    self.server_url,
                    content=request_bytes,
                    timeout=timeout,
                )
                duration = max(0.001, time.perf_counter() - start)
                if resp.status_code == 200:
                    total_bytes = len(request_bytes) + len(resp.content)
                    self._bandwidth_bps = max(1000, int((total_bytes * 8) / duration))
                    self._latency_ms = duration * 500.0
                    return resp.content
            except Exception:
                continue
        return None

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass
