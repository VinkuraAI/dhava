"""Unit tests for TCP and HTTP transports."""

from __future__ import annotations

import socket
import threading
import time

from transports.http import HTTPTransport
from transports.tcp import TCPTransport
from utils.serialization import frame_payload, unframe_payload


def test_tcp_transport_socket_exchange() -> None:
    # Find open port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    def _mock_tcp_server() -> None:
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("127.0.0.1", port))
        server_sock.listen(1)
        try:
            conn, _ = server_sock.accept()
            with conn:
                raw_data = b""
                while True:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    raw_data += chunk
                    try:
                        payload, _ = unframe_payload(raw_data)
                        resp_payload = b"ECHO:" + payload
                        conn.sendall(frame_payload(resp_payload))
                        break
                    except Exception:
                        continue
        except Exception:
            pass
        finally:
            server_sock.close()

    server_thread = threading.Thread(target=_mock_tcp_server, daemon=True)
    server_thread.start()
    time.sleep(0.05)

    transport = TCPTransport(host="127.0.0.1", port=port, timeout=2.0)
    response = transport.send_receive(b"HELLO_DDIL", timeout=2.0)
    assert response == b"ECHO:HELLO_DDIL"
    transport.close()


def test_http_transport_status() -> None:
    transport = HTTPTransport(server_url="https://localhost:9999/api/sync", timeout=1.0)
    assert transport.name() == "http"
    assert transport.estimate_bandwidth() > 0
    assert transport.estimate_latency() > 0
    transport.close()
