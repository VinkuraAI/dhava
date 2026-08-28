"""Unit tests for wire protocol framing and serialization."""

from __future__ import annotations

import pytest

from protocol import SyncPullResponse, SyncPushRequest
from utils.serialization import frame_payload, unframe_payload


def test_binary_framing_envelope() -> None:
    raw_payload = b"Telemetry raw data stream " * 30
    framed = frame_payload(raw_payload)

    # Starts with magic "DDIL\x01"
    assert framed.startswith(b"DDIL\x01")

    extracted, remaining = unframe_payload(framed)
    assert extracted == raw_payload
    assert remaining == b""


def test_binary_framing_checksum_mismatch() -> None:
    raw_payload = b"Important dispatch directive"
    framed = bytearray(frame_payload(raw_payload))

    # Corrupt a payload byte
    framed[-1] ^= 0x55

    with pytest.raises(ValueError, match="digest mismatch"):
        unframe_payload(bytes(framed))


def test_sync_push_and_pull_serialization() -> None:
    push = SyncPushRequest.create(
        node_id="edge-node-01",
        sender_vector_clock={"edge-node-01": 5},
        encrypted_payload=b"encrypted_content_bytes",
        raw_payload=b"raw_content_bytes",
        operation_count=3,
    )
    serialized = push.serialize()
    deserialized = SyncPushRequest.deserialize(serialized)

    assert deserialized.node_id == "edge-node-01"
    assert deserialized.operation_count == 3
    assert deserialized.encrypted_payload == b"encrypted_content_bytes"

    pull = SyncPullResponse.create(
        node_id="hq-server",
        session_id=push.session_id,
        status="ok",
        acked_op_ids=["op-1", "op-2"],
        encrypted_payload=b"server_delta_bytes",
        raw_payload=b"server_raw_bytes",
        operation_count=1,
    )
    pull_ser = pull.serialize()
    pull_deser = SyncPullResponse.deserialize(pull_ser)

    assert pull_deser.status == "ok"
    assert pull_deser.acked_op_ids == ["op-1", "op-2"]
    assert pull_deser.encrypted_payload == b"server_delta_bytes"
