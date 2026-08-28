"""Unit tests for CryptoLayer (AES-256-GCM, X25519, Ed25519, HKDF, zstd/gzip)."""

from __future__ import annotations

import pytest

from crypto import CryptoLayer


def test_crypto_pack_unpack_zstd(crypto_key: bytes) -> None:
    crypto = CryptoLayer(encryption_key=crypto_key, compression="zstd")
    data = b"Sovereign operational intelligence payload with high entropy and repetition " * 50

    packed = crypto.pack(data)
    assert packed != data
    assert len(packed) > 28

    unpacked = crypto.unpack(packed)
    assert unpacked == data


def test_crypto_pack_unpack_gzip(crypto_key: bytes) -> None:
    crypto = CryptoLayer(encryption_key=crypto_key, compression="gzip")
    data = b"Gzip compression test payload " * 100

    packed = crypto.pack(data)
    unpacked = crypto.unpack(packed)
    assert unpacked == data


def test_crypto_tamper_detection(crypto_key: bytes) -> None:
    crypto = CryptoLayer(encryption_key=crypto_key, compression="zstd")
    data = b"Sensitive border telemetry"
    packed = crypto.pack(data)

    # Tamper with the ciphertext byte
    tampered = bytearray(packed)
    tampered[20] ^= 0xFF

    from cryptography.exceptions import InvalidTag

    with pytest.raises((ValueError, InvalidTag)):
        crypto.unpack(bytes(tampered))


def test_asymmetric_signatures_and_keys() -> None:
    key_mat = CryptoLayer.generate_node_key_material("border-post-01")
    assert len(key_mat.signing_key) == 32
    assert len(key_mat.signing_public_key) == 32
    assert len(key_mat.encryption_key) == 32
    assert len(key_mat.store_key) == 32

    message = b"Order directive: sector B patrol dispatch"
    sig = CryptoLayer.sign(key_mat.signing_key, message)
    assert len(sig) == 64

    # Valid verification
    assert CryptoLayer.verify(key_mat.signing_public_key, message, sig) is True

    # Tampered message
    assert CryptoLayer.verify(key_mat.signing_public_key, b"Tampered order", sig) is False
