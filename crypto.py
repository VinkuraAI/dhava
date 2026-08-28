"""Cryptographic operations (AES-256-GCM, Ed25519, X25519, HKDF) and compression pipeline."""

from __future__ import annotations

import gzip
import os
from typing import Literal

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ed25519, x25519
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from models import NodeKeyMaterial

try:
    import zstandard as zstd  # type: ignore

    _ZSTD_AVAILABLE = True
except ImportError:
    _ZSTD_AVAILABLE = False

CompressionAlgorithm = Literal["zstd", "gzip", "none"]


class CryptoLayer:
    """
    Handles payload compression and authenticated encryption.
    Pack pipeline: raw_bytes -> compress -> AES-256-GCM encrypt -> [12-byte nonce + ciphertext + tag]
    Unpack pipeline: [nonce + ciphertext + tag] -> AES-256-GCM decrypt -> decompress -> raw_bytes
    """

    def __init__(
        self,
        encryption_key: bytes,
        compression: CompressionAlgorithm = "zstd",
    ) -> None:
        if len(encryption_key) != 32:
            raise ValueError(
                f"Encryption key must be exactly 32 bytes (256 bits), got {len(encryption_key)}"
            )
        self.encryption_key = encryption_key
        self.compression = compression
        self._aesgcm = AESGCM(self.encryption_key)

    def pack(self, data: bytes) -> bytes:
        """Compress and encrypt data."""
        compressed = self._compress(data)
        nonce = os.urandom(12)
        ciphertext = self._aesgcm.encrypt(nonce, compressed, None)
        return nonce + ciphertext

    def unpack(self, packed_data: bytes) -> bytes:
        """Decrypt and decompress data."""
        if len(packed_data) < 28:  # 12-byte nonce + 16-byte GCM tag minimum
            raise ValueError(f"Packed payload too short: {len(packed_data)} bytes")
        nonce = packed_data[:12]
        ciphertext = packed_data[12:]
        compressed = self._aesgcm.decrypt(nonce, ciphertext, None)
        return self._decompress(compressed)

    def _compress(self, data: bytes) -> bytes:
        if self.compression == "zstd":
            if _ZSTD_AVAILABLE:
                cctx = zstd.ZstdCompressor(level=3)
                return cctx.compress(data)
            # Fallback to gzip if zstd requested but not installed
            return gzip.compress(data)
        elif self.compression == "gzip":
            return gzip.compress(data)
        elif self.compression == "none":
            return data
        else:
            raise ValueError(f"Unsupported compression algorithm: {self.compression}")

    def _decompress(self, data: bytes) -> bytes:
        if self.compression == "zstd":
            if _ZSTD_AVAILABLE:
                try:
                    dctx = zstd.ZstdDecompressor()
                    return dctx.decompress(data)
                except Exception:
                    # Might have been compressed with gzip fallback
                    return gzip.decompress(data)
            return gzip.decompress(data)
        elif self.compression == "gzip":
            return gzip.decompress(data)
        elif self.compression == "none":
            return data
        else:
            raise ValueError(f"Unsupported compression algorithm: {self.compression}")

    @staticmethod
    def generate_key() -> bytes:
        """Generate a cryptographically secure 256-bit AES key."""
        return AESGCM.generate_key(bit_length=256)

    @staticmethod
    def derive_key(master_key: bytes, context: str, length: int = 32) -> bytes:
        """Derive subkeys using HKDF-SHA256."""
        hkdf = HKDF(
            algorithm=hashes.SHA256(),
            length=length,
            salt=None,
            info=context.encode("utf-8"),
        )
        return hkdf.derive(master_key)

    @classmethod
    def generate_node_key_material(cls, node_id: str) -> NodeKeyMaterial:
        """Generate complete cryptographic key bundle for a node."""
        ed_private = ed25519.Ed25519PrivateKey.generate()
        ed_public = ed_private.public_key()
        master_key = cls.generate_key()

        return NodeKeyMaterial(
            node_id=node_id,
            signing_key=ed_private.private_bytes_raw(),
            signing_public_key=ed_public.public_bytes_raw(),
            encryption_key=master_key,
            store_key=cls.derive_key(master_key, f"store:{node_id}"),
            outbox_key=cls.derive_key(master_key, f"outbox:{node_id}"),
            audit_key=cls.derive_key(master_key, f"audit:{node_id}"),
            media_key=cls.derive_key(master_key, f"media:{node_id}"),
        )

    @staticmethod
    def sign(signing_key_bytes: bytes, message: bytes) -> bytes:
        """Sign message using Ed25519 private key."""
        priv_key = ed25519.Ed25519PrivateKey.from_private_bytes(signing_key_bytes)
        return priv_key.sign(message)

    @staticmethod
    def verify(public_key_bytes: bytes, message: bytes, signature: bytes) -> bool:
        """Verify Ed25519 signature."""
        try:
            pub_key = ed25519.Ed25519PublicKey.from_public_bytes(public_key_bytes)
            pub_key.verify(signature, message)
            return True
        except (InvalidSignature, Exception):
            return False

    @staticmethod
    def compute_ecdh_shared_key(
        local_x25519_private: bytes, remote_x25519_public: bytes
    ) -> bytes:
        """Compute X25519 ECDH shared secret and derive AES-256 session key."""
        priv_key = x25519.X25519PrivateKey.from_private_bytes(local_x25519_private)
        pub_key = x25519.X25519PublicKey.from_public_bytes(remote_x25519_public)
        shared_secret = priv_key.exchange(pub_key)
        return CryptoLayer.derive_key(shared_secret, "ddil-session-key", 32)
