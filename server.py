"""HQ Server-side sync endpoint coordinating multiple edge nodes."""

from __future__ import annotations

import base64
import hashlib
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from audit import AuditLogger
from backends.sqlite import SQLiteBackend
from conflict import ConflictResolver, ResolutionAction
from crypto import CryptoLayer
from models import Operation, OperationType, Priority
from protocol import SyncPullResponse, SyncPushRequest
from store import LocalStore
from utils.serialization import json_dumps, json_loads, pack_msgpack, unpack_msgpack
from vector_clock import VectorClock


@dataclass
class NodeInfo:
    """Registered edge node status and telemetry."""

    node_id: str
    last_sync: float | None
    last_sync_status: str
    total_ops_synced: int
    pending_ops_on_node: int | None
    public_key: bytes | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ServerConfig:
    max_concurrent_syncs: int = 20
    max_payload_size_bytes: int = 50_000_000
    audit_enabled: bool = True
    node_registration_required: bool = False


class DDILSyncServer:
    """
    HQ Central Sync Server managing synchronization requests from multiple distributed edge nodes.
    """

    def __init__(
        self,
        server_node_id: str,
        local_store: LocalStore,
        conflict_resolver: ConflictResolver,
        crypto: CryptoLayer,
        audit: AuditLogger,
        config: ServerConfig | None = None,
    ) -> None:
        self.server_node_id = server_node_id
        self.local_store = local_store
        self.conflict_resolver = conflict_resolver
        self.crypto = crypto
        self.audit = audit
        self.config = config or ServerConfig()

        self._lamport_counter = 0
        self._global_vector_clock = VectorClock({self.server_node_id: 0})
        self._lock = threading.RLock()

    @classmethod
    def create(
        cls,
        server_node_id: str,
        db_path: str | Path = ":memory:",
        encryption_key: bytes | None = None,
        config: ServerConfig | None = None,
    ) -> DDILSyncServer:
        key = encryption_key or CryptoLayer.generate_key()
        backend = SQLiteBackend(db_path=db_path)
        store = LocalStore(backend, node_id=server_node_id)
        cfg = config or ServerConfig()
        resolver = ConflictResolver(node_id=server_node_id)
        crypto_layer = CryptoLayer(encryption_key=key)
        audit = AuditLogger(backend, node_id=server_node_id)

        return cls(
            server_node_id=server_node_id,
            local_store=store,
            conflict_resolver=resolver,
            crypto=crypto_layer,
            audit=audit,
            config=cfg,
        )

    def handle_sync_request(self, request: SyncPushRequest) -> SyncPullResponse:
        """Process incoming push from edge node and build pull response with new server delta."""
        with self._lock:
            # Check registration if required
            if self.config.node_registration_required:
                node_info = self.get_node_status(request.node_id)
                if not node_info:
                    return SyncPullResponse.create(
                        node_id=self.server_node_id,
                        session_id=request.session_id,
                        status="error",
                        acked_op_ids=[],
                        error=f"Node '{request.node_id}' is not registered on this server",
                    )

            acked_op_ids: list[str] = []
            rejected_op_ids: dict[str, str] = {}
            conflicts_count = 0

            # 1. Decrypt and unpack incoming operations
            if request.encrypted_payload:
                try:
                    decrypted_raw = self.crypto.unpack(request.encrypted_payload)
                    actual_hash = hashlib.sha256(decrypted_raw).hexdigest()
                    if actual_hash != request.payload_hash:
                        return SyncPullResponse.create(
                            node_id=self.server_node_id,
                            session_id=request.session_id,
                            status="error",
                            acked_op_ids=[],
                            error="Payload hash mismatch (integrity check failed)",
                        )

                    ops_data = unpack_msgpack(decrypted_raw)
                    for op_dict in ops_data:
                        op = Operation.from_dict(op_dict)
                        self._lamport_counter = (
                            max(self._lamport_counter, op.lamport_timestamp) + 1
                        )
                        self._global_vector_clock = self._global_vector_clock.merge(
                            op.vector_clock
                        )

                        local_record = self.local_store.get(
                            op.collection, op.record_id, include_deleted=True
                        )
                        resolution = self.conflict_resolver.resolve(local_record, op)

                        if resolution.comparison == "concurrent":
                            conflicts_count += 1
                            if self.config.audit_enabled:
                                self.audit.log(
                                    action_type="conflict_resolved",
                                    details=resolution.audit_detail,
                                )

                        if resolution.action in (
                            ResolutionAction.APPLY_REMOTE,
                            ResolutionAction.MERGE_FIELDS,
                        ):
                            self.local_store.apply_operation(op)

                        acked_op_ids.append(op.op_id)

                except Exception as exc:
                    return SyncPullResponse.create(
                        node_id=self.server_node_id,
                        session_id=request.session_id,
                        status="error",
                        acked_op_ids=[],
                        error=f"Failed to unpack operations: {exc}",
                    )

            # 2. Determine delta operations on server that sender does not have
            sender_vc = VectorClock(request.sender_vector_clock)
            delta_ops = self._get_operations_for_peer(sender_vc, request.node_id)

            # 3. Serialize and encrypt delta operations for peer
            raw_delta_bytes = pack_msgpack([op.to_dict() for op in delta_ops])
            encrypted_delta = self.crypto.pack(raw_delta_bytes) if delta_ops else b""

            # 4. Update node registration telemetry
            self._update_node_sync_telemetry(
                node_id=request.node_id,
                ops_synced=len(acked_op_ids),
                status="ok",
            )

            # 5. Build response
            return SyncPullResponse.create(
                node_id=self.server_node_id,
                session_id=request.session_id,
                status="ok",
                acked_op_ids=acked_op_ids,
                rejected_op_ids=rejected_op_ids,
                encrypted_payload=encrypted_delta,
                raw_payload=raw_delta_bytes if delta_ops else b"",
                operation_count=len(delta_ops),
            )

    def _get_operations_for_peer(
        self, peer_vc: VectorClock, peer_node_id: str
    ) -> list[Operation]:
        """Find records modified since the peer's known vector clock."""
        delta_ops: list[Operation] = []
        collections = self.local_store.get_all_collections()

        for coll in collections:
            records = self.local_store.query(coll, limit=500, include_deleted=True)
            for rec in records:
                # If record last modified by someone other than peer, and record clock dominates peer clock
                rec_vc = VectorClock(rec.vector_clock)
                if rec.last_modified_by != peer_node_id:
                    # Compare: does the server record have newer versions not in peer_vc?
                    comparison = peer_vc.compare(rec_vc)
                    if comparison in ("before", "concurrent"):
                        op_type = (
                            OperationType.DELETE
                            if rec.deleted
                            else OperationType.UPDATE
                        )
                        delta_ops.append(
                            Operation(
                                op_id=f"srv-{rec.collection}-{rec.record_id}-{rec.version}",
                                node_id=rec.last_modified_by,
                                op_type=op_type,
                                collection=rec.collection,
                                record_id=rec.record_id,
                                payload=rec.data,
                                timestamp=rec.last_modified,
                                vector_clock=rec.vector_clock,
                                lamport_timestamp=rec.version,
                                priority=Priority.P2,
                            )
                        )
        return delta_ops

    def register_node(
        self,
        node_id: str,
        public_key: bytes | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Register an authorized edge node."""
        with self._lock, self.local_store.backend.transaction():
            pub_b64 = base64.b64encode(public_key).decode("utf-8") if public_key else None
            meta_json = json_dumps(metadata or {})
            now = time.time()
            query = """
            INSERT INTO sync_nodes (node_id, public_key, registered_at, last_sync_at, last_sync_status, total_ops_synced, metadata)
            VALUES (?, ?, ?, NULL, 'never', 0, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                public_key = COALESCE(excluded.public_key, sync_nodes.public_key),
                metadata = excluded.metadata;
            """
            self.local_store.backend.execute(query, (node_id, pub_b64, now, meta_json))

    def list_nodes(self) -> list[NodeInfo]:
        """List all registered edge nodes."""
        with self._lock:
            query = "SELECT * FROM sync_nodes ORDER BY registered_at ASC;"
            rows = self.local_store.backend.fetchall(query)
            return [self._row_to_node_info(r) for r in rows]

    def get_node_status(self, node_id: str) -> NodeInfo | None:
        """Get telemetry for a specific node."""
        with self._lock:
            query = "SELECT * FROM sync_nodes WHERE node_id = ?;"
            row = self.local_store.backend.fetchone(query, (node_id,))
            return self._row_to_node_info(row) if row else None

    def _update_node_sync_telemetry(self, node_id: str, ops_synced: int, status: str) -> None:
        now = time.time()
        # Ensure node exists in table
        self.register_node(node_id)
        query = """
        UPDATE sync_nodes
        SET last_sync_at = ?,
            last_sync_status = ?,
            total_ops_synced = total_ops_synced + ?
        WHERE node_id = ?;
        """
        self.local_store.backend.execute(query, (now, status, ops_synced, node_id))

    def _row_to_node_info(self, row: dict[str, Any]) -> NodeInfo:
        pub_raw = (
            base64.b64decode(row["public_key"].encode("utf-8"))
            if row.get("public_key")
            else None
        )
        meta = (
            json_loads(row["metadata"])
            if isinstance(row["metadata"], str)
            else row["metadata"]
        )
        return NodeInfo(
            node_id=str(row["node_id"]),
            last_sync=float(row["last_sync_at"]) if row.get("last_sync_at") is not None else None,
            last_sync_status=str(row.get("last_sync_status", "never")),
            total_ops_synced=int(row.get("total_ops_synced", 0)),
            pending_ops_on_node=None,
            public_key=pub_raw,
            metadata=meta or {},
        )
