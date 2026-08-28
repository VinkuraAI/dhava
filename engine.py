"""DDIL Sync Engine: The central orchestrator for offline-first synchronization."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from audit import AuditLogger
from backends.sqlite import SQLiteBackend
from conflict import ConflictResolver, ResolutionAction
from crypto import CompressionAlgorithm, CryptoLayer
from models import (
    Operation,
    OperationType,
    Priority,
    Record,
    SyncSession,
)
from outbox import OutboxQueue
from protocol import SyncPullResponse, SyncPushRequest
from store import LocalStore
from transport import Transport, TransportManager
from utils.serialization import pack_msgpack, unpack_msgpack
from vector_clock import VectorClock


class EngineState(str, Enum):
    DISCONNECTED = "disconnected"  # No transport available (offline mode)
    CONNECTED = "connected"        # Transport available, idle
    SYNCING = "syncing"            # Sync session actively executing
    ERROR = "error"                # Engine encountered unrecoverable error


@dataclass
class EngineConfig:
    """Operational tuning parameters for sync scheduling, bandwidth adaptivity, and retention."""

    sync_interval_seconds: float = 60.0
    retry_interval_seconds: float = 15.0
    max_sync_attempts: int = 5
    max_payload_size_bytes: int = 10_000_000
    batch_size: int = 100

    low_bandwidth_threshold_bps: int = 128_000
    high_bandwidth_threshold_bps: int = 1_000_000
    clock_skew_tolerance_seconds: float = 1.0

    purge_synced_after_seconds: float = 2_592_000  # 30 days
    max_outbox_size: int = 100_000
    audit_enabled: bool = True
    compression: CompressionAlgorithm = "zstd"


@dataclass
class EngineStatus:
    """Live telemetry snapshot of the sync engine."""

    state: EngineState
    active_transport: str | None
    pending_ops: int
    pending_by_priority: dict[str, int]
    last_sync: SyncSession | None
    last_error: str | None
    uptime_seconds: float
    node_id: str


class _HybridCreate:
    def __init__(self, class_fn: Any, instance_fn: Any) -> None:
        self.class_fn = class_fn
        self.instance_fn = instance_fn

    def __get__(self, instance: Any, owner: Any = None) -> Any:
        if instance is None:
            return self.class_fn.__get__(owner, owner)
        return self.instance_fn.__get__(instance, owner)


class DDILSyncEngine:
    """
    Main DDIL synchronization engine.
    Encapsulates local storage, persistent priority outbox, vector clock causality,
    cryptographic packaging, and multi-transport failover.
    """

    def __init__(
        self,
        node_id: str,
        local_store: LocalStore,
        outbox: OutboxQueue,
        conflict_resolver: ConflictResolver,
        transport_manager: TransportManager,
        crypto: CryptoLayer,
        audit: AuditLogger,
        config: EngineConfig | None = None,
    ) -> None:
        self.node_id = node_id
        self.local_store = local_store
        self.outbox = outbox
        self.conflict_resolver = conflict_resolver
        self.transport_manager = transport_manager
        self.crypto = crypto
        self.audit = audit
        self.config = config or EngineConfig()

        self._start_time = time.time()
        self._state = EngineState.DISCONNECTED
        self._last_sync: SyncSession | None = None
        self._last_error: str | None = None
        self._status_callbacks: list[Callable[[EngineStatus], None]] = []

        self._lamport_counter = 0
        self._global_vector_clock = VectorClock({self.node_id: 0})
        self._lock = threading.RLock()

        # Background daemon management
        self._stop_event = threading.Event()
        self._worker_thread: threading.Thread | None = None

        # Reset any in-flight operations on boot
        self.outbox.reset_in_flight()

    @classmethod
    def _create_engine(
        cls,
        node_id: str,
        db_path: str | Path = ":memory:",
        transports: list[Transport] | None = None,
        encryption_key: bytes | None = None,
        config: EngineConfig | None = None,
    ) -> DDILSyncEngine:
        """Convenience factory method to instantiate an engine with standard SQLite storage."""
        key = encryption_key or CryptoLayer.generate_key()
        backend = SQLiteBackend(db_path=db_path)
        store = LocalStore(backend, node_id=node_id)
        outbox = OutboxQueue(backend)
        cfg = config or EngineConfig()
        resolver = ConflictResolver(
            node_id=node_id,
            clock_skew_tolerance=cfg.clock_skew_tolerance_seconds,
        )
        tm = TransportManager(transports or [])
        crypto_layer = CryptoLayer(encryption_key=key, compression=cfg.compression)
        audit = AuditLogger(backend, node_id=node_id)

        return cls(
            node_id=node_id,
            local_store=store,
            outbox=outbox,
            conflict_resolver=resolver,
            transport_manager=tm,
            crypto=crypto_layer,
            audit=audit,
            config=cfg,
        )

    # --- Application CRUD API (Always non-blocking and fully offline) ---

    def _create_record(
        self,
        collection: str,
        record_id: str,
        data: dict[str, Any],
        priority: Priority = Priority.P2,
        user_id: str | None = None,
        authority: str | None = None,
        source_module: str = "app",
    ) -> Record:
        """Create a new record locally and enqueue an operation into the outbox."""
        with self._lock:
            self._lamport_counter += 1
            vc = self.local_store.get_vector_clock(collection, record_id)
            new_vc = vc.increment(self.node_id)
            self._global_vector_clock = self._global_vector_clock.merge(new_vc)

            op = Operation.create(
                node_id=self.node_id,
                op_type=OperationType.CREATE,
                collection=collection,
                record_id=record_id,
                payload=data,
                vector_clock=new_vc.to_dict(),
                lamport_timestamp=self._lamport_counter,
                priority=priority,
                user_id=user_id,
                authority=authority,
                source_module=source_module,
            )

            record = self.local_store.apply_operation(op)
            self.outbox.enqueue(op)

            if self.config.audit_enabled:
                self.audit.log(
                    action_type="local_write",
                    details={
                        "op_id": op.op_id,
                        "collection": collection,
                        "record_id": record_id,
                        "op_type": "create",
                        "priority": priority.value,
                    },
                    user_id=user_id,
                    authority=authority,
                )

            return record

    create = _HybridCreate(_create_engine, _create_record)
    create_engine = _create_engine
    create_record = _create_record

    def update(
        self,
        collection: str,
        record_id: str,
        data: dict[str, Any],
        priority: Priority = Priority.P2,
        user_id: str | None = None,
        authority: str | None = None,
        source_module: str = "app",
    ) -> Record:
        """Update an existing record (full replacement) and enqueue into outbox."""
        with self._lock:
            self._lamport_counter += 1
            vc = self.local_store.get_vector_clock(collection, record_id)
            new_vc = vc.increment(self.node_id)
            self._global_vector_clock = self._global_vector_clock.merge(new_vc)

            op = Operation.create(
                node_id=self.node_id,
                op_type=OperationType.UPDATE,
                collection=collection,
                record_id=record_id,
                payload=data,
                vector_clock=new_vc.to_dict(),
                lamport_timestamp=self._lamport_counter,
                priority=priority,
                user_id=user_id,
                authority=authority,
                source_module=source_module,
            )

            record = self.local_store.apply_operation(op)
            self.outbox.enqueue(op)

            if self.config.audit_enabled:
                self.audit.log(
                    action_type="local_write",
                    details={
                        "op_id": op.op_id,
                        "collection": collection,
                        "record_id": record_id,
                        "op_type": "update",
                        "priority": priority.value,
                    },
                    user_id=user_id,
                    authority=authority,
                )

            return record

    def merge(
        self,
        collection: str,
        record_id: str,
        fields: dict[str, Any],
        priority: Priority = Priority.P2,
        user_id: str | None = None,
        authority: str | None = None,
        source_module: str = "app",
    ) -> Record:
        """Partially update specific fields of a record and enqueue into outbox."""
        with self._lock:
            self._lamport_counter += 1
            vc = self.local_store.get_vector_clock(collection, record_id)
            new_vc = vc.increment(self.node_id)
            self._global_vector_clock = self._global_vector_clock.merge(new_vc)

            op = Operation.create(
                node_id=self.node_id,
                op_type=OperationType.MERGE,
                collection=collection,
                record_id=record_id,
                payload=fields,
                vector_clock=new_vc.to_dict(),
                lamport_timestamp=self._lamport_counter,
                priority=priority,
                user_id=user_id,
                authority=authority,
                source_module=source_module,
            )

            record = self.local_store.apply_operation(op)
            self.outbox.enqueue(op)

            if self.config.audit_enabled:
                self.audit.log(
                    action_type="local_write",
                    details={
                        "op_id": op.op_id,
                        "collection": collection,
                        "record_id": record_id,
                        "op_type": "merge",
                        "priority": priority.value,
                    },
                    user_id=user_id,
                    authority=authority,
                )

            return record

    def delete(
        self,
        collection: str,
        record_id: str,
        priority: Priority = Priority.P2,
        user_id: str | None = None,
        authority: str | None = None,
        source_module: str = "app",
    ) -> None:
        """Mark record as deleted (tombstone) and enqueue into outbox."""
        with self._lock:
            self._lamport_counter += 1
            vc = self.local_store.get_vector_clock(collection, record_id)
            new_vc = vc.increment(self.node_id)
            self._global_vector_clock = self._global_vector_clock.merge(new_vc)

            op = Operation.create(
                node_id=self.node_id,
                op_type=OperationType.DELETE,
                collection=collection,
                record_id=record_id,
                payload={},
                vector_clock=new_vc.to_dict(),
                lamport_timestamp=self._lamport_counter,
                priority=priority,
                user_id=user_id,
                authority=authority,
                source_module=source_module,
            )

            self.local_store.apply_operation(op)
            self.outbox.enqueue(op)

            if self.config.audit_enabled:
                self.audit.log(
                    action_type="local_write",
                    details={
                        "op_id": op.op_id,
                        "collection": collection,
                        "record_id": record_id,
                        "op_type": "delete",
                        "priority": priority.value,
                    },
                    user_id=user_id,
                    authority=authority,
                )

    def get(self, collection: str, record_id: str) -> Record | None:
        """Query record from local store."""
        return self.local_store.get(collection, record_id)

    def query(
        self,
        collection: str,
        filters: dict[str, Any] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Record]:
        """Query records from local store."""
        return self.local_store.query(collection, filters=filters, limit=limit, offset=offset)

    def count(self, collection: str, filters: dict[str, Any] | None = None) -> int:
        """Count records in local store."""
        return self.local_store.count(collection, filters=filters)

    # --- Sync Execution Core ---

    def sync_now(self, target_peer_id: str = "server") -> SyncSession:
        """
        Manually trigger an atomic push-pull synchronization cycle.
        Returns the completed SyncSession record.
        """
        session = SyncSession(
            session_id=str(time.time()),
            peer_node_id=target_peer_id,
            transport="none",
            started_at=time.time(),
        )

        transport = self.transport_manager.get_active_transport()
        if not transport:
            session.status = "failed"
            session.error = "No active transport available"
            session.ended_at = time.time()
            self._state = EngineState.DISCONNECTED
            self._last_sync = session
            return session

        session.transport = transport.name()
        self._state = EngineState.SYNCING

        try:
            # 1. Determine priority cutoff and batch size based on bandwidth
            bandwidth_bps = transport.estimate_bandwidth()
            batch_limit = self._calculate_batch_size(bandwidth_bps)
            max_priority = self._calculate_priority_cutoff(bandwidth_bps)

            # 2. Acquire outbox lock and read pending operations
            self.outbox.lock()
            pending_ops = self.outbox.get_pending(
                limit=batch_limit,
                max_priority=max_priority,
            )

            # 3. Serialize and encrypt push payload
            raw_ops_payload = pack_msgpack([op.to_dict() for op in pending_ops])
            encrypted_payload = self.crypto.pack(raw_ops_payload)

            push_req = SyncPushRequest.create(
                node_id=self.node_id,
                sender_vector_clock=self._global_vector_clock.to_dict(),
                encrypted_payload=encrypted_payload,
                raw_payload=raw_ops_payload,
                operation_count=len(pending_ops),
                compression=self.config.compression,
                session_id=session.session_id,
            )

            # 4. Transmit request and await response
            req_bytes = push_req.serialize()
            push_start = time.perf_counter()

            resp_bytes, transport_res = self.transport_manager.exchange(req_bytes)
            session.push_duration = time.perf_counter() - push_start
            session.bytes_pushed = len(req_bytes)
            session.ops_pushed = len(pending_ops)

            if not transport_res.success or resp_bytes is None:
                err_msg = transport_res.error or "Peer did not respond"
                self.outbox.mark_failed([op.op_id for op in pending_ops], err_msg)
                session.status = "failed"
                session.error = err_msg
                session.ended_at = time.time()
                self._last_error = err_msg
                self._state = EngineState.CONNECTED
                self._last_sync = session
                return session

            # 5. Process pull response
            pull_start = time.perf_counter()
            pull_resp = SyncPullResponse.deserialize(resp_bytes)
            session.bytes_pulled = len(resp_bytes)

            if pull_resp.status == "error":
                err_msg = pull_resp.error or "Remote server returned error"
                self.outbox.mark_failed([op.op_id for op in pending_ops], err_msg)
                session.status = "failed"
                session.error = err_msg
                session.ended_at = time.time()
                self._last_error = err_msg
                self._state = EngineState.CONNECTED
                self._last_sync = session
                return session

            # 6. Mark acknowledged ops as synced
            if pull_resp.acked_op_ids:
                self.outbox.mark_synced(pull_resp.acked_op_ids, peer_node_id=pull_resp.node_id)

            # 7. Decrypt and apply pulled remote operations
            pulled_ops_count = 0
            conflicts_detected = 0
            conflicts_resolved = 0

            if pull_resp.encrypted_payload:
                decrypted_pull_raw = self.crypto.unpack(pull_resp.encrypted_payload)
                pulled_ops_dicts = unpack_msgpack(decrypted_pull_raw)

                for op_dict in pulled_ops_dicts:
                    remote_op = Operation.from_dict(op_dict)
                    pulled_ops_count += 1

                    # Update local lamport counter
                    self._lamport_counter = (
                        max(self._lamport_counter, remote_op.lamport_timestamp) + 1
                    )
                    self._global_vector_clock = self._global_vector_clock.merge(
                        remote_op.vector_clock
                    )

                    local_record = self.local_store.get(
                        remote_op.collection, remote_op.record_id, include_deleted=True
                    )
                    resolution = self.conflict_resolver.resolve(local_record, remote_op)

                    if resolution.comparison == "concurrent":
                        conflicts_detected += 1

                    if resolution.action in (
                        ResolutionAction.APPLY_REMOTE,
                        ResolutionAction.MERGE_FIELDS,
                    ):
                        self.local_store.apply_operation(remote_op)
                        conflicts_resolved += 1

                    if self.config.audit_enabled and resolution.comparison == "concurrent":
                        self.audit.log(
                            action_type="conflict_resolved",
                            details=resolution.audit_detail,
                        )

            session.pull_duration = time.perf_counter() - pull_start
            session.ops_pulled = pulled_ops_count
            session.conflicts_detected = conflicts_detected
            session.conflicts_resolved = conflicts_resolved
            session.status = "completed"
            session.ended_at = time.time()

            if self.config.audit_enabled:
                self.audit.log(
                    action_type="sync_push",
                    details={
                        "peer_node_id": pull_resp.node_id,
                        "transport": transport.name(),
                        "operations_sent": session.ops_pushed,
                        "bytes_transmitted": session.bytes_pushed,
                        "duration_seconds": session.push_duration,
                    },
                )
                if session.ops_pulled > 0:
                    self.audit.log(
                        action_type="sync_pull",
                        details={
                            "peer_node_id": pull_resp.node_id,
                            "transport": transport.name(),
                            "operations_received": session.ops_pulled,
                            "bytes_received": session.bytes_pulled,
                            "duration_seconds": session.pull_duration,
                        },
                    )

            self._state = EngineState.CONNECTED
            self._last_sync = session
            self._notify_status_change()
            return session

        except Exception as exc:
            session.status = "failed"
            session.error = str(exc)
            session.ended_at = time.time()
            self._last_error = str(exc)
            self._state = EngineState.ERROR
            self._last_sync = session
            return session

        finally:
            self.outbox.unlock()

    def _calculate_batch_size(self, bandwidth_bps: int) -> int:
        if bandwidth_bps > self.config.high_bandwidth_threshold_bps:
            return self.config.batch_size
        elif bandwidth_bps > self.config.low_bandwidth_threshold_bps:
            return min(self.config.batch_size, 50)
        elif bandwidth_bps > 16_000:
            return min(self.config.batch_size, 20)
        else:
            return min(self.config.batch_size, 5)

    def _calculate_priority_cutoff(self, bandwidth_bps: int) -> Priority:
        if bandwidth_bps >= self.config.high_bandwidth_threshold_bps:
            return Priority.P4
        elif bandwidth_bps >= self.config.low_bandwidth_threshold_bps:
            return Priority.P3
        elif bandwidth_bps >= 16_000:
            return Priority.P2
        else:
            return Priority.P0

    # --- Background Loop and Lifecycle ---

    def start(self) -> None:
        """Start the background synchronization monitor thread."""
        with self._lock:
            if self._worker_thread and self._worker_thread.is_alive():
                return
            self._stop_event.clear()
            self._worker_thread = threading.Thread(
                target=self._background_loop,
                name=f"DDILSync-{self.node_id}",
                daemon=True,
            )
            self._worker_thread.start()

    def stop(self) -> None:
        """Stop background worker cleanly."""
        self._stop_event.set()
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=3.0)
        self.transport_manager.close_all()

    def _background_loop(self) -> None:
        last_purge = time.time()
        while not self._stop_event.is_set():
            try:
                active = self.transport_manager.get_active_transport()
                if active:
                    if self._state == EngineState.DISCONNECTED:
                        self._state = EngineState.CONNECTED

                    # If pending ops exist, trigger sync
                    if self.outbox.pending_count() > 0:
                        self.sync_now()
                else:
                    self._state = EngineState.DISCONNECTED

                # Periodic retention purge
                if time.time() - last_purge > 3600:
                    self.outbox.purge_synced(self.config.purge_synced_after_seconds)
                    last_purge = time.time()

            except Exception as e:
                self._last_error = str(e)
            self._stop_event.wait(self.config.sync_interval_seconds)

    def get_status(self) -> EngineStatus:
        """Get live telemetry and pending queue stats."""
        with self._lock:
            active = self.transport_manager.get_active_transport()
            pending_count = self.outbox.pending_count()
            breakdown = self.outbox.pending_breakdown()
            return EngineStatus(
                state=self._state,
                active_transport=active.name() if active else None,
                pending_ops=pending_count,
                pending_by_priority=breakdown,
                last_sync=self._last_sync,
                last_error=self._last_error,
                uptime_seconds=time.time() - self._start_time,
                node_id=self.node_id,
            )

    def register_status_callback(self, callback: Callable[[EngineStatus], None]) -> None:
        with self._lock:
            self._status_callbacks.append(callback)

    def _notify_status_change(self) -> None:
        status = self.get_status()
        for cb in self._status_callbacks:
            try:
                cb(status)
            except Exception:
                pass
