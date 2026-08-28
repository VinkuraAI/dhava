"""Persistent outbox queue with priority ordering (P0-P4) and crash resilience."""

from __future__ import annotations

import threading
import time
from typing import Any

from backends.base import StorageBackend
from models import Operation, OperationType, Priority, SyncState
from utils.serialization import json_dumps, json_loads


class OutboxQueue:
    """
    Persistent queue for storing operations pending synchronization.
    Guarantees FIFO delivery within each priority tier (P0 down to P4).
    """

    def __init__(self, backend: StorageBackend) -> None:
        self.backend = backend
        self._lock = threading.RLock()
        self._sync_locked = False

    def enqueue(self, op: Operation) -> None:
        """Enqueue an operation into persistent storage."""
        with self._lock, self.backend.transaction():
            payload_json = json_dumps(op.payload)
            vc_json = json_dumps(op.vector_clock)

            query = """
            INSERT INTO outbox (
                op_id, node_id, op_type, collection, record_id, payload,
                timestamp, vector_clock, lamport_timestamp, priority,
                user_id, authority, source_module, sync_state,
                synced_at, sync_attempts, last_error, enqueued_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            self.backend.execute(
                query,
                (
                    op.op_id,
                    op.node_id,
                    op.op_type.value if hasattr(op.op_type, "value") else str(op.op_type),
                    op.collection,
                    op.record_id,
                    payload_json,
                    op.timestamp,
                    vc_json,
                    op.lamport_timestamp,
                    op.priority.value if hasattr(op.priority, "value") else str(op.priority),
                    op.user_id,
                    op.authority,
                    op.source_module,
                    op.sync_state.value if hasattr(op.sync_state, "value") else str(op.sync_state),
                    op.synced_at,
                    op.sync_attempts,
                    op.last_error,
                    op.enqueued_at,
                ),
            )

    def pending_count(self, priority: Priority | None = None) -> int:
        """Count pending operations, optionally filtered by specific priority level."""
        with self._lock:
            if priority is not None:
                query = "SELECT COUNT(*) as count FROM outbox WHERE sync_state = 'pending' AND priority = ?"
                row = self.backend.fetchone(query, (priority.value,))
            else:
                query = "SELECT COUNT(*) as count FROM outbox WHERE sync_state = 'pending'"
                row = self.backend.fetchone(query)
            return int(row["count"]) if row else 0

    def pending_breakdown(self) -> dict[str, int]:
        """Get count of pending operations grouped by priority tier."""
        with self._lock:
            query = """
            SELECT priority, COUNT(*) as count
            FROM outbox
            WHERE sync_state = 'pending'
            GROUP BY priority;
            """
            rows = self.backend.fetchall(query)
            counts = {p.value: 0 for p in Priority}
            for row in rows:
                p_name = str(row["priority"])
                if p_name in counts:
                    counts[p_name] = int(row["count"])
            return counts

    def get_pending(
        self,
        limit: int = 100,
        priority: Priority | None = None,
        max_priority: Priority | None = None,
        collection: str | None = None,
    ) -> list[Operation]:
        """
        Retrieve pending operations ordered strictly by priority (P0 first) then timestamp (oldest first).
        """
        with self._lock:
            conditions = ["sync_state = 'pending'"]
            params: list[Any] = []

            if priority is not None:
                conditions.append("priority = ?")
                params.append(priority.value)
            elif max_priority is not None:
                # Allowed priorities up to max_priority
                allowed = [p.value for p in Priority if p.level <= max_priority.level]
                placeholders = ",".join("?" for _ in allowed)
                conditions.append(f"priority IN ({placeholders})")
                params.extend(allowed)

            if collection is not None:
                conditions.append("collection = ?")
                params.append(collection)

            where_clause = " AND ".join(conditions)
            query = f"""
            SELECT * FROM outbox
            WHERE {where_clause}
            ORDER BY
                CASE priority
                    WHEN 'P0' THEN 0
                    WHEN 'P1' THEN 1
                    WHEN 'P2' THEN 2
                    WHEN 'P3' THEN 3
                    WHEN 'P4' THEN 4
                    ELSE 5
                END ASC,
                timestamp ASC
            LIMIT ?;
            """
            params.append(limit)
            rows = self.backend.fetchall(query, tuple(params))
            return [self._row_to_operation(r) for r in rows]

    def mark_synced(self, op_ids: list[str], peer_node_id: str | None = None) -> None:
        """Mark operations as successfully synced with timestamp."""
        if not op_ids:
            return
        with self._lock, self.backend.transaction():
            now = time.time()
            placeholders = ",".join("?" for _ in op_ids)
            query = f"""
            UPDATE outbox
            SET sync_state = 'synced', synced_at = ?
            WHERE op_id IN ({placeholders});
            """
            params: list[Any] = [now]
            params.extend(op_ids)
            self.backend.execute(query, tuple(params))

    def mark_failed(self, op_ids: list[str], error: str) -> None:
        """Mark operations as failed and increment sync attempts."""
        if not op_ids:
            return
        with self._lock, self.backend.transaction():
            placeholders = ",".join("?" for _ in op_ids)
            query = f"""
            UPDATE outbox
            SET sync_state = 'failed',
                sync_attempts = sync_attempts + 1,
                last_error = ?
            WHERE op_id IN ({placeholders});
            """
            params: list[Any] = [error]
            params.extend(op_ids)
            self.backend.execute(query, tuple(params))

    def mark_conflicted(self, op_id: str, conflict_detail: dict[str, Any] | None = None) -> None:
        """Mark operation as conflicted."""
        with self._lock, self.backend.transaction():
            query = """
            UPDATE outbox
            SET sync_state = 'conflicted',
                last_error = ?
            WHERE op_id = ?;
            """
            detail_str = json_dumps(conflict_detail) if conflict_detail else "Conflict detected"
            self.backend.execute(query, (detail_str, op_id))

    def reset_in_flight(self) -> int:
        """Reset failed or in-flight sync attempts back to 'pending' on startup or crash recovery."""
        with self._lock, self.backend.transaction():
            query = "UPDATE outbox SET sync_state = 'pending' WHERE sync_state = 'failed';"
            return self.backend.execute(query)

    def purge_synced(self, older_than_seconds: float = 2_592_000) -> int:
        """Purge synced operations older than the specified age (default 30 days)."""
        with self._lock, self.backend.transaction():
            cutoff = time.time() - older_than_seconds
            query = "DELETE FROM outbox WHERE sync_state = 'synced' AND synced_at < ?;"
            return self.backend.execute(query, (cutoff,))

    def lock(self) -> bool:
        """Acquire synchronization lock."""
        with self._lock:
            if self._sync_locked:
                return False
            self._sync_locked = True
            return True

    def unlock(self) -> None:
        """Release synchronization lock."""
        with self._lock:
            self._sync_locked = False

    def is_locked(self) -> bool:
        with self._lock:
            return self._sync_locked

    def _row_to_operation(self, row: dict[str, Any]) -> Operation:
        payload = (
            json_loads(row["payload"])
            if isinstance(row["payload"], str)
            else row["payload"]
        )
        vc = (
            json_loads(row["vector_clock"])
            if isinstance(row["vector_clock"], str)
            else row["vector_clock"]
        )
        return Operation(
            op_id=str(row["op_id"]),
            node_id=str(row["node_id"]),
            op_type=OperationType(row["op_type"]),
            collection=str(row["collection"]),
            record_id=str(row["record_id"]),
            payload=payload,
            timestamp=float(row["timestamp"]),
            vector_clock=vc,
            lamport_timestamp=int(row["lamport_timestamp"]),
            priority=Priority(row["priority"]),
            user_id=str(row["user_id"]) if row.get("user_id") is not None else None,
            authority=str(row["authority"]) if row.get("authority") is not None else None,
            source_module=str(row.get("source_module", "app")),
            sync_state=SyncState(row.get("sync_state", "pending")),
            synced_at=float(row["synced_at"]) if row.get("synced_at") is not None else None,
            sync_attempts=int(row.get("sync_attempts", 0)),
            last_error=str(row["last_error"]) if row.get("last_error") is not None else None,
            enqueued_at=float(row.get("enqueued_at", row["timestamp"])),
        )
