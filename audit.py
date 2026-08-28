"""Immutable audit logger for sovereign compliance and after-action forensics."""

from __future__ import annotations

import csv
import io
import threading
from typing import Any

from backends.base import StorageBackend
from models import AuditEntry
from utils.serialization import json_dumps, json_loads


class AuditLogger:
    """
    Append-only persistent audit trail.
    Records every local write, transport switch, sync push/pull, and conflict resolution.
    """

    def __init__(self, backend: StorageBackend, node_id: str = "local") -> None:
        self.backend = backend
        self.node_id = node_id
        self._lock = threading.RLock()

    def log(
        self,
        action_type: str,
        details: dict[str, Any],
        user_id: str | None = None,
        authority: str | None = None,
        timestamp: float | None = None,
    ) -> AuditEntry:
        """Create and persist an immutable audit log record."""
        entry = AuditEntry.create(
            action_type=action_type,
            node_id=self.node_id,
            details=details,
            user_id=user_id,
            authority=authority,
            timestamp=timestamp,
        )
        with self._lock, self.backend.transaction():
            query = """
            INSERT INTO audit_log (audit_id, timestamp, action_type, node_id, user_id, authority, details)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """
            self.backend.execute(
                query,
                (
                    entry.audit_id,
                    entry.timestamp,
                    entry.action_type,
                    entry.node_id,
                    entry.user_id,
                    entry.authority,
                    json_dumps(entry.details),
                ),
            )
        return entry

    def query(
        self,
        action_type: str | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
        user_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[AuditEntry]:
        """Query audit log entries with optional filters."""
        with self._lock:
            conditions: list[str] = []
            params: list[Any] = []

            if action_type is not None:
                conditions.append("action_type = ?")
                params.append(action_type)

            if user_id is not None:
                conditions.append("user_id = ?")
                params.append(user_id)

            if start_time is not None:
                conditions.append("timestamp >= ?")
                params.append(start_time)

            if end_time is not None:
                conditions.append("timestamp <= ?")
                params.append(end_time)

            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            query = f"""
            SELECT * FROM audit_log
            {where_clause}
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?;
            """
            params.extend([limit, offset])

            rows = self.backend.fetchall(query, tuple(params))
            return [self._row_to_entry(r) for r in rows]

    def count(
        self,
        action_type: str | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
    ) -> int:
        """Count audit records matching the specified criteria."""
        with self._lock:
            conditions: list[str] = []
            params: list[Any] = []

            if action_type is not None:
                conditions.append("action_type = ?")
                params.append(action_type)

            if start_time is not None:
                conditions.append("timestamp >= ?")
                params.append(start_time)

            if end_time is not None:
                conditions.append("timestamp <= ?")
                params.append(end_time)

            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            query = f"SELECT COUNT(*) as count FROM audit_log {where_clause};"

            row = self.backend.fetchone(query, tuple(params))
            return int(row["count"]) if row else 0

    def export(
        self,
        start_time: float | None = None,
        end_time: float | None = None,
        format: str = "json",
    ) -> bytes:
        """Export audit trail in JSON or CSV format."""
        entries = self.query(
            start_time=start_time,
            end_time=end_time,
            limit=1_000_000,
        )

        if format.lower() == "json":
            data = [e.to_dict() for e in entries]
            return json_dumps(data).encode("utf-8")
        elif format.lower() == "csv":
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(
                ["audit_id", "timestamp", "action_type", "node_id", "user_id", "authority", "details"]
            )
            for e in entries:
                writer.writerow(
                    [
                        e.audit_id,
                        e.timestamp,
                        e.action_type,
                        e.node_id,
                        e.user_id or "",
                        e.authority or "",
                        json_dumps(e.details),
                    ]
                )
            return output.getvalue().encode("utf-8")
        else:
            raise ValueError(f"Unsupported export format: {format}")

    def _row_to_entry(self, row: dict[str, Any]) -> AuditEntry:
        details = (
            json_loads(row["details"])
            if isinstance(row["details"], str)
            else row["details"]
        )
        return AuditEntry(
            audit_id=str(row["audit_id"]),
            timestamp=float(row["timestamp"]),
            action_type=str(row["action_type"]),
            node_id=str(row["node_id"]),
            details=details,
            user_id=str(row["user_id"]) if row.get("user_id") is not None else None,
            authority=str(row["authority"]) if row.get("authority") is not None else None,
        )
