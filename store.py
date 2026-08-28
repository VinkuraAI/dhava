"""LocalStore implementation providing ACID operations, query filters, and versioning."""

from __future__ import annotations

from typing import Any

from backends.base import StorageBackend
from models import Operation, OperationType, Record
from utils.serialization import json_dumps, json_loads
from vector_clock import VectorClock


class LocalStore:
    """The local source of truth for all records stored on this node."""

    def __init__(self, backend: StorageBackend, node_id: str) -> None:
        self.backend = backend
        self.node_id = node_id

    def get(
        self, collection: str, record_id: str, include_deleted: bool = False
    ) -> Record | None:
        """Fetch a single record by collection and ID."""
        query = "SELECT * FROM records WHERE collection = ? AND record_id = ?"
        row = self.backend.fetchone(query, (collection, record_id))
        if row is None:
            return None

        record = self._row_to_record(row)
        if record.deleted and not include_deleted:
            return None
        return record

    def query(
        self,
        collection: str,
        filters: dict[str, Any] | None = None,
        limit: int = 100,
        offset: int = 0,
        include_deleted: bool = False,
    ) -> list[Record]:
        """Query active records with optional in-memory dictionary filtering."""
        query = "SELECT * FROM records WHERE collection = ?"
        params: list[Any] = [collection]

        if not include_deleted:
            query += " AND deleted = 0"

        query += " ORDER BY last_modified DESC"

        rows = self.backend.fetchall(query, tuple(params))
        records = [self._row_to_record(r) for r in rows]

        if filters:
            records = [r for r in records if self._matches_filters(r.data, filters)]

        return records[offset : offset + limit]

    def count(
        self,
        collection: str,
        filters: dict[str, Any] | None = None,
        include_deleted: bool = False,
    ) -> int:
        """Count matching records in a collection."""
        if filters:
            # When filters are specified, evaluate against filtered records
            return len(self.query(collection, filters=filters, limit=1_000_000, include_deleted=include_deleted))

        query = "SELECT COUNT(*) as count FROM records WHERE collection = ?"
        params: list[Any] = [collection]
        if not include_deleted:
            query += " AND deleted = 0"

        row = self.backend.fetchone(query, tuple(params))
        return int(row["count"]) if row else 0

    def get_all_collections(self) -> list[str]:
        """List distinct non-empty collection names."""
        query = "SELECT DISTINCT collection FROM records WHERE deleted = 0 ORDER BY collection ASC"
        rows = self.backend.fetchall(query)
        return [str(r["collection"]) for r in rows]

    def get_vector_clock(self, collection: str, record_id: str) -> VectorClock:
        """Fetch the current vector clock of a record, or empty VectorClock if not found."""
        rec = self.get(collection, record_id, include_deleted=True)
        if rec is None:
            return VectorClock({})
        return VectorClock(rec.vector_clock)

    def apply_operation(self, op: Operation) -> Record:
        """Apply an operation to the store atomically."""
        with self.backend.transaction():
            if op.op_type == OperationType.CREATE:
                return self._apply_create(op)
            elif op.op_type == OperationType.UPDATE:
                return self._apply_update(op)
            elif op.op_type == OperationType.MERGE:
                return self._apply_merge(op)
            elif op.op_type == OperationType.DELETE:
                return self._apply_delete(op)
            else:
                raise ValueError(f"Unknown operation type: {op.op_type}")

    def _apply_create(self, op: Operation) -> Record:
        existing = self.get(op.collection, op.record_id, include_deleted=True)
        version = (existing.version + 1) if existing else max(1, op.lamport_timestamp)

        data_json = json_dumps(op.payload)
        vc_json = json_dumps(op.vector_clock)

        query = """
        INSERT INTO records (collection, record_id, data, version, vector_clock, last_modified, last_modified_by, deleted, deleted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL)
        ON CONFLICT(collection, record_id) DO UPDATE SET
            data = excluded.data,
            version = excluded.version,
            vector_clock = excluded.vector_clock,
            last_modified = excluded.last_modified,
            last_modified_by = excluded.last_modified_by,
            deleted = 0,
            deleted_at = NULL;
        """
        self.backend.execute(
            query,
            (
                op.collection,
                op.record_id,
                data_json,
                version,
                vc_json,
                op.timestamp,
                op.node_id,
            ),
        )
        return Record(
            record_id=op.record_id,
            collection=op.collection,
            data=op.payload,
            version=version,
            vector_clock=op.vector_clock,
            last_modified=op.timestamp,
            last_modified_by=op.node_id,
            deleted=False,
            deleted_at=None,
        )

    def _apply_update(self, op: Operation) -> Record:
        existing = self.get(op.collection, op.record_id, include_deleted=True)
        version = (existing.version + 1) if existing else max(1, op.lamport_timestamp)

        data_json = json_dumps(op.payload)
        vc_json = json_dumps(op.vector_clock)

        query = """
        INSERT INTO records (collection, record_id, data, version, vector_clock, last_modified, last_modified_by, deleted, deleted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL)
        ON CONFLICT(collection, record_id) DO UPDATE SET
            data = excluded.data,
            version = excluded.version,
            vector_clock = excluded.vector_clock,
            last_modified = excluded.last_modified,
            last_modified_by = excluded.last_modified_by,
            deleted = 0,
            deleted_at = NULL;
        """
        self.backend.execute(
            query,
            (
                op.collection,
                op.record_id,
                data_json,
                version,
                vc_json,
                op.timestamp,
                op.node_id,
            ),
        )
        return Record(
            record_id=op.record_id,
            collection=op.collection,
            data=op.payload,
            version=version,
            vector_clock=op.vector_clock,
            last_modified=op.timestamp,
            last_modified_by=op.node_id,
            deleted=False,
            deleted_at=None,
        )

    def _apply_merge(self, op: Operation) -> Record:
        existing = self.get(op.collection, op.record_id, include_deleted=True)
        base_data: dict[str, Any] = dict(existing.data) if existing else {}
        base_data.update(op.payload)

        version = (existing.version + 1) if existing else max(1, op.lamport_timestamp)
        data_json = json_dumps(base_data)
        vc_json = json_dumps(op.vector_clock)

        query = """
        INSERT INTO records (collection, record_id, data, version, vector_clock, last_modified, last_modified_by, deleted, deleted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL)
        ON CONFLICT(collection, record_id) DO UPDATE SET
            data = excluded.data,
            version = excluded.version,
            vector_clock = excluded.vector_clock,
            last_modified = excluded.last_modified,
            last_modified_by = excluded.last_modified_by,
            deleted = 0,
            deleted_at = NULL;
        """
        self.backend.execute(
            query,
            (
                op.collection,
                op.record_id,
                data_json,
                version,
                vc_json,
                op.timestamp,
                op.node_id,
            ),
        )
        return Record(
            record_id=op.record_id,
            collection=op.collection,
            data=base_data,
            version=version,
            vector_clock=op.vector_clock,
            last_modified=op.timestamp,
            last_modified_by=op.node_id,
            deleted=False,
            deleted_at=None,
        )

    def _apply_delete(self, op: Operation) -> Record:
        existing = self.get(op.collection, op.record_id, include_deleted=True)
        version = (existing.version + 1) if existing else max(1, op.lamport_timestamp)
        data = existing.data if existing else {}
        data_json = json_dumps(data)
        vc_json = json_dumps(op.vector_clock)

        query = """
        INSERT INTO records (collection, record_id, data, version, vector_clock, last_modified, last_modified_by, deleted, deleted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
        ON CONFLICT(collection, record_id) DO UPDATE SET
            version = excluded.version,
            vector_clock = excluded.vector_clock,
            last_modified = excluded.last_modified,
            last_modified_by = excluded.last_modified_by,
            deleted = 1,
            deleted_at = excluded.deleted_at;
        """
        self.backend.execute(
            query,
            (
                op.collection,
                op.record_id,
                data_json,
                version,
                vc_json,
                op.timestamp,
                op.node_id,
                op.timestamp,
            ),
        )
        return Record(
            record_id=op.record_id,
            collection=op.collection,
            data=data,
            version=version,
            vector_clock=op.vector_clock,
            last_modified=op.timestamp,
            last_modified_by=op.node_id,
            deleted=True,
            deleted_at=op.timestamp,
        )

    def _row_to_record(self, row: dict[str, Any]) -> Record:
        data = json_loads(row["data"]) if isinstance(row["data"], str) else row["data"]
        vc = (
            json_loads(row["vector_clock"])
            if isinstance(row["vector_clock"], str)
            else row["vector_clock"]
        )
        return Record(
            record_id=str(row["record_id"]),
            collection=str(row["collection"]),
            data=data,
            version=int(row["version"]),
            vector_clock=vc,
            last_modified=float(row["last_modified"]),
            last_modified_by=str(row["last_modified_by"]),
            deleted=bool(row["deleted"]),
            deleted_at=float(row["deleted_at"]) if row.get("deleted_at") is not None else None,
        )

    def _matches_filters(self, data: dict[str, Any], filters: dict[str, Any]) -> bool:
        for key, expected in filters.items():
            if key not in data or data[key] != expected:
                return False
        return True
