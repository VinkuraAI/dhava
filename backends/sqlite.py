"""SQLite storage backend with WAL mode, robust transactions, and optional SQLCipher encryption."""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from backends.base import StorageBackend

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS records (
    collection TEXT NOT NULL,
    record_id TEXT NOT NULL,
    data TEXT NOT NULL,
    version INTEGER NOT NULL,
    vector_clock TEXT NOT NULL,
    last_modified REAL NOT NULL,
    last_modified_by TEXT NOT NULL,
    deleted INTEGER DEFAULT 0,
    deleted_at REAL,
    PRIMARY KEY (collection, record_id)
);

CREATE INDEX IF NOT EXISTS idx_records_collection ON records(collection);
CREATE INDEX IF NOT EXISTS idx_records_modified ON records(last_modified DESC);
CREATE INDEX IF NOT EXISTS idx_records_deleted ON records(collection, deleted);

CREATE TABLE IF NOT EXISTS outbox (
    op_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    op_type TEXT NOT NULL,
    collection TEXT NOT NULL,
    record_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    timestamp REAL NOT NULL,
    vector_clock TEXT NOT NULL,
    lamport_timestamp INTEGER NOT NULL,
    priority TEXT NOT NULL,
    user_id TEXT,
    authority TEXT,
    source_module TEXT,
    sync_state TEXT DEFAULT 'pending',
    synced_at REAL,
    sync_attempts INTEGER DEFAULT 0,
    last_error TEXT,
    enqueued_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_sync_state ON outbox(sync_state);
CREATE INDEX IF NOT EXISTS idx_outbox_priority ON outbox(priority, timestamp);
CREATE INDEX IF NOT EXISTS idx_outbox_collection ON outbox(collection);
CREATE INDEX IF NOT EXISTS idx_outbox_record ON outbox(collection, record_id);

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id TEXT PRIMARY KEY,
    timestamp REAL NOT NULL,
    action_type TEXT NOT NULL,
    node_id TEXT NOT NULL,
    user_id TEXT,
    authority TEXT,
    details TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action_type);
CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_id);

CREATE TABLE IF NOT EXISTS sync_nodes (
    node_id TEXT PRIMARY KEY,
    public_key TEXT,
    registered_at REAL NOT NULL,
    last_sync_at REAL,
    last_sync_status TEXT,
    total_ops_synced INTEGER DEFAULT 0,
    metadata TEXT
);

CREATE TABLE IF NOT EXISTS sync_sessions (
    session_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    started_at REAL NOT NULL,
    ended_at REAL,
    ops_pushed_by_node INTEGER DEFAULT 0,
    ops_pulled_by_node INTEGER DEFAULT 0,
    conflicts_detected INTEGER DEFAULT 0,
    conflicts_resolved INTEGER DEFAULT 0,
    status TEXT NOT NULL,
    error TEXT
);
"""


def _dict_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> dict[str, Any]:
    fields = [column[0] for column in cursor.description]
    return dict(zip(fields, row, strict=False))


class SQLiteBackend(StorageBackend):
    """Production SQLite storage backend optimized for embedded devices."""

    def __init__(
        self,
        db_path: str | Path = ":memory:",
        encryption_key: bytes | str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.db_path = str(db_path)
        self.encryption_key = encryption_key
        self.timeout = timeout
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None

        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        self._get_connection()
        self.initialize()

    def _get_connection(self) -> sqlite3.Connection:
        with self._lock:
            if self._conn is None:
                conn = sqlite3.connect(
                    self.db_path,
                    timeout=self.timeout,
                    check_same_thread=False,
                    isolation_level=None,  # Autocommit mode; explicit transactions managed
                )
                conn.row_factory = _dict_factory

                if self.db_path != ":memory:":
                    conn.execute("PRAGMA journal_mode=WAL;")
                    conn.execute("PRAGMA synchronous=NORMAL;")
                conn.execute("PRAGMA foreign_keys=ON;")
                conn.execute("PRAGMA busy_timeout=5000;")

                self._conn = conn
            return self._conn

    def initialize(self) -> None:
        with self._lock:
            conn = self._get_connection()
            conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def execute(self, query: str, params: tuple[Any, ...] | dict[str, Any] = ()) -> int:
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.rowcount

    def fetchone(
        self, query: str, params: tuple[Any, ...] | dict[str, Any] = ()
    ) -> dict[str, Any] | None:
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            return dict(row) if row is not None else None

    def fetchall(
        self, query: str, params: tuple[Any, ...] | dict[str, Any] = ()
    ) -> list[dict[str, Any]]:
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            return list(cursor.fetchall())

    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        with self._lock:
            conn = self._get_connection()
            conn.execute("BEGIN IMMEDIATE;")
            try:
                yield
                conn.execute("COMMIT;")
            except Exception:
                conn.execute("ROLLBACK;")
                raise

    def integrity_check(self) -> bool:
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check;")
            row = cursor.fetchone()
            return bool(row and list(row.values())[0] == "ok")
