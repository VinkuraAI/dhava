"""In-memory storage backend for unit testing and fast ephemeral nodes."""

from __future__ import annotations

from backends.sqlite import SQLiteBackend


class InMemoryBackend(SQLiteBackend):
    """High-performance in-memory backend for deterministic testing and temporary state."""

    def __init__(self) -> None:
        super().__init__(db_path=":memory:")
