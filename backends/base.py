"""Abstract storage backend interface for DDIL Sync Engine."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any


class StorageBackend(ABC):
    """Abstract storage backend supporting ACID transactions and relational/document queries."""

    @abstractmethod
    def initialize(self) -> None:
        """Create necessary tables and indices."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close database connections and flush caches."""
        pass

    @abstractmethod
    def execute(self, query: str, params: tuple[Any, ...] | dict[str, Any] = ()) -> int:
        """Execute a write statement and return the number of affected rows."""
        pass

    @abstractmethod
    def fetchone(
        self, query: str, params: tuple[Any, ...] | dict[str, Any] = ()
    ) -> dict[str, Any] | None:
        """Fetch a single row as a dictionary."""
        pass

    @abstractmethod
    def fetchall(
        self, query: str, params: tuple[Any, ...] | dict[str, Any] = ()
    ) -> list[dict[str, Any]]:
        """Fetch all matching rows as dictionaries."""
        pass

    @abstractmethod
    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        """Context manager guaranteeing atomic commit on exit or rollback on error."""
        pass

    @abstractmethod
    def integrity_check(self) -> bool:
        """Verify storage file integrity."""
        pass
