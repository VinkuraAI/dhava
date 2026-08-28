"""Unit tests for LocalStore (CRUD, queries, tombstones, versioning)."""

from __future__ import annotations

from models import Operation, OperationType
from store import LocalStore


def test_store_crud(local_store: LocalStore) -> None:
    # 1. Create
    op_create = Operation.create(
        node_id="test-node-01",
        op_type=OperationType.CREATE,
        collection="events",
        record_id="rec-001",
        payload={"sensor": "cam-1", "status": "active"},
        vector_clock={"test-node-01": 1},
        lamport_timestamp=1,
    )
    rec = local_store.apply_operation(op_create)
    assert rec.record_id == "rec-001"
    assert rec.collection == "events"
    assert rec.data == {"sensor": "cam-1", "status": "active"}
    assert rec.version == 1
    assert rec.deleted is False

    # 2. Get
    fetched = local_store.get("events", "rec-001")
    assert fetched is not None
    assert fetched.data["sensor"] == "cam-1"

    # 3. Update
    op_update = Operation.create(
        node_id="test-node-01",
        op_type=OperationType.UPDATE,
        collection="events",
        record_id="rec-001",
        payload={"sensor": "cam-1", "status": "alert"},
        vector_clock={"test-node-01": 2},
        lamport_timestamp=2,
    )
    rec_updated = local_store.apply_operation(op_update)
    assert rec_updated.version == 2
    assert rec_updated.data["status"] == "alert"

    # 4. Partial Merge
    op_merge = Operation.create(
        node_id="test-node-01",
        op_type=OperationType.MERGE,
        collection="events",
        record_id="rec-001",
        payload={"battery": 95},
        vector_clock={"test-node-01": 3},
        lamport_timestamp=3,
    )
    rec_merged = local_store.apply_operation(op_merge)
    assert rec_merged.version == 3
    assert rec_merged.data == {"sensor": "cam-1", "status": "alert", "battery": 95}

    # 5. Delete (Tombstone)
    op_del = Operation.create(
        node_id="test-node-01",
        op_type=OperationType.DELETE,
        collection="events",
        record_id="rec-001",
        payload={},
        vector_clock={"test-node-01": 4},
        lamport_timestamp=4,
    )
    rec_del = local_store.apply_operation(op_del)
    assert rec_del.deleted is True

    # Regular get should return None
    assert local_store.get("events", "rec-001") is None
    # With include_deleted=True should return tombstoned record
    assert local_store.get("events", "rec-001", include_deleted=True) is not None


def test_store_query_and_filters(local_store: LocalStore) -> None:
    for i in range(10):
        op = Operation.create(
            node_id="test-node-01",
            op_type=OperationType.CREATE,
            collection="targets",
            record_id=f"tgt-{i}",
            payload={"sector": "B" if i % 2 == 0 else "A", "val": i},
            vector_clock={"test-node-01": i + 1},
            lamport_timestamp=i + 1,
        )
        local_store.apply_operation(op)

    all_targets = local_store.query("targets")
    assert len(all_targets) == 10

    sector_b = local_store.query("targets", filters={"sector": "B"})
    assert len(sector_b) == 5

    count_b = local_store.count("targets", filters={"sector": "B"})
    assert count_b == 5

    collections = local_store.get_all_collections()
    assert "targets" in collections
