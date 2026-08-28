"""Unit tests for ConflictResolver (Vector Clocks, LWW, skew, tiebreaking)."""

from __future__ import annotations

from conflict import ConflictResolver, ResolutionAction
from models import Operation, OperationType, Record


def test_conflict_resolver_non_existent_local() -> None:
    resolver = ConflictResolver(node_id="node_a")
    remote_op = Operation.create(
        node_id="node_b",
        op_type=OperationType.CREATE,
        collection="events",
        record_id="evt-1",
        payload={"x": 1},
        vector_clock={"node_b": 1},
        lamport_timestamp=1,
    )
    res = resolver.resolve(None, remote_op)
    assert res.action == ResolutionAction.APPLY_REMOTE
    assert res.winner == "remote"


def test_conflict_resolver_causal_ordering() -> None:
    resolver = ConflictResolver(node_id="node_a")

    # Local is causally after remote (local has {a: 2}, remote has {a: 1}) -> remote is stale
    local_rec = Record(
        record_id="evt-1",
        collection="events",
        data={"val": "local_new"},
        version=2,
        vector_clock={"node_a": 2},
        last_modified=100.0,
        last_modified_by="node_a",
    )
    stale_remote_op = Operation.create(
        node_id="node_b",
        op_type=OperationType.UPDATE,
        collection="events",
        record_id="evt-1",
        payload={"val": "remote_old"},
        vector_clock={"node_a": 1},
        lamport_timestamp=1,
        timestamp=200.0,  # Even with higher timestamp, causality says local is newer
    )
    res = resolver.resolve(local_rec, stale_remote_op)
    assert res.action == ResolutionAction.DISCARD_REMOTE
    assert res.winner == "local"
    assert res.comparison == "after"

    # Local is causally before remote -> remote is newer -> apply remote
    newer_remote_op = Operation.create(
        node_id="node_b",
        op_type=OperationType.UPDATE,
        collection="events",
        record_id="evt-1",
        payload={"val": "remote_new"},
        vector_clock={"node_a": 2, "node_b": 1},
        lamport_timestamp=3,
        timestamp=150.0,
    )
    res2 = resolver.resolve(local_rec, newer_remote_op)
    assert res2.action == ResolutionAction.APPLY_REMOTE
    assert res2.winner == "remote"
    assert res2.comparison == "before"


def test_conflict_resolver_concurrent_lww() -> None:
    resolver = ConflictResolver(node_id="node_a", clock_skew_tolerance=1.0)

    local_rec = Record(
        record_id="evt-1",
        collection="events",
        data={"val": "local"},
        version=1,
        vector_clock={"node_a": 1},
        last_modified=100.0,
        last_modified_by="node_a",
    )

    # Remote is concurrent with significantly newer timestamp -> Remote wins LWW
    remote_op_newer = Operation.create(
        node_id="node_b",
        op_type=OperationType.UPDATE,
        collection="events",
        record_id="evt-1",
        payload={"val": "remote_newer"},
        vector_clock={"node_b": 1},
        lamport_timestamp=1,
        timestamp=150.0,
    )
    res = resolver.resolve(local_rec, remote_op_newer)
    assert res.action == ResolutionAction.APPLY_REMOTE
    assert res.winner == "remote"
    assert res.comparison == "concurrent"
    assert res.tiebreaker == "remote_timestamp_newer"

    # Remote is concurrent with timestamp within skew tolerance (1.0s) -> node_id tiebreaker
    # Compare "node_b" vs "node_a" -> "node_b" > "node_a" -> remote wins
    remote_op_skew = Operation.create(
        node_id="node_b",
        op_type=OperationType.UPDATE,
        collection="events",
        record_id="evt-1",
        payload={"val": "remote_skew"},
        vector_clock={"node_b": 1},
        lamport_timestamp=1,
        timestamp=100.5,
    )
    res_skew = resolver.resolve(local_rec, remote_op_skew)
    assert res_skew.action == ResolutionAction.APPLY_REMOTE
    assert res_skew.winner == "remote"
    assert "node_id_tiebreaker" in str(res_skew.tiebreaker)
