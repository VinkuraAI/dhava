"""Unit tests for Vector Clock causality logic."""

from __future__ import annotations

from vector_clock import VectorClock


def test_vector_clock_increment() -> None:
    vc = VectorClock({})
    assert vc.get("node_a") == 0

    vc1 = vc.increment("node_a")
    assert vc1.get("node_a") == 1
    assert vc1.to_dict() == {"node_a": 1}

    vc2 = vc1.increment("node_a")
    assert vc2.get("node_a") == 2

    vc3 = vc2.increment("node_b")
    assert vc3.get("node_a") == 2
    assert vc3.get("node_b") == 1


def test_vector_clock_merge() -> None:
    vc1 = VectorClock({"node_a": 2, "node_b": 1})
    vc2 = VectorClock({"node_a": 1, "node_b": 4, "node_c": 3})

    merged = vc1.merge(vc2)
    assert merged.get("node_a") == 2
    assert merged.get("node_b") == 4
    assert merged.get("node_c") == 3


def test_vector_clock_comparisons() -> None:
    vc1 = VectorClock({"a": 1, "b": 1})
    vc2 = VectorClock({"a": 1, "b": 1})
    assert vc1.compare(vc2) == "equal"

    # Causally before (happened-before)
    vc_before = VectorClock({"a": 1})
    vc_after = VectorClock({"a": 2, "b": 1})
    assert vc_before.compare(vc_after) == "before"
    assert vc_after.compare(vc_before) == "after"
    assert vc_after.dominates(vc_before) is True
    assert vc_before.dominates(vc_after) is False

    # Concurrent (independent offline modifications)
    vc_conc1 = VectorClock({"a": 2, "b": 1})
    vc_conc2 = VectorClock({"a": 1, "b": 3})
    assert vc_conc1.compare(vc_conc2) == "concurrent"
    assert vc_conc2.compare(vc_conc1) == "concurrent"
    assert vc_conc1.dominates(vc_conc2) is False


def test_vector_clock_empty_and_serialization() -> None:
    vc_empty1 = VectorClock({})
    vc_empty2 = VectorClock.from_dict(None)
    assert vc_empty1.compare(vc_empty2) == "equal"

    data = {"node_1": 5, "node_2": 8}
    vc = VectorClock.from_dict(data)
    assert vc.to_dict() == data
