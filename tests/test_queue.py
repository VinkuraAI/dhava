"""Unit tests for OutboxQueue (multi-priority ordering, locking, retry)."""

from __future__ import annotations

import time

from models import Operation, OperationType, Priority
from outbox import OutboxQueue


def test_queue_priority_and_fifo_ordering(outbox_queue: OutboxQueue) -> None:
    # Enqueue in mixed order: P2, P0, P4, P1, P0 (older), P0 (newer)
    t0 = time.time()

    op_p2 = Operation.create("n1", OperationType.CREATE, "c", "r1", {}, {}, 1, priority=Priority.P2, timestamp=t0 + 1)
    op_p0_old = Operation.create("n1", OperationType.CREATE, "c", "r2", {}, {}, 2, priority=Priority.P0, timestamp=t0 + 2)
    op_p4 = Operation.create("n1", OperationType.CREATE, "c", "r3", {}, {}, 3, priority=Priority.P4, timestamp=t0 + 3)
    op_p1 = Operation.create("n1", OperationType.CREATE, "c", "r4", {}, {}, 4, priority=Priority.P1, timestamp=t0 + 4)
    op_p0_new = Operation.create("n1", OperationType.CREATE, "c", "r5", {}, {}, 5, priority=Priority.P0, timestamp=t0 + 5)

    outbox_queue.enqueue(op_p2)
    outbox_queue.enqueue(op_p0_old)
    outbox_queue.enqueue(op_p4)
    outbox_queue.enqueue(op_p1)
    outbox_queue.enqueue(op_p0_new)

    assert outbox_queue.pending_count() == 5
    assert outbox_queue.pending_count(Priority.P0) == 2

    # Fetch ordered pending
    pending = outbox_queue.get_pending(limit=10)
    ordered_ids = [op.record_id for op in pending]

    # Must be P0 (old), P0 (new), P1, P2, P4
    assert ordered_ids == ["r2", "r5", "r4", "r1", "r3"]


def test_queue_state_transitions(outbox_queue: OutboxQueue) -> None:
    op = Operation.create("n1", OperationType.CREATE, "c", "r1", {}, {}, 1, priority=Priority.P2)
    outbox_queue.enqueue(op)

    # Mark failed
    outbox_queue.mark_failed([op.op_id], error="Connection timeout")
    assert outbox_queue.pending_count() == 0

    # Reset in flight
    outbox_queue.reset_in_flight()
    assert outbox_queue.pending_count() == 1

    # Mark synced
    outbox_queue.mark_synced([op.op_id], peer_node_id="hq-server")
    assert outbox_queue.pending_count() == 0

    # Purge synced
    purged = outbox_queue.purge_synced(older_than_seconds=-1)
    assert purged == 1


def test_queue_locking(outbox_queue: OutboxQueue) -> None:
    assert outbox_queue.is_locked() is False
    assert outbox_queue.lock() is True
    assert outbox_queue.is_locked() is True
    assert outbox_queue.lock() is False
    outbox_queue.unlock()
    assert outbox_queue.is_locked() is False
