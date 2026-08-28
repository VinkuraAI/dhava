"""Unit tests for AuditLogger (append-only trail, query filters, exports)."""

from __future__ import annotations

import json
import time

from audit import AuditLogger


def test_audit_logging_and_querying(audit_logger: AuditLogger) -> None:
    t0 = time.time()

    audit_logger.log("local_write", {"op_id": "op-1", "type": "create"}, user_id="operator_1", timestamp=t0)
    audit_logger.log("sync_push", {"ops_sent": 10}, user_id="system", timestamp=t0 + 1)
    audit_logger.log("conflict_resolved", {"winner": "remote"}, user_id="system", timestamp=t0 + 2)

    assert audit_logger.count() == 3
    assert audit_logger.count("local_write") == 1
    assert audit_logger.count("sync_push") == 1

    # Filter by user
    user_entries = audit_logger.query(user_id="operator_1")
    assert len(user_entries) == 1
    assert user_entries[0].action_type == "local_write"

    # Export JSON
    json_bytes = audit_logger.export(start_time=t0 - 10, format="json")
    exported_data = json.loads(json_bytes.decode("utf-8"))
    assert len(exported_data) == 3

    # Export CSV
    csv_bytes = audit_logger.export(start_time=t0 - 10, format="csv")
    csv_str = csv_bytes.decode("utf-8")
    assert "local_write" in csv_str
    assert "sync_push" in csv_str
