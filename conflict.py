"""Conflict resolution engine combining Vector Clocks and Last-Write-Wins (LWW)."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from models import Operation, OperationType, Record
from vector_clock import VectorClock


class ResolutionAction(str, Enum):
    APPLY_REMOTE = "apply_remote"          # Remote operation is newer or wins LWW
    DISCARD_REMOTE = "discard_remote"      # Remote operation is stale or lost LWW
    MERGE_FIELDS = "merge_fields"          # Partial field merge from remote
    CONFLICT_NOTIFIED = "conflict_notified"# Logged concurrent conflict


@dataclass
class Resolution:
    """Outcome and detailed explanation of a conflict evaluation."""

    action: ResolutionAction
    reason: str
    winner: str  # "local", "remote", or "merge"
    local_vector_clock: dict[str, int] | None
    remote_vector_clock: dict[str, int] | None
    comparison: str
    local_timestamp: float | None
    remote_timestamp: float
    tiebreaker: str | None
    audit_detail: dict[str, Any]


class ConflictResolver:
    """
    Deterministic conflict resolver.
    Uses Vector Clocks for causal ordering and falls back to Last-Write-Wins (LWW)
    with node_id tiebreaking only when operations are truly concurrent.
    """

    def __init__(self, node_id: str, clock_skew_tolerance: float = 1.0) -> None:
        self.node_id = node_id
        self.clock_skew_tolerance = clock_skew_tolerance

    def resolve(self, local_record: Record | None, remote_op: Operation) -> Resolution:
        """
        Evaluate incoming remote operation against the local state.
        Returns a Resolution with action, winner, and full audit diagnostics.
        """
        # Case 1: No local record or local record is tombstoned
        if local_record is None or local_record.deleted:
            if remote_op.op_type == OperationType.DELETE:
                action = ResolutionAction.DISCARD_REMOTE
                reason = "Remote delete for non-existent or already deleted local record"
                winner = "local"
            else:
                action = (
                    ResolutionAction.MERGE_FIELDS
                    if remote_op.op_type == OperationType.MERGE
                    else ResolutionAction.APPLY_REMOTE
                )
                reason = "No active local record exists, applying remote operation"
                winner = "remote"

            audit_detail = {
                "collection": remote_op.collection,
                "record_id": remote_op.record_id,
                "remote_op_id": remote_op.op_id,
                "resolution": action.value,
                "reason": reason,
                "comparison": "non_existent",
                "remote_timestamp": remote_op.timestamp,
            }
            return Resolution(
                action=action,
                reason=reason,
                winner=winner,
                local_vector_clock=local_record.vector_clock if local_record else None,
                remote_vector_clock=remote_op.vector_clock,
                comparison="non_existent",
                local_timestamp=local_record.last_modified if local_record else None,
                remote_timestamp=remote_op.timestamp,
                tiebreaker=None,
                audit_detail=audit_detail,
            )

        # Case 2: Compare vector clocks
        local_vc = VectorClock(local_record.vector_clock)
        remote_vc = VectorClock(remote_op.vector_clock)
        comparison = local_vc.compare(remote_vc)

        if comparison == "after":
            # Local is causally after remote -> remote is stale -> discard
            action = ResolutionAction.DISCARD_REMOTE
            reason = "Local record is causally after remote operation (remote is stale)"
            winner = "local"
            tiebreaker = None

        elif comparison == "before":
            # Local is causally before remote -> remote knows about local and is newer -> apply
            action = (
                ResolutionAction.MERGE_FIELDS
                if remote_op.op_type == OperationType.MERGE
                else ResolutionAction.APPLY_REMOTE
            )
            reason = "Remote operation is causally after local record"
            winner = "remote"
            tiebreaker = None

        elif comparison == "equal":
            # Exactly same causal history -> discard duplicate
            action = ResolutionAction.DISCARD_REMOTE
            reason = "Records have identical causal history"
            winner = "local"
            tiebreaker = None

        else:
            # comparison == 'concurrent': True concurrent modification -> Apply LWW
            local_ts = local_record.last_modified
            remote_ts = remote_op.timestamp

            if remote_ts > local_ts + self.clock_skew_tolerance:
                winner = "remote"
                action = (
                    ResolutionAction.MERGE_FIELDS
                    if remote_op.op_type == OperationType.MERGE
                    else ResolutionAction.APPLY_REMOTE
                )
                tiebreaker = "remote_timestamp_newer"
            elif local_ts > remote_ts + self.clock_skew_tolerance:
                winner = "local"
                action = ResolutionAction.DISCARD_REMOTE
                tiebreaker = "local_timestamp_newer"
            else:
                # Within clock skew window -> use deterministic node_id tiebreaker
                if remote_op.node_id > local_record.last_modified_by:
                    winner = "remote"
                    action = (
                        ResolutionAction.MERGE_FIELDS
                        if remote_op.op_type == OperationType.MERGE
                        else ResolutionAction.APPLY_REMOTE
                    )
                    tiebreaker = (
                        f"node_id_tiebreaker: {remote_op.node_id} > {local_record.last_modified_by}"
                    )
                else:
                    winner = "local"
                    action = ResolutionAction.DISCARD_REMOTE
                    tiebreaker = f"node_id_tiebreaker: {local_record.last_modified_by} >= {remote_op.node_id}"

            reason = f"Concurrent modification resolved by LWW ({tiebreaker})"

        audit_detail = {
            "collection": remote_op.collection,
            "record_id": remote_op.record_id,
            "remote_op_id": remote_op.op_id,
            "local_node": local_record.last_modified_by,
            "remote_node": remote_op.node_id,
            "resolution": action.value,
            "winner": winner,
            "reason": reason,
            "comparison": comparison,
            "local_timestamp": local_record.last_modified,
            "remote_timestamp": remote_op.timestamp,
            "tiebreaker": tiebreaker,
        }

        return Resolution(
            action=action,
            reason=reason,
            winner=winner,
            local_vector_clock=local_record.vector_clock,
            remote_vector_clock=remote_op.vector_clock,
            comparison=comparison,
            local_timestamp=local_record.last_modified,
            remote_timestamp=remote_op.timestamp,
            tiebreaker=tiebreaker,
            audit_detail=audit_detail,
        )
