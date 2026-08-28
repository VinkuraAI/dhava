"""Vector clock implementation for causal ordering and conflict detection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

ComparisonResult = Literal["before", "after", "equal", "concurrent"]


@dataclass(frozen=True)
class VectorClock:
    """
    Tracks causal history across distributed nodes.

    Comparison semantics:
    - 'equal': Identical causal history
    - 'before': self happened-before other (self <= other for all nodes, < for at least one)
    - 'after': self happened-after other (self >= other for all nodes, > for at least one)
    - 'concurrent': Independent offline writes (neither dominates)
    """

    clocks: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "clocks",
            {str(k): int(v) for k, v in self.clocks.items() if int(v) > 0},
        )

    def increment(self, node_id: str) -> VectorClock:
        """Create a new VectorClock with the given node's counter incremented by 1."""
        new_clocks = dict(self.clocks)
        new_clocks[node_id] = new_clocks.get(node_id, 0) + 1
        return VectorClock(new_clocks)

    def merge(self, other: VectorClock | dict[str, int]) -> VectorClock:
        """Merge with another vector clock by taking the component-wise maximum."""
        other_clocks = other.clocks if isinstance(other, VectorClock) else other
        all_nodes = set(self.clocks.keys()) | set(other_clocks.keys())
        merged = {
            node: max(self.clocks.get(node, 0), other_clocks.get(node, 0))
            for node in all_nodes
        }
        return VectorClock(merged)

    def compare(self, other: VectorClock | dict[str, int]) -> ComparisonResult:
        """Compare causal order against another VectorClock."""
        other_clocks = other.clocks if isinstance(other, VectorClock) else other
        all_nodes = set(self.clocks.keys()) | set(other_clocks.keys())
        if not all_nodes:
            return "equal"

        self_leq_other = True
        other_leq_self = True

        for node in all_nodes:
            c1 = self.clocks.get(node, 0)
            c2 = other_clocks.get(node, 0)
            if c1 > c2:
                self_leq_other = False
            if c2 > c1:
                other_leq_self = False

        if self_leq_other and other_leq_self:
            return "equal"
        if self_leq_other:
            return "before"
        if other_leq_self:
            return "after"
        return "concurrent"

    def dominates(self, other: VectorClock | dict[str, int]) -> bool:
        """Returns True if self is causally after or equal to other."""
        res = self.compare(other)
        return res in ("after", "equal")

    def get(self, node_id: str, default: int = 0) -> int:
        return self.clocks.get(node_id, default)

    def to_dict(self) -> dict[str, int]:
        return dict(self.clocks)

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> VectorClock:
        if not data:
            return cls({})
        return cls({str(k): int(v) for k, v in data.items()})
