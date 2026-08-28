# Causal Conflict Resolution & Vector Clocks

## 1. Why Vector Clocks Matter

In distributed edge networks, wall-clock timestamps are inherently unreliable due to clock drift, unsynchronized RTCs, and deliberate radio silence preventing NTP sync.

If two outposts edit the same patrol roster while disconnected:
- **Node A** writes at local time `10:00:00`
- **Node B** writes at local time `10:00:05`

If Node B's hardware clock happened to be running 10 seconds fast, a pure timestamp-based comparison would falsely declare Node B as causally newer even if Node A's operation had causal precedence.

---

## 2. The Vector Clock Algorithm

A `VectorClock` maintains a mapping of `{node_id: counter}` representing the logical operations witnessed by that node.

```python
vc_a = VectorClock({"node_a": 1})
vc_b = VectorClock({"node_b": 1})
```

When comparing two clocks `VC1` and `VC2`:
1. **Equal**: `VC1[n] == VC2[n]` for all nodes. Identical causal ancestry.
2. **Before (Happened-Before)**: `VC1[n] <= VC2[n]` for all `n`, and `<` for at least one `n`. The state of `VC2` knows about and succeeds `VC1`.
3. **After (Happened-After)**: `VC1[n] >= VC2[n]` for all `n`, and `>` for at least one `n`. `VC1` succeeds `VC2`.
4. **Concurrent**: Neither dominates. Both nodes made modifications without knowledge of the other.

---

## 3. Conflict Resolution Strategy

When an incoming remote operation is evaluated against a local record:

```
                      +-----------------------------+
                      | Incoming Remote Operation   |
                      +-----------------------------+
                                     |
                                     v
                        Compare Vector Clocks
                                     |
         +---------------------------+---------------------------+
         |                           |                           |
         v                           v                           v
     [AFTER]                     [BEFORE]                  [CONCURRENT]
Local is newer             Remote is newer              True Conflict!
Discard remote             Apply remote to store                 |
                                                                 v
                                                        Evaluate Timestamps
                                                        (LWW with Tolerance)
                                                                 |
                                                +----------------+----------------+
                                                |                                 |
                                                v                                 v
                                    |Delta| > Skew Tolerance            |Delta| <= Skew Tolerance
                                    Higher timestamp wins               Deterministic node_id
                                                                        lexicographical tiebreaker
```

---

## 4. Forensic Audit Trail

Every conflict is logged to the persistent `audit_log` table with full diagnostic metadata:
- Local & Remote Vector Clocks
- Local & Remote Node IDs
- Timestamps and applied tiebreaker rule
- Winning state and reason
