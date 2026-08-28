# DDIL Sync Engine Architecture

## 1. System Philosophy

In Denied, Disrupted, Intermittent, and Limited (DDIL) bandwidth environments, the network is not a reliable bus; it is an opportunistic, intermittent side channel.

Traditional synchronization systems rely on persistent sockets, real-time message brokers, or full-state synchronization. Under real-world field conditions—such as a remote border post with sub-zero temperatures, tactical operations in dense mountain valleys, or humanitarian relief zones—these systems fail.

**DDIL Sync Engine** enforces an inverted architectural paradigm:
- **Local-First & Offline-Default**: All writes, queries, and state mutations execute entirely against the local storage node with zero blocking network dependencies.
- **Delta-Based Atomic Outbox**: Instead of syncing whole tables or documents, only discrete delta operations (`create`, `update`, `merge`, `delete`) are captured into a durable, crash-resilient SQLite outbox.
- **Multi-Tier Priority Scheduling**: Writes are classified into priority bands from **P0 (Critical life-safety/alarms)** down to **P4 (Bulk data/media)**. Lower-priority traffic is automatically throttled or deferred under constrained bandwidth.
- **Authenticated Cryptographic Framing**: Sync envelopes are compressed with `zstd` (or `gzip`) and encrypted with AES-256-GCM.
- **Causality & Deterministic Resolution**: Vector Clocks track causal ancestry; Last-Write-Wins (LWW) with millisecond clock skew tolerance and node-ID tiebreakers resolves true concurrent modifications deterministically and auditably.

---

## 2. Component Diagram

```
+-------------------------------------------------------------+
|                     APPLICATION LAYER                       |
|         (Web UI, Sensor Daemons, Edge Analytics)            |
+-------------------------------------------------------------+
                              | (Local CRUD API)
                              v
+-------------------------------------------------------------+
|                     DDIL SYNC ENGINE                        |
|                                                             |
|  +------------------+             +----------------------+  |
|  |    LocalStore    | <---------> |     OutboxQueue      |  |
|  | (SQLite Storage) |             | (P0-P4 Multi-Tier)   |  |
|  +------------------+             +----------------------+  |
|           ^                                  |              |
|           |                                  v              |
|  +------------------+             +----------------------+  |
|  | ConflictResolver | <---------- |     CryptoLayer      |  |
|  |   (VC + LWW)     |             | (AES-GCM + zstd/gz)  |  |
|  +------------------+             +----------------------+  |
|           |                                  |              |
|           v                                  v              |
|  +------------------+             +----------------------+  |
|  |   AuditLogger    |             |   TransportManager   |  |
|  |   (Immutable)    |             |  (Ranking & Failover)|  |
|  +------------------+             +----------------------+  |
+-------------------------------------------------------------+
                                       |
                   +-------------------+-------------------+
                   |                   |                   |
                   v                   v                   v
              [HTTP/LTE]           [Raw TCP]       [File/Sneakernet]
                   |                   |                   |
                   +-------------------+-------------------+
                                       |
                                       v
                              [HQ Sync Server]
```

---

## 3. Storage Model & WAL Durability

Storage uses SQLite configured in **Write-Ahead Logging (WAL)** mode with `PRAGMA synchronous = NORMAL`. This guarantees:
1. Readers never block writers, and writers never block readers.
2. Committed transactions survive abrupt power failure or hardware reboots.
3. On restart, unacknowledged or interrupted operations are reset from `failed` to `pending`.
