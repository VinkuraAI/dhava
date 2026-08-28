# Deployment Patterns for DDIL Sync Engine

## 1. Hub-and-Spoke (Star Topology)

```
[Outpost A] ──── LTE/Satcom ────+
[Outpost B] ──── LTE/Satcom ────┼────> [HQ Central Sync Server]
[Outpost C] ──── LTE/Satcom ────+
```
- Standard deployment for border command, remote clinics, and weather stations.
- Each edge outpost pushes local operations and pulls HQ updates.
- HQ server acts as the central authority and repository.

---

## 2. Tactical Mesh (Peer-to-Peer)

```
[Convoy Unit 1] <──── Mesh Radio ────> [Convoy Unit 2]
       ^                                      ^
       |                                      |
       +────────────── Mesh Radio ────────────+
```
- No central HQ reachable.
- Units discover each other over tactical radio or direct Wi-Fi and perform pairwise synchronization using `TCPTransport`.

---

## 3. Air-Gapped Sneakernet (Physical Media)

```
[Isolated Forward Base] ─── (Write Bundle) ───> [USB Drive]
                                                     │
                                            (Courier Transport)
                                                     │
                                                     v
[HQ Central Server]     <─── (Read Bundle) ──── [USB Drive]
```
- In totally denied environments with electromagnetic jamming or radio silence policy.
- Payloads are encrypted with AES-256-GCM before writing to disk, ensuring zero risk of compromise if media is lost or captured.

---

## 4. Hierarchical Tiered Sync

```
[Outpost Alpha] ─── Mesh ───> [Sector HQ] ─── LTE ───> [State HQ] ─── Fiber ───> [National HQ]
```
- Low-latency tactical updates propagate within sectors over radio.
- Aggregated updates flow upward to national operations centers as higher-bandwidth links become available.
