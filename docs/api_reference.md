# DDIL Sync Engine API Reference

## 1. Engine Core (`engine.DDILSyncEngine`)

```python
from engine import DDILSyncEngine
from models import Priority, Record
from transport import Transport
engine = DDILSyncEngine.create(
    node_id: str,
    db_path: str = ":memory:",
    transports: list[Transport] | None = None,
    encryption_key: bytes | None = None,
    compression: str = "zstd",
    config: EngineConfig | None = None,
)
```

### Methods
- `engine.create(collection, record_id, data, priority=Priority.P2, user_id=None, authority=None) -> Record`: Insert a record and enqueue a create delta.
- `engine.update(collection, record_id, data, priority=Priority.P2, user_id=None, authority=None) -> Record`: Update an existing record.
- `engine.merge(collection, record_id, fields, priority=Priority.P2, user_id=None, authority=None) -> Record`: Partial update.
- `engine.delete(collection, record_id, priority=Priority.P2, user_id=None, authority=None) -> None`: Tombstone a record.
- `engine.get(collection, record_id) -> Record | None`: Fetch a record by ID.
- `engine.query(collection, filters=None, limit=100, offset=0) -> list[Record]`: Query collection records.
- `engine.sync_now() -> SyncSession`: Perform an immediate push/pull sync cycle.
- `engine.start()`: Launch background daemon and network watcher.
- `engine.stop()`: Gracefully shut down background workers.
- `engine.get_status() -> EngineStatus`: Get current network state and queue telemetry.
- `engine.export_audit_log(start_time, end_time, format="json") -> bytes`: Export forensic audit entries.

---

## 2. Server Core (`server.DDILSyncServer`)

```python
server = DDILSyncServer.create(
    server_node_id: str,
    db_path: str = ":memory:",
    encryption_key: bytes | None = None,
    compression: str = "zstd",
    config: ServerConfig | None = None,
)
```

### Methods
- `server.handle_sync_request(request: SyncPushRequest) -> SyncPullResponse`: Ingest edge push request, resolve conflicts, and construct delta pull response.
- `server.register_node(node_id, public_key, metadata)`: Provision a new edge client node.
- `server.list_nodes() -> list[NodeInfo]`: Inspect connected node health and sync timestamps.
