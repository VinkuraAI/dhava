"""Command Line Interface (CLI) for Dhava DDIL Sync Engine."""

from __future__ import annotations

import base64
import json
import sys
import time
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from backends.sqlite import SQLiteBackend
from crypto import CryptoLayer
from engine import DDILSyncEngine, EngineConfig
from models import Priority
from transport import HTTPTransport

app = typer.Typer(
    name="dhava",
    help="Offline-First Sync Engine CLI for Denied, Disrupted, Intermittent, and Limited environments.",
    add_completion=False,
)
console = Console()


def get_default_key_path() -> Path:
    dhava_path = Path.home() / ".dhava" / "node_key.bin"
    if dhava_path.exists():
        return dhava_path
    legacy_path = Path.home() / ".ddil-sync" / "node_key.bin"
    if legacy_path.exists():
        return legacy_path
    return dhava_path


def get_default_db_path() -> Path:
    dhava_path = Path.home() / ".dhava" / "store.db"
    if dhava_path.exists():
        return dhava_path
    legacy_path = Path.home() / ".ddil-sync" / "store.db"
    if legacy_path.exists():
        return legacy_path
    return dhava_path


def load_or_create_key(key_path: Path | None = None) -> bytes:
    path = key_path or get_default_key_path()
    if path.exists():
        return path.read_bytes()
    path.parent.mkdir(parents=True, exist_ok=True)
    key = CryptoLayer.generate_key()
    path.write_bytes(key)
    path.chmod(0o600)
    return key


@app.command()
def init(
    node_id: str = typer.Option("node-01", "--node-id", "-n", help="Unique node identifier"),
    db_path: Path = typer.Option(get_default_db_path(), "--db", "-d", help="Database file path"),
    key_path: Path = typer.Option(get_default_key_path(), "--key", "-k", help="Secret key path"),
) -> None:
    """Initialize a new node database and cryptographic keys."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    load_or_create_key(key_path)

    backend = SQLiteBackend(db_path=db_path)
    backend.initialize()

    key_mat = CryptoLayer.generate_node_key_material(node_id)
    pub_b64 = base64.b64encode(key_mat.signing_public_key).decode("utf-8")

    console.print(
        Panel.fit(
            f"[bold green]Node Initialized Successfully[/bold green]\n\n"
            f"[bold]Node ID:[/bold] {node_id}\n"
            f"[bold]Database:[/bold] {db_path}\n"
            f"[bold]Key File:[/bold] {key_path}\n"
            f"[bold]Ed25519 Public Key:[/bold] {pub_b64}",
            title="DDIL Sync Engine",
            border_style="green",
        )
    )


@app.command()
def status(
    node_id: str = typer.Option("node-01", "--node-id", "-n"),
    db_path: Path = typer.Option(get_default_db_path(), "--db", "-d"),
    key_path: Path = typer.Option(get_default_key_path(), "--key", "-k"),
) -> None:
    """Display node status, pending outbox breakdown, and transport metrics."""
    if not db_path.exists():
        console.print(f"[red]Database not found at {db_path}. Run 'dhava init' first.[/red]")
        raise typer.Exit(code=1)

    key = load_or_create_key(key_path)
    engine = DDILSyncEngine.create(node_id=node_id, db_path=db_path, encryption_key=key)
    eng_status = engine.get_status()

    table = Table(title=f"Node Status: {node_id}", border_style="blue")
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="bold white")

    table.add_row("State", eng_status.state.value.upper())
    table.add_row("Total Pending Ops", str(eng_status.pending_ops))

    for p, count in eng_status.pending_by_priority.items():
        table.add_row(f"  └─ {p}", str(count))

    table.add_row("Active Transport", eng_status.active_transport or "None (Offline)")
    table.add_row("Uptime", f"{eng_status.uptime_seconds:.1f}s")

    console.print(table)


@app.command()
def write(
    collection: str = typer.Argument(..., help="Collection name (e.g. events, sensor_data)"),
    record_id: str = typer.Argument(..., help="Record ID"),
    data_json: str = typer.Argument(..., help="JSON data payload"),
    priority: str = typer.Option("P2", "--priority", "-p", help="Priority P0-P4"),
    node_id: str = typer.Option("node-01", "--node-id", "-n"),
    db_path: Path = typer.Option(get_default_db_path(), "--db", "-d"),
    key_path: Path = typer.Option(get_default_key_path(), "--key", "-k"),
) -> None:
    """Write or update a record in the local store (works offline)."""
    try:
        data = json.loads(data_json)
    except Exception as exc:
        console.print(f"[red]Invalid JSON: {exc}[/red]")
        raise typer.Exit(code=1) from exc

    key = load_or_create_key(key_path)
    engine = DDILSyncEngine.create(node_id=node_id, db_path=db_path, encryption_key=key)

    p_enum = Priority(priority.upper())
    rec = engine.create(collection, record_id, data, priority=p_enum)

    console.print(
        f"[green]Record '{record_id}' stored in collection '{collection}' (version {rec.version}, priority {p_enum.value}).[/green]"
    )


@app.command()
def query(
    collection: str = typer.Argument(..., help="Collection name"),
    node_id: str = typer.Option("node-01", "--node-id", "-n"),
    db_path: Path = typer.Option(get_default_db_path(), "--db", "-d"),
    key_path: Path = typer.Option(get_default_key_path(), "--key", "-k"),
    limit: int = typer.Option(20, "--limit", "-l"),
) -> None:
    """Query records from a collection."""
    key = load_or_create_key(key_path)
    engine = DDILSyncEngine.create(node_id=node_id, db_path=db_path, encryption_key=key)

    records = engine.query(collection, limit=limit)
    if not records:
        console.print(f"[yellow]No records found in collection '{collection}'.[/yellow]")
        return

    table = Table(title=f"Collection: {collection} ({len(records)} records)")
    table.add_column("Record ID", style="cyan")
    table.add_column("Version", style="magenta")
    table.add_column("Last Modified", style="dim")
    table.add_column("Payload", style="white")

    for r in records:
        ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r.last_modified))
        table.add_row(r.record_id, str(r.version), ts_str, json.dumps(r.data))

    console.print(table)


@app.command()
def sync(
    server_url: str | None = typer.Option(None, "--server", "-s", help="HQ Server sync URL"),
    node_id: str = typer.Option("node-01", "--node-id", "-n"),
    db_path: Path = typer.Option(get_default_db_path(), "--db", "-d"),
    key_path: Path = typer.Option(get_default_key_path(), "--key", "-k"),
) -> None:
    """Execute manual synchronization cycle."""
    key = load_or_create_key(key_path)
    transports = []
    if server_url:
        transports.append(HTTPTransport(server_url=server_url))

    engine = DDILSyncEngine.create(
        node_id=node_id,
        db_path=db_path,
        transports=transports,
        encryption_key=key,
    )

    console.print("[yellow]Initiating sync cycle...[/yellow]")
    session = engine.sync_now()

    if session.status == "completed":
        console.print(
            f"[bold green]Sync Completed Successfully[/bold green]\n"
            f"Pushed: {session.ops_pushed} ops ({session.bytes_pushed} bytes in {session.push_duration:.2f}s)\n"
            f"Pulled: {session.ops_pulled} ops ({session.bytes_pulled} bytes in {session.pull_duration:.2f}s)\n"
            f"Conflicts Resolved: {session.conflicts_resolved}"
        )
    else:
        console.print(f"[bold red]Sync Failed:[/bold red] {session.error}")


@app.command()
def daemon(
    interval: float = typer.Option(10.0, "--interval", "-i", help="Sync interval in seconds"),
    server_url: str | None = typer.Option(None, "--server", "-s", help="HQ Server sync URL"),
    node_id: str = typer.Option("node-01", "--node-id", "-n"),
    db_path: Path = typer.Option(get_default_db_path(), "--db", "-d"),
    key_path: Path = typer.Option(get_default_key_path(), "--key", "-k"),
) -> None:
    """Run continuous background synchronization daemon."""
    key = load_or_create_key(key_path)
    transports = []
    if server_url:
        transports.append(HTTPTransport(server_url=server_url))

    cfg = EngineConfig(sync_interval_seconds=interval)
    engine = DDILSyncEngine.create(
        node_id=node_id,
        db_path=db_path,
        transports=transports,
        encryption_key=key,
        config=cfg,
    )

    console.print(f"[bold green]Starting DDIL Sync Daemon for node '{node_id}'...[/bold green]")
    engine.start()

    try:
        while True:
            time.sleep(2)
            st = engine.get_status()
            sys.stdout.write(
                f"\r[Status: {st.state.value.upper()} | Pending: {st.pending_ops} | Transport: {st.active_transport or 'None'}] "
            )
            sys.stdout.flush()
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopping daemon...[/yellow]")
        engine.stop()
        console.print("[green]Daemon stopped cleanly.[/green]")


@app.command()
def benchmark(
    count: int = typer.Option(1000, "--count", "-c", help="Number of records to benchmark"),
) -> None:
    """Run performance and compression benchmark."""
    console.print(f"[bold blue]Running DDIL Sync Engine Benchmark with {count} operations...[/bold blue]")

    key = CryptoLayer.generate_key()
    crypto_zstd = CryptoLayer(encryption_key=key, compression="zstd")
    crypto_gzip = CryptoLayer(encryption_key=key, compression="gzip")

    # Generate test payloads
    payloads = [
        {
            "id": f"evt-{i}",
            "sensor": f"EO-{i % 10}",
            "type": "vehicle_detection",
            "confidence": 0.945,
            "coordinates": [28.6139 + (i * 0.0001), 77.2090 + (i * 0.0001)],
            "sector": f"Sector-{chr(65 + (i % 6))}",
            "metadata": {"operator": f"op_{i % 5}", "auth": "duty_officer"},
        }
        for i in range(count)
    ]

    from utils.serialization import pack_msgpack

    raw_bytes = pack_msgpack(payloads)
    raw_size = len(raw_bytes)

    # Zstd benchmark
    start = time.perf_counter()
    zstd_packed = crypto_zstd.pack(raw_bytes)
    zstd_pack_time = time.perf_counter() - start

    start = time.perf_counter()
    _ = crypto_zstd.unpack(zstd_packed)
    zstd_unpack_time = time.perf_counter() - start

    # Gzip benchmark
    start = time.perf_counter()
    gzip_packed = crypto_gzip.pack(raw_bytes)
    gzip_pack_time = time.perf_counter() - start

    start = time.perf_counter()
    _ = crypto_gzip.unpack(gzip_packed)
    gzip_unpack_time = time.perf_counter() - start

    table = Table(title="Compression & Encryption Benchmark")
    table.add_column("Pipeline", style="cyan")
    table.add_column("Original Size", style="white")
    table.add_column("Packed Size", style="bold green")
    table.add_column("Ratio", style="yellow")
    table.add_column("Pack Time", style="magenta")
    table.add_column("Unpack Time", style="magenta")

    table.add_row(
        "MessagePack + zstd + AES-GCM",
        f"{raw_size:,} B",
        f"{len(zstd_packed):,} B",
        f"{len(zstd_packed) / raw_size * 100:.1f}%",
        f"{zstd_pack_time * 1000:.2f} ms",
        f"{zstd_unpack_time * 1000:.2f} ms",
    )

    table.add_row(
        "MessagePack + gzip + AES-GCM",
        f"{raw_size:,} B",
        f"{len(gzip_packed):,} B",
        f"{len(gzip_packed) / raw_size * 100:.1f}%",
        f"{gzip_pack_time * 1000:.2f} ms",
        f"{gzip_unpack_time * 1000:.2f} ms",
    )

    console.print(table)
    console.print("[green]✓ Benchmark complete.[/green]")


if __name__ == "__main__":
    app()
