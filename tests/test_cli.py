"""Unit tests for the Typer CLI application."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from cli import app

runner = CliRunner()


def test_cli_init_and_status(temp_dir: Path) -> None:
    db_file = temp_dir / "cli_test.db"
    key_file = temp_dir / "cli_key.bin"

    # Init
    res_init = runner.invoke(
        app,
        ["init", "--node-id", "cli-node-1", "--db", str(db_file), "--key", str(key_file)],
    )
    assert res_init.exit_code == 0
    assert db_file.exists()
    assert key_file.exists()

    # Status
    res_status = runner.invoke(
        app,
        ["status", "--node-id", "cli-node-1", "--db", str(db_file), "--key", str(key_file)],
    )
    assert res_status.exit_code == 0
    assert "Node Status: cli-node-1" in res_status.stdout


def test_cli_write_and_query(temp_dir: Path) -> None:
    db_file = temp_dir / "cli_test.db"
    key_file = temp_dir / "cli_key.bin"

    # Write
    res_write = runner.invoke(
        app,
        [
            "write",
            "patrols",
            "p-01",
            '{"sector": "Alpha", "status": "active"}',
            "--priority",
            "P0",
            "--db",
            str(db_file),
            "--key",
            str(key_file),
        ],
    )
    assert res_write.exit_code == 0
    assert "stored in collection 'patrols'" in res_write.stdout

    # Query
    res_query = runner.invoke(
        app,
        ["query", "patrols", "--db", str(db_file), "--key", str(key_file)],
    )
    assert res_query.exit_code == 0
    assert "p-01" in res_query.stdout


def test_cli_benchmark() -> None:
    res_bench = runner.invoke(app, ["benchmark", "--count", "50"])
    assert res_bench.exit_code == 0
    assert "Benchmark complete" in res_bench.stdout
