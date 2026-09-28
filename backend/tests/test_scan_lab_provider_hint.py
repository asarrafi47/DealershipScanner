"""_provider_hint reads the real dealer_recipes schema and never poisons the caller's
transaction (Railway fleet run 2026-09-28: InFailedSqlTransaction in 7 of 8 shards)."""
import sqlite3

from backend.scripts.scan_lab_report import _provider_hint


def _conn():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE dealer_recipes (dealer_id TEXT PRIMARY KEY, recipes_json TEXT NOT NULL, "
        "recipe_count INTEGER NOT NULL DEFAULT 0, provider_hint TEXT, max_saved_at REAL NOT NULL DEFAULT 0, "
        "last_ok_at REAL NOT NULL DEFAULT 0, stale_count INTEGER NOT NULL DEFAULT 0, updated_at TEXT, scan_hints TEXT)"
    )
    return conn


def test_provider_hint_reads_current_schema():
    conn = _conn()
    conn.execute("INSERT INTO dealer_recipes (dealer_id, recipes_json, provider_hint) VALUES ('a-com', '[]', 'typesense')")
    assert _provider_hint(conn, "a-com") == "typesense"
    assert _provider_hint(conn, "b-com") is None


def test_provider_hint_without_table_returns_none_and_rolls_back():
    conn = sqlite3.connect(":memory:")
    assert _provider_hint(conn, "a-com") is None
    conn.execute("SELECT 1").fetchone()  # connection still usable
