"""C1 review: V030 is claimed in the migration registry and the migration files form one clean chain."""

from __future__ import annotations

import re
from pathlib import Path

from backend.scripts.migrate import discover_migrations

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "docs" / "NEXT_PHASES_PLAN_2026_10.md"


def _registry_rows() -> dict[str, str]:
    rows = {}
    for line in REGISTRY.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^\s*\| (V\d{3}) \| (.+?) \| (.+?) \|\s*$", line)
        if m:
            rows[m.group(1)] = m.group(2)
    return rows


def test_the_registry_lists_the_chat_usage_file_as_v030():
    rows = _registry_rows()
    assert "chat_usage_buckets" in rows["V030"] and "V030__chat_usage.sql" in rows["V030"]


def test_no_registry_number_is_used_twice_and_the_queued_rows_moved_up():
    text = REGISTRY.read_text(encoding="utf-8")
    numbers = re.findall(r"^\s*\| (V\d{3}) \|", text, flags=re.M)
    table = numbers[numbers.index("V026"):numbers.index("V026") + 16]
    assert len(set(table)) == len(table)
    rows = _registry_rows()
    assert "users.auth_epoch" in rows["V033"] and "V030" not in rows["V033"]


def test_the_schema_files_run_v001_to_v029_then_chat_usage_with_no_gap():
    migrations = discover_migrations(ROOT / "migrations")
    versions = [m.version for m in migrations]
    assert versions == sorted(versions) and len(set(versions)) == len(versions)
    assert versions[:29] == list(range(1, 30)) and versions[29] == 30
    assert [m.path.name for m in migrations if m.version == 30] == ["V030__chat_usage.sql"]


def test_the_chat_usage_migration_is_additive_only():
    sql = (ROOT / "migrations" / "V030__chat_usage.sql").read_text(encoding="utf-8").upper()
    for banned in ("DROP ", "ALTER TABLE", "DELETE FROM", "TRUNCATE", "UPDATE "):
        assert banned not in sql, banned
    assert sql.count("CREATE TABLE IF NOT EXISTS") == 2
