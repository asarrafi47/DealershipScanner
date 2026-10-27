"""
Snapshot helper for repair/backfill scripts (P13A.7).

Prior-value snapshots land in a timestamped table inside the ``backups`` schema, not in
``public``, and are never overwritten: a second snapshot under the same name raises
``SnapshotExistsError`` instead of silently skipping (``CREATE TABLE IF NOT EXISTS``) or
clobbering (a JSON file opened with ``"w"``). Names are validated before they reach SQL.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import re
from typing import Any, Sequence

BACKUP_SCHEMA = "backups"
_IDENT = re.compile(r"[a-z_][a-z0-9_]*")


class SnapshotExistsError(RuntimeError):
    """A snapshot with this name already exists; refusing to skip or overwrite it."""


def snapshot_ts(now: _dt.datetime | None = None) -> str:
    return (now or _dt.datetime.now()).strftime("%Y%m%d_%H%M%S")


def parse_snapshot_name(name: str) -> tuple[str, str]:
    """Split ``table`` or ``schema.table`` into validated ``(schema, table)``.

    A bare name is read as ``backups.<name>``; legacy snapshots in ``public`` must be
    passed schema-qualified (``--repair-provenance-from public.epa_extended_specs_bak_...``).
    """
    parts = name.strip().lower().split(".")
    if len(parts) == 1:
        parts = [BACKUP_SCHEMA, parts[0]]
    if len(parts) != 2 or not all(_IDENT.fullmatch(p) for p in parts):
        raise ValueError(f"unsafe snapshot table name: {name!r}")
    return parts[0], parts[1]


def _scalar(row: Any) -> Any:
    return next(iter(row.values())) if isinstance(row, dict) else row[0]


def create_snapshot_table(
    conn,
    base: str,
    select_sql: str,
    params: Sequence[Any] = (),
    *,
    ts: str | None = None,
    expected_rows: int | None = None,
) -> str:
    """``CREATE TABLE backups.<base>_<ts> AS <select_sql>``; returns the qualified name.

    Raises ``SnapshotExistsError`` if the table exists, and ``RuntimeError`` (before
    returning) if ``expected_rows`` is given and the copy has a different row count.
    The caller owns the transaction.
    """
    schema, table = parse_snapshot_name(f"{BACKUP_SCHEMA}.{base}_{ts or snapshot_ts()}")
    qualified = f"{schema}.{table}"
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    cur.execute("SELECT to_regclass(%s)", (qualified,))
    if _scalar(cur.fetchone()) is not None:
        raise SnapshotExistsError(f"snapshot {qualified} already exists; not overwriting")
    cur.execute(f"CREATE TABLE {qualified} AS {select_sql}", tuple(params))
    if expected_rows is not None:
        cur.execute(f"SELECT COUNT(*) FROM {qualified}")
        n = int(_scalar(cur.fetchone()))
        if n != expected_rows:
            raise RuntimeError(f"snapshot {qualified} has {n} rows, expected {expected_rows}")
    return qualified


def write_json_snapshot(directory: str, base: str, payload: Any, *, ts: str | None = None) -> str:
    """Write ``<directory>/<base>_<ts>.json`` exclusively; raise if the file exists."""
    if not re.fullmatch(r"[A-Za-z0-9_]+", base):
        raise ValueError(f"unsafe snapshot file name: {base!r}")
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, f"{base}_{ts or snapshot_ts()}.json")
    try:
        f = open(path, "x", encoding="utf-8")
    except FileExistsError as exc:
        raise SnapshotExistsError(f"snapshot file {path} already exists; not overwriting") from exc
    with f:
        json.dump(payload, f, indent=2, default=str)
    return path
