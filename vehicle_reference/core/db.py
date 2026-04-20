from __future__ import annotations

import sqlite3
from pathlib import Path

from vehicle_reference.core.paths import REF_SCHEMA_PATH


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def apply_schema(conn: sqlite3.Connection, schema_path: Path | None = None) -> None:
    path = schema_path or REF_SCHEMA_PATH
    sql = path.read_text(encoding="utf-8")
    conn.executescript(sql)
    conn.commit()
