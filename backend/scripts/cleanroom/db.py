"""Read-only database access for the clean-room acceptance tool.

Postgres sessions are opened with ``-c default_transaction_read_only=on -c
statement_timeout=<ms>`` and then asked for ``default_transaction_read_only``
and ``transaction_read_only``; anything but ``on`` raises ``ReadOnlyRequired``
before a single metric query runs. SQLite files are opened ``mode=ro`` with
``PRAGMA query_only=ON`` and checked the same way. Metric code only ever sees a
``Target`` that passed the check.
"""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Sequence

PG_HOST_DEFAULT = "/tmp"
PG_PORT_DEFAULT = 5432
STATEMENT_TIMEOUT_MS = 120_000
READ_ONLY_PGOPTIONS = f"-c default_transaction_read_only=on -c statement_timeout={STATEMENT_TIMEOUT_MS}"


class ReadOnlyRequired(RuntimeError):
    """The session would accept writes; the tool refuses to run on it."""


@dataclass
class Target:
    """One verified read-only connection plus what it points at."""

    name: str
    dialect: str  # "postgres" | "sqlite"
    conn: Any
    dsn: str = ""
    timings: dict[str, float] = field(default_factory=dict)

    @property
    def is_pg(self) -> bool:
        return self.dialect == "postgres"

    def sql(self, text: str) -> str:
        """Placeholders are written ``%s``; sqlite3 wants ``?``."""
        return text if self.is_pg else text.replace("%s", "?")

    def rows(self, text: str, params: Sequence[Any] = (), label: str | None = None) -> list[tuple]:
        t0 = time.monotonic()
        cur = self.conn.cursor()
        try:
            cur.execute(self.sql(text), tuple(params))
            out = list(cur.fetchall())
        finally:
            try:
                cur.close()
            except Exception:
                pass
        if label:
            self.timings[label] = round(self.timings.get(label, 0.0) + time.monotonic() - t0, 3)
        return out

    def one(self, text: str, params: Sequence[Any] = (), label: str | None = None) -> tuple:
        r = self.rows(text, params, label)
        return r[0] if r else ()

    def dict_rows(self, text: str, params: Sequence[Any] = (), label: str | None = None) -> Iterator[dict]:
        t0 = time.monotonic()
        cur = self.conn.cursor()
        try:
            cur.execute(self.sql(text), tuple(params))
            cols = [d[0] for d in cur.description]
            while True:
                batch = cur.fetchmany(5000)
                if not batch:
                    break
                for r in batch:
                    yield dict(zip(cols, r))
        finally:
            try:
                cur.close()
            except Exception:
                pass
            if label:
                self.timings[label] = round(self.timings.get(label, 0.0) + time.monotonic() - t0, 3)

    def has_table(self, table: str) -> bool:
        if self.is_pg:
            r = self.one("SELECT to_regclass(%s) IS NOT NULL", (f"public.{table}",))
            return bool(r and r[0])
        r = self.one("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=%s", (table,))
        return bool(r and r[0])

    def json_text(self, column: str, *path: str | int) -> str:
        """SQL expression extracting a JSON text value from a TEXT column."""
        if self.is_pg:
            return f"({column}::jsonb #>> '{{{','.join(str(p) for p in path)}}}')"
        jp = "$" + "".join(f"[{p}]" if isinstance(p, int) else f".{p}" for p in path)
        return f"json_extract({column}, '{jp}')"

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


def is_sqlite_target(db: str) -> bool:
    return db.startswith("sqlite:") or db.endswith((".db", ".sqlite", ".sqlite3")) or (
        os.sep in db and Path(db).is_file()
    )


def pg_dsn(db: str, host: str = PG_HOST_DEFAULT, port: int = PG_PORT_DEFAULT) -> str:
    return f"dbname={db} host={host} port={int(port)}"


def assert_read_only(conn: Any, dialect: str) -> None:
    """Raise ``ReadOnlyRequired`` unless *conn* is a read-only session."""
    cur = conn.cursor()
    try:
        if dialect == "postgres":
            cur.execute("SHOW default_transaction_read_only")
            dflt = str((cur.fetchone() or [""])[0]).strip().lower()
            cur.execute("SHOW transaction_read_only")
            now = str((cur.fetchone() or [""])[0]).strip().lower()
            if dflt != "on" or now != "on":
                raise ReadOnlyRequired(
                    f"refusing to run: default_transaction_read_only={dflt or '?'} "
                    f"transaction_read_only={now or '?'} (both must be on)"
                )
        else:
            cur.execute("PRAGMA query_only")
            v = (cur.fetchone() or [0])[0]
            if int(v or 0) != 1:
                raise ReadOnlyRequired("refusing to run: SQLite connection is not query_only")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _connect_pg(dsn: str) -> Any:
    import psycopg

    return psycopg.connect(dsn, options=READ_ONLY_PGOPTIONS, autocommit=True)


def _connect_sqlite(db: str) -> Any:
    path = db[len("sqlite:///"):] if db.startswith("sqlite:///") else db
    if not Path(path).is_file():
        raise FileNotFoundError(path)
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    return conn


Connector = Callable[[str, str], Any]


def default_connector(dialect: str, dsn_or_path: str) -> Any:
    return _connect_pg(dsn_or_path) if dialect == "postgres" else _connect_sqlite(dsn_or_path)


def open_target(
    db: str,
    *,
    dsn: str | None = None,
    host: str = PG_HOST_DEFAULT,
    port: int = PG_PORT_DEFAULT,
    connector: Connector | None = None,
) -> Target:
    """Connect read-only and verify it; raise ``ReadOnlyRequired`` otherwise."""
    connector = connector or default_connector
    if is_sqlite_target(db):
        dialect, where = "sqlite", db
    else:
        dialect, where = "postgres", dsn or pg_dsn(db, host, port)
    conn = connector(dialect, where)
    try:
        assert_read_only(conn, dialect)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        raise
    return Target(name=db, dialect=dialect, conn=conn, dsn=where)


def chunked(seq: Sequence[Any], n: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]
