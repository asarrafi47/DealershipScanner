"""
The versioned chain in ``migrations/`` is the inventory schema.

Three layers of proof that retiring the runtime DDL (``inventory_pg`` and the
per-module ``ensure_*`` helpers) loses nothing:

(a) static: every table, column, index and named constraint the runtime DDL
    produced on an empty database (captured in
    ``fixtures/runtime_ddl_schema_golden.json`` on 2026-10-01) is created by some
    ``migrations/*.sql`` file. Postgres auto-named constraints (``*_pkey``,
    ``*_key``, ``*_fkey``) are allowed by pattern, since no file spells them.
(b) unit: :mod:`backend.db.schema_version` (version discovery, warn / strict /
    auto-migrate) against a fake connection -- no database.
(c) live, ``@pytest.mark.integration``: when ``SCHEMA_PARITY_PG_DSN`` names a
    Postgres server where the test may create and drop scratch databases, build
    one database from the chain (V001..V018, record V019 as a baseline, apply the
    rest -- V019 duplicates V001's objects, see migrations/README.md) and one from
    the legacy runtime DDL, then require the migrations database to be a superset
    of the runtime one (tables, columns, index and constraint definitions).
    Skipped otherwise; never points at the inventory database itself.
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid
from pathlib import Path
from typing import Any

import pytest

from backend.db import schema_version as sv

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = REPO_ROOT / "migrations"
GOLDEN = Path(__file__).resolve().parent / "fixtures" / "runtime_ddl_schema_golden.json"

# ---------------------------------------------------------------------------
# (a) static: golden runtime schema is covered by migrations/*.sql
# ---------------------------------------------------------------------------

_IDENT = r'(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)'
_QUALIFIED = rf"(?:{_IDENT}\.)?({_IDENT})"
_CREATE_TABLE_RE = re.compile(
    rf"CREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?{_QUALIFIED}\s*\(",
    re.IGNORECASE,
)
_ALTER_TABLE_RE = re.compile(
    rf"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?{_QUALIFIED}(.*?);",
    re.IGNORECASE | re.DOTALL,
)
_ADD_COLUMN_RE = re.compile(
    rf"\bADD\s+(?:COLUMN\s+)?(?:IF\s+NOT\s+EXISTS\s+)?({_IDENT})", re.IGNORECASE
)
_CREATE_INDEX_RE = re.compile(
    rf"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?({_IDENT})\s+ON\b",
    re.IGNORECASE,
)
_NAMED_CONSTRAINT_RE = re.compile(rf"\bCONSTRAINT\s+({_IDENT})", re.IGNORECASE)
_TABLE_ENTRY_KEYWORDS = {
    "constraint", "primary", "unique", "foreign", "check", "exclude", "like",
}
# Postgres names these itself when the DDL does not (table_pkey, table_col_key,
# table_col_fkey); identifiers are truncated at 63 bytes.
_AUTO_NAMED_RE = re.compile(r"_(pkey|key|fkey)$")


def _unquote(name: str) -> str:
    return name[1:-1] if name.startswith('"') else name.lower()


def _strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", sql)


def _balanced_body(sql: str, open_idx: int) -> str:
    depth = 0
    for i in range(open_idx, len(sql)):
        ch = sql[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return sql[open_idx + 1 : i]
    return sql[open_idx + 1 :]


def _split_top_level(body: str) -> list[str]:
    parts, depth, cur = [], 0, []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    return [p.strip() for p in parts if p.strip()]


def _migration_inventory() -> dict[str, Any]:
    """Tables -> columns, index names and constraint names the chain creates."""
    tables: dict[str, set[str]] = {}
    indexes: set[str] = set()
    constraints: set[str] = set()
    files = sorted(MIGRATIONS_DIR.glob("V*__*.sql"))
    assert files, f"no migrations under {MIGRATIONS_DIR}"
    for path in files:
        sql = _strip_sql_comments(path.read_text(encoding="utf-8"))
        for m in _CREATE_TABLE_RE.finditer(sql):
            table = _unquote(m.group(1))
            cols = tables.setdefault(table, set())
            for entry in _split_top_level(_balanced_body(sql, m.end() - 1)):
                first = re.match(_IDENT, entry)
                if first and first.group(0).lower() not in _TABLE_ENTRY_KEYWORDS:
                    cols.add(_unquote(first.group(0)))
        for m in _ALTER_TABLE_RE.finditer(sql):
            table = _unquote(m.group(1))
            for add in _ADD_COLUMN_RE.finditer(m.group(2)):
                word = add.group(1).lower()
                if word in _TABLE_ENTRY_KEYWORDS:
                    continue  # ADD CONSTRAINT / ADD PRIMARY KEY ...
                tables.setdefault(table, set()).add(_unquote(add.group(1)))
        indexes.update(_unquote(m.group(1)) for m in _CREATE_INDEX_RE.finditer(sql))
        constraints.update(_unquote(m.group(1)) for m in _NAMED_CONSTRAINT_RE.finditer(sql))
    return {"tables": tables, "indexes": indexes, "constraints": constraints}


@pytest.fixture(scope="module")
def golden() -> dict[str, Any]:
    return json.loads(GOLDEN.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def chain() -> dict[str, Any]:
    return _migration_inventory()


def test_golden_fixture_is_not_empty(golden):
    # Guards the guard: an emptied fixture would make every check below vacuous.
    assert len(golden["tables"]) >= 20
    assert golden["indexes"] and golden["constraints"]


def test_every_runtime_table_and_column_is_created_by_a_migration(golden, chain):
    missing_tables = sorted(set(golden["tables"]) - set(chain["tables"]))
    assert not missing_tables, f"tables only the runtime DDL creates: {missing_tables}"
    missing_cols = sorted(
        f"{t}.{c}"
        for t, cols in golden["tables"].items()
        for c in cols
        if c not in chain["tables"].get(t, set())
    )
    assert not missing_cols, f"columns only the runtime DDL creates: {missing_cols}"


def _auto_named(name: str, table: str) -> bool:
    return bool(_AUTO_NAMED_RE.search(name)) and (
        name.startswith(table[:20]) or len(name) >= 60
    )


def test_every_runtime_index_is_created_by_a_migration(golden, chain):
    missing = sorted(
        f"{ix['table']}.{ix['name']}"
        for ix in golden["indexes"]
        if ix["name"] not in chain["indexes"]
        and ix["name"] not in chain["constraints"]  # PK / UNIQUE constraint indexes
        and not _auto_named(ix["name"], ix["table"])
    )
    assert not missing, f"indexes only the runtime DDL creates: {missing}"


def test_every_runtime_named_constraint_is_created_by_a_migration(golden, chain):
    missing = sorted(
        f"{c['table']}.{c['name']} ({c['def']})"
        for c in golden["constraints"]
        if c["name"] not in chain["constraints"]
        and not _auto_named(c["name"], c["table"])
    )
    assert not missing, f"constraints only the runtime DDL creates: {missing}"


def test_v025_zip_index_is_the_runtime_gap_it_claims_to_close(golden, chain):
    assert any(ix["name"] == "idx_cars_active_zip" for ix in golden["indexes"])
    assert "idx_cars_active_zip" in chain["indexes"]


# ---------------------------------------------------------------------------
# (b) unit: backend/db/schema_version.py against a fake connection
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, db: "_FakeConn"):
        self.db = db
        self._rows: list[tuple] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.db.queries.append(sql)
        if "to_regclass" in sql:
            self._rows = [("schema_migrations" if self.db.versions is not None else None,)]
        elif "FROM public.schema_migrations" in sql:
            self._rows = [(v,) for v in sorted(self.db.versions or ())]
        else:
            raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def close(self) -> None:
        pass


class _FakeConn:
    def __init__(self, versions: set[int] | None):
        self.versions = versions
        self.queries: list[str] = []
        self.rollbacks = 0

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def rollback(self) -> None:
        self.rollbacks += 1


@pytest.fixture
def mig_dir(tmp_path, monkeypatch):
    for name in ("V001__base.sql", "V002__two.sql", "V003__three.sql", "notes.sql", "V4_bad.sql"):
        (tmp_path / name).write_text("SELECT 1;\n", encoding="utf-8")
    monkeypatch.setattr(sv, "MIGRATIONS_DIR", tmp_path)
    monkeypatch.delenv("INVENTORY_SCHEMA_CHECK", raising=False)
    monkeypatch.delenv("INVENTORY_AUTO_MIGRATE", raising=False)
    sv.reset_schema_check_cache()
    yield tmp_path
    sv.reset_schema_check_cache()


def test_expected_schema_version_is_highest_well_formed_file(mig_dir, tmp_path_factory):
    assert sv.expected_schema_version() == 3
    assert sv.expected_schema_version(tmp_path_factory.mktemp("empty")) == 0
    assert sv.expected_schema_version(mig_dir / "does-not-exist") == 0


def test_expected_schema_version_matches_repo_chain():
    highest = max(int(re.match(r"V(\d+)__", p.name).group(1)) for p in MIGRATIONS_DIR.glob("V*__*.sql"))
    assert sv.expected_schema_version() == highest >= 25


def test_current_database_returns_true_and_caches(mig_dir):
    conn = _FakeConn({1, 2, 3})
    assert sv.ensure_schema_current(conn) is True
    assert conn.rollbacks == 1  # no transaction left open
    n = len(conn.queries)
    assert sv.ensure_schema_current(conn) is True
    assert len(conn.queries) == n  # cached: no second catalog read


def test_warn_mode_logs_operator_command_and_returns_false(mig_dir, caplog):
    conn = _FakeConn({1, 2})
    with caplog.at_level(logging.WARNING, logger=sv.__name__):
        assert sv.ensure_schema_current(conn) is False
        assert sv.ensure_schema_current(conn) is False  # still behind, re-checked
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, "warning is logged once per process"
    assert "missing V003" in warnings[0]
    assert sv.MIGRATE_COMMAND in warnings[0]
    assert "legacy runtime DDL" in warnings[0]


def test_warn_mode_never_migrated_database(mig_dir, caplog):
    with caplog.at_level(logging.WARNING, logger=sv.__name__):
        assert sv.ensure_schema_current(_FakeConn(None)) is False
    assert "no schema_migrations table" in caplog.text


def test_unknown_mode_reads_as_warn(mig_dir, monkeypatch):
    monkeypatch.setenv("INVENTORY_SCHEMA_CHECK", "loud")
    assert sv.schema_check_mode() == "warn"
    assert sv.ensure_schema_current(_FakeConn({1})) is False


def test_strict_mode_raises(mig_dir, monkeypatch):
    monkeypatch.setenv("INVENTORY_SCHEMA_CHECK", "strict")
    with pytest.raises(sv.SchemaNotMigratedError) as exc:
        sv.ensure_schema_current(_FakeConn({1, 2}))
    assert "V003" in str(exc.value) and "strict" in str(exc.value)


def test_strict_mode_passes_current_database(mig_dir, monkeypatch):
    monkeypatch.setenv("INVENTORY_SCHEMA_CHECK", "STRICT")
    assert sv.ensure_schema_current(_FakeConn({1, 2, 3})) is True


def test_auto_migrate_runs_migrate_apply_first(mig_dir, monkeypatch):
    from backend.scripts import migrate

    conn = _FakeConn({1})
    calls: list[list[str]] = []

    def fake_main(argv):
        calls.append(list(argv))
        conn.versions = {1, 2, 3}
        return 0

    monkeypatch.setattr(migrate, "main", fake_main)
    monkeypatch.setenv("INVENTORY_AUTO_MIGRATE", "1")
    assert sv.ensure_schema_current(conn) is True
    assert calls == [["--apply"]]


def test_auto_migrate_failure_still_falls_back_in_warn_mode(mig_dir, monkeypatch):
    from backend.scripts import migrate

    monkeypatch.setattr(migrate, "main", lambda argv: 1)
    monkeypatch.setenv("INVENTORY_AUTO_MIGRATE", "yes")
    assert sv.ensure_schema_current(_FakeConn({1})) is False


def test_describe_gap_messages():
    assert sv.describe_gap({1, 2, 3}, 3) is None
    assert sv.describe_gap({1, 2, 3, 4}, 3) is None
    assert sv.MIGRATE_COMMAND in sv.describe_gap(None, 3)
    behind_v018 = sv.describe_gap(set(range(1, 19)), 25)
    assert sv.BASELINE_V019_COMMAND in behind_v018
    assert "at V018" in behind_v018 and "expected V025" in behind_v018
    assert sv.BASELINE_V019_COMMAND not in sv.describe_gap(set(range(1, 20)), 25)


def _fresh_init(monkeypatch, conn):
    from backend.db import inventory_pg

    legacy: list[Any] = []
    monkeypatch.setattr(inventory_pg, "_legacy_postgres_inventory_ddl", lambda c: legacy.append(c))
    inventory_pg.reset_postgres_inventory_schema_cache()
    try:
        inventory_pg.init_postgres_inventory(conn)
    finally:
        inventory_pg.reset_postgres_inventory_schema_cache()
    return legacy


def test_init_postgres_inventory_warn_mode_uses_legacy_ddl(mig_dir, monkeypatch):
    conn = _FakeConn({1, 2})
    assert _fresh_init(monkeypatch, conn) == [conn]


def test_init_postgres_inventory_current_database_skips_ddl(mig_dir, monkeypatch):
    assert _fresh_init(monkeypatch, _FakeConn({1, 2, 3})) == []


def test_init_postgres_inventory_strict_mode_refuses(mig_dir, monkeypatch):
    monkeypatch.setenv("INVENTORY_SCHEMA_CHECK", "strict")
    with pytest.raises(sv.SchemaNotMigratedError):
        _fresh_init(monkeypatch, _FakeConn({1, 2}))


# ---------------------------------------------------------------------------
# (c) live: migrations DB vs runtime-DDL DB on a scratch Postgres server
# ---------------------------------------------------------------------------

_PARITY_DSN_ENV = "SCHEMA_PARITY_PG_DSN"


def _with_dbname(dsn: str, dbname: str) -> str:
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    params = conninfo_to_dict(dsn)
    params["dbname"] = dbname
    return make_conninfo(**params)


def _snapshot(dsn: str) -> dict[str, Any]:
    import psycopg

    with psycopg.connect(dsn) as c, c.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' AND table_type='BASE TABLE'"
        )
        tables = {r[0] for r in cur.fetchall()}
        cur.execute(
            "SELECT table_name, column_name, data_type, is_nullable, "
            "COALESCE(column_default, '') FROM information_schema.columns "
            "WHERE table_schema='public'"
        )
        cols = {(r[0], r[1]): tuple(r[2:]) for r in cur.fetchall()}
        cur.execute(
            "SELECT tablename, regexp_replace(indexdef, '^CREATE (UNIQUE )?INDEX \\S+ ON ', ''), "
            "indexdef ~ '^CREATE UNIQUE', indexname FROM pg_indexes WHERE schemaname='public'"
        )
        idx = {(r[0], r[1], r[2]): r[3] for r in cur.fetchall()}
        cur.execute(
            "SELECT conrelid::regclass::text, contype, pg_get_constraintdef(oid), conname "
            "FROM pg_constraint WHERE connamespace='public'::regnamespace "
            "AND contype IN ('p','u','f','c')"
        )
        cons = {(r[0], r[1], r[2]): r[3] for r in cur.fetchall()}
    return {"tables": tables, "cols": cols, "idx": idx, "cons": cons}


def _build_from_migrations(dsn: str) -> None:
    import psycopg

    from backend.scripts import migrate

    conn = psycopg.connect(dsn, autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute(migrate._CREATE_TABLE_SQL)
        conn.commit()
        cur.close()
        for mig in migrate.discover_migrations(MIGRATIONS_DIR):
            if mig.version == 19:
                # V019 re-creates V001 objects: record it, as every real DB does.
                cur = conn.cursor()
                migrate._record(cur, mig)
                conn.commit()
                cur.close()
            else:
                migrate.apply_migration(conn, mig)
    finally:
        conn.close()


def _build_from_runtime_ddl(dsn: str, monkeypatch) -> None:
    from backend.db import comments_db, dictionary_schema, incomplete_listings_db, inventory_pg
    from backend.db.dealer_portal_db import init_dealer_portal_db
    from backend.db.dealerships_db import ensure_dealerships_table
    from backend.db.repositories import grid_cards_repo

    monkeypatch.setenv("INVENTORY_DATABASE_URL", dsn)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("INVENTORY_SQLITE_TESTS", raising=False)
    monkeypatch.delenv("INVENTORY_AUTO_MIGRATE", raising=False)
    monkeypatch.setenv("INVENTORY_SCHEMA_CHECK", "warn")
    inventory_pg.reset_postgres_inventory_schema_cache()  # also resets schema_version
    incomplete_listings_db.reset_incomplete_listings_schema_cache()
    grid_cards_repo.reset_grid_cards_state()
    try:
        conn = inventory_pg.pg_connect()
        try:
            inventory_pg.init_postgres_inventory(conn)  # no schema_migrations -> legacy DDL
            conn.commit()
            cur = conn.cursor()
            ensure_dealerships_table(cur)
            comments_db.ensure_comment_tables(cur)
            comments_db.ensure_comment_flags_table(cur)
            comments_db.ensure_comment_attachments_table(cur)
            comments_db.ensure_dealer_ratings_table(cur)
            dictionary_schema.ensure_dictionary_tables(cur, postgres=True)
            conn.commit()
            incomplete_listings_db._ensure_schema(conn)
            conn.commit()
        finally:
            conn.close()
        init_dealer_portal_db()
        assert grid_cards_repo.ensure_grid_cards_table()
    finally:
        inventory_pg.reset_postgres_inventory_schema_cache()
        incomplete_listings_db.reset_incomplete_listings_schema_cache()
        grid_cards_repo.reset_grid_cards_state()


@pytest.mark.integration
def test_migrations_build_a_superset_of_the_runtime_ddl(monkeypatch):
    admin_dsn = (os.environ.get(_PARITY_DSN_ENV) or "").strip()
    if not admin_dsn:
        pytest.skip(f"{_PARITY_DSN_ENV} not set (needs a server where scratch DBs may be created)")
    psycopg = pytest.importorskip("psycopg")

    tag = uuid.uuid4().hex[:10]
    mig_db, rt_db = f"schema_parity_mig_{tag}", f"schema_parity_rt_{tag}"
    try:
        admin = psycopg.connect(admin_dsn, autocommit=True)
    except Exception as exc:  # pragma: no cover - depends on the environment
        pytest.skip(f"cannot reach {_PARITY_DSN_ENV}: {exc}")
    try:
        try:
            admin.execute(f'CREATE DATABASE "{mig_db}"')
            admin.execute(f'CREATE DATABASE "{rt_db}"')
        except psycopg.errors.InsufficientPrivilege as exc:
            pytest.skip(f"{_PARITY_DSN_ENV} may not create databases: {exc}")

        mig_dsn, rt_dsn = _with_dbname(admin_dsn, mig_db), _with_dbname(admin_dsn, rt_db)
        _build_from_migrations(mig_dsn)
        _build_from_runtime_ddl(rt_dsn, monkeypatch)
        a, b = _snapshot(mig_dsn), _snapshot(rt_dsn)

        problems: list[str] = []
        problems += [f"table only in runtime DDL: {t}" for t in sorted(b["tables"] - a["tables"])]
        for key, val in sorted(b["cols"].items()):
            if key[0] not in a["tables"]:
                continue
            if key not in a["cols"]:
                problems.append(f"column only in runtime DDL: {key} {val}")
            elif a["cols"][key] != val:
                problems.append(f"column differs: {key} migrations={a['cols'][key]} runtime={val}")
        for key, name in sorted(b["idx"].items()):
            if key[0] in a["tables"] and key not in a["idx"]:
                problems.append(f"index only in runtime DDL: {name} {key}")
        for key, name in sorted(b["cons"].items()):
            if key[0] in a["tables"] and key not in a["cons"]:
                problems.append(f"constraint only in runtime DDL: {name} {key}")
        assert not problems, "\n".join(problems)
    finally:
        for db in (mig_db, rt_db):
            try:
                admin.execute(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)')
            except Exception:
                pass
        admin.close()
