"""SQLite -> Postgres statement adapter (``backend.db.inventory_pg``), P4.7.

Every statement a SQLite-shaped call site runs through ``InventoryCursor`` on
Postgres passes ``adapt_sql_for_postgres_execute``: OR REPLACE / OR IGNORE
mappings, function rewrites, then ``qmarks_to_percent_s``. The placeholder
rewrite used to know only ``'`` literals, so one apostrophe in a ``--`` comment
flipped every later ``?``. UPSERT_CARS_SQL adapted correctly only because the
apostrophes in its comments happened to balance. These tests pin the
comment-aware rewrite, the mappings, and (differentially) that every SQL
constant the old adapter handled correctly adapts byte for byte the same.

The tests are pure (no database) except two:

* ``test_do_nothing_is_not_or_ignore_on_sqlite`` runs on in-memory SQLite,
  whose UPSERT ``ON CONFLICT DO NOTHING`` has the same scope as Postgres's;
* ``test_do_nothing_is_not_or_ignore_on_postgres`` (``integration``) runs only
  when ``PG_ADAPTER_TEST_DSN`` names a local throwaway ``pytest_*`` database,
  and touches nothing but a TEMP table in a rolled-back transaction.
"""

from __future__ import annotations

import ast
import logging
import os
import re
import sqlite3
from pathlib import Path

import pytest

from backend.db import inventory_pg
from backend.db.inventory_pg import (
    adapt_sql_for_postgres_execute,
    qmarks_to_percent_s,
    sql_lexical_runs,
)
from backend.scanner.upsert.sql import UPSERT_CARS_SQL

REPO_ROOT = Path(__file__).resolve().parents[2]
_DML_KEYWORDS = {"SELECT", "INSERT", "UPDATE", "DELETE", "WITH"}


# ---------------------------------------------------------------------------
# The pre-P4.7 adapter, frozen verbatim, as the reference for differential tests
# ---------------------------------------------------------------------------


def _legacy_qmarks_to_percent_s(sql: str) -> str:
    out: list[str] = []
    i = 0
    n = len(sql)
    in_single = False
    while i < n:
        c = sql[i]
        if not in_single:
            if c == "'":
                in_single = True
                out.append(c)
                i += 1
                continue
            if c == "?":
                out.append("%s")
                i += 1
                continue
            out.append(c)
            i += 1
            continue
        if c == "'":
            if i + 1 < n and sql[i + 1] == "'":
                out.append("''")
                i += 2
                continue
            in_single = False
            out.append(c)
            i += 1
            continue
        if c == "%":
            out.append("%%")
            i += 1
            continue
        out.append(c)
        i += 1
    return "".join(out)


def _legacy_adapt(sql: str) -> str | None:
    """Pre-P4.7 ``adapt_sql_for_postgres_execute`` (shared helpers are unchanged)."""
    stripped = sql.strip()
    if stripped.upper().startswith("PRAGMA"):
        return None
    low = stripped.upper()
    if low.startswith("INSERT OR REPLACE"):
        alt = inventory_pg.adapt_insert_or_replace_pg(sql)
        if alt:
            sql = alt
    elif low.startswith("INSERT OR IGNORE"):
        alt = inventory_pg.adapt_insert_or_ignore_pg(sql)
        if alt:
            sql = alt
        else:
            sql = re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO", "INSERT INTO", sql, count=1, flags=re.IGNORECASE)
            sql += " ON CONFLICT DO NOTHING"
    sql = inventory_pg.adapt_sqlite_functions_to_pg(sql)
    sql = _legacy_qmarks_to_percent_s(sql)
    return sql


# ---------------------------------------------------------------------------
# Lexical runs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "",
        "SELECT 1",
        "SELECT 'it''s' AS a, \"col\"\"x\" FROM t -- c\n/* a /* b */ c */ WHERE x = ?",
        "SELECT 'unterminated ?",
        "SELECT 1 /* unterminated ? ",
        "SELECT 1 -- no newline ?",
        UPSERT_CARS_SQL,
    ],
)
def test_lexical_runs_join_back_to_the_input(sql):
    runs = sql_lexical_runs(sql)
    assert "".join(text for _, text in runs) == sql
    assert all(text for _, text in runs)


def test_lexical_runs_kinds():
    sql = "SELECT 'a?' , \"b?\" -- c?\n/* d? */ ?"
    assert sql_lexical_runs(sql) == [
        ("code", "SELECT "),
        ("string", "'a?'"),
        ("code", " , "),
        ("ident", '"b?"'),
        ("code", " "),
        ("line_comment", "-- c?"),
        ("code", "\n"),
        ("block_comment", "/* d? */"),
        ("code", " ?"),
    ]


# ---------------------------------------------------------------------------
# Placeholders: literals, escapes, comments, identifiers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        # outside literals
        ("SELECT * FROM t WHERE a = ? AND b = ?", "SELECT * FROM t WHERE a = %s AND b = %s"),
        # inside a literal: a ? is data
        ("SELECT * FROM t WHERE a = '?' AND b = ?", "SELECT * FROM t WHERE a = '?' AND b = %s"),
        # '' escapes a quote and does not end the literal
        ("SELECT 'it''s ?' , ?", "SELECT 'it''s ?' , %s"),
        ("SELECT '''' , ?", "SELECT '''' , %s"),
        ("SELECT '' , ?", "SELECT '' , %s"),
        # apostrophe in a -- comment does not open a literal
        ("SELECT ? -- the dealer's row\n, ?", "SELECT %s -- the dealer's row\n, %s"),
        # apostrophe in a /* */ comment does not open a literal
        ("SELECT ? /* it's */ , ?", "SELECT %s /* it's */ , %s"),
        # ? in a comment is prose, not a placeholder
        ("SELECT ? -- why?\n", "SELECT %s -- why?\n"),
        ("SELECT /* why? */ ?", "SELECT /* why? */ %s"),
        # % in a comment is doubled: psycopg scans comments for placeholders too
        ("SELECT ? -- 100% sure\n", "SELECT %s -- 100%% sure\n"),
        ("SELECT /* 5% */ ?", "SELECT /* 5%% */ %s"),
        # a \r ends a line comment, as in the Postgres lexer
        ("SELECT 1 -- note\r, ?", "SELECT 1 -- note\r, %s"),
        # nested block comments, as Postgres reads them
        ("SELECT /* a /* b' */ c? */ ?", "SELECT /* a /* b' */ c? */ %s"),
        # double-quoted identifiers: ?, ' and % are part of the name
        ('SELECT "odd?col", "it\'s" FROM t WHERE a = ?', 'SELECT "odd?col", "it\'s" FROM t WHERE a = %s'),
        ('SELECT "a""b?" , ?', 'SELECT "a""b?" , %s'),
        ('SELECT "pct%" , ?', 'SELECT "pct%%" , %s'),
        # comment markers inside a literal are data
        ("SELECT 'a -- b ?' , ?", "SELECT 'a -- b ?' , %s"),
        ("SELECT '/* ? */' , ?", "SELECT '/* ? */' , %s"),
        # % in a literal is doubled (LIKE patterns), in code left alone
        ("SELECT * FROM t WHERE u LIKE 'http%' AND a = ?", "SELECT * FROM t WHERE u LIKE 'http%%' AND a = %s"),
        ("SELECT a % b, %s, '%%'", "SELECT a % b, %s, '%%%%'"),
        # unterminated runs extend to the end, like the old adapter
        ("SELECT 'open ? %", "SELECT 'open ? %%"),
        ("SELECT ? -- trailing ?", "SELECT %s -- trailing ?"),
    ],
)
def test_qmarks_to_percent_s(sql, expected):
    assert qmarks_to_percent_s(sql) == expected


def test_legacy_adapter_flipped_after_a_comment_apostrophe():
    """The bug this unit fixes, pinned against the frozen pre-P4.7 code."""
    sql = "SELECT ? -- the dealer's row\n, ? WHERE u LIKE 'a%'"
    assert _legacy_qmarks_to_percent_s(sql).count("%s") == 1
    assert qmarks_to_percent_s(sql) == "SELECT %s -- the dealer's row\n, %s WHERE u LIKE 'a%%'"


# ---------------------------------------------------------------------------
# Function rewrites through the full adapter
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT id FROM cars WHERE image_url LIKE 'http%'", "SELECT id FROM cars WHERE image_url LIKE 'http%%'"),
        ("SELECT IFNULL(price, 0) FROM cars", "SELECT COALESCE(price, 0) FROM cars"),
        ("SELECT ifnull (a, ?) FROM t", "SELECT COALESCE(a, %s) FROM t"),
        (
            "INSERT INTO t (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            "INSERT INTO t (k, v) VALUES (%s, %s) ON CONFLICT(k) DO UPDATE SET v = EXCLUDED.v",
        ),
        ("UPDATE t SET at = datetime('now') WHERE id = ?", "UPDATE t SET at = CURRENT_TIMESTAMP WHERE id = %s"),
        ('UPDATE t SET at = datetime("now")', "UPDATE t SET at = CURRENT_TIMESTAMP"),
        ("SELECT INSTR(title, ?) FROM cars", "SELECT strpos(title, %s) FROM cars"),
        (
            "SELECT GROUP_CONCAT(DISTINCT make) FROM cars",
            "SELECT STRING_AGG(DISTINCT make, ',') FROM cars",
        ),
        ("SELECT group_concat(vin) FROM cars", "SELECT STRING_AGG(vin, ',') FROM cars"),
        ("SELECT id FROM t ORDER BY datetime(c.scraped_at) DESC", "SELECT id FROM t ORDER BY c.scraped_at DESC"),
    ],
)
def test_function_rewrites(sql, expected):
    assert adapt_sql_for_postgres_execute(sql) == expected


# ---------------------------------------------------------------------------
# OR REPLACE / OR IGNORE mappings, PRAGMA
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (
            "INSERT OR REPLACE INTO incomplete_listings_meta (k, v) VALUES (?, ?)",
            "INSERT INTO incomplete_listings_meta (k, v) VALUES (%s, %s) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
        ),
        (
            "INSERT OR REPLACE INTO dealer_geopoints (dealer_url, lat, lng) VALUES (?, ?, ?)",
            "INSERT INTO dealer_geopoints (dealer_url, lat, lng) VALUES (%s, %s, %s) "
            "ON CONFLICT (dealer_url) DO UPDATE SET lat = EXCLUDED.lat, lng = EXCLUDED.lng",
        ),
        (
            "insert or replace into model_specs (make, model, hp) values (?, ?, ?)",
            "INSERT INTO model_specs (make, model, hp) VALUES (%s, %s, %s) "
            "ON CONFLICT (make, model) DO UPDATE SET hp = EXCLUDED.hp",
        ),
        (
            "INSERT OR IGNORE INTO saved_cars (user_id, car_id) VALUES (?, ?)",
            "INSERT INTO saved_cars (user_id, car_id) VALUES (%s, %s) ON CONFLICT (user_id, car_id) DO NOTHING",
        ),
        (
            "INSERT OR IGNORE INTO cars (vin, title) VALUES (?, ?)",
            "INSERT INTO cars (vin, title) VALUES (%s, %s) ON CONFLICT (vin) DO NOTHING",
        ),
        # unmapped OR IGNORE: the generic fallback
        (
            "INSERT OR IGNORE INTO dealer_tags (dealer_id, tag) VALUES (?, ?)",
            "INSERT INTO dealer_tags (dealer_id, tag) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        ),
        # trailing whitespace alone is kept: byte-identical to the old fallback
        (
            "\n  INSERT OR IGNORE INTO dealer_tags (dealer_id, tag)\n  VALUES (?, ?)\n  ",
            "\n  INSERT INTO dealer_tags (dealer_id, tag)\n  VALUES (%s, %s)\n   ON CONFLICT DO NOTHING",
        ),
    ],
)
def test_insert_or_mappings(sql, expected):
    assert adapt_sql_for_postgres_execute(sql) == expected


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT OR IGNORE INTO dealer_tags (dealer_id, tag) VALUES (?, ?);",
        "INSERT OR IGNORE INTO dealer_tags (dealer_id, tag) VALUES (?, ?) ;\n",
        "INSERT OR IGNORE INTO dealer_tags (dealer_id, tag) VALUES (?, ?);;",
        "INSERT OR IGNORE INTO dealer_tags (dealer_id, tag) VALUES (?, ?); -- the tag's row",
        "INSERT OR IGNORE INTO dealer_tags (dealer_id, tag) VALUES (?, ?) -- trailing note",
        "INSERT OR IGNORE INTO dealer_tags (dealer_id, tag) VALUES (?, ?) /* note */ ;",
    ],
)
def test_or_ignore_fallback_strips_terminator_and_trailing_comment(sql):
    """``...;`` + `` ON CONFLICT DO NOTHING`` was two statements; a trailing
    ``--`` comment swallowed the clause."""
    assert adapt_sql_for_postgres_execute(sql) == (
        "INSERT INTO dealer_tags (dealer_id, tag) VALUES (%s, %s) ON CONFLICT DO NOTHING"
    )
    assert _legacy_adapt(sql) != adapt_sql_for_postgres_execute(sql)


def test_or_ignore_mapping_survives_a_trailing_semicolon():
    assert adapt_sql_for_postgres_execute("INSERT OR IGNORE INTO cars (vin) VALUES (?);") == (
        "INSERT INTO cars (vin) VALUES (%s) ON CONFLICT (vin) DO NOTHING"
    )


def test_or_ignore_terminator_inside_a_literal_is_kept():
    sql = "INSERT OR IGNORE INTO notes (body) VALUES ('ends;')"
    assert adapt_sql_for_postgres_execute(sql) == ("INSERT INTO notes (body) VALUES ('ends;') ON CONFLICT DO NOTHING")


def test_unmapped_or_replace_warns_and_passes_through(caplog):
    sql = "INSERT OR REPLACE INTO haiku_spec_cache (cache_key, make) VALUES (?, ?)"
    with caplog.at_level(logging.WARNING, logger="backend.db.inventory_pg"):
        out = adapt_sql_for_postgres_execute(sql)
    assert out == "INSERT OR REPLACE INTO haiku_spec_cache (cache_key, make) VALUES (%s, %s)"
    assert any("INSERT OR REPLACE without Postgres mapping" in r.getMessage() for r in caplog.records)


def test_mapped_or_replace_does_not_warn(caplog):
    with caplog.at_level(logging.WARNING, logger="backend.db.inventory_pg"):
        adapt_sql_for_postgres_execute("INSERT OR REPLACE INTO incomplete_listings_meta (k, v) VALUES (?, ?)")
    assert not caplog.records


@pytest.mark.parametrize("sql", ["PRAGMA foreign_keys = ON", "  pragma table_info(cars)", "\nPRAGMA journal_mode=WAL;"])
def test_pragma_returns_none(sql):
    assert adapt_sql_for_postgres_execute(sql) is None


# ---------------------------------------------------------------------------
# DO NOTHING is not OR IGNORE
# ---------------------------------------------------------------------------

_OR_IGNORE_SQL = "INSERT OR IGNORE INTO adapter_probe (id, name, qty) VALUES (?, ?, ?)"
_PROBE_DDL = (
    "CREATE {temp} TABLE adapter_probe (id INTEGER PRIMARY KEY, name TEXT NOT NULL, qty INTEGER CHECK (qty >= 0))"
)


def test_do_nothing_is_not_or_ignore_on_sqlite():
    """SQLite ``OR IGNORE`` skips a row that breaks NOT NULL or CHECK; the
    adapted ``ON CONFLICT DO NOTHING`` skips only unique conflicts and raises on
    the rest. SQLite's own UPSERT ``DO NOTHING`` has the Postgres scope, so it
    shows here, without a server, what the adapted statement does on Postgres:
    a SQLite-era call site that relied on OR IGNORE to drop bad rows will raise
    instead (NotNullViolation / CheckViolation in psycopg).
    """
    adapted = adapt_sql_for_postgres_execute(_OR_IGNORE_SQL)
    assert adapted == "INSERT INTO adapter_probe (id, name, qty) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
    as_sqlite = adapted.replace("%s", "?")

    con = sqlite3.connect(":memory:")
    try:
        con.execute(_PROBE_DDL.format(temp=""))
        con.execute(_OR_IGNORE_SQL, (1, "kept", 1))
        # OR IGNORE: NOT NULL, CHECK and duplicate key are all silently skipped
        for bad in ((2, None, 1), (3, "x", -1), (1, "dup", 1)):
            assert con.execute(_OR_IGNORE_SQL, bad).rowcount == 0
        # DO NOTHING: only the duplicate key is skipped
        assert con.execute(as_sqlite, (1, "dup", 1)).rowcount == 0
        with pytest.raises(sqlite3.IntegrityError, match="NOT NULL"):
            con.execute(as_sqlite, (2, None, 1))
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            con.execute(as_sqlite, (3, "x", -1))
        assert con.execute("SELECT id, name FROM adapter_probe").fetchall() == [(1, "kept")]
    finally:
        con.close()


_PG_DSN_ENV = "PG_ADAPTER_TEST_DSN"


def _throwaway_pg_dsn() -> str | None:
    """The opt-in DSN, only if it names a local ``pytest_*`` DB (D-TC9 guard)."""
    dsn = (os.environ.get(_PG_DSN_ENV) or "").strip()
    if not dsn:
        return None
    from psycopg.conninfo import conninfo_to_dict

    params = conninfo_to_dict(dsn)
    db = str(params.get("dbname") or "")
    host = str(params.get("host") or "")
    port = str(params.get("port") or os.environ.get("PGPORT") or "5432")
    if not db.startswith("pytest_"):
        pytest.fail(f"{_PG_DSN_ENV} must name a throwaway pytest_* database, got {db!r}")
    if host and not (host.startswith("/") or host in ("localhost", "127.0.0.1", "::1")):
        pytest.fail(f"{_PG_DSN_ENV} must be a local server, got host {host!r}")
    if port == "15432":
        pytest.fail(f"{_PG_DSN_ENV} must not use the 15432 tunnel port")
    return dsn


@pytest.mark.integration
def test_do_nothing_is_not_or_ignore_on_postgres():
    dsn = _throwaway_pg_dsn()
    if dsn is None:
        pytest.skip(f"set {_PG_DSN_ENV} to a local pytest_* database to run")
    import psycopg
    from psycopg import errors

    adapted = adapt_sql_for_postgres_execute(_OR_IGNORE_SQL)
    with psycopg.connect(dsn, autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(_PROBE_DDL.format(temp="TEMP"))
                cur.execute(adapted, (1, "kept", 1))
                cur.execute(adapted, (1, "dup", 1))
                assert cur.rowcount == 0
                cur.execute("SAVEPOINT s")
                with pytest.raises(errors.NotNullViolation):
                    cur.execute(adapted, (2, None, 1))
                cur.execute("ROLLBACK TO SAVEPOINT s")
                with pytest.raises(errors.CheckViolation):
                    cur.execute(adapted, (3, "x", -1))
                cur.execute("ROLLBACK TO SAVEPOINT s")
                cur.execute("SELECT id, name FROM adapter_probe")
                assert cur.fetchall() == [(1, "kept")]
        finally:
            conn.rollback()


# ---------------------------------------------------------------------------
# Acceptance: UPSERT_CARS_SQL no longer depends on balanced comment apostrophes
# ---------------------------------------------------------------------------

_UPSERT_ANCHOR = "ON CONFLICT(vin) DO UPDATE SET\n"


def test_upsert_cars_sql_adapts_to_its_placeholder_count():
    adapted = adapt_sql_for_postgres_execute(UPSERT_CARS_SQL)
    assert "?" not in adapted
    assert adapted.count("%s") == UPSERT_CARS_SQL.count("?") == 59
    assert "LIKE 'http%%'" in adapted
    assert adapted == _legacy_adapt(UPSERT_CARS_SQL)


@pytest.mark.parametrize(
    "comment",
    [
        "-- the dealer's row\n",
        "/* the dealer's row */\n",
        "-- odd ' quote, a ? and 50% of a comment\n",
    ],
)
def test_apostrophe_in_an_upsert_comment_keeps_the_placeholder_count(comment):
    assert UPSERT_CARS_SQL.count(_UPSERT_ANCHOR) == 1
    edited = UPSERT_CARS_SQL.replace(_UPSERT_ANCHOR, _UPSERT_ANCHOR + "                    " + comment)
    adapted = adapt_sql_for_postgres_execute(edited)
    assert adapted.count("%s") == 59
    assert adapted.replace("%%", "").count("%") == 59  # every bare % is a placeholder
    # the pre-P4.7 adapter miscounted the same statement
    legacy = _legacy_adapt(edited)
    assert legacy.count("%s") != 59 or legacy.replace("%%", "").count("%") != 59


# ---------------------------------------------------------------------------
# Differential: what the old adapter got right is unchanged, byte for byte
# ---------------------------------------------------------------------------


def _first_keyword(sql: str) -> str | None:
    for kind, text in sql_lexical_runs(sql):
        if kind in ("line_comment", "block_comment"):
            continue
        if kind != "code":
            return None
        words = text.split()
        if words:
            return words[0].split("(")[0].upper()
    return None


def _old_adapter_was_right(sql: str) -> bool:
    """The spec's filter (no quote in a comment), widened to the other inputs
    the new adapter changes on purpose: a ``?`` or ``%`` in a comment, a
    ``'``/``?``/``%`` in a double-quoted identifier, and an OR IGNORE statement
    with a trailing ``;`` or comment."""
    if sql.strip().upper().startswith("INSERT OR IGNORE") and (inventory_pg._strip_statement_terminator(sql) != sql):
        return False
    for kind, text in sql_lexical_runs(sql):
        if kind in ("line_comment", "block_comment") and any(c in text for c in "'\"?%"):
            return False
        if kind == "ident" and any(c in text for c in "'?%"):
            return False
    return True


def _python_sources() -> list[Path]:
    out: list[Path] = []
    for top in ("backend", "scripts"):
        out.extend(sorted((REPO_ROOT / top).rglob("*.py")))
    return out


@pytest.fixture(scope="module")
def parsed_sources() -> list[tuple[str, ast.Module]]:
    trees = []
    for path in _python_sources():
        trees.append((str(path.relative_to(REPO_ROOT)), ast.parse(path.read_text(encoding="utf-8"))))
    return trees


def _module_level_sql_constants(parsed) -> dict[str, str]:
    found: dict[str, str] = {}
    for rel, tree in parsed:
        for node in tree.body:
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, ast.AnnAssign):
                targets = [node.target]
            else:
                continue
            value = node.value
            if not (isinstance(value, ast.Constant) and isinstance(value.value, str)):
                continue
            if _first_keyword(value.value) not in _DML_KEYWORDS:
                continue
            for target in targets:
                if isinstance(target, ast.Name):
                    found[f"{rel}:{target.id}"] = value.value
    return found


def test_module_level_sql_constants_adapt_identically(parsed_sources):
    constants = _module_level_sql_constants(parsed_sources)
    compared = {key: sql for key, sql in constants.items() if _old_adapter_was_right(sql)}
    # The corpus must stay real: on 2026-10-18, 12 constants (one is this file's
    # _OR_IGNORE_SQL), 11 compared (UPSERT_CARS_SQL has apostrophes in its
    # comments and is pinned by the acceptance tests above).
    assert len(compared) >= 8, sorted(constants)
    for name in (
        "backend/catalog/linker.py:_SET_LINK_SQL",
        "backend/db/repositories/grid_cards_repo.py:_UPSERT_SQL",
        "backend/scripts/import_epa_master.py:_INSERT_SQL",
    ):
        assert name in compared, name
    assert "backend/scanner/upsert/sql.py:UPSERT_CARS_SQL" in constants
    changed = [
        key
        for key, sql in compared.items()
        if qmarks_to_percent_s(sql) != _legacy_qmarks_to_percent_s(sql)
        or adapt_sql_for_postgres_execute(sql) != _legacy_adapt(sql)
    ]
    assert changed == []


def test_every_dml_string_literal_adapts_identically(parsed_sources, caplog):
    """Wider than the spec: every string literal in backend/ and scripts/ that
    starts with a DML keyword (inline ``cur.execute("...")`` SQL included)."""
    compared = 0
    changed = []
    with caplog.at_level(logging.ERROR, logger="backend.db.inventory_pg"):
        for rel, tree in parsed_sources:
            for node in ast.walk(tree):
                if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
                    continue
                sql = node.value
                if _first_keyword(sql) not in _DML_KEYWORDS or not _old_adapter_was_right(sql):
                    continue
                compared += 1
                if qmarks_to_percent_s(sql) != _legacy_qmarks_to_percent_s(sql) or (
                    adapt_sql_for_postgres_execute(sql) != _legacy_adapt(sql)
                ):
                    changed.append(f"{rel}:{node.lineno}")
    assert compared >= 500
    assert changed == []


# ---------------------------------------------------------------------------
# P14A.5: the adapted statements execute on a real Postgres (pg tier)
# ---------------------------------------------------------------------------
#
# The tests above prove the rewrite text. These run the rewritten text on the
# migration-built schema (``pg_db``), each inside a transaction that is rolled
# back, so a statement the adapter breaks fails here and not in the first scan.


def _upsert_bind(vin: str = "1HGCM82633A004352", *, dealer_id: str = "d-adapter", guard_on: bool = False, **fields):
    """Bind values for UPSERT_CARS_SQL through the product's own row preparation."""
    from backend.scanner.upsert.rows import normalize_vehicle_row
    from backend.scanner.upsert.sql import prepare_row, upsert_params

    base = {
        "vin": vin,
        "title": "2020 Honda Accord EX",
        "year": 2020,
        "make": "Honda",
        "model": "Accord",
        "trim": "EX",
        "price": 21000,
        "mileage": 31000,
        "image_url": "https://img.example.test/a.jpg",
        "dealer_name": "Adapter Honda",
        "dealer_url": "https://adapter.example.test",
        "dealer_id": dealer_id,
    }
    base.update(fields)
    row = prepare_row(normalize_vehicle_row(base), None)
    assert row is not None
    return upsert_params(
        row,
        now="2026-10-20T10:00:00.000001Z",
        price_history_json=None,
        guard_on=guard_on,
        guard_cutoff_iso="2026-10-20T08:00:00Z",
    )


@pytest.mark.pg
def test_adapted_upsert_cars_sql_plans_on_postgres(pg_db):
    adapted = adapt_sql_for_postgres_execute(UPSERT_CARS_SQL)
    params = _upsert_bind()
    assert adapted.count("%s") == len(params) == 59
    with pg_db.connect() as conn:
        plan = conn.execute("EXPLAIN " + adapted, params).fetchall()
        conn.rollback()
    assert plan and "Insert on cars" in plan[0][0]


@pytest.mark.pg
def test_adapted_upsert_cars_sql_inserts_then_updates_on_postgres(pg_db):
    conn = pg_db.get_conn()  # InventoryCursor: the adapter is in the path, as in write_rows
    try:
        cur = conn.cursor()
        cur.execute(UPSERT_CARS_SQL, _upsert_bind(price=21000))
        assert cur.rowcount == 1
        # a rescan with a missing price keeps the stored one; keep-if-nonempty on trim
        cur.execute(UPSERT_CARS_SQL, _upsert_bind(price=None, trim="", mileage=None))
        assert cur.rowcount == 1
        cur.execute("SELECT price, trim, listing_active, dealer_id FROM cars WHERE vin = ?", ("1HGCM82633A004352",))
        price, trim, active, dealer = cur.fetchone()
        assert (price, trim, int(active), dealer) == (21000, "EX", 1, "d-adapter")
        cur.execute("SELECT count(*) FROM cars")
        assert cur.fetchone()[0] == 1
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.pg
def test_adapted_upsert_backstop_where_refuses_a_fresh_foreign_owner_on_postgres(pg_db):
    conn = pg_db.get_conn()
    try:
        cur = conn.cursor()
        cur.execute(UPSERT_CARS_SQL, _upsert_bind(dealer_id="owner"))
        # guard on, cutoff older than the owner's scraped_at: the WHERE refuses the move
        cur.execute(UPSERT_CARS_SQL, _upsert_bind(dealer_id="claimant", guard_on=True))
        assert cur.rowcount == 0
        cur.execute("SELECT dealer_id FROM cars WHERE vin = ?", ("1HGCM82633A004352",))
        assert cur.fetchone()[0] == "owner"
        # guard off: the same claim goes through
        cur.execute(UPSERT_CARS_SQL, _upsert_bind(dealer_id="claimant", guard_on=False))
        assert cur.rowcount == 1
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.pg
def test_representative_adapted_statements_execute_on_postgres(pg_db):
    """One statement per adapter rule, run against real tables and rolled back."""
    conn = pg_db.get_conn()
    try:
        cur = conn.cursor()
        # OR IGNORE (cars special case) twice: the second is a silent no-op, not an error
        ins = "INSERT OR IGNORE INTO cars (vin, title, dealer_id) VALUES (?, ?, ?)"
        cur.execute(ins, ("ADAPTVIN0000000001", "t", "d1"))
        assert cur.rowcount == 1
        cur.execute(ins, ("ADAPTVIN0000000001", "t2", "d2"))
        assert cur.rowcount == 0
        # OR REPLACE (model_specs special case): second run updates
        rep = "INSERT OR REPLACE INTO model_specs (make, model, cylinders) VALUES (?, ?, ?)"
        cur.execute(rep, ("Adapt", "One", 4))
        cur.execute(rep, ("Adapt", "One", 6))
        cur.execute("SELECT cylinders FROM model_specs WHERE make = ? AND model = ?", ("Adapt", "One"))
        assert cur.fetchone()[0] == 6
        # IFNULL / GROUP_CONCAT / INSTR / datetime('now'), with a literal % and a '?' in a comment
        cur.execute(
            "SELECT IFNULL(NULL, 'x'), INSTR('abc', 'b'), datetime('now') IS NOT NULL, "
            "(SELECT GROUP_CONCAT(DISTINCT dealer_id) FROM cars), "
            "'50%' LIKE '50%' -- it's a ? in a comment\n"
        )
        row = cur.fetchone()
        assert row[0] == "x" and row[1] == 2 and row[2] is True and row[3] == "d1" and row[4] is True
        # PRAGMA is skipped instead of reaching the server
        cur.execute("PRAGMA foreign_keys = ON")
    finally:
        conn.rollback()
        conn.close()
