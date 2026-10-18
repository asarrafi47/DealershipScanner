"""
The versioned chain in ``migrations/`` is the inventory schema.

Proof that retiring the runtime DDL (``inventory_pg`` and the per-module
``ensure_*`` helpers) loses nothing, and that a fresh build is complete:

(a) static: every table, column, index and named constraint in two captured
    goldens is created by a migration file that RUNS on a fresh build (V019 is
    recorded there, not run, so its text does not count):

    * ``fixtures/runtime_ddl_schema_golden.json``: the ``backend/db`` runtime DDL
      on an empty database, captured 2026-10-01;
    * ``fixtures/module_ddl_schema_golden.json``: the module ``ensure_*`` helpers
      and the scripts' DDL, captured by ``backend/scripts/capture_module_ddl_golden.py``
      (P4.3; never hand-edited).

    Postgres auto-named constraints (``*_pkey``, ``*_key``, ``*_fkey``) are allowed
    by pattern, since no file spells them. V026 is also checked on its own terms:
    guarded, additive, nothing on ``cars``, and the two runtime tables copied
    exactly from the code that creates them.
(b) unit: :mod:`backend.db.schema_version` (version discovery, warn / strict /
    auto-migrate) against a fake connection -- no database.
(c) live, ``@pytest.mark.integration``: when ``SCHEMA_PARITY_PG_DSN`` names a LOCAL
    Postgres server where ``pytest_*`` databases may be created (D-TC9; never the
    inventory database, never port 15432), build the chain with
    ``migrate.main(['--apply'])`` and require: every relation and named constraint
    the files create exists; the module golden's columns, indexes, constraints and
    sequences exist with the same definitions; a database built from the legacy
    runtime DDL is a subset; V026 on a V025 database creates exactly its objects,
    is idempotent, rebuilds a NULLS DISTINCT ``uq_option_rejections_standing`` and
    refuses duplicate groups; a fresh capture equals the committed module golden.
    Skipped otherwise.

V019 (``migrations/README.md``): 48 of its 53 objects are V001's, so a fresh
build records it without running it (``migrate.RECORD_ONLY``). Its 5 V019-only
objects (``car_move_log`` with its sequence and primary key,
``cars_trim_quarantine``, ``package_values_msrp_quarantine``) and the
``idx_car_move_log_car`` index are created, guarded, by V026.
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pytest

from backend.db import schema_version as sv
from backend.scripts import capture_module_ddl_golden as cmdg
from backend.scripts import migrate
from backend.scripts import schema_drift_report as sdr

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = REPO_ROOT / "migrations"
GOLDEN = Path(__file__).resolve().parent / "fixtures" / "runtime_ddl_schema_golden.json"
MODULE_GOLDEN = Path(__file__).resolve().parent / "fixtures" / "module_ddl_schema_golden.json"
V026 = MIGRATIONS_DIR / "V026__chain_completeness.sql"

# ---------------------------------------------------------------------------
# (a) static: the golden schemas are covered by migrations/*.sql
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
_CREATE_SEQUENCE_RE = re.compile(
    rf"CREATE\s+SEQUENCE\s+(?:IF\s+NOT\s+EXISTS\s+)?{_QUALIFIED}", re.IGNORECASE
)
_NAMED_CONSTRAINT_RE = re.compile(rf"\bCONSTRAINT\s+({_IDENT})", re.IGNORECASE)
_TABLE_ENTRY_KEYWORDS = {
    "constraint", "primary", "unique", "foreign", "check", "exclude", "like",
}
# Postgres names these itself when the DDL does not (table_pkey, table_col_key,
# table_col_fkey); identifiers are truncated at 63 bytes.
_AUTO_NAMED_RE = re.compile(r"_(pkey|key|fkey)$")
# Files a fresh build records without running (V019): their text creates nothing there.
RECORD_ONLY_VERSIONS = frozenset(migrate.RECORD_ONLY)


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


def _migration_files(skip_versions: frozenset[int] = frozenset()) -> list[Path]:
    files = [
        p for p in sorted(MIGRATIONS_DIR.glob("V*__*.sql"))
        if int(re.match(r"V(\d+)__", p.name).group(1)) not in skip_versions
    ]
    assert files, f"no migrations under {MIGRATIONS_DIR}"
    return files


def _sql_inventory(sql: str, into: dict[str, Any]) -> None:
    sql = _strip_sql_comments(sql)
    tables = into["tables"]
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
    into["indexes"].update(_unquote(m.group(1)) for m in _CREATE_INDEX_RE.finditer(sql))
    into["sequences"].update(_unquote(m.group(1)) for m in _CREATE_SEQUENCE_RE.finditer(sql))
    into["constraints"].update(_unquote(m.group(1)) for m in _NAMED_CONSTRAINT_RE.finditer(sql))


def _migration_inventory(skip_versions: frozenset[int] = RECORD_ONLY_VERSIONS) -> dict[str, Any]:
    """Tables -> columns, index, sequence and constraint names the files create.

    By default only the files a fresh build runs (``migrate.RECORD_ONLY`` files are
    recorded there, not run).
    """
    inv: dict[str, Any] = {"tables": {}, "indexes": set(), "sequences": set(), "constraints": set()}
    for path in _migration_files(skip_versions):
        _sql_inventory(path.read_text(encoding="utf-8"), inv)
    return inv


@pytest.fixture(scope="module")
def golden() -> dict[str, Any]:
    return json.loads(GOLDEN.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def module_golden() -> dict[str, Any]:
    return json.loads(MODULE_GOLDEN.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def chain() -> dict[str, Any]:
    return _migration_inventory()


def _auto_named(name: str, table: str) -> bool:
    return bool(_AUTO_NAMED_RE.search(name)) and (
        name.startswith(table[:20]) or len(name) >= 60
    )


def _missing_tables_and_columns(gold: dict[str, Any], chain: dict[str, Any]) -> tuple[list[str], list[str]]:
    missing_tables = sorted(set(gold["tables"]) - set(chain["tables"]))
    missing_cols = sorted(
        f"{t}.{c}"
        for t, cols in gold["tables"].items()
        for c in cols
        if c not in chain["tables"].get(t, set())
    )
    return missing_tables, missing_cols


def _missing_indexes(gold: dict[str, Any], chain: dict[str, Any]) -> list[str]:
    return sorted(
        f"{ix['table']}.{ix['name']}"
        for ix in gold["indexes"]
        if ix["name"] not in chain["indexes"]
        and ix["name"] not in chain["constraints"]  # PK / UNIQUE constraint indexes
        and not _auto_named(ix["name"], ix["table"])
    )


def _missing_constraints(gold: dict[str, Any], chain: dict[str, Any]) -> list[str]:
    return sorted(
        f"{c['table']}.{c['name']} ({c['def']})"
        for c in gold["constraints"]
        if c["name"] not in chain["constraints"]
        and not _auto_named(c["name"], c["table"])
    )


def test_golden_fixture_is_not_empty(golden):
    # Guards the guard: an emptied fixture would make every check below vacuous.
    assert len(golden["tables"]) >= 20
    assert golden["indexes"] and golden["constraints"]


def test_every_runtime_table_and_column_is_created_by_a_migration(golden, chain):
    missing_tables, missing_cols = _missing_tables_and_columns(golden, chain)
    assert not missing_tables, f"tables only the runtime DDL creates: {missing_tables}"
    assert not missing_cols, f"columns only the runtime DDL creates: {missing_cols}"


def test_every_runtime_index_is_created_by_a_migration(golden, chain):
    missing = _missing_indexes(golden, chain)
    assert not missing, f"indexes only the runtime DDL creates: {missing}"


def test_every_runtime_named_constraint_is_created_by_a_migration(golden, chain):
    missing = _missing_constraints(golden, chain)
    assert not missing, f"constraints only the runtime DDL creates: {missing}"


def test_v025_zip_index_is_the_runtime_gap_it_claims_to_close(golden, chain):
    assert any(ix["name"] == "idx_cars_active_zip" for ix in golden["indexes"])
    assert "idx_cars_active_zip" in chain["indexes"]


# --- module golden (P4.3) ---------------------------------------------------

_V026_TABLES = (
    "car_move_log", "cars_trim_quarantine", "package_values_msrp_quarantine",
    "review_reports", "haiku_spec_cache",
    "image_summary_restore_log", "make_canon_log", "model_canon_log", "package_value_purge_log",
)


def test_module_golden_fixture_is_not_empty_and_was_captured(module_golden):
    # Guards the guard, and ties the fixture to the capture script's current step list.
    assert len(module_golden["tables"]) >= 15
    for t in ("review_reports", "haiku_spec_cache", "car_move_log", "cars_trim_quarantine",
              "package_values_msrp_quarantine", "dealer_reviews", "vin_owner_conflicts"):
        assert module_golden["tables"].get(t), t
    assert any(ix["name"] == "uq_option_rejections_standing" for ix in module_golden["indexes"])
    steps = cmdg.build_steps()
    assert [(s["source"], s["call"], s["db"]) for s in module_golden["sources"]] == [
        (st.source, st.call, st.db) for st in steps
    ], "the golden was captured from another step list: re-run capture_module_ddl_golden.py"
    assert all(s["tables"] or s["indexes"] for s in module_golden["sources"])
    assert module_golden["excluded"] == [{"what": w, "why": y} for w, y in cmdg.EXCLUDED]


def test_every_module_table_and_column_is_created_by_a_migration(module_golden, chain):
    missing_tables, missing_cols = _missing_tables_and_columns(module_golden, chain)
    assert not missing_tables, f"tables only module DDL creates: {missing_tables}"
    assert not missing_cols, f"columns only module DDL creates: {missing_cols}"


def test_every_module_index_is_created_by_a_migration(module_golden, chain):
    missing = _missing_indexes(module_golden, chain)
    assert not missing, f"indexes only module DDL creates: {missing}"


def test_every_module_named_constraint_and_sequence_is_created_by_a_migration(module_golden, chain):
    missing = _missing_constraints(module_golden, chain)
    assert not missing, f"constraints only module DDL creates: {missing}"
    missing_seq = sorted(s["name"] for s in module_golden["sequences"] if s["name"] not in chain["sequences"])
    assert not missing_seq, f"sequences only module DDL creates: {missing_seq}"


def test_without_v026_the_fresh_chain_misses_what_v026_adopts(module_golden):
    # The coverage above is V026's doing: drop it and the V019-only and runtime
    # tables are missing again (so the static check is not vacuous).
    without = _migration_inventory(RECORD_ONLY_VERSIONS | {26})
    missing_tables, _ = _missing_tables_and_columns(module_golden, without)
    assert set(missing_tables) == {
        "car_move_log", "cars_trim_quarantine", "package_values_msrp_quarantine", "review_reports", "haiku_spec_cache",
    }
    assert "option_rejections.uq_option_rejections_standing" in _missing_indexes(module_golden, without)


# --- V026 on its own terms ----------------------------------------------------


def _v026_sql() -> str:
    return _strip_sql_comments(V026.read_text(encoding="utf-8"))


def _norm_ddl(sql: str) -> str:
    """Whitespace-, case-, quote- and ``public.``-insensitive form of one statement."""
    sql = re.sub(r"\s+", " ", sql.replace('"', "")).strip().rstrip(";").strip().lower()
    return sql.replace("public.", "").replace("( ", "(").replace(" )", ")")


def _v026_statement(prefix: str) -> str:
    """The V026 statement that starts with ``prefix`` (up to its ``;``)."""
    sql = _v026_sql()
    i = sql.find(prefix)
    assert i >= 0, prefix
    return sql[i : sql.index(";", i) + 1]


def test_v026_creates_every_listed_object(chain):
    inv: dict[str, Any] = {"tables": {}, "indexes": set(), "sequences": set(), "constraints": set()}
    _sql_inventory(V026.read_text(encoding="utf-8"), inv)
    assert set(inv["tables"]) == set(_V026_TABLES)
    assert inv["indexes"] == {"idx_car_move_log_car", "uq_option_rejections_standing"}
    assert inv["sequences"] == {
        "car_move_log_id_seq", "image_summary_restore_log_id_seq", "make_canon_log_id_seq",
        "model_canon_log_id_seq", "package_value_purge_log_id_seq",
    }
    assert inv["constraints"] == {
        "car_move_log_pkey", "image_summary_restore_log_pkey", "make_canon_log_pkey",
        "model_canon_log_pkey", "package_value_purge_log_pkey",
    }
    assert not any("embeddings" in t for t in inv["tables"])  # D-DB7: none exist to adopt


def test_v026_is_guarded_and_additive_and_never_touches_cars():
    sql = _v026_sql()
    assert re.match(r"\s*SET LOCAL lock_timeout = '5s';", sql), "first statement must cap lock waits"
    # nothing deletes or rewrites data, and the only DROP is the guarded index rebuild
    assert not re.search(r"\b(DELETE|UPDATE|INSERT|TRUNCATE)\b", sql, re.I)
    assert re.findall(r"\bDROP\s+\w+[^;]*;", sql, re.I) == ["DROP INDEX public.uq_option_rejections_standing;"]
    assert not re.search(r"\bALTER\s+TABLE\s+(?:ONLY\s+)?(?:public\.)?\w+\s+(?:DROP|ALTER\s+COLUMN|RENAME)", sql, re.I)
    # no statement names the cars table (no lock on it)
    assert not re.search(r"(?<![\w.])(?:public\.)?cars(?![\w])", sql, re.I), "V026 must not touch cars"
    # every CREATE TABLE / SEQUENCE is IF NOT EXISTS; every CREATE INDEX sits behind a
    # to_regclass guard (CREATE INDEX IF NOT EXISTS would still lock the table)
    for m in re.finditer(r"CREATE\s+(TABLE|SEQUENCE)\s+(?!IF NOT EXISTS)", sql, re.I):
        pytest.fail(f"unguarded CREATE {m.group(1)} at {m.start()}")
    for m in re.finditer(r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)", sql, re.I):
        assert re.search(rf"to_regclass\('public\.{m.group(1)}'\)", sql[: m.start()]), m.group(1)
    # every ADD CONSTRAINT / OWNED BY sits inside the nearest open IF NOT EXISTS (catalog) block
    for m in re.finditer(r"ADD CONSTRAINT (\w+)|OWNED BY public\.(\w+)", sql):
        before = sql[: m.start()]
        opened = before.rfind("IF NOT EXISTS (")
        assert opened >= 0 and "END IF" not in before[opened:], m.group(0)


def test_v026_review_reports_is_exactly_the_reviews_store_ddl():
    from backend.reviews import store

    assert _norm_ddl(_v026_statement("CREATE TABLE IF NOT EXISTS public.review_reports")) == _norm_ddl(
        store._DDL_REPORTS_PG
    )


def test_v026_haiku_spec_cache_is_exactly_the_enrichment_ddl():
    from backend.enrichment import service

    assert _norm_ddl(_v026_statement("CREATE TABLE IF NOT EXISTS public.haiku_spec_cache")) == _norm_ddl(
        service._HAIKU_CACHE_PG_DDL
    )


def test_v026_v019_only_tables_keep_the_v019_column_types():
    v019 = _strip_sql_comments((MIGRATIONS_DIR / "V019__missing_reference_tables.sql").read_text(encoding="utf-8"))
    for table in ("cars_trim_quarantine", "package_values_msrp_quarantine"):
        old = v019[v019.index(f"CREATE TABLE public.{table} (") :]
        old = old[: old.index(";") + 1]
        new = _v026_statement(f"CREATE TABLE IF NOT EXISTS public.{table} (")
        assert _norm_ddl(new).replace("if not exists ", "") == _norm_ddl(old), table
    # car_move_log: V019's columns, with the nextval default V019 set by ALTER ... SET DEFAULT
    old = v019[v019.index("CREATE TABLE public.car_move_log (") :]
    old = _norm_ddl(old[: old.index(";") + 1])
    new = _norm_ddl(_v026_statement("CREATE TABLE IF NOT EXISTS public.car_move_log (")).replace("if not exists ", "")
    assert new == old.replace(
        "(id bigint not null,", "(id bigint default nextval('car_move_log_id_seq'::regclass) not null,", 1)


def test_v026_raises_on_duplicate_option_rejections_and_never_dedupes():
    sql = _v026_sql()
    assert "RAISE EXCEPTION 'V026: option_rejections has % duplicate group(s)" in sql
    assert "GROUP BY car_id, option_name, price, reason" in sql and "HAVING count(*) > 1" in sql
    assert "NULLS NOT DISTINCT" in sql and "server_version_num" in sql


# --- the capture script (pure parts and guards) ---------------------------------


def test_capture_reads_the_scripts_ddl_from_their_source():
    ddl = cmdg.module_constant("backend/scripts/apply_attribution_moves.py", "_DDL")
    assert "CREATE TABLE IF NOT EXISTS car_move_log" in ddl and "idx_car_move_log_car" in ddl
    stmts = cmdg.execute_literals("backend/scripts/quarantine_package_value_msrps.py", "_ensure_quarantine_table")
    assert len(stmts) == 2 and "(LIKE package_values)" in stmts[0] and "ADD COLUMN IF NOT EXISTS label" in stmts[1]
    (idx,) = cmdg.execute_literals("backend/scripts/feed_package_registry_from_vision.py", "main", ("CREATE",))
    assert idx.startswith("CREATE UNIQUE INDEX IF NOT EXISTS uq_option_rejections_standing ")
    with pytest.raises(LookupError):
        cmdg.module_constant("backend/scripts/apply_attribution_moves.py", "_NO_SUCH")
    with pytest.raises(LookupError):
        cmdg.execute_literals("backend/scripts/apply_attribution_moves.py", "no_such_function")
    steps = cmdg.build_steps()
    assert len({(s.source, s.call) for s in steps}) == len(steps) and {s.db for s in steps} == {"empty", "v001_v014"}


def _tiny(tables: dict[str, dict[str, str]], **over: Any) -> dict[str, Any]:
    s = sdr.empty_snapshot()
    s["tables"] = set(tables)
    s["columns"] = {(t, c): (ty, "YES", "") for t, cols in tables.items() for c, ty in cols.items()}
    s["column_types"] = {(t, c): (ty, "", "", "") for t, cols in tables.items() for c, ty in cols.items()}
    s.update(over)
    return s


def test_capture_delta_and_assemble_are_pure():
    before = _tiny({"a": {"id": "integer"}})
    after = _tiny({"a": {"id": "integer", "note": "text"}, "b": {"k": "numeric(10,2)"}},
                  indexes={"b_pkey": ("b", "public.b USING btree (k)", True)},
                  constraints={("b", "b_pkey"): ("p", "PRIMARY KEY (k)")},
                  sequences={"b_k_seq": ("b.k", "bigint")})
    d = cmdg.delta(before, after)
    assert d["tables"] == {"a": {"note": ["text", "YES", ""]}, "b": {"k": ["numeric(10,2)", "YES", ""]}}
    assert d["indexes"] == [{"name": "b_pkey", "table": "b", "def": "public.b USING btree (k)"}]
    assert d["constraints"] == [{"name": "b_pkey", "table": "b", "type": "p", "def": "PRIMARY KEY (k)"}]
    assert d["sequences"] == [{"name": "b_k_seq", "owned_by": "b.k"}]
    assert cmdg._is_empty_delta(cmdg.delta(after, after))
    step = cmdg.Step("x.py", "ensure_x", "empty", lambda conn: None)
    doc = cmdg.assemble([(step, d)])
    assert list(doc["tables"]) == ["a", "b"] and doc["sources"][0]["tables"] == ["a", "b"]
    assert cmdg.render(doc) == cmdg.render(cmdg.assemble([(step, d)]))  # deterministic
    assert before == _tiny({"a": {"id": "integer"}})  # inputs untouched


@pytest.mark.parametrize("dsn", [
    "postgresql://db.example.com/postgres",
    "postgresql://localhost:15432/postgres",
    "host=10.0.0.5 dbname=postgres",
])
def test_capture_refuses_a_non_local_scratch_server_before_connecting(dsn):
    with pytest.raises(sdr.Refused):
        cmdg.capture(dsn)


@pytest.mark.parametrize("name", ["cars", "cars_cleanroom", "pytest_chain_ab", "pytest_module_ddl_", "module_ddl_x"])
def test_capture_scratch_names_are_pytest_module_ddl_only(name):
    with pytest.raises(cmdg.Refused):
        cmdg.check_scratch_name(name)
    cmdg.check_scratch_name("pytest_module_ddl_0123abcd_e")


def test_capture_pins_the_inventory_dsn_and_restores_the_environment(monkeypatch):
    from backend.db import inventory_pg

    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql:///cars?host=/tmp")
    monkeypatch.setenv("DATABASE_URL", "postgresql:///other?host=/tmp")
    monkeypatch.setenv("INVENTORY_AUTO_MIGRATE", "1")
    scratch = "postgresql:///pytest_module_ddl_x_e?host=/tmp&port=5432"
    with cmdg.inventory_env(scratch):
        assert inventory_pg.inventory_postgres_dsn() == scratch
        assert "DATABASE_URL" not in os.environ and "INVENTORY_AUTO_MIGRATE" not in os.environ
    assert os.environ["INVENTORY_DATABASE_URL"] == "postgresql:///cars?host=/tmp"
    assert os.environ["DATABASE_URL"] == "postgresql:///other?host=/tmp"
    assert os.environ["INVENTORY_AUTO_MIGRATE"] == "1"
    with pytest.raises(cmdg.Refused):
        with cmdg.inventory_env("host=/tmp dbname=pytest_module_ddl_x"):  # not a URL: resolver ignores it
            pass


# ---------------------------------------------------------------------------
# (b) unit: backend/db/schema_version.py against a fake connection
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, db: "_FakeConn"):
        self.db = db
        self._rows: list[tuple] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.db.queries.append(sql)
        if "to_regclass('public.cars')" in sql:
            self._rows = [("cars" if self.db.has_cars else None,)]
        elif "to_regclass" in sql:
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
    def __init__(self, versions: set[int] | None, has_cars: bool = False):
        self.versions = versions
        self.has_cars = has_cars
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
    assert sv.expected_schema_version() == highest >= 26  # V026 (P4.3)


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
    assert sv.MIGRATE_COMMAND in caplog.text and "--baseline" not in caplog.text


def test_never_migrated_populated_database_gets_the_baseline_hint(mig_dir, caplog):
    conn = _FakeConn(None, has_cars=True)
    with caplog.at_level(logging.WARNING, logger=sv.__name__):
        assert sv.ensure_schema_current(conn) is False
    assert sv.BASELINE_V001_COMMAND in caplog.text
    assert any("public.cars" in q for q in conn.queries)


def test_cars_is_not_probed_when_schema_migrations_exists(mig_dir):
    conn = _FakeConn({1, 2})
    sv.ensure_schema_current(conn)
    assert not any("public.cars" in q for q in conn.queries)


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

    # No schema_migrations: the hint follows to_regclass('public.cars').
    empty = sv.describe_gap(None, 3, has_cars=False)
    assert sv.MIGRATE_COMMAND in empty and "fresh build" in empty and "--baseline" not in empty
    legacy = sv.describe_gap(None, 3, has_cars=True)
    assert sv.BASELINE_V001_COMMAND in legacy and "legacy" in legacy
    unknown = sv.describe_gap(None, 3)
    assert sv.MIGRATE_COMMAND in unknown and sv.BASELINE_V001_COMMAND in unknown

    # Behind V019: plain migrate --apply records V019 itself when that is safe.
    behind_v018 = sv.describe_gap(set(range(1, 19)), 25)
    assert sv.MIGRATE_COMMAND in behind_v018 and "records V019" in behind_v018
    assert "at V018" in behind_v018 and "expected V025" in behind_v018
    assert "--baseline 19" not in behind_v018
    past_v019 = sv.describe_gap(set(range(1, 20)), 25)
    assert sv.MIGRATE_COMMAND in past_v019 and "V019" not in past_v019.split("Run:")[1]
    # The old name survives as an alias of the plain command.
    assert sv.BASELINE_V019_COMMAND == sv.MIGRATE_COMMAND


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
# (c) live: a fresh chain on a scratch Postgres server (opt-in, D-TC9)
# ---------------------------------------------------------------------------

_PARITY_DSN_ENV = "SCHEMA_PARITY_PG_DSN"
_SCRATCH_RE = re.compile(r"^pytest_schema_[a-z0-9_]{1,40}$")


@pytest.fixture(scope="module")
def pg_admin() -> str:
    """The guarded admin DSN: a LOCAL server only (never via the 15432 tunnel)."""
    admin_dsn = (os.environ.get(_PARITY_DSN_ENV) or "").strip()
    if not admin_dsn:
        pytest.skip(f"{_PARITY_DSN_ENV} not set (needs a LOCAL server where pytest_* DBs may be created)")
    pytest.importorskip("psycopg")
    try:
        sdr.check_scratch_server(admin_dsn)
    except sdr.Refused as exc:
        pytest.skip(f"{_PARITY_DSN_ENV} is not a local scratch server: {exc}")
    return admin_dsn


@contextmanager
def _scratch_db(admin_dsn: str, label: str) -> Iterator[str]:
    """A fresh ``pytest_schema_<label>_<hex>`` database (URL DSN), dropped afterwards."""
    import psycopg

    name = f"pytest_schema_{label}_{uuid.uuid4().hex[:8]}"
    assert _SCRATCH_RE.match(name) and name not in sdr.PROTECTED_DBS
    try:
        admin = psycopg.connect(admin_dsn, autocommit=True)
    except Exception as exc:  # pragma: no cover - environment
        pytest.skip(f"cannot reach {_PARITY_DSN_ENV}: {exc}")
    try:
        try:
            admin.execute(f'CREATE DATABASE "{name}" TEMPLATE template0')
        except psycopg.errors.InsufficientPrivilege as exc:
            pytest.skip(f"{_PARITY_DSN_ENV} may not create databases: {exc}")
        yield sdr.with_dbname(admin_dsn, name)
    finally:
        try:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        finally:
            admin.close()


def _migrate(dsn: str, *argv: str) -> int:
    """``migrate.main`` against ``dsn`` only (the resolver must return exactly it)."""
    with cmdg.inventory_env(dsn):
        return migrate.main(list(argv))


def _live(dsn: str) -> dict[str, Any]:
    import psycopg

    with psycopg.connect(dsn) as conn:
        return sdr.snapshot(conn)


def _max_version(dsn: str) -> int:
    import psycopg

    with psycopg.connect(dsn) as conn:
        return conn.execute("SELECT max(version) FROM public.schema_migrations").fetchone()[0]


@pytest.fixture(scope="module")
def fresh_chain(pg_admin) -> Iterator[dict[str, Any]]:
    """The chain built the way every fresh database is: ``migrate.main(['--apply'])``."""
    with _scratch_db(pg_admin, "chain") as dsn:
        used, files = sdr.build_chain(dsn, MIGRATIONS_DIR, "runner")
        assert used == "migrate.main(['--apply'])" and max(files) >= 26
        yield {"dsn": dsn, "snap": _live(dsn)}


@pytest.mark.integration
def test_fresh_chain_has_every_migration_relation(fresh_chain):
    # Every CREATE TABLE | SEQUENCE | INDEX and named CONSTRAINT any file spells,
    # V019's included: a fresh build records V019, so V001 and V026 must cover it.
    named = _migration_inventory(skip_versions=frozenset())
    snap = fresh_chain["snap"]
    constraints = {name for (_t, name) in snap["constraints"]}
    missing = (
        [f"table {t}" for t in sorted(named["tables"]) if t not in snap["tables"]]
        + [f"sequence {s}" for s in sorted(named["sequences"]) if s not in snap["sequences"]]
        + [f"index {i}" for i in sorted(named["indexes"]) if i not in snap["indexes"]]
        + [f"constraint {c}" for c in sorted(named["constraints"]) if c not in constraints]
    )
    assert not missing, f"named by migrations/ but absent after a fresh build: {missing}"
    for name in ("car_move_log", "car_move_log_id_seq", "cars_trim_quarantine", "package_values_msrp_quarantine"):
        assert name in snap["tables"] | set(snap["sequences"]), name
    assert "idx_car_move_log_car" in snap["indexes"] and ("car_move_log", "car_move_log_pkey") in snap["constraints"]


# D-DB7 (a): the chain's index is NULLS NOT DISTINCT; the script's runtime DDL is not.
_NULLS_NOT_DISTINCT_BY_DESIGN = {"uq_option_rejections_standing"}


@pytest.mark.integration
def test_fresh_chain_builds_the_module_golden_with_the_same_definitions(fresh_chain, module_golden):
    snap = fresh_chain["snap"]
    problems: list[str] = []
    for t, cols in module_golden["tables"].items():
        for col, want in cols.items():
            key = (t, col)
            if key not in snap["columns"]:
                problems.append(f"column missing: {t}.{col}")
                continue
            got = [snap["column_types"][key][0], snap["columns"][key][1], snap["columns"][key][2]]
            if got != want:
                problems.append(f"column differs: {t}.{col} chain={got} module={want}")
    for ix in module_golden["indexes"]:
        got = snap["indexes"].get(ix["name"])
        want = (ix["table"], ix["def"] + (" NULLS NOT DISTINCT" if ix["name"] in _NULLS_NOT_DISTINCT_BY_DESIGN else ""))
        if got is None or (got[0], got[1]) != want:
            problems.append(f"index differs: {ix['name']} chain={got} module={want}")
    for c in module_golden["constraints"]:
        got = snap["constraints"].get((c["table"], c["name"]))
        if got != (c["type"], c["def"]):
            problems.append(f"constraint differs: {c['table']}.{c['name']} chain={got} module={c}")
    for s in module_golden["sequences"]:
        got = snap["sequences"].get(s["name"])
        if got is None or got[0] != s["owned_by"]:
            problems.append(f"sequence differs: {s['name']} chain={got} module={s}")
    assert not problems, "\n".join(problems)


@pytest.mark.integration
def test_module_golden_matches_a_fresh_capture(pg_admin):
    # A hand-edited golden proves nothing: it must be what the capture builds today.
    assert cmdg.render(cmdg.capture(pg_admin)) == MODULE_GOLDEN.read_text(encoding="utf-8")


# --- V026 on a V025 database (prod and local cars are at V025) ----------------

# What V026 adds to a fresh V001..V025 build (V019 recorded): the folded table
# items carry their columns, sequences, keys and indexes.
_V026_ON_V025 = {("chain_only", "table", t) for t in _V026_TABLES} | {
    ("chain_only", "index", "uq_option_rejections_standing"),
}


@pytest.mark.integration
def test_v026_on_a_v025_build_creates_exactly_its_objects_and_is_idempotent(pg_admin, caplog):
    import psycopg

    with _scratch_db(pg_admin, "v025") as dsn:
        assert _migrate(dsn, "--apply", "--target", "25") == 0
        assert _max_version(dsn) == 25
        before = _live(dsn)
        caplog.clear()
        with caplog.at_level(logging.INFO, logger="migrate"):
            assert _migrate(dsn, "--dry-run", "--target", "26") == 0
        would = [r.getMessage() for r in caplog.records if r.getMessage().startswith("would ")]
        assert len(would) == 1 and "V026__chain_completeness.sql" in would[0], would  # N5 exit gate
        assert _live(dsn) == before  # the dry run wrote nothing
        assert _migrate(dsn, "--apply", "--target", "26") == 0
        assert _max_version(dsn) == 26
        after = _live(dsn)
        added = sdr.diff_snapshots(after, before)  # "chain" side = after: chain_only = created
        assert {(d.kind, d.category, d.name) for d in added} == _V026_ON_V025
        kids = {d.name: set(d.children) for d in added if d.category == "table"}
        assert {"sequence car_move_log_id_seq", "constraint car_move_log_pkey",
                "index idx_car_move_log_car"} <= kids["car_move_log"]
        assert "constraint review_reports_review_id_ip_hash_key" in kids["review_reports"]
        for t in ("image_summary_restore_log", "make_canon_log", "model_canon_log", "package_value_purge_log"):
            assert {f"sequence {t}_id_seq", f"constraint {t}_pkey"} <= kids[t], t
        assert after["indexes"]["uq_option_rejections_standing"][1].endswith("NULLS NOT DISTINCT")
        # A second run of the file changes nothing.
        with psycopg.connect(dsn) as conn:
            conn.execute(V026.read_text(encoding="utf-8"))
        assert _live(dsn) == after


@pytest.mark.integration
def test_v026_rebuilds_the_runtime_index_and_refuses_duplicates(pg_admin):
    import psycopg

    runtime_index = cmdg.execute_literals(
        "backend/scripts/feed_package_registry_from_vision.py", "main", ("CREATE",))[0]
    v026 = V026.read_text(encoding="utf-8")
    row = ("INSERT INTO option_rejections (car_id, option_name, price, reason) "
           "VALUES (7, 'Sport Package', NULL, 'uncorroborated')")
    with _scratch_db(pg_admin, "v025dup") as dsn:
        assert _migrate(dsn, "--apply", "--target", "25") == 0
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(runtime_index)  # prod/local shape: NULLS DISTINCT
            conn.execute(row)
            conn.execute(row)  # allowed by NULLS DISTINCT: a duplicate under V026's key
        # Duplicate group: the file stops, nothing is applied or recorded, no row is touched.
        assert _migrate(dsn, "--apply", "--target", "26") != 0
        assert _max_version(dsn) == 25
        with psycopg.connect(dsn) as conn:
            assert conn.execute("SELECT count(*) FROM option_rejections").fetchone()[0] == 2
            assert conn.execute("SELECT to_regclass('public.review_reports')").fetchone()[0] is None
            nnd = conn.execute("SELECT indnullsnotdistinct FROM pg_index "
                               "WHERE indexrelid = 'public.uq_option_rejections_standing'::regclass").fetchone()[0]
            assert nnd is False
        # An unexpected index under the name: refused, never replaced.
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute("DELETE FROM option_rejections WHERE id = (SELECT max(id) FROM option_rejections)")
            conn.execute("DROP INDEX uq_option_rejections_standing")
            conn.execute("CREATE UNIQUE INDEX uq_option_rejections_standing ON option_rejections (option_name, reason)")
        with psycopg.connect(dsn) as conn, pytest.raises(psycopg.errors.RaiseException, match="is not the unique btree"):
            conn.execute(v026)
        # The runtime index, deduplicated data: rebuilt as NULLS NOT DISTINCT, rows kept.
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute("DROP INDEX uq_option_rejections_standing")
            conn.execute(runtime_index)
        assert _migrate(dsn, "--apply", "--target", "26") == 0
        with psycopg.connect(dsn) as conn:
            assert conn.execute("SELECT count(*) FROM option_rejections").fetchone()[0] == 1
            nnd = conn.execute("SELECT indnullsnotdistinct FROM pg_index "
                               "WHERE indexrelid = 'public.uq_option_rejections_standing'::regclass").fetchone()[0]
            assert nnd is True
            # NULLS NOT DISTINCT: the script's ON CONFLICT DO NOTHING now skips the NULL-price repeat.
            conn.execute(row + " ON CONFLICT DO NOTHING")
            assert conn.execute("SELECT count(*) FROM option_rejections").fetchone()[0] == 1


# --- runtime-DDL parity (backend/db) ---------------------------------------------


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
def test_migrations_build_a_superset_of_the_runtime_ddl(fresh_chain, pg_admin, monkeypatch):
    mig_dsn = fresh_chain["dsn"]
    with _scratch_db(pg_admin, "runtime") as rt_dsn:
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
