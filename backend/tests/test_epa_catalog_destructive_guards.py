"""P1A.2: the EPA catalog reloads cannot wipe ``epa_master`` under live links.

Two scripts could replace the catalog with fresh ids:

* ``build_epa_master_pg.py --rebuild`` ran ``DELETE FROM epa_master`` (cascading to
  ``epa_extended_specs``) and re-inserted from the stale root ``DICTIONARY/`` copy.
  Every write mode now exits 2 during argument handling, before any connection,
  and the source is ``dictionary_paths.EPA_DIR`` read recursively.
* ``import_epa_master.py --yes`` / ``import_csv(append=False)`` deletes and reloads
  from vehicles.csv. It now refuses while any car is linked or any extended-specs
  row exists, and still works on an empty, unlinked catalog.
"""
from __future__ import annotations

import ast
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

from backend.scripts import build_epa_master_pg as builder
from backend.scripts import import_epa_master as importer

REPO_ROOT = Path(__file__).resolve().parents[2]
BUILDER_PATH = REPO_ROOT / "backend" / "scripts" / "build_epa_master_pg.py"
FAKE_DSN = "postgresql://p1a2-guard-test.invalid:1/never"


# ---------------------------------------------------------------------------
# build_epa_master_pg.py
# ---------------------------------------------------------------------------


@pytest.fixture
def no_db_connection(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Point the inventory at a fake Postgres DSN and record every connect attempt."""
    attempts: list[str] = []

    def _refuse_connect(*args, **kwargs):
        attempts.append("connect")
        raise AssertionError("the builder opened a database connection")

    from backend.db import inventory_pg

    monkeypatch.setenv("INVENTORY_DATABASE_URL", FAKE_DSN)
    monkeypatch.setattr(inventory_pg, "pg_connect", _refuse_connect)
    monkeypatch.setattr(psycopg, "connect", _refuse_connect)
    return attempts


def _write_epa_csv(path: Path, rows: list[tuple[int, str, str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["Year,Make,Model,Trim,engineOptions,mpg_city,mpg_highway"]
    lines += [f"{y},{mk},{md},{tr},2.0L I4,25,33" for y, mk, md, tr in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


@pytest.mark.parametrize(
    "argv",
    [["--rebuild"], ["--rebuild", "--dry-run"], []],
    ids=["rebuild", "rebuild-with-dry-run", "default-mode"],
)
def test_builder_write_modes_refuse_before_connecting(argv, no_db_connection, capsys):
    assert builder.main(argv) == 2
    assert no_db_connection == []
    assert "writes disabled until the id-preserving builder lands (P10B.2)" in capsys.readouterr().err


@pytest.mark.parametrize("kwargs", [{}, {"rebuild": True}, {"dry_run": True, "rebuild": True}])
def test_builder_library_entry_refuses_writes_before_connecting(kwargs, no_db_connection):
    with pytest.raises(builder.BuildRefused, match="P10B.2"):
        builder.build_epa_master(**kwargs)
    assert no_db_connection == []


def test_builder_rebuild_cli_exits_2_without_connecting(tmp_path: Path):
    """The exit-gate command, as a real process: exit 2, no connection attempted.

    The DSN points at an unresolvable host, so a connection attempt would surface as
    a psycopg OperationalError traceback and exit 1, never 2.
    """
    env = dict(os.environ)
    env.update(
        INVENTORY_DATABASE_URL=FAKE_DSN,
        DATABASE_URL=FAKE_DSN,
        PROJECT_DOTENV_DISABLE="1",
        PYTHONPATH=str(REPO_ROOT),
    )
    proc = subprocess.run(
        [sys.executable, str(BUILDER_PATH), "--rebuild"],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 2, proc.stderr
    assert "writes disabled until the id-preserving builder lands (P10B.2)" in proc.stderr
    assert "OperationalError" not in proc.stderr


def test_builder_reads_dictionary_epa_dir_recursively(scratch_dictionary_root: Path):
    epa = scratch_dictionary_root / "epa"
    _write_epa_csv(epa / "Acura" / "2020_Acura_RDX_EPA.csv", [(2020, "Acura", "RDX", "Base")])
    _write_epa_csv(epa / "Ford" / "nested" / "2021_Ford_F-150_EPA.csv", [(2021, "Ford", "F-150", "XL")])
    (epa / "Ford" / "2021_Ford_F-150_Complete_Options.csv").write_text("Year\n", encoding="utf-8")

    assert builder.default_source_dir() == epa
    found = builder.find_epa_files(min_files=2)
    assert [p.name for p in found] == ["2020_Acura_RDX_EPA.csv", "2021_Ford_F-150_EPA.csv"]


def test_builder_source_dir_override_is_recursive(tmp_path: Path):
    _write_epa_csv(tmp_path / "a" / "b" / "c" / "2019_Kia_Soul_EPA.csv", [(2019, "Kia", "Soul", "LX")])
    assert [p.name for p in builder.find_epa_files(tmp_path, min_files=1)] == ["2019_Kia_Soul_EPA.csv"]


def test_builder_never_reads_root_dictionary_folder():
    src = BUILDER_PATH.read_text(encoding="utf-8")
    assert '"DICTIONARY"' not in src
    assert "dictionary_paths" in src


def test_builder_has_no_write_sql():
    """No reachable write: no string the builder executes holds DELETE/INSERT/UPDATE/TRUNCATE.

    Docstrings are skipped (the module docstring explains what ``--rebuild`` used to do).
    """
    tree = ast.parse(BUILDER_PATH.read_text(encoding="utf-8"))
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.ClassDef))
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }
    strings = [
        node.value.upper()
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings
    ]
    assert any("SELECT" in s for s in strings)  # the dry-run read is still there
    for verb in ("DELETE FROM", "INSERT INTO", "UPDATE ", "TRUNCATE"):
        assert not [s for s in strings if verb in s], verb


def test_builder_short_source_dir_refused(tmp_path: Path, no_db_connection, capsys):
    for i in range(3):
        _write_epa_csv(tmp_path / f"20{10 + i}_Kia_Soul_EPA.csv", [(2010 + i, "Kia", "Soul", "LX")])

    with pytest.raises(builder.BuildRefused, match="fewer than --min-files 10"):
        builder.find_epa_files(tmp_path, min_files=10)

    rc = builder.main(["--dry-run", "--source-dir", str(tmp_path), "--min-files", "10"])
    assert rc == 2
    assert "found 3 *_EPA.csv file(s)" in capsys.readouterr().err
    assert no_db_connection == []


def test_builder_default_min_files_refuses_a_small_tree(tmp_path: Path, no_db_connection):
    _write_epa_csv(tmp_path / "2019_Kia_Soul_EPA.csv", [(2019, "Kia", "Soul", "LX")])
    assert builder.DEFAULT_MIN_FILES == 10_000
    assert builder.main(["--dry-run", "--source-dir", str(tmp_path)]) == 2
    assert no_db_connection == []


@pytest.mark.parametrize("bad", [0, -5])
def test_builder_min_files_below_one_refused(tmp_path: Path, bad: int):
    with pytest.raises(builder.BuildRefused, match="at least 1"):
        builder.find_epa_files(tmp_path, min_files=bad)


def test_builder_missing_source_dir_refused(tmp_path: Path):
    with pytest.raises(builder.BuildRefused, match="not found"):
        builder.find_epa_files(tmp_path / "absent", min_files=1)


class _ReadOnlyPgConn:
    """A psycopg-shaped connection that records statements and rejects writes."""

    def __init__(self, existing: list[tuple]):
        self.existing = existing
        self.read_only = False
        self.statements: list[str] = []
        self.closed = False

    def cursor(self):
        conn = self

        class _Cur:
            def execute(self, sql, params=None):
                conn.statements.append(sql)
                if not conn.read_only:
                    raise AssertionError("dry run queried before setting read_only")
                if not sql.lstrip().upper().startswith("SELECT"):
                    raise AssertionError(f"dry run issued a non-SELECT: {sql}")

            def fetchall(self):
                return list(conn.existing)

        return _Cur()

    def rollback(self):
        pass

    def commit(self):
        raise AssertionError("dry run committed")

    def close(self):
        self.closed = True


def test_builder_dry_run_counts_on_a_read_only_connection(tmp_path: Path, monkeypatch):
    _write_epa_csv(
        tmp_path / "Kia" / "2019_Kia_Soul_EPA.csv",
        [(2019, "Kia", "Soul", "LX"), (2019, "Kia", "Soul", "GT-Line"), (2019, "Kia", "Soul", "LX")],
    )
    _write_epa_csv(tmp_path / "Kia" / "2020_Kia_Rio_EPA.csv", [(2020, "Kia", "Rio", ""), (0, "", "", "")])
    fake = _ReadOnlyPgConn(existing=[(2019, "kia", "soul", "lx")])

    from backend.db import inventory_pg

    monkeypatch.setenv("INVENTORY_DATABASE_URL", FAKE_DSN)
    monkeypatch.setattr(inventory_pg, "pg_connect", lambda: fake)

    stats = builder.build_epa_master(dry_run=True, source_dir=tmp_path, min_files=2)
    assert stats == {"files": 2, "would_insert": 2, "skipped_existing": 2, "skipped_bad_row": 1}
    assert fake.read_only is True
    assert fake.closed is True
    assert len(fake.statements) == 1


# ---------------------------------------------------------------------------
# import_epa_master.py
# ---------------------------------------------------------------------------

_VEHICLES_HEADER = "id,year,make,model,cylinders,displ,trany,drive,fuelType,city08,highway08,cityE,highwayE,atvType"


def _write_vehicles_csv(path: Path, rows: list[tuple[int, int, str, str]]) -> Path:
    lines = [_VEHICLES_HEADER]
    lines += [f"{vid},{y},{mk},{md},4,2.0,Automatic 8-spd,FWD,Regular,25,33,0,0," for vid, y, mk, md in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _epa_rows(db: Path) -> list[tuple]:
    with sqlite3.connect(str(db)) as raw:
        return raw.execute("SELECT id, epa_vehicle_id, year, make, model FROM epa_master ORDER BY id").fetchall()


def _seed_catalog(db: Path, rows: list[tuple[int, int, int, str, str]]) -> None:
    with sqlite3.connect(str(db)) as raw:
        raw.executemany(
            "INSERT INTO epa_master (id, epa_vehicle_id, year, make, model) VALUES (?, ?, ?, ?, ?)", rows
        )


def _linked_ids(db: Path) -> list[int]:
    with sqlite3.connect(str(db)) as raw:
        return [r[0] for r in raw.execute("SELECT epa_master_id FROM cars WHERE epa_master_id IS NOT NULL")]


@pytest.fixture
def linked_inventory(sqlite_inventory):
    """A catalog row (id 7) and one car linked to it."""
    _seed_catalog(sqlite_inventory.path, [(7, 4001, 2024, "Kia", "Soul")])
    sqlite_inventory.add_cars([{"make": "Kia", "model": "Soul", "year": 2024, "epa_master_id": 7}])
    return sqlite_inventory


def test_full_replace_refuses_when_a_car_is_linked(linked_inventory, tmp_path: Path):
    csv_path = _write_vehicles_csv(tmp_path / "vehicles.csv", [(5001, 2025, "Kia", "Soul")])
    before = _epa_rows(linked_inventory.path)

    conn = linked_inventory.get_conn()
    try:
        with pytest.raises(RuntimeError) as info:
            importer.import_csv(str(csv_path), conn)
    finally:
        conn.close()

    assert isinstance(info.value, importer.FullReplaceRefused)
    assert info.value.linked_cars == 1
    assert info.value.extended_specs == 0
    assert info.value.catalog_rows == 1
    assert _epa_rows(linked_inventory.path) == before
    assert _linked_ids(linked_inventory.path) == [7]


def test_full_replace_refuses_when_extended_specs_exist(sqlite_inventory, tmp_path: Path):
    _seed_catalog(sqlite_inventory.path, [(3, 4001, 2024, "Kia", "Soul")])
    with sqlite3.connect(str(sqlite_inventory.path)) as raw:
        raw.execute("CREATE TABLE epa_extended_specs (epa_master_id INTEGER PRIMARY KEY, hp INTEGER)")
        raw.execute("INSERT INTO epa_extended_specs VALUES (3, 147)")
    csv_path = _write_vehicles_csv(tmp_path / "vehicles.csv", [(5001, 2025, "Kia", "Soul")])

    conn = sqlite_inventory.get_conn()
    try:
        with pytest.raises(importer.FullReplaceRefused) as info:
            importer.import_csv(str(csv_path), conn)
    finally:
        conn.close()
    assert (info.value.linked_cars, info.value.extended_specs) == (0, 1)
    assert [r[0] for r in _epa_rows(sqlite_inventory.path)] == [3]


def test_full_replace_allowed_on_empty_unlinked_catalog(sqlite_inventory, tmp_path: Path):
    """No epa_extended_specs table on SQLite: the probe reads it as empty and the load runs."""
    sqlite_inventory.add_cars([{"make": "Kia", "model": "Soul", "year": 2024}])
    csv_path = _write_vehicles_csv(
        tmp_path / "vehicles.csv", [(5001, 2025, "Kia", "Soul"), (5002, 2025, "Kia", "Rio")]
    )

    conn = sqlite_inventory.get_conn()
    try:
        n = importer.import_csv(str(csv_path), conn)
    finally:
        conn.close()

    assert n == 2
    assert [(r[1], r[3], r[4]) for r in _epa_rows(sqlite_inventory.path)] == [
        (5001, "Kia", "Soul"),
        (5002, "Kia", "Rio"),
    ]


def test_full_replace_rolls_back_when_a_link_appears_during_the_load(sqlite_inventory, tmp_path: Path, monkeypatch):
    _seed_catalog(sqlite_inventory.path, [(9, 4001, 2024, "Kia", "Soul")])
    sqlite_inventory.add_cars([{"make": "Kia", "model": "Soul", "year": 2024}])
    csv_path = _write_vehicles_csv(tmp_path / "vehicles.csv", [(5001, 2025, "Kia", "Soul")])

    real_insert = importer._insert_batch

    def insert_then_link(conn, batch):
        real_insert(conn, batch)
        conn.execute("UPDATE cars SET epa_master_id = 9")

    monkeypatch.setattr(importer, "_insert_batch", insert_then_link)
    conn = sqlite_inventory.get_conn()
    try:
        with pytest.raises(importer.FullReplaceRefused):
            importer.import_csv(str(csv_path), conn)
    finally:
        conn.close()

    assert [r[0] for r in _epa_rows(sqlite_inventory.path)] == [9]
    assert _linked_ids(sqlite_inventory.path) == []


def test_append_years_unchanged_with_links(linked_inventory, tmp_path: Path):
    csv_path = _write_vehicles_csv(
        tmp_path / "vehicles.csv",
        [(4001, 2024, "Kia", "Soul"), (5001, 2027, "Kia", "Soul"), (5002, 2026, "Kia", "Rio")],
    )
    conn = linked_inventory.get_conn()
    try:
        n = importer.import_csv(str(csv_path), conn, years={2027}, append=True)
    finally:
        conn.close()

    assert n == 1
    assert [(r[0], r[1]) for r in _epa_rows(linked_inventory.path)][0] == (7, 4001)
    assert sorted(r[1] for r in _epa_rows(linked_inventory.path)) == [4001, 5001]
    assert _linked_ids(linked_inventory.path) == [7]


def test_cli_yes_refuses_with_counts_before_downloading(linked_inventory, monkeypatch, capsys):
    def _no_download(*a, **kw):
        raise AssertionError("downloaded vehicles.csv despite a linked catalog")

    monkeypatch.setattr(importer.urllib.request, "urlretrieve", _no_download)
    before = _epa_rows(linked_inventory.path)

    assert importer.main(["--yes"]) == 2

    err = capsys.readouterr().err
    assert "1 car(s) have cars.epa_master_id set" in err
    assert "epa_extended_specs holds 0 row(s)" in err
    assert "epa_master has 1 row(s)" in err
    assert _epa_rows(linked_inventory.path) == before


# --- Postgres-shaped behaviour of the extended-specs probe ------------------


class _PgLikeConn:
    """Mimics Postgres transaction semantics: after a failed statement, every
    statement but ROLLBACK TO SAVEPOINT raises InFailedSqlTransaction."""

    def __init__(self, *, linked: int = 0, xspecs: int | Exception = 0, catalog: int = 0):
        self.linked = linked
        self.xspecs = xspecs
        self.catalog = catalog
        self.aborted = False
        self.statements: list[str] = []

    def cursor(self):
        conn = self

        class _Cur:
            def __init__(self):
                self._row = None

            def execute(self, sql, params=None):
                s = " ".join(sql.split())
                conn.statements.append(s)
                if conn.aborted and not s.upper().startswith("ROLLBACK"):
                    raise psycopg.errors.InFailedSqlTransaction("current transaction is aborted")
                if s.upper().startswith("ROLLBACK TO SAVEPOINT"):
                    conn.aborted = False
                elif "FROM epa_extended_specs" in s:
                    if isinstance(conn.xspecs, Exception):
                        conn.aborted = True
                        raise conn.xspecs
                    self._row = (conn.xspecs,)
                elif "FROM cars" in s:
                    self._row = (conn.linked,)
                elif "FROM epa_master" in s:
                    self._row = (conn.catalog,)
                return self

            def fetchone(self):
                return self._row

        return _Cur()

    def execute(self, sql, params=None):
        return self.cursor().execute(sql, params)


def test_probe_missing_table_on_postgres_leaves_transaction_usable():
    conn = _PgLikeConn(xspecs=psycopg.errors.UndefinedTable('relation "epa_extended_specs" does not exist'))
    importer.assert_full_replace_allowed(conn)  # no links, absent table: allowed
    assert conn.aborted is False
    assert "ROLLBACK TO SAVEPOINT epa_xspecs_probe" in conn.statements
    # The transaction still accepts statements after the probe.
    assert conn.execute("SELECT COUNT(*) FROM epa_master").fetchone() == (0,)


def test_probe_missing_table_still_refuses_linked_cars():
    conn = _PgLikeConn(linked=5, catalog=70, xspecs=psycopg.errors.UndefinedTable("missing"))
    with pytest.raises(importer.FullReplaceRefused) as info:
        importer.assert_full_replace_allowed(conn)
    assert (info.value.linked_cars, info.value.extended_specs, info.value.catalog_rows) == (5, 0, 70)


def test_probe_other_errors_fail_closed():
    conn = _PgLikeConn(xspecs=psycopg.errors.InsufficientPrivilege("permission denied"))
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        importer.assert_full_replace_allowed(conn)


def test_probe_counts_extended_specs_on_postgres():
    conn = _PgLikeConn(linked=0, xspecs=49_912, catalog=70_496)
    with pytest.raises(importer.FullReplaceRefused) as info:
        importer.assert_full_replace_allowed(conn)
    assert info.value.extended_specs == 49_912
    assert "SAVEPOINT epa_xspecs_probe" in conn.statements
    assert "RELEASE SAVEPOINT epa_xspecs_probe" in conn.statements
