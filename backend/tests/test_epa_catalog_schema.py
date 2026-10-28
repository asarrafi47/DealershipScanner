"""
P10A.3 (N22): V036, the epa_master flag / provenance columns, the archive tables and the two
indexes, plus their mirrors in the SQLite DDL and the legacy Postgres lists.

Static checks read the SQL and the mirror lists as text (no database). The ``pg`` tests build
the chain in a throwaway database and apply V036 twice.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from backend.tests import pg_harness as pgh

REPO_ROOT = Path(__file__).resolve().parents[2]
V036 = REPO_ROOT / "migrations" / "V036__epa_master_flags_provenance.sql"
V038 = REPO_ROOT / "migrations" / "V038__epa_catalog_constraints.sql"

NEW_MASTER_COLS = ("t_charger", "s_charger", "source", "natural_key", "loaded_at")
MIRRORS = (
    "backend/db/repositories/schema_repo.py",
    "backend/scanner/database.py",
    "backend/db/dictionary_schema.py",
    "backend/db/inventory_pg.py",
)


def _sql() -> str:
    return re.sub(r"--[^\n]*", "", V036.read_text(encoding="utf-8"))


def test_migration_is_additive_and_idempotent_by_text():
    sql = _sql()
    assert "SET LOCAL lock_timeout = '5s'" in sql
    for col in NEW_MASTER_COLS:
        assert re.search(rf"ADD COLUMN IF NOT EXISTS {col}\b", sql), col
    for create in re.findall(r"CREATE (?:UNIQUE )?(TABLE|INDEX)\s+(IF NOT EXISTS)?", sql):
        assert create[1], "every CREATE must be IF NOT EXISTS"
    assert not re.search(r"\b(DROP|DELETE|UPDATE|TRUNCATE)\b", sql, re.I)
    assert "CREATE INDEX CONCURRENTLY" not in sql  # the runner wraps each file in a transaction


def test_migration_names_the_two_indexes_the_plan_requires():
    sql = _sql()
    assert re.search(r"idx_epa_master_vehicle_id\s+ON public\.epa_master \(epa_vehicle_id\)", sql)
    assert re.search(
        r"idx_cars_epa_master_id\s+ON public\.cars \(epa_master_id\) WHERE epa_master_id IS NOT NULL", sql
    )


@pytest.mark.parametrize("path", MIRRORS)
def test_every_mirror_lists_the_new_columns(path):
    text = (REPO_ROOT / path).read_text(encoding="utf-8")
    for col in NEW_MASTER_COLS:
        assert f'("{col}",' in text, f"{path} lacks {col}"


@pytest.fixture(scope="module")
def pg_admin() -> str:
    from backend.scripts import schema_drift_report as sdr

    admin_dsn = pgh.admin_dsn_or_skip()
    try:
        sdr.check_scratch_server(admin_dsn)
    except sdr.Refused as exc:
        pytest.fail(f"TEST_PG_ADMIN_DSN is not a local scratch server: {exc}", pytrace=False)
    return admin_dsn


@pytest.fixture(scope="module")
def chain_dsn(pg_admin):
    from backend.scripts import schema_drift_report as sdr

    with pgh.scratch_db(pg_admin) as dsn:
        sdr.build_chain(dsn, REPO_ROOT / "migrations", "runner")
        yield dsn


def _cols(conn, table):
    rows = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s",
        (table,),
    ).fetchall()
    return dict(rows)


@pytest.mark.pg
def test_chain_has_new_columns_archives_and_indexes(chain_dsn):
    import psycopg

    with psycopg.connect(chain_dsn) as conn:
        master = _cols(conn, "epa_master")
        for col in NEW_MASTER_COLS:
            assert col in master, col
        assert master["loaded_at"] == "timestamp with time zone"
        archive = _cols(conn, "epa_master_archive")
        assert set(master) <= set(archive)
        assert {"archived_at", "archive_reason", "superseded_by"} <= set(archive)
        ext = _cols(conn, "epa_extended_specs")
        ext_archive = _cols(conn, "epa_extended_specs_archive")
        assert set(ext) <= set(ext_archive) and {"archived_at", "archive_reason"} <= set(ext_archive)
        idx = dict(conn.execute("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname='public'").fetchall())
        assert "epa_vehicle_id" in idx["idx_epa_master_vehicle_id"]
        assert "WHERE (epa_master_id IS NOT NULL)" in idx["idx_cars_epa_master_id"]


@pytest.mark.pg
def test_migration_reapplies_cleanly(chain_dsn):
    import psycopg

    with psycopg.connect(chain_dsn) as conn:
        before = _cols(conn, "epa_master")
        conn.execute(V036.read_text(encoding="utf-8"))
        conn.commit()
        assert _cols(conn, "epa_master") == before


# --- P10D.7 (N25): V038 unique key and RESTRICT FKs (NOT VALID) ---------------------------------


def _v038() -> str:
    return re.sub(r"--[^\n]*", "", V038.read_text(encoding="utf-8"))


def test_v038_text_is_restrict_not_valid_and_non_destructive():
    sql = _v038()
    assert "SET LOCAL lock_timeout = '5s'" in sql
    assert re.search(
        r"CREATE UNIQUE INDEX IF NOT EXISTS ux_epa_master_source_natural_key\s+"
        r"ON public\.epa_master \(source, natural_key\)",
        sql,
    )
    assert sql.count("ON DELETE RESTRICT NOT VALID") == 2
    assert "ON DELETE CASCADE" not in sql
    assert "VALIDATE CONSTRAINT" not in sql  # the later release's job
    assert not re.search(r"\b(DELETE FROM|UPDATE|TRUNCATE|DROP TABLE)\b", sql, re.I)


def test_legacy_runtime_ddl_mirrors_restrict():
    text = (REPO_ROOT / "backend/db/inventory_pg.py").read_text(encoding="utf-8")
    assert "REFERENCES epa_master(id) ON DELETE RESTRICT" in text
    assert "REFERENCES epa_master(id) ON DELETE CASCADE" not in text


def test_sqlite_migrator_copies_epa_master_before_cars():
    from backend.scripts import migrate_inventory_sqlite_to_postgres as mig

    assert mig._TABLES.index("epa_master") < mig._TABLES.index("cars")


def _fk(conn, table, name):
    return conn.execute(
        "SELECT confdeltype, convalidated FROM pg_constraint "
        "WHERE conrelid = %s::regclass AND conname = %s",
        (f"public.{table}", name),
    ).fetchone()


@pytest.mark.pg
def test_v038_constraints_exist_and_reapply_cleanly(chain_dsn):
    import psycopg

    with psycopg.connect(chain_dsn) as conn:
        assert _fk(conn, "cars", "cars_epa_master_id_fkey") == ("r", False)
        assert _fk(conn, "epa_extended_specs", "epa_extended_specs_epa_master_id_fkey") == ("r", False)
        idx = dict(conn.execute("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname='public'").fetchall())
        assert "UNIQUE" in idx["ux_epa_master_source_natural_key"]
        conn.execute(V038.read_text(encoding="utf-8"))
        conn.commit()
        assert _fk(conn, "cars", "cars_epa_master_id_fkey") == ("r", False)
        assert _fk(conn, "epa_extended_specs", "epa_extended_specs_epa_master_id_fkey") == ("r", False)


@pytest.mark.pg
def test_v038_refuses_deleting_a_linked_or_spec_bearing_catalog_row(chain_dsn):
    import psycopg
    from psycopg import errors

    with psycopg.connect(chain_dsn) as conn:
        cols = "(id, year, make, model, source, natural_key)"
        conn.execute(f"INSERT INTO epa_master {cols} VALUES (910001, 2024, 'T', 'M', 'epa', 'vid:910001')")
        conn.execute(f"INSERT INTO epa_master {cols} VALUES (910002, 2024, 'T', 'M', 'epa', 'vid:910002')")
        conn.execute("INSERT INTO epa_extended_specs (epa_master_id, year, make, model) VALUES (910002, 2024, 'T', 'M')")
        conn.execute("INSERT INTO cars (vin, epa_master_id) VALUES ('V038TESTVIN0000001', 910001)")
        conn.commit()
        with pytest.raises(errors.ForeignKeyViolation):
            conn.execute("DELETE FROM epa_master WHERE id = 910001")
        conn.rollback()
        with pytest.raises(errors.ForeignKeyViolation):
            conn.execute("DELETE FROM epa_master WHERE id = 910002")  # refused, not cascaded
        conn.rollback()
        assert conn.execute("SELECT count(*) FROM epa_extended_specs WHERE epa_master_id = 910002").fetchone()[0] == 1
        with pytest.raises(errors.UniqueViolation):
            conn.execute(f"INSERT INTO epa_master {cols} VALUES (910003, 2024, 'T', 'M', 'epa', 'vid:910001')")
        conn.rollback()
        # a NULL natural_key (unresolved legacy row) is not constrained
        conn.execute(f"INSERT INTO epa_master {cols} VALUES (910004, 2024, 'T', 'M', 'epa', NULL)")
        conn.execute(f"INSERT INTO epa_master {cols} VALUES (910005, 2024, 'T', 'M', 'epa', NULL)")
        conn.rollback()
        # a new link to a missing catalog row is refused even though the FK is NOT VALID
        with pytest.raises(errors.ForeignKeyViolation):
            conn.execute("INSERT INTO cars (vin, epa_master_id) VALUES ('V038TESTVIN0000002', 999999999)")
        conn.rollback()
        conn.execute("DELETE FROM cars WHERE vin LIKE 'V038TESTVIN%'")
        conn.execute("DELETE FROM epa_extended_specs WHERE epa_master_id = 910002")
        conn.execute("DELETE FROM epa_master WHERE id IN (910001, 910002)")
        conn.commit()
