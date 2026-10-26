"""The Postgres harness itself (P14A.1): its guards, its cleanup and its isolation.

Two tiers in one file. The un-marked tests need no server: they pin the safety
refusals (the harness must never reach ``cars``, ``cars_cleanroom``, a Railway
host or the 15432 tunnel), the skip accounting and the cache-reset sweep. The
``pg`` tests need ``TEST_PG_ADMIN_DSN`` (a LOCAL server), e.g.

    TEST_PG_ADMIN_DSN='postgresql:///postgres?host=/tmp' \\
        python -m pytest backend/tests/test_pg_harness.py -m pg -q

and leave no ``pytest_<epoch>_*`` database behind.
"""

from __future__ import annotations

import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.tests import pg_harness as pgh
from backend.tests.conftest import _ENV_GATE_SKIP_REASONS

REPO_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# Safety guards (no server)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _no_libpq_env(monkeypatch):
    for k in ("PGHOST", "PGHOSTADDR", "PGPORT", "PGDATABASE", "CI", "GITHUB_ACTIONS"):
        monkeypatch.delenv(k, raising=False)


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql:///postgres?host=/tmp",
        "postgresql://localhost/postgres",
        "postgresql://u:p@127.0.0.1:5432/template1",
        "postgresql://[::1]/postgres",
        "host=/var/run/postgresql dbname=postgres",
    ],
)
def test_local_admin_dsns_are_accepted(dsn):
    pgh.assert_safe_admin_dsn(dsn)


@pytest.mark.parametrize(
    "dsn, why",
    [
        ("postgresql:///cars?host=/tmp", "dbname"),
        ("postgresql:///cars_cleanroom?host=/tmp", "dbname"),
        ("postgresql:///railway?host=/tmp", "dbname"),
        ("postgresql://localhost/cars", "dbname"),
        ("postgresql://u:p@monorail.proxy.rlwy.net:12345/postgres", "Railway"),
        ("postgresql://u:p@db.up.railway.app/postgres", "Railway"),
        ("postgresql://u:p@postgres.railway.internal/postgres", "Railway"),
        ("postgresql://u:p@db.example.com/postgres", "local"),
        ("postgresql://u:p@10.0.0.5/postgres", "local"),
        ("postgresql://localhost:15432/postgres", "tunnel"),
        ("postgresql:///postgres?host=/tmp&port=15432", "tunnel"),
        ("postgresql://localhost/postgres?hostaddr=10.1.2.3", "hostaddr"),
        ("postgresql://postgres/postgres", "local"),  # CI alias, but not in CI
        ("", "empty"),
        ("   ", "empty"),
    ],
)
def test_unsafe_admin_dsns_are_refused(dsn, why):
    with pytest.raises(pgh.HarnessRefused):
        pgh.assert_safe_admin_dsn(dsn)


def test_libpq_env_fallbacks_are_checked(monkeypatch):
    monkeypatch.setenv("PGHOST", "monorail.proxy.rlwy.net")
    with pytest.raises(pgh.HarnessRefused):
        pgh.assert_safe_admin_dsn("dbname=postgres")
    monkeypatch.delenv("PGHOST")
    monkeypatch.setenv("PGPORT", "15432")
    with pytest.raises(pgh.HarnessRefused):
        pgh.assert_safe_admin_dsn("host=/tmp dbname=postgres")
    monkeypatch.delenv("PGPORT")
    monkeypatch.setenv("PGDATABASE", "cars")
    with pytest.raises(pgh.HarnessRefused):
        pgh.assert_safe_admin_dsn("host=/tmp")


def test_ci_service_alias_is_accepted_only_in_ci(monkeypatch):
    dsn = "postgresql://postgres:postgres@postgres:5432/postgres"
    with pytest.raises(pgh.HarnessRefused):
        pgh.assert_safe_admin_dsn(dsn)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    pgh.assert_safe_admin_dsn(dsn)
    # The alias does not open the door to a lookalike.
    with pytest.raises(pgh.HarnessRefused):
        pgh.assert_safe_admin_dsn("postgresql://postgres.railway.internal/postgres")


def test_refusal_message_never_echoes_the_password():
    with pytest.raises(pgh.HarnessRefused) as exc:
        pgh.assert_safe_admin_dsn("postgresql://u:hunter2@db.example.com/postgres")
    assert "hunter2" not in str(exc.value)


def test_connect_admin_refuses_before_connecting():
    # Would raise OperationalError (no such server) if it got as far as libpq.
    with pytest.raises(pgh.HarnessRefused):
        pgh.connect_admin("postgresql:///cars?host=/tmp")


def test_admin_dsn_comes_only_from_its_own_variable(monkeypatch):
    monkeypatch.delenv(pgh.ADMIN_DSN_ENV, raising=False)
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql:///cars?host=/tmp")
    monkeypatch.setenv("DATABASE_URL", "postgresql:///cars?host=/tmp")
    assert pgh.admin_dsn() is None
    monkeypatch.setenv(pgh.ADMIN_DSN_ENV, "  postgresql:///postgres?host=/tmp ")
    assert pgh.admin_dsn() == "postgresql:///postgres?host=/tmp"
    monkeypatch.setenv(pgh.ADMIN_DSN_ENV, "   ")
    assert pgh.admin_dsn() is None


@pytest.mark.parametrize(
    "name",
    ["cars", "cars_cleanroom", "postgres", "template0", "template1", "railway",
     "pytest_schema_chain_ab12cd34", "pytest_cars", "pytest_1700000000_", "pytest_1700000000_xyz",
     "cars_cr_n6", "pytest_1700000000_deadbeef; DROP DATABASE cars", 'pytest_1700000000_deadbeef"']
)
def test_only_harness_names_may_be_created_or_dropped(name):
    with pytest.raises(pgh.HarnessRefused):
        pgh.assert_scratch_name(name)
    with pytest.raises(pgh.HarnessRefused):
        pgh.drop("postgresql:///postgres?host=/tmp", name)  # refused before any connection


def test_generated_names_pass_their_own_guard():
    for kind in ("db", "tmpl", "db"):
        name = pgh.new_name(kind)
        pgh.assert_scratch_name(name)
        assert ("_tmpl_" in name) == (kind == "tmpl")
    assert pgh.new_name() != pgh.new_name()


def test_dsn_for_swaps_only_the_database():
    out = pgh.dsn_for("postgresql://u:p@localhost:5433/postgres?sslmode=disable", "pytest_1700000000_aaaaaaaa")
    assert out.startswith("postgresql:///pytest_1700000000_aaaaaaaa?")
    assert "port=5433" in out and "host=localhost" in out and "user=u" in out
    assert "dbname" not in out and "/postgres" not in out
    assert pgh.dsn_for("host=/tmp dbname=postgres", "pytest_1700000000_bbbbbbbb") == (
        "postgresql:///pytest_1700000000_bbbbbbbb?host=/tmp"
    )


# ---------------------------------------------------------------------------
# Skip accounting (no server)
# ---------------------------------------------------------------------------


def test_the_no_dsn_skip_reason_is_counted_as_an_env_gate():
    assert any(r in pgh.SKIP_REASON_NO_DSN for r in _ENV_GATE_SKIP_REASONS)
    assert any(r in "pg tier unavailable: role is not a superuser" for r in _ENV_GATE_SKIP_REASONS)


def _run_probe(require: bool) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if k not in (pgh.ADMIN_DSN_ENV, pgh.REQUIRE_PG_ENV)}
    env["TESTS_BLOCK_NETWORK"] = "1"
    if require:
        env[pgh.REQUIRE_PG_ENV] = "1"
    return subprocess.run(
        [sys.executable, "-m", "pytest",
         "backend/tests/test_pg_harness.py::test_a_cloned_database_is_the_inventory",
         "-m", "pg", "-q", "-p", "no:cacheprovider", "-rs"],
        cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=120,
    )


def test_without_a_dsn_pg_tests_skip_and_require_pg_fails_them():
    skipped = _run_probe(require=False)
    assert skipped.returncode == 0, skipped.stdout + skipped.stderr
    assert "1 skipped" in skipped.stdout and pgh.SKIP_REASON_NO_DSN in skipped.stdout
    assert "ASSET-GATED SKIPS: 1" in skipped.stdout  # counted in the summary
    required = _run_probe(require=True)
    assert required.returncode == 1, required.stdout + required.stderr
    assert "REQUIRE_PG=1" in required.stdout and "skipped" not in required.stdout.splitlines()[-1]


# ---------------------------------------------------------------------------
# Cache reset (no server)
# ---------------------------------------------------------------------------


def test_every_module_level_ready_flag_is_reset_or_reviewed():
    """A new ``_*_OK`` / ``_*_ready`` / ``_*_CACHE`` flag or inventory ``lru_cache`` must be added
    to pg_harness._FLAG_RESETS / _LRU_RESETS (reset between tests) or to _SWEEP_REVIEWED (with
    the reason it does not depend on the database)."""
    assert pgh.sweep_unaccounted() == []


def test_the_sweep_sees_the_known_flags():
    found = pgh.sweep_cache_flags()
    for name in ("backend.db.inventory_pg._PG_INV_SCHEMA_OK",
                 "backend.scanner.database._vin_owner_table_ready", "backend.utils.msrp_trust._BAND_LOADED",
                 "backend.enrichment.trim_ladder.evidence._active_make_spellings"):
        assert name in found, name


def test_the_sweep_catches_a_new_flag(tmp_path):
    (tmp_path / "newmod.py").write_text("_WIDGET_TABLE_OK = False\n_unrelated = 1\n", encoding="utf-8")
    assert pgh.sweep_cache_flags(tmp_path) == {"backend.newmod._WIDGET_TABLE_OK": "flag"}
    assert pgh.sweep_unaccounted(tmp_path) == ["backend.newmod._WIDGET_TABLE_OK"]


def test_every_listed_reset_target_exists():
    """A renamed flag would otherwise be skipped silently."""
    import importlib

    for mod_name, attr, _kind in pgh._FLAG_RESETS:
        assert hasattr(importlib.import_module(mod_name), attr), (mod_name, attr)
    for mod_name, attr in (*pgh._LRU_RESETS, *pgh._RESET_FUNCTIONS):
        assert callable(getattr(importlib.import_module(mod_name), attr)), (mod_name, attr)


def test_reset_pg_process_caches_clears_flags_and_the_shared_connection():
    from backend.db import inventory_pg
    from backend.scanner import database as scanner_database
    from backend.utils import msrp_trust

    closed = []
    inventory_pg._shared_read.raw = SimpleNamespace(close=lambda: closed.append(True))
    inventory_pg._PG_INV_SCHEMA_OK = True
    scanner_database._vin_owner_table_ready = True
    msrp_trust._BAND_LOADED = True
    msrp_trust._BAND_CACHE[("x",)] = (1, 2, 3)
    try:
        pgh.reset_pg_process_caches()
        assert closed == [True] and inventory_pg.current_shared_read_raw() is None
        assert inventory_pg._PG_INV_SCHEMA_OK is False
        assert scanner_database._vin_owner_table_ready is False
        assert msrp_trust._BAND_LOADED is False and not msrp_trust._BAND_CACHE
    finally:
        inventory_pg._shared_read.raw = None


# ---------------------------------------------------------------------------
# The pg tier (needs TEST_PG_ADMIN_DSN)
# ---------------------------------------------------------------------------


@pytest.mark.pg
def test_the_template_is_the_migration_chain_head(pg_template):
    import psycopg

    from backend.db.schema_version import expected_schema_version

    assert pg_template.version == expected_schema_version() >= 26
    assert pg_template.name.startswith("pytest_") and "_tmpl_" in pg_template.name
    with psycopg.connect(pg_template.dsn) as conn:
        assert conn.execute("SELECT to_regclass('public.cars')").fetchone()[0] == "cars"


@pytest.mark.pg
def test_a_cloned_database_is_the_inventory(pg_db):
    from backend.db import inventory_db, inventory_pg

    assert inventory_pg.is_inventory_postgres()
    assert inventory_pg.inventory_postgres_dsn() == pg_db.dsn
    assert os.environ["INVENTORY_SQLITE_TESTS"] == "" and os.environ["INVENTORY_SCHEMA_CHECK"] == "strict"
    assert os.environ["DATABASE_URL"] == "" and os.environ["INVENTORY_AUTO_MIGRATE"] == ""
    conn = inventory_db.get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT current_database()")
        row = cur.fetchone()
        assert (row[0] if not isinstance(row, dict) else next(iter(row.values()))) == pg_db.name
    finally:
        conn.close()
    assert pg_db.name not in pgh.PROTECTED_DBS


@pytest.mark.pg
def test_a_row_written_here_is_invisible_in_the_next_test(pg_db):
    assert pg_db.add_cars([{"make": "Toyota", "model": "Camry", "year": 2024}]) == 1
    with pg_db.connect() as conn:
        assert conn.execute("SELECT count(*) FROM cars").fetchone()[0] == 1


@pytest.mark.pg
def test_the_next_test_starts_from_an_empty_cars_table(pg_db):
    with pg_db.connect() as conn:
        assert conn.execute("SELECT count(*) FROM cars").fetchone()[0] == 0


@pytest.mark.pg
def test_add_cars_fills_vin_and_activity_and_rejects_unknown_columns(pg_db):
    pg_db.add_cars([{"make": "Honda"}, {"make": "Kia", "vin": "KNDPM3AC0K7000001", "listing_active": 0}])
    with pg_db.connect() as conn:
        rows = conn.execute("SELECT make, vin, listing_active FROM cars ORDER BY make").fetchall()
    assert rows[0][0] == "Honda" and rows[0][1].startswith("TESTVIN") and rows[0][2] == 1
    assert rows[1] == ("Kia", "KNDPM3AC0K7000001", 0)
    with pytest.raises(KeyError):
        pg_db.add_cars([{"no_such_column": 1}])


@pytest.mark.pg
def test_seed_helpers_fill_required_columns_and_serialise_json(pg_db):
    assert pg_db.add_dealerships([{}, {"name": "Named", "city": "Raleigh"}]) == 2
    assert pg_db.add_dealer_geopoints([{"lat": 35.2, "lon": -80.8}, {}]) == 2
    assert pg_db.add_dealer_catalog([{"dealer_id": "d-1", "provider": "p"}, {}]) == 2
    assert pg_db.add_jobs([{"dealer_id": "d-1"}, {"job_type": "discover"}]) == 2
    assert pg_db.add_recipes([{"dealer_id": "d-1", "recipes_json": [{"a": 1}]}, {}]) == 2
    with pg_db.connect() as conn:
        names = [r[0] for r in conn.execute("SELECT name FROM dealerships ORDER BY id").fetchall()]
        assert names == ["Test Dealer 0", "Named"]
        assert conn.execute("SELECT count(DISTINCT website_url) FROM dealerships").fetchone()[0] == 2
        assert conn.execute("SELECT count(DISTINCT dealer_url) FROM dealer_geopoints").fetchone()[0] == 2
        assert conn.execute("SELECT updated_at FROM dealer_catalog WHERE dealer_id = 'd-1'").fetchone()[0]
        jobs = conn.execute("SELECT job_type, status FROM dealer_jobs ORDER BY id").fetchall()
        assert jobs == [("scan", "queued"), ("discover", "queued")]
        assert conn.execute(
            "SELECT recipes_json FROM dealer_recipes WHERE dealer_id = 'd-1'"
        ).fetchone()[0] == '[{"a": 1}]'


@pytest.mark.pg
@pytest.mark.parametrize("method", ["add_dealerships", "add_dealer_geopoints", "add_dealer_catalog", "add_jobs", "add_recipes"])
def test_seed_helpers_reject_unknown_columns(pg_db, method):
    with pytest.raises(KeyError):
        getattr(pg_db, method)([{"no_such_column": 1}])


@pytest.mark.pg
def test_a_stale_ready_flag_does_not_leak_into_the_next_test(pg_db):
    from backend.db import inventory_pg

    # The fixture reset the flag before this test; the next test's fixture must reset it again.
    assert inventory_pg._PG_INV_SCHEMA_OK is False
    inventory_pg._PG_INV_SCHEMA_OK = True


@pytest.mark.pg
def test_the_flag_set_by_the_previous_test_was_reset(pg_db):
    from backend.db import inventory_pg

    assert inventory_pg._PG_INV_SCHEMA_OK is False


@pytest.mark.pg
def test_clone_refuses_a_template_with_open_connections(pg_template):
    import psycopg

    holder = psycopg.connect(pg_template.dsn)
    try:
        with pytest.raises(RuntimeError, match="open connection"):
            pgh.clone(pg_template)
    finally:
        holder.close()


@pytest.mark.pg
def test_clone_and_drop_leave_nothing_behind_and_are_fast(pg_template):
    admin = pg_template.admin
    before = set(pgh.leftover_databases(admin))
    times = []
    for _ in range(5):
        t0 = time.perf_counter()
        name, _dsn = pgh.clone(pg_template)
        assert name in pgh.leftover_databases(admin)
        pgh.drop(admin, name)
        times.append(time.perf_counter() - t0)
        assert name not in pgh.leftover_databases(admin)
    assert set(pgh.leftover_databases(admin)) == before
    # Budget: median clone + drop under 0.5 s (P14A.1). Loose here so a busy
    # machine does not flake; the number is printed for the record.
    median = statistics.median(times)
    print(f"clone+drop median {median:.3f}s over {len(times)}")
    assert median < 2.0, times


@pytest.mark.pg
def test_drop_with_force_removes_a_database_that_has_a_connection(pg_template):
    import psycopg

    name, dsn = pgh.clone(pg_template)
    conn = psycopg.connect(dsn)
    try:
        pgh.drop(pg_template.admin, name)
        assert name not in pgh.leftover_databases(pg_template.admin)
    finally:
        try:
            conn.close()
        except Exception:
            pass


@pytest.mark.pg
def test_janitor_drops_only_old_harness_databases(pg_template):
    admin = pg_template.admin
    old = f"pytest_{int(time.time()) - 7 * 3600}_deadbeef"
    young = f"pytest_{int(time.time()) - 60}_cafebabe"
    foreign = "pytest_schema_janitor_probe"
    conn = pgh.connect_admin(admin)
    try:
        for n in (old, young, foreign):
            conn.execute(f'CREATE DATABASE "{n}" TEMPLATE template0')
        dropped = pgh.janitor(admin)
        names = {r[0] for r in conn.execute("SELECT datname FROM pg_database WHERE datname LIKE 'pytest\\_%'").fetchall()}
        assert old in dropped and old not in names
        assert young in names and foreign in names
        assert pg_template.name in names  # this session's template is young
    finally:
        for n in (old, young, foreign):
            conn.execute(f'DROP DATABASE IF EXISTS "{n}" WITH (FORCE)')
        conn.close()


@pytest.mark.pg
def test_the_server_is_checked_for_what_the_chain_needs(pg_template):
    conn = pgh.connect_admin(pg_template.admin)
    try:
        pgh.check_server(conn)  # superuser or raises PgUnavailable (skips the session otherwise)
        assert conn.execute("SELECT current_database()").fetchone()[0] in pgh.ADMIN_DBNAMES
    finally:
        conn.close()


@pytest.mark.pg
def test_the_data_databases_are_never_the_harness_target(pg_template):
    assert pg_template.name not in pgh.PROTECTED_DBS
    with pytest.raises(pgh.HarnessRefused):
        pgh.drop(pg_template.admin, "cars")
    with pytest.raises(pgh.HarnessRefused):
        pgh.drop(pg_template.admin, "cars_cleanroom")
