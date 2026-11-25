"""V026 -> V032 upgrade rehearsal on a POPULATED database (``pg`` tier).

Release 1.6.0 carries migrations V027 to V032 and production sits at V026. Every other pg test
builds the chain on an empty database, so none of them shows what the upgrade does to a database
that already holds cars. This file does, on a scratch Postgres only (the pg harness refuses
anything else):

* builds V001..V026 with the real runner, fills ``cars`` (and ``incomplete_listings``) with
  synthetic rows (``UPGRADE_REHEARSAL_ROWS``, default 20000; nothing here is real dealer data),
* applies V027..V032 with the real runner and checks that no row changed, that legacy rows get the
  column defaults, that ``cars`` and ``incomplete_listings`` were neither rewritten nor updated in
  place (same file, same row placement and writers: the "metadata only" claim in V028, and the same
  property of V027), that the schema diff against V026 is exactly the expected columns and tables
  (nothing else created, altered or indexed), that the result equals a fresh V001..V032 build, and
  that a second run is a no-op,
* shows what V028 does to a database that is in use: its ALTER TABLE on ``cars`` needs ACCESS
  EXCLUSIVE, so ANY open transaction that touched ``cars`` (a scan's write transaction, an idle web
  connection mid-request) makes it wait. While it waits, new readers queue behind it. After
  ``--lock-timeout`` it fails, rolls back and records nothing; a retry once the holder is gone
  works,
* runs the real 1.6.0 scanner write path (``upsert_vehicles``) against the upgraded database and
  against a database that is still at V027, which is why the migration goes first.

Every apply here stops at ``TARGET`` (32): the file rehearses the 1.6.0 release (V027..V032), so a
later migration must not change its results. Only the two scanner tests apply everything, because
strict schema mode wants the database at the highest version.

What this does NOT prove: production data volume (the synthetic table is small; the physical-layout
check is what shows the cost does not grow with row count), production's own lock holders, or
the Railway deploy itself.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Iterator

import pytest

from backend.scanner import database as sdb
from backend.scanner.database import upsert_vehicles
from backend.scripts import migrate
from backend.scripts import schema_drift_report as sdr
from backend.tests import pg_harness as pgh

pytestmark = pytest.mark.pg

def _rows_from_env() -> int:
    try:
        return max(1000, int(os.environ.get("UPGRADE_REHEARSAL_ROWS", "") or 20000))
    except ValueError:  # a bad value must not break collection in the offline job
        return 20000


ROWS = _rows_from_env()
TARGET = 32  # the release under rehearsal: V032 is the highest migration 1.6.0 carries
INCOMPLETE_EVERY = 10  # every 10th car also has an incomplete_listings row

# The tables V029..V032 create (V027 and V028 only add columns).
NEW_TABLES = (
    "admin_users", "chat_usage_buckets", "app_settings", "auth_phone_identities",
    "auth_phone_challenges", "auth_phone_sends", "auth_phone_wrong_tries",
    "auth_phone_lookup_cache", "auth_session_epochs", "loop_files",
)
ROOFTOP_COLUMNS = ("rooftop_tier", "rooftop_evidence")
WINDOW_COLUMNS = ("first_seen_at", "last_scraped_at", "scans_survived", "pending")

# Exactly what V026 -> V032 adds, as schema_drift_report.snapshot() sees it: (data type, nullable, default).
EXPECTED_NEW_COLUMNS = {
    ("cars", "rooftop_tier"): ("text", "NO", "'none'::text"),
    ("cars", "rooftop_evidence"): ("text", "YES", ""),
    ("incomplete_listings", "first_seen_at"): ("text", "YES", ""),
    ("incomplete_listings", "last_scraped_at"): ("text", "YES", ""),
    ("incomplete_listings", "scans_survived"): ("integer", "NO", "1"),
    ("incomplete_listings", "pending"): ("integer", "NO", "0"),
}
# Each new table with the number of objects (columns, keys, indexes, sequences) it brings.
EXPECTED_NEW_TABLES = {
    "admin_users": 10, "app_settings": 5, "auth_phone_challenges": 25, "auth_phone_identities": 21,
    "auth_phone_lookup_cache": 9, "auth_phone_sends": 27, "auth_phone_wrong_tries": 5,
    "auth_session_epochs": 5, "chat_usage_buckets": 13, "loop_files": 9,
}

_POPULATE_CARS = """
INSERT INTO public.cars (
    vin, title, year, make, model, "trim", price, mileage, fuel_type, cylinders, transmission,
    drivetrain, exterior_color, interior_color, image_url, dealer_name, dealer_url, scraped_at,
    dealer_id, stock_number, gallery, source_url, body_style, engine_description, condition,
    description, data_quality_score, listing_active, first_seen_at, zip_code, dealership_registry_id
)
SELECT
    '1N4' || lpad(g::text, 14, '0'),
    (2015 + g % 10) || ' ' || (ARRAY['Toyota','Honda','Ford','Chevrolet','BMW','Kia'])[1 + g % 6] || ' Model ' || (g % 40),
    2015 + g % 10,
    (ARRAY['Toyota','Honda','Ford','Chevrolet','BMW','Kia'])[1 + g % 6],
    'Model ' || (g % 40),
    (ARRAY['Base','SE','XLE','Sport','Limited'])[1 + g % 5],
    15000 + (g % 400) * 100,
    5000 + (g % 900) * 100,
    (ARRAY['Gasoline','Hybrid','Electric','Diesel'])[1 + g % 4],
    (ARRAY[4, 6, 8])[1 + g % 3],
    'Automatic',
    (ARRAY['FWD','AWD','RWD','4WD'])[1 + g % 4],
    (ARRAY['White','Black','Silver','Blue','Red'])[1 + g % 5],
    'Black',
    'https://img.example.test/' || g || '/0.jpg',
    'Test Dealer ' || (g % 300),
    'https://dealer' || (g % 300) || '.example.test',
    to_char(now() - (g % 90) * interval '1 day', 'YYYY-MM-DD"T"HH24:MI:SS'),
    'dealer-' || (g % 300),
    'STK' || g,
    '["https://img.example.test/' || g || '/1.jpg","https://img.example.test/' || g || '/2.jpg",'
        || '"https://img.example.test/' || g || '/3.jpg","https://img.example.test/' || g || '/4.jpg"]',
    'https://dealer' || (g % 300) || '.example.test/vdp/' || g,
    (ARRAY['Sedan','SUV','Truck','Coupe'])[1 + g % 4],
    '2.5L I4 DOHC 16V',
    'Used',
    repeat('Well maintained one owner vehicle with a clean history report. ', 8) || md5(g::text),
    0.5 + (g % 50) / 100.0,
    CASE WHEN g % 10 = 0 THEN 0 ELSE 1 END,
    to_char(now() - (g % 120) * interval '1 day', 'YYYY-MM-DD"T"HH24:MI:SS'),
    lpad((10000 + g % 80000)::text, 5, '0'),
    g % 300
FROM generate_series(1, __ROWS__) AS g
"""

_POPULATE_INCOMPLETE = """
INSERT INTO public.incomplete_listings (car_id, vin, missing_fields_json, updated_at)
SELECT id, vin, '["mileage","price"]', scraped_at FROM public.cars WHERE id % __EVERY__ = 0
"""

# What a row looks like to anyone reading the old columns: the fingerprint must not change.
_FINGERPRINT_CARS = """
SELECT count(*), md5(string_agg(concat_ws('|', id, vin, make, model, price, mileage, dealer_id,
                                          listing_active, scraped_at, description), ',' ORDER BY id))
FROM public.cars
"""
# Where every row physically sits and which transaction wrote it. A metadata-only ALTER leaves both
# alone; a table rewrite or an UPDATE backfill (which would hold the ACCESS EXCLUSIVE lock for its
# whole run) changes them.
_PHYSICAL = """
SELECT md5(string_agg(ctid::text || ':' || xmin::text, ',' ORDER BY %(key)s)), pg_relation_size('public.%(table)s')
FROM public.%(table)s
"""
_FINGERPRINT_INCOMPLETE = """
SELECT count(*), md5(string_agg(concat_ws('|', car_id, vin, missing_fields_json, updated_at), ',' ORDER BY car_id))
FROM public.incomplete_listings
"""


def _connect(dsn: str, *, autocommit: bool = False) -> Any:
    import psycopg

    return psycopg.connect(dsn, autocommit=autocommit)


def _one(dsn: str, sql: str, params: tuple = ()) -> Any:
    with _connect(dsn) as conn:
        return conn.execute(sql, params).fetchone()


def _max_version(dsn: str) -> int:
    return _one(dsn, "SELECT max(version) FROM public.schema_migrations")[0]


def _columns(dsn: str, table: str) -> set[str]:
    with _connect(dsn) as conn:
        return {r[0] for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s",
            (table,),
        ).fetchall()}


def _relfilenode(dsn: str, table: str) -> int:
    """The file behind a table. ALTER TABLE ... ADD COLUMN with a constant default keeps it; a rewrite changes it."""
    return _one(dsn, "SELECT relfilenode FROM pg_class WHERE oid = %s::regclass", (f"public.{table}",))[0]


def _physical(dsn: str, table: str) -> tuple:
    """(relfilenode, row placement and writer fingerprint, heap size) of *table*."""
    key = "car_id" if table == "incomplete_listings" else "id"
    return (_relfilenode(dsn, table), *_one(dsn, _PHYSICAL % {"key": key, "table": table}))


def _migrate(dsn: str, *argv: str) -> int:
    return pgh.migrate_main(dsn, *argv)


def _wait_for(predicate, *, what: str, timeout: float = 30.0, interval: float = 0.02) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for: {what}")


def _ungranted_on_cars(monitor: Any) -> set[str]:
    """Lock modes that some backend is still waiting for on ``cars`` right now."""
    return {r[0] for r in monitor.execute(
        "SELECT mode FROM pg_locks WHERE relation = 'public.cars'::regclass AND NOT granted "
        "AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"
    ).fetchall()}


@pytest.fixture(scope="module")
def pg_admin() -> str:
    """The guarded admin DSN (``TEST_PG_ADMIN_DSN``): a LOCAL server only."""
    admin_dsn = pgh.admin_dsn_or_skip()
    try:
        sdr.check_scratch_server(admin_dsn)
    except sdr.Refused as exc:
        pytest.fail(f"TEST_PG_ADMIN_DSN is not a local scratch server: {exc}", pytrace=False)
    return admin_dsn


@pytest.fixture(scope="module")
def populated_v026(pg_admin) -> Iterator[pgh.Template]:
    """A template database at exactly V026 holding ROWS synthetic cars (prod's schema level today)."""
    name = pgh.new_name("tmpl")
    conn = pgh.connect_admin(pg_admin)
    try:
        try:
            pgh.check_server(conn)
        except pgh.PgUnavailable as exc:
            pytest.skip(f"pg tier unavailable: {exc}")
        conn.execute(f'CREATE DATABASE "{name}" TEMPLATE template0')
    finally:
        conn.close()
    dsn = pgh.dsn_for(pg_admin, name)
    try:
        assert _migrate(dsn, "--apply", "--target", "26") == 0
        assert _max_version(dsn) == 26
        with _connect(dsn, autocommit=True) as c:
            c.execute(_POPULATE_CARS.replace("__ROWS__", str(ROWS)))  # no params: the SQL has literal %
            c.execute(_POPULATE_INCOMPLETE.replace("__EVERY__", str(INCOMPLETE_EVERY)))
            c.execute("VACUUM (ANALYZE) public.cars")  # settles autovacuum's insert trigger before the clones
        assert _one(dsn, "SELECT count(*) FROM public.cars")[0] == ROWS
        yield pgh.Template(pg_admin, name, dsn, 26)
    finally:
        pgh.drop(pg_admin, name)


@pytest.fixture()
def v026(pg_admin, populated_v026) -> Iterator[str]:
    """A private copy of the populated V026 database (DSN)."""
    name, dsn = pgh.clone(populated_v026)
    try:
        yield dsn
    finally:
        pgh.drop(pg_admin, name)


@pytest.fixture()
def v027(v026) -> str:
    """The same copy after V027 only: the state a failed V028 leaves behind."""
    assert _migrate(v026, "--apply", "--target", "27") == 0
    assert _max_version(v026) == 27
    return v026


# --- the upgrade itself -------------------------------------------------------------------------


def test_the_fixture_is_a_v026_database_with_the_expected_pending_files(v026, caplog):
    import logging

    assert _columns(v026, "cars").isdisjoint(ROOFTOP_COLUMNS)
    assert _columns(v026, "incomplete_listings").isdisjoint(WINDOW_COLUMNS)
    with _connect(v026) as conn:
        existing = {r[0] for r in conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'").fetchall()}
    assert existing.isdisjoint(NEW_TABLES)
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="migrate"):
        assert _migrate(v026, "--dry-run", "--target", str(TARGET)) == 0  # lists, writes nothing
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "would apply 6" in text, text  # exactly V027..V032
    assert _max_version(v026) == 26


def test_v027_to_v032_apply_to_a_populated_v026_without_touching_a_row(v026):
    cars_before = _one(v026, _FINGERPRINT_CARS)
    incomplete_before = _one(v026, _FINGERPRINT_INCOMPLETE)
    assert cars_before[0] == ROWS and incomplete_before[0] == ROWS // INCOMPLETE_EVERY
    physical_before = {t: _physical(v026, t) for t in ("cars", "incomplete_listings")}
    applied_before = _one(v026, "SELECT count(*) FROM public.schema_migrations")[0]
    with _connect(v026) as conn:
        schema_before = sdr.snapshot(conn)

    assert _migrate(v026, "--apply", "--target", str(TARGET)) == 0

    assert _max_version(v026) == TARGET
    with _connect(v026) as conn:
        recorded = [r[0] for r in conn.execute(
            "SELECT version FROM public.schema_migrations WHERE version > 26 ORDER BY version").fetchall()]
    assert recorded == [27, 28, 29, 30, 31, 32]
    assert _one(v026, "SELECT count(*) FROM public.schema_migrations")[0] == applied_before + 6

    # The schema diff against V026 is exactly the six columns and ten tables, with their definitions:
    # no extra index, constraint, trigger or default on cars or incomplete_listings, no other change.
    with _connect(v026) as conn:
        schema_after = sdr.snapshot(conn)
    added = sdr.diff_snapshots(schema_after, schema_before)  # "chain" side = after: chain_only = created
    assert {(d.kind, d.category, d.name) for d in added} == (
        {("chain_only", "column", f"{t}.{c}") for t, c in EXPECTED_NEW_COLUMNS}
        | {("chain_only", "table", t) for t in EXPECTED_NEW_TABLES}
    )
    assert {d.name: len(d.children) for d in added if d.category == "table"} == EXPECTED_NEW_TABLES
    assert {k: schema_after["columns"][k] for k in EXPECTED_NEW_COLUMNS} == EXPECTED_NEW_COLUMNS

    # No row moved or changed: the old columns fingerprint identically.
    assert _one(v026, _FINGERPRINT_CARS) == cars_before
    assert _one(v026, _FINGERPRINT_INCOMPLETE) == incomplete_before

    # Metadata only: neither existing table was rewritten or updated in place (same file, same row placement
    # and writers, same heap size), so the cost does not depend on the row count.
    assert {t: _physical(v026, t) for t in physical_before} == physical_before

    # Legacy rows carry the column defaults (V028: 'none' and NULL; V027: 1, 0 and NULL).
    assert _one(v026, "SELECT count(*) FROM public.cars WHERE rooftop_tier = 'none' AND rooftop_evidence IS NULL")[0] == ROWS
    assert _one(
        v026,
        "SELECT count(*) FROM public.incomplete_listings WHERE scans_survived = 1 AND pending = 0 "
        "AND first_seen_at IS NULL AND last_scraped_at IS NULL",
    )[0] == ROWS // INCOMPLETE_EVERY

    # V029..V032 only add new, empty tables.
    for table in NEW_TABLES:
        assert _one(v026, f"SELECT count(*) FROM public.{table}")[0] == 0, table


def test_the_upgraded_database_equals_a_fresh_v032_build(v026, pg_admin):
    """State-dependent DDL (IF NOT EXISTS skipping something that already exists) would show here.

    Both sides use the same files, so an edit to a migration is caught by the exact diff above, not by this.
    """
    assert _migrate(v026, "--apply", "--target", str(TARGET)) == 0
    with pgh.scratch_db(pg_admin) as fresh:
        assert _migrate(fresh, "--apply", "--target", str(TARGET)) == 0
        assert _max_version(fresh) == TARGET
        with _connect(v026) as a, _connect(fresh) as b:
            upgraded, built = sdr.snapshot(a), sdr.snapshot(b)
    drift = sdr.diff_snapshots(built, upgraded)
    assert not drift, [(d.kind, d.category, d.name) for d in drift]


def test_the_physical_check_would_notice_a_rewrite_or_a_backfill(v026):
    """Controls for the 'metadata only' assertions: a volatile default rewrites, an UPDATE backfill rewrites rows."""
    before = _physical(v026, "incomplete_listings")
    with _connect(v026) as conn:
        conn.execute("ALTER TABLE public.incomplete_listings ADD COLUMN _rewrite_control double precision NOT NULL DEFAULT random()")
    rewritten = _physical(v026, "incomplete_listings")
    assert rewritten[0] != before[0]  # new relfilenode

    cars_before = _physical(v026, "cars")
    with _connect(v026) as conn:
        conn.execute("UPDATE public.cars SET price = price")  # what a data backfill inside V028 would do
    updated = _physical(v026, "cars")
    assert updated[0] == cars_before[0]  # same file ...
    assert updated[1:] != cars_before[1:]  # ... but every row moved and carries a new writer


def test_a_second_apply_is_a_no_op_and_the_files_are_idempotent(v026):
    assert _migrate(v026, "--apply", "--target", str(TARGET)) == 0
    with _connect(v026) as conn:
        stamped = conn.execute("SELECT version, checksum, applied_at FROM public.schema_migrations ORDER BY version").fetchall()
    physical = {t: _physical(v026, t) for t in ("cars", "incomplete_listings")}
    assert _migrate(v026, "--apply", "--target", str(TARGET)) == 0
    with _connect(v026) as conn:
        assert conn.execute(
            "SELECT version, checksum, applied_at FROM public.schema_migrations ORDER BY version").fetchall() == stamped
    # Running V027 and V028 by hand a second time (what a half-recorded run would do) changes nothing either.
    with _connect(v026) as conn:
        for path in sorted(migrate.MIGRATIONS_DIR.glob("V02[78]__*.sql")):
            conn.execute(path.read_text(encoding="utf-8"))
    assert {t: _physical(v026, t) for t in physical} == physical


def test_a_writer_that_names_only_v026_columns_keeps_working_on_the_upgraded_schema(v026):
    """What an older scanner image sends (an INSERT and an upsert with V026-era column lists) against the V028 schema."""
    assert _migrate(v026, "--apply", "--target", str(TARGET)) == 0
    with _connect(v026) as conn:
        conn.execute(
            "INSERT INTO public.cars (vin, make, model, price, dealer_id, listing_active) "
            "VALUES ('OLDWRITER00000001', 'Kia', 'Old', 1, 'old-writer', 1)")
        conn.execute(
            "INSERT INTO public.cars (vin, make, model, price, dealer_id, listing_active) "
            "VALUES ('1N400000000000001', 'Kia', 'Upsert', 2, 'dealer-1', 1) "
            "ON CONFLICT (vin) DO UPDATE SET price = excluded.price, model = excluded.model")
        conn.commit()
        assert conn.execute(
            "SELECT rooftop_tier, rooftop_evidence FROM public.cars WHERE vin = 'OLDWRITER00000001'"
        ).fetchone() == ("none", None)
        assert conn.execute(
            "SELECT price, model, rooftop_tier FROM public.cars WHERE vin = '1N400000000000001'"
        ).fetchone() == (2.0, "Upsert", "none")
        assert conn.execute("SELECT count(*) FROM public.cars").fetchone()[0] == ROWS + 1


# --- what V028 does to a database that is in use -------------------------------------------------

_HOLDERS = {
    # Both leave a transaction open on ``cars``, as an idle web connection or a scan mid-dealer would.
    "reader": "SELECT count(*) FROM public.cars",
    "writer": "UPDATE public.cars SET price = price WHERE id = 1",
}


def test_the_runner_default_lock_timeout_is_five_seconds():
    assert migrate.DEFAULT_LOCK_TIMEOUT == "5s"


@pytest.mark.parametrize("holder", sorted(_HOLDERS))
def test_v028_fails_cleanly_behind_an_open_transaction_and_a_retry_succeeds(v027, holder, caplog):
    """An open transaction on ``cars`` blocks V028's ACCESS EXCLUSIVE; the runner rolls back and records nothing."""
    import logging

    cars_before = _one(v027, _FINGERPRINT_CARS)
    blocker = _connect(v027)
    try:
        blocker.execute(_HOLDERS[holder])  # transaction stays open
        caplog.clear()
        started = time.monotonic()
        with caplog.at_level(logging.INFO, logger="migrate"):
            rc = _migrate(v027, "--apply", "--target", str(TARGET), "--lock-timeout", "300ms")
        waited = time.monotonic() - started
    finally:
        blocker.rollback()
        blocker.close()
    assert rc == 1
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "FAILED V028" in text and "lock_timeout (300ms) expired" in text, text
    # It waited for the lock, then gave up at the 300 ms we asked for: well under the 5 s default.
    assert 0.25 <= waited < 4.5, waited
    # Nothing half-applied: V027 (committed before) stays, V028 to V032 are not recorded, the column is absent.
    assert _max_version(v027) == 27
    assert _columns(v027, "cars").isdisjoint(ROOFTOP_COLUMNS)
    assert _one(v027, _FINGERPRINT_CARS) == cars_before
    # Once the holder is gone the same command finishes the job.
    assert _migrate(v027, "--apply", "--target", str(TARGET)) == 0
    assert _max_version(v027) == TARGET
    assert set(ROOFTOP_COLUMNS) <= _columns(v027, "cars")


def test_a_waiting_v028_queues_new_readers_behind_it_until_the_holder_ends(v027):
    """The reason the timeout is 5 s and not 'wait': readers that are compatible with the holder still queue behind the ALTER."""
    blocker = _connect(v027)
    monitor = _connect(v027, autocommit=True)
    reader_conn = _connect(v027, autocommit=True)
    outcome: dict[str, Any] = {}

    def run_migrate() -> None:
        try:
            outcome["rc"] = _migrate(v027, "--apply", "--target", str(TARGET), "--lock-timeout", "120s")
        except BaseException as exc:  # reported by the asserts below instead of dying inside the thread
            outcome["migrate_error"] = repr(exc)

    def run_reader() -> None:
        try:
            outcome["count"] = reader_conn.execute("SELECT count(*) FROM public.cars").fetchone()[0]
            outcome["reader_done"] = time.monotonic()
        except BaseException as exc:
            outcome["reader_error"] = repr(exc)

    migrator = threading.Thread(target=run_migrate, daemon=True)
    reader = threading.Thread(target=run_reader, daemon=True)
    try:
        blocker.execute(_HOLDERS["reader"])  # ACCESS SHARE, held open
        migrator.start()
        _wait_for(lambda: "AccessExclusiveLock" in _ungranted_on_cars(monitor), what="V028's ALTER waiting for cars")
        reader.start()
        # The new reader asks for ACCESS SHARE, which the blocker already holds compatibly, yet it waits:
        # Postgres queues it behind the pending ACCESS EXCLUSIVE request.
        _wait_for(lambda: "AccessShareLock" in _ungranted_on_cars(monitor), what="a new reader queued behind the ALTER")
        assert "count" not in outcome and "rc" not in outcome  # both are stuck right now
        released = time.monotonic()
        blocker.rollback()  # the holder ends: the ALTER proceeds, the reader follows
    finally:
        blocker.rollback()  # whatever happened above, let both threads finish before the connections close
        for thread in (migrator, reader):
            if thread.ident is not None:
                thread.join(timeout=60)
        for conn in (blocker, monitor, reader_conn):
            conn.close()
    assert not migrator.is_alive() and not reader.is_alive()
    assert "migrate_error" not in outcome and "reader_error" not in outcome, outcome
    assert outcome["rc"] == 0
    assert outcome["count"] == ROWS
    assert outcome["reader_done"] >= released
    assert _max_version(v027) == TARGET


# --- the 1.6.0 scanner against both schemas ------------------------------------------------------


@pytest.fixture()
def scanner_env(monkeypatch):
    """The scanner write path pointed at a scratch DSN; network post-write steps stubbed, as test_upsert_pg does."""

    def point_at(dsn: str, schema_check: str = "strict") -> None:
        monkeypatch.setenv("INVENTORY_DATABASE_URL", dsn)
        monkeypatch.setenv("DATABASE_URL", "")
        monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "")
        monkeypatch.setenv("INVENTORY_SCHEMA_CHECK", schema_check)
        monkeypatch.setenv("INVENTORY_AUTO_MIGRATE", "")
        pgh.reset_pg_process_caches()

    monkeypatch.setattr("backend.utils.project_env.load_project_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(sdb, "_vin_owner_table_ready", False)
    monkeypatch.delenv("SCANNER_VIN_OWNER_GUARD_HOURS", raising=False)
    monkeypatch.setattr(
        "backend.enrichment.spec_structured_backfill.apply_structured_spec_backfill_for_car", lambda *a, **k: {})
    monkeypatch.setattr("backend.catalog.linker.link_cars_by_vins", lambda vins: 0)
    yield point_at
    pgh.reset_pg_process_caches()


def _scanner_row(vin: str, dealer_id: str, **extra) -> dict:
    row = {
        "vin": vin, "title": "2023 Nissan Altima SV", "year": 2023, "make": "Nissan", "model": "Altima",
        "trim": "SV", "price": 31000, "mileage": 12000, "image_url": "https://img.example.test/altima.jpg",
        "exterior_color": "Pearl White", "interior_color": "Charcoal", "transmission": "CVT",
        "drivetrain": "FWD", "fuel_type": "Gasoline", "dealer_id": dealer_id, "dealer_name": dealer_id,
        "dealer_url": f"https://{dealer_id}.example.test",
    }
    row.update(extra)
    return row


def test_the_1_6_0_scanner_writes_to_the_upgraded_populated_database(v026, scanner_env):
    assert _migrate(v026, "--apply") == 0  # everything: strict schema mode wants the highest version
    scanner_env(v026)
    existing_vin = "1N400000000000007"  # a legacy row the synthetic load created (dealer-7)
    new_vin = "5N1AT3BB0PC000001"
    assert _one(v026, "SELECT dealer_id, rooftop_tier FROM public.cars WHERE vin = %s", (existing_vin,)) == ("dealer-7", "none")
    stats: dict = {}
    written = upsert_vehicles([
        _scanner_row(existing_vin, "dealer-7", price=12345, _rooftop_evidence_tier="strong"),
        _scanner_row(new_vin, "dealer-new", _rooftop_evidence_tier="weak"),
    ], stats)
    assert written == 2, stats
    assert float(_one(v026, "SELECT price FROM public.cars WHERE vin = %s", (existing_vin,))[0]) == 12345.0
    assert _one(v026, "SELECT rooftop_tier FROM public.cars WHERE vin = %s", (existing_vin,))[0] == "strong"
    assert _one(v026, "SELECT rooftop_tier FROM public.cars WHERE vin = %s", (new_vin,))[0] == "weak"
    assert _one(v026, "SELECT count(*) FROM public.cars")[0] == ROWS + 1


@pytest.mark.parametrize("schema_check", ["strict", "warn"])
def test_the_1_6_0_scanner_cannot_write_until_v028_is_applied(v027, scanner_env, schema_check):
    """Why the order is migrate, then deploy the scanner image: its INSERT names the V028 columns.

    Strict mode (what the cloud scan loop runs) refuses up front, and its message names V028 because that is
    the first migration the database lacks; warn mode (the unset default, which prod runs today) gets as far
    as the INSERT and Postgres rejects the column. Either way nothing is written.
    """
    import psycopg

    from backend.db.schema_version import SchemaNotMigratedError

    assert _columns(v027, "cars").isdisjoint(ROOFTOP_COLUMNS)
    scanner_env(v027, schema_check)
    before = _one(v027, _FINGERPRINT_CARS)
    expected = SchemaNotMigratedError if schema_check == "strict" else psycopg.errors.UndefinedColumn
    with pytest.raises(expected) as info:
        upsert_vehicles([_scanner_row("5N1AT3BB0PC000002", "dealer-new")], {})
    assert ("V028" if schema_check == "strict" else "rooftop_tier") in str(info.value), str(info.value)
    assert _one(v027, _FINGERPRINT_CARS) == before  # the failed write left nothing behind
    assert _columns(v027, "cars").isdisjoint(ROOFTOP_COLUMNS)  # and runtime DDL did not add the columns behind the runner's back
