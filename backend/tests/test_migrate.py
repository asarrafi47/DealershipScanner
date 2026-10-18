"""The migration chain is only trustworthy if order and immutability hold."""

import logging
import os
import re
import threading
import time
import uuid
from pathlib import Path

import pytest

from backend.scripts import migrate


def _write(directory: Path, filename: str, sql: str = "SELECT 1;\n") -> Path:
    path = directory / filename
    path.write_text(sql, encoding="utf-8")
    return path


def test_migrations_apply_in_numeric_not_lexical_order(tmp_path):
    """Sorted as text, V010 lands before V9 and the chain runs in an order no
    database has ever seen."""
    for name in ("V009__nine.sql", "V010__ten.sql", "V002__two.sql", "V001__one.sql"):
        _write(tmp_path, name)
    versions = [m.version for m in migrate.discover_migrations(tmp_path)]
    assert versions == [1, 2, 9, 10]


def test_non_migration_files_are_ignored(tmp_path):
    _write(tmp_path, "V001__one.sql")
    _write(tmp_path, "rollback_notes.sql")
    _write(tmp_path, "V2_missing_double_underscore.sql")
    found = migrate.discover_migrations(tmp_path)
    assert [m.path.name for m in found] == ["V001__one.sql"]


def test_duplicate_version_is_refused(tmp_path):
    _write(tmp_path, "V003__add_index.sql")
    _write(tmp_path, "V3__add_index_again.sql")
    with pytest.raises(SystemExit) as exc:
        migrate.discover_migrations(tmp_path)
    assert "duplicate migration version 3" in str(exc.value)


def test_only_unapplied_versions_are_pending(tmp_path):
    _write(tmp_path, "V001__one.sql")
    _write(tmp_path, "V002__two.sql")
    _write(tmp_path, "V003__three.sql")
    available = migrate.discover_migrations(tmp_path)
    applied = {m.version: m.checksum for m in available[:2]}

    plan = migrate.plan_migrations(available, applied)
    assert [m.version for m in plan.pending] == [3]
    assert not plan.blocked


def test_editing_an_applied_migration_blocks_the_run(tmp_path):
    """
    Every database that already ran the old text has the old schema while
    reporting the same version, so this must stop the run, not be re-applied.
    """
    path = _write(tmp_path, "V001__one.sql", "CREATE TABLE foo (id INT);\n")
    applied = {1: migrate.discover_migrations(tmp_path)[0].checksum}

    path.write_text("CREATE TABLE foo (id BIGINT);\n", encoding="utf-8")
    plan = migrate.plan_migrations(migrate.discover_migrations(tmp_path), applied)

    assert plan.blocked
    assert not plan.pending, "a drifted migration must never be queued for re-run"
    drifted, recorded = plan.drifted[0]
    assert drifted.version == 1
    assert recorded != drifted.checksum


def test_checksum_ignores_line_ending_style(tmp_path):
    """A CRLF checkout is not someone editing an applied migration."""
    lf = _write(tmp_path, "V001__one.sql", "CREATE TABLE foo (id INT);\n")
    unix_sum = migrate.discover_migrations(tmp_path)[0].checksum

    lf.write_bytes(b"CREATE TABLE foo (id INT);\r\n")
    assert migrate.discover_migrations(tmp_path)[0].checksum == unix_sum


def test_new_file_below_the_high_water_mark_blocks_the_run(tmp_path):
    """
    V002 appearing after V005 applied means a rebuilt database runs the chain in
    a different order than production did.
    """
    _write(tmp_path, "V001__one.sql")
    _write(tmp_path, "V005__five.sql")
    available = migrate.discover_migrations(tmp_path)
    applied = {m.version: m.checksum for m in available}

    _write(tmp_path, "V002__late_arrival.sql")
    plan = migrate.plan_migrations(migrate.discover_migrations(tmp_path), applied)

    assert plan.blocked
    assert [m.version for m in plan.out_of_order] == [2]


def test_applied_version_with_no_file_is_reported_but_not_blocking(tmp_path):
    """The database is still the sum of what ran; it just can't be rebuilt."""
    _write(tmp_path, "V001__one.sql")
    available = migrate.discover_migrations(tmp_path)
    applied = {1: available[0].checksum, 2: "sha256:deleted"}

    plan = migrate.plan_migrations(available, applied)
    assert plan.missing == [2]
    assert not plan.blocked


def test_parse_filename_shape():
    assert migrate.parse_filename("V007__add_cars_epa_master_id.sql") == (
        7, "add_cars_epa_master_id",
    )
    assert migrate.parse_filename("V007_add_cars.sql") is None
    assert migrate.parse_filename("baseline.sql") is None


def test_baseline_strips_psql_only_meta_commands():
    """
    pg_dump >= 17.6 wraps plain output in \\restrict/\\unrestrict, and a 17.x
    client emits SET transaction_timeout even against an older server. The chain
    runs through psycopg, not psql, so both are fatal in a baseline.
    """
    from backend.scripts.dump_baseline_schema import _strip_psql_only

    dump = (
        "-- PostgreSQL database dump\n"
        "\\restrict LZyhUHAabc123\n"
        "SET statement_timeout = 0;\n"
        "SET transaction_timeout = 0;\n"
        "CREATE TABLE public.cars (vin text);\n"
        "\\unrestrict LZyhUHAabc123\n"
    )
    body, dropped = _strip_psql_only(dump)

    assert "\\restrict" not in body
    assert "\\unrestrict" not in body
    assert "transaction_timeout" not in body
    assert "SET statement_timeout = 0;" in body, "must not strip valid SETs"
    assert "CREATE TABLE public.cars (vin text);" in body
    assert len(dropped) == 3


# ---------------------------------------------------------------------------
# P4.2: conditional record-only, --record-only, --target, target log,
# lock_timeout, advisory lock. A fake connection stands in for Postgres.
# ---------------------------------------------------------------------------

_SHARED = ("public.shared_a", "public.shared_b")
_TEST_RULES = {2: migrate.RecordOnlyRule(created_by=1, objects=_SHARED)}


class _LockNotAvailable(Exception):
    sqlstate = "55P03"


class _FakeCursor:
    def __init__(self, db: "_FakePg"):
        self.db = db
        self._rows: list[tuple] = []

    def execute(self, sql: str, params=None) -> None:
        db = self.db
        s = " ".join(sql.split())
        db.statements.append((s, params))
        self._rows = []
        if s == "SELECT to_regclass('public.schema_migrations')":
            self._rows = [("schema_migrations" if db.table else None,)]
        elif s.startswith("SELECT version, checksum FROM public.schema_migrations"):
            self._rows = [(v, c) for v, (_n, c) in sorted(db.rows.items())]
        elif s.startswith("SELECT checksum FROM public.schema_migrations WHERE version"):
            hit = db.rows.get(params[0])
            self._rows = [(hit[1],)] if hit else []
        elif "unnest(" in s and "to_regclass" in s:
            self._rows = [(n,) for n in params[0] if n not in db.relations]
        elif s.startswith("SELECT set_config('lock_timeout'"):
            db.lock_settings.append(params[0])
            self._rows = [(params[0],)]
        elif s.startswith("SELECT pg_advisory_xact_lock"):
            db.locks.append(params[0])
            if db.on_lock is not None:
                db.on_lock(db)
            db.locked = True
        elif s.startswith("CREATE TABLE IF NOT EXISTS schema_migrations"):
            db.mutate()
            db.table = True
        elif s == "SET search_path TO DEFAULT":
            pass
        elif s.startswith("INSERT INTO public.schema_migrations"):
            db.mutate()
            version, name, checksum = params
            assert version not in db.rows, f"duplicate key V{version}"
            db.rows[version] = (name, checksum)
        else:  # a migration file's body
            db.mutate()
            db.bodies.append(sql)
            if "FAIL" in sql:
                raise RuntimeError("boom")
            if "LOCKED" in sql:
                raise _LockNotAvailable("canceling statement due to lock timeout")
            for m in re.finditer(r"CREATE TABLE (\w+)", sql):
                db.relations.add(f"public.{m.group(1)}")

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def close(self) -> None:
        pass


class _FakePg:
    """schema_migrations rows, relations to_regclass sees, and transactions."""

    def __init__(self, applied=None, relations=(), table=None):
        self.rows: dict[int, tuple[str, str]] = dict(applied or {})
        self.table = bool(applied) if table is None else table
        self.relations = set(relations)
        self.statements: list[tuple[str, object]] = []
        self.bodies: list[str] = []
        self.locks: list[int] = []
        self.lock_settings: list[str] = []
        self.unlocked_writes: list[str] = []
        self.on_lock = None
        self.locked = False
        self._snapshot = None

    def mutate(self) -> None:
        if not self.locked:
            self.unlocked_writes.append(self.statements[-1][0])
        if self._snapshot is None:
            self._snapshot = (self.table, dict(self.rows), set(self.relations))

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self._snapshot = None
        self.locked = False

    def rollback(self) -> None:
        if self._snapshot is not None:
            self.table, self.rows, self.relations = self._snapshot
        self._snapshot = None
        self.locked = False

    def close(self) -> None:
        pass

    def writes(self) -> list[str]:
        return [s for s, _ in self.statements
                if s.startswith(("INSERT", "CREATE", "SELECT pg_advisory")) or s in self.bodies]


def _chain(directory: Path, count: int, overrides: dict[int, str] | None = None) -> list:
    bodies = {
        1: "CREATE TABLE shared_a (id int); CREATE TABLE shared_b (id int);\n",
        2: "CREATE TABLE shared_a (id int); -- re-creates V001's table, fails if run\n",
    }
    bodies.update(overrides or {})
    for v in range(1, count + 1):
        _write(directory, f"V{v:03d}__step{v}.sql", bodies.get(v, f"SELECT {v};\n"))
    return migrate.discover_migrations(directory)


def _applied(available, versions) -> dict[int, tuple[str, str]]:
    return {m.version: (m.name, m.checksum) for m in available if m.version in versions}


@pytest.fixture
def run(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(migrate, "RECORD_ONLY", dict(_TEST_RULES))
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql:///scratch?host=/tmp")
    caplog.set_level(logging.INFO, logger="migrate")

    def _run(db: _FakePg, *argv: str) -> int:
        monkeypatch.setattr(migrate, "_connect", lambda: db)
        caplog.clear()
        return migrate.main(["--dir", str(tmp_path), *argv])

    return _run


def _ran(db: _FakePg, mig) -> bool:
    return mig.path.read_text(encoding="utf-8") in db.bodies


# --- the three record-only cases --------------------------------------------


def test_fresh_build_records_the_duplicate_file(tmp_path, run, caplog):
    chain = _chain(tmp_path, 3)
    db = _FakePg()
    assert run(db, "--apply") == 0
    assert sorted(db.rows) == [1, 2, 3]
    assert _ran(db, chain[0]) and _ran(db, chain[2])
    assert not _ran(db, chain[1]), "V002 re-creates V001's tables: record it, never run it"
    assert "fresh build" in caplog.text


def test_all_shared_objects_present_records(tmp_path, run, caplog):
    chain = _chain(tmp_path, 3)
    db = _FakePg(applied=_applied(chain, {1}), relations=_SHARED)
    assert run(db, "--apply") == 0
    assert sorted(db.rows) == [1, 2, 3]
    assert not _ran(db, chain[1]) and _ran(db, chain[2])
    assert "all 2 objects it shares with V001 already exist" in caplog.text


@pytest.mark.parametrize("mode", [[], ["--apply"]], ids=["dry-run", "apply"])
def test_missing_shared_objects_refuses_before_any_write(tmp_path, run, caplog, mode):
    """V001 baselined, V002's tables never created: recording V002 would mark
    this database current without them, and running it would fail."""
    chain = _chain(tmp_path, 3)
    db = _FakePg(applied=_applied(chain, {1}), relations={"public.shared_a"})
    assert run(db, *mode) == 1
    assert sorted(db.rows) == [1]
    assert db.writes() == []
    refusal = [r.getMessage() for r in caplog.records if r.getMessage().startswith("REFUSE")]
    assert refusal and "V002__step2.sql" in refusal[0] and "public.shared_b" in refusal[0]
    assert "public.shared_a" not in refusal[0]


def test_baseline_of_v001_then_condition_decides(tmp_path, run):
    """--baseline 1 records V001 without running it, so V002 is not a fresh
    build: it is recorded only because the objects already exist."""
    chain = _chain(tmp_path, 2)
    db = _FakePg(table=False, relations=_SHARED)
    assert run(db, "--baseline", "1", "--apply") == 0
    assert sorted(db.rows) == [1, 2] and db.bodies == []

    db = _FakePg(table=False)
    assert run(db, "--baseline", "1", "--apply") == 1
    assert db.rows == {}


def test_record_under_the_lock_rechecks_the_objects(tmp_path, run, caplog):
    """The objects vanish between the plan and the write: refuse, roll back."""
    chain = _chain(tmp_path, 3)
    db = _FakePg(applied=_applied(chain, {1}), relations=_SHARED)
    db.on_lock = lambda d: d.relations.discard("public.shared_b")
    assert run(db, "--apply") == 1
    assert sorted(db.rows) == [1]
    assert "public.shared_b" in caplog.text


# --- --record-only ------------------------------------------------------------


def test_record_only_is_explicit(tmp_path, run, caplog):
    chain = _chain(tmp_path, 4)
    db = _FakePg(applied=_applied(chain, {1}))  # V002's objects are missing
    assert run(db, "--record-only", "2,3", "--apply") == 0
    assert sorted(db.rows) == [1, 2, 3, 4]
    assert not _ran(db, chain[1]) and not _ran(db, chain[2])
    assert _ran(db, chain[3]), "files not named still run"
    assert "--record-only" in caplog.text


def test_record_only_validation(tmp_path, run, caplog):
    chain = _chain(tmp_path, 3)
    db = _FakePg(applied=_applied(chain, {1, 2, 3}))
    assert run(db, "--record-only", "9", "--apply") == 2
    assert "names no migration file: V009" in caplog.text
    assert run(db, "--record-only", "3", "--target", "2") == 2
    assert run(db, "--record-only", "2", "--apply") == 0, "already applied: nothing to do"
    assert "database is up to date" in caplog.text
    with pytest.raises(SystemExit):
        run(db, "--record-only", "two")


# --- --target -------------------------------------------------------------------


def test_target_stops_at_n(tmp_path, run, caplog):
    chain = _chain(tmp_path, 5)
    db = _FakePg(applied=_applied(chain, {1, 2}))
    assert run(db, "--apply", "--target", "3") == 0
    assert sorted(db.rows) == [1, 2, 3]
    assert not _ran(db, chain[3]) and not _ran(db, chain[4])
    assert "2 pending file(s) above --target 3 stay pending: V004, V005" in caplog.text


def test_dry_run_with_target_lists_only_files_up_to_n(tmp_path, run, caplog):
    chain = _chain(tmp_path, 5)
    db = _FakePg(applied=_applied(chain, {1, 2, 3}))
    assert run(db, "--dry-run", "--target", "4") == 0
    would = [r.getMessage() for r in caplog.records if r.getMessage().startswith("would")]
    assert len(would) == 1 and "V004__step4.sql" in would[0]
    assert db.writes() == []


def test_target_with_only_held_files_is_not_up_to_date(tmp_path, run, caplog):
    chain = _chain(tmp_path, 4)
    db = _FakePg(applied=_applied(chain, {1, 2, 3}))
    assert run(db, "--target", "3") == 0
    assert "database is up to date" not in caplog.text
    assert "nothing pending at or below --target 3" in caplog.text


def test_target_above_the_chain_is_refused(tmp_path, run, caplog):
    _chain(tmp_path, 3)
    assert run(_FakePg(), "--target", "7") == 2
    assert "above the highest migration file (V003)" in caplog.text
    with pytest.raises(SystemExit):
        run(_FakePg(), "--target", "0")


# --- dry run --------------------------------------------------------------------


def test_dry_run_prints_would_record_and_writes_nothing(tmp_path, run, caplog):
    _chain(tmp_path, 3)
    db = _FakePg()
    assert run(db) == 0
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("would")]
    assert [line.split()[1] for line in lines] == ["apply", "record", "apply"]
    assert "V002__step2.sql" in lines[1] and "fresh build" in lines[1]
    assert db.writes() == [] and db.lock_settings == []
    assert "would apply 2 and record 1 migration(s) through V003" in caplog.text


# --- advisory lock, lock_timeout, concurrent runners ---------------------------


def test_advisory_lock_is_taken_in_every_write_transaction(tmp_path, run):
    _chain(tmp_path, 3)
    db = _FakePg()
    assert run(db, "--apply") == 0
    assert db.unlocked_writes == [], "every write runs after pg_advisory_xact_lock in its transaction"
    # create schema_migrations + V001 + V002 (record) + V003
    assert db.locks == [migrate.MIGRATE_LOCK_KEY] * 4
    assert 0 < migrate.MIGRATE_LOCK_KEY < 2**63


def test_lock_timeout_is_set_before_each_file(tmp_path, run):
    _chain(tmp_path, 3, overrides={3: "SELECT 3; -- three\n"})
    db = _FakePg(applied=_applied(migrate.discover_migrations(tmp_path), {1, 2}))
    assert run(db, "--apply") == 0
    stmts = [s for s, _ in db.statements]
    body = stmts.index("SELECT 3; -- three")
    assert stmts[body - 1] == "SELECT set_config('lock_timeout', %s, true)"
    assert db.statements[body - 1][1] == ("5s",)
    # the advisory lock waits --lock-wait; the file gets --lock-timeout
    assert db.lock_settings == [migrate.DEFAULT_LOCK_WAIT, "5s"]

    db = _FakePg(applied=_applied(migrate.discover_migrations(tmp_path), {1, 2}))
    assert run(db, "--apply", "--lock-timeout", "2s", "--lock-wait", "30s") == 0
    assert db.lock_settings == ["30s", "2s"]
    with pytest.raises(SystemExit):
        run(db, "--apply", "--lock-timeout", "5s; DROP TABLE cars")


def test_second_runner_waits_then_skips_what_the_first_applied(tmp_path, run, caplog):
    """While this runner waited for the lock, another one applied V002: the
    re-read under the lock sees it, so V002 never runs twice."""
    chain = _chain(tmp_path, 3)
    db = _FakePg(applied=_applied(chain, {1}), relations=_SHARED)

    def other_runner_applies_v002(d):
        if 2 not in d.rows:
            d.rows[2] = (chain[1].name, chain[1].checksum)

    db.on_lock = other_runner_applies_v002
    assert run(db, "--apply") == 0
    assert sorted(db.rows) == [1, 2, 3]
    assert "recorded by a concurrent migrate meanwhile; skipped" in caplog.text
    assert "applied 1, recorded 0, skipped 1" in caplog.text


def test_concurrent_runner_with_another_checksum_fails(tmp_path, run, caplog):
    chain = _chain(tmp_path, 3)
    db = _FakePg(applied=_applied(chain, {1, 2}))
    db.on_lock = lambda d: d.rows.setdefault(3, ("step3", "sha256:other"))
    assert run(db, "--apply") == 1
    assert "two code versions are migrating at once" in caplog.text


def test_lock_wait_expiry_exits_1(tmp_path, run, caplog):
    chain = _chain(tmp_path, 3)
    db = _FakePg(applied=_applied(chain, {1, 2}))

    def held_elsewhere(d):
        raise _LockNotAvailable("canceling statement due to lock timeout")

    db.on_lock = held_elsewhere
    assert run(db, "--apply", "--lock-wait", "1s") == 1
    assert sorted(db.rows) == [1, 2]
    assert "another migrate held the migration lock for more than 1s" in caplog.text


def test_file_lock_timeout_rolls_back_with_a_hint(tmp_path, run, caplog):
    chain = _chain(tmp_path, 3, overrides={3: "ALTER TABLE cars ADD COLUMN x int; -- LOCKED\n"})
    db = _FakePg(applied=_applied(chain, {1, 2}))
    assert run(db, "--apply") == 1
    assert sorted(db.rows) == [1, 2]
    assert "lock_timeout (5s) expired" in caplog.text


def test_failed_file_rolls_back_and_stops(tmp_path, run):
    chain = _chain(tmp_path, 4, overrides={3: "SELECT 'FAIL';\n"})
    db = _FakePg(applied=_applied(chain, {1, 2}))
    assert run(db, "--apply") == 1
    assert sorted(db.rows) == [1, 2]
    assert not _ran(db, chain[3])


# --- target log -----------------------------------------------------------------


def test_target_is_logged_without_credentials(tmp_path, run, caplog, monkeypatch):
    chain = _chain(tmp_path, 2)
    monkeypatch.setenv(
        "INVENTORY_DATABASE_URL",
        "postgresql://scanner:s3cr3t-pw@db.example.net:6543/railway?sslmode=require",
    )
    assert run(_FakePg(applied=_applied(chain, {1}), relations=_SHARED), "--apply") == 0
    first = caplog.records[0].getMessage()
    assert first == "target: host=db.example.net port=6543 db=railway (apply)"
    assert "s3cr3t" not in caplog.text and "scanner" not in caplog.text
    assert "sslmode" not in caplog.text


def test_describe_target_shapes():
    assert migrate.describe_target("postgresql:///cars?host=/tmp&port=5432") == (
        "host=unix-socket:/tmp port=5432 db=cars"
    )
    assert migrate.describe_target(None) == "(no inventory DSN)"
    assert migrate.describe_target("postgresql://u:p@h/") == "host=h port=5432 db=(default)"
    assert migrate.describe_target("not a dsn ===") == "(unparseable DSN)"


# --- classify (pure) ------------------------------------------------------------


def test_classify_run_record_refuse():
    rules = _TEST_RULES
    c = migrate.classify
    assert c(3, rules=rules).action == migrate.RUN
    assert c(3, record_only=frozenset({3}), rules=rules).action == migrate.RECORD
    assert c(3, baseline_through=5, rules=rules).reason == "--baseline 5"
    fresh = c(2, runs_earlier=frozenset({1}), rules=rules)
    assert fresh.action == migrate.RECORD and fresh.verify == _SHARED
    present = c(2, missing_objects=[], rules=rules)
    assert present.action == migrate.RECORD and present.verify == _SHARED
    refused = c(2, missing_objects=["public.shared_b"], rules=rules)
    assert refused.action == migrate.REFUSE and "1 of the 2" in refused.reason
    explicit = c(2, record_only=frozenset({2}), missing_objects=["public.shared_b"], rules=rules)
    assert explicit.action == migrate.RECORD and explicit.verify == ()
    with pytest.raises(ValueError):
        c(2, rules=rules)  # the objects were never looked up


def test_select_for_run_splits_at_target(tmp_path):
    chain = _chain(tmp_path, 4)
    assert migrate.select_for_run(chain, None) == (chain, [])
    assert migrate.select_for_run(chain, 2) == (chain[:2], chain[2:])


# --- the real V019 rule ---------------------------------------------------------

_REPO_MIGRATIONS = Path(__file__).resolve().parents[2] / "migrations"


def _named_relations(sql: str) -> set[str]:
    sql = re.sub(r"--[^\n]*", "", sql)
    names = re.findall(r"CREATE (?:TABLE|SEQUENCE) (?:IF NOT EXISTS )?public\.(\w+)", sql)
    names += re.findall(r"ADD CONSTRAINT (\w+) (?:PRIMARY KEY|UNIQUE)", sql)
    return set(names)


def test_v019_rule_names_exactly_the_objects_it_shares_with_v001():
    v019 = _named_relations((_REPO_MIGRATIONS / "V019__missing_reference_tables.sql").read_text(encoding="utf-8"))
    v001 = _named_relations((_REPO_MIGRATIONS / "V001__baseline.sql").read_text(encoding="utf-8"))
    rule = migrate.RECORD_ONLY[19]
    assert rule.created_by == 1
    assert {f"public.{n}" for n in v019 & v001} == set(rule.objects)
    assert len(rule.objects) == len(set(rule.objects)) == 48
    assert v019 - v001 == {
        "car_move_log", "car_move_log_id_seq", "car_move_log_pkey",
        "cars_trim_quarantine", "package_values_msrp_quarantine",
    }


def test_real_chain_dry_run_on_an_empty_database_records_v019(run, caplog, monkeypatch):
    """The repo's own chain, fresh: V019 is the only file recorded."""
    monkeypatch.setattr(migrate, "RECORD_ONLY", {19: migrate.RecordOnlyRule(1, migrate.V019_SHARED_WITH_V001)})
    db = _FakePg()
    monkeypatch.setattr(migrate, "_connect", lambda: db)
    caplog.clear()
    assert migrate.main(["--dir", str(_REPO_MIGRATIONS)]) == 0
    recorded = [r.getMessage() for r in caplog.records if r.getMessage().startswith("would record")]
    assert len(recorded) == 1 and "V019__missing_reference_tables.sql" in recorded[0]


# ---------------------------------------------------------------------------
# Live Postgres (opt-in): TEST_PG_ADMIN_DSN names a LOCAL server where the test
# may create and drop pytest_* databases (D-TC9). Never cars or cars_cleanroom,
# never a remote host, never the mini tunnel (port 15432).
# ---------------------------------------------------------------------------

_ADMIN_DSN_ENV = "TEST_PG_ADMIN_DSN"
_LOCAL_HOSTS = {"", "localhost", "127.0.0.1", "::1"}


def _guarded_admin_dsn() -> str:
    dsn = (os.environ.get(_ADMIN_DSN_ENV) or "").strip()
    if not dsn:
        pytest.skip(f"{_ADMIN_DSN_ENV} not set (a local server where pytest_* DBs may be created)")
    psycopg = pytest.importorskip("psycopg")
    params = psycopg.conninfo.conninfo_to_dict(dsn)
    host = str(params.get("host") or "")
    if not (host in _LOCAL_HOSTS or host.startswith("/")):
        pytest.fail(f"{_ADMIN_DSN_ENV} must name a local server, not {host!r}")
    if str(params.get("port") or "5432") == "15432":
        pytest.fail(f"{_ADMIN_DSN_ENV} points at the mini tunnel (port 15432)")
    if params.get("dbname") in ("cars", "cars_cleanroom"):
        pytest.fail(f"{_ADMIN_DSN_ENV} must not name an inventory database")
    return dsn


@pytest.fixture
def scratch_pg(monkeypatch):
    from urllib.parse import urlencode

    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    admin_dsn = _guarded_admin_dsn()
    name = f"pytest_migrate_{uuid.uuid4().hex[:10]}"
    assert name.startswith("pytest_") and name not in ("cars", "cars_cleanroom")
    admin = psycopg.connect(admin_dsn, autocommit=True)
    try:
        admin.execute(f'CREATE DATABASE "{name}"')
        params = conninfo_to_dict(admin_dsn)
        params["dbname"] = name
        # URI form (migrate only accepts postgresql:// DSNs), every keyword in the query.
        dsn = "postgresql://?" + urlencode({k: str(v) for k, v in params.items()})
        monkeypatch.setenv("INVENTORY_DATABASE_URL", dsn)
        yield dsn
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()



def _versions(dsn: str) -> list[int]:
    import psycopg

    with psycopg.connect(dsn) as c:
        if c.execute("SELECT to_regclass('public.schema_migrations')").fetchone()[0] is None:
            return []
        return [r[0] for r in c.execute("SELECT version FROM schema_migrations ORDER BY 1")]


@pytest.mark.integration
def test_live_second_migrate_waits_for_the_lock_then_skips(scratch_pg, tmp_path):
    import psycopg

    _write(tmp_path, "V001__one.sql", "CREATE TABLE t1 (id int);\n")
    _write(tmp_path, "V002__two.sql", "CREATE TABLE t2 (id int);\n")
    holder = psycopg.connect(scratch_pg)  # another migrate, mid-transaction
    holder.execute("SELECT pg_advisory_xact_lock(%s)", (migrate.MIGRATE_LOCK_KEY,))
    result: list[int] = []
    worker = threading.Thread(
        target=lambda: result.append(migrate.main(["--apply", "--dir", str(tmp_path)]))
    )
    worker.start()
    time.sleep(1.5)
    assert worker.is_alive(), "the second runner must wait while the lock is held"
    assert _versions(scratch_pg) == []
    holder.rollback()  # the first runner's transaction ends
    worker.join(timeout=60)
    assert result == [0] and _versions(scratch_pg) == [1, 2]

    # With a short --lock-wait it exits 1 instead, writing nothing.
    _write(tmp_path, "V003__three.sql", "CREATE TABLE t3 (id int);\n")
    holder.execute("SELECT pg_advisory_xact_lock(%s)", (migrate.MIGRATE_LOCK_KEY,))
    started = time.monotonic()
    assert migrate.main(["--apply", "--dir", str(tmp_path), "--lock-wait", "1s"]) == 1
    assert time.monotonic() - started < 30
    holder.rollback()
    holder.close()
    assert _versions(scratch_pg) == [1, 2]


@pytest.mark.integration
def test_live_lock_timeout_aborts_a_blocked_file(scratch_pg, tmp_path):
    import psycopg

    _write(tmp_path, "V001__one.sql", "CREATE TABLE t1 (id int);\n")
    assert migrate.main(["--apply", "--dir", str(tmp_path)]) == 0
    _write(tmp_path, "V002__alter.sql", "ALTER TABLE t1 ADD COLUMN c int;\n")
    holder = psycopg.connect(scratch_pg)
    holder.execute("LOCK TABLE t1 IN ACCESS SHARE MODE")  # a reader mid-transaction
    started = time.monotonic()
    assert migrate.main(["--apply", "--dir", str(tmp_path), "--lock-timeout", "1s"]) == 1
    assert time.monotonic() - started < 30
    assert _versions(scratch_pg) == [1]
    holder.rollback()
    holder.close()
    assert migrate.main(["--apply", "--dir", str(tmp_path)]) == 0
    assert _versions(scratch_pg) == [1, 2]


@pytest.mark.integration
def test_live_record_only_rule_fresh_and_refused(scratch_pg, tmp_path, monkeypatch):
    import psycopg

    monkeypatch.setattr(migrate, "RECORD_ONLY", {2: migrate.RecordOnlyRule(1, ("public.t1", "public.t1_pkey"))})
    _write(tmp_path, "V001__one.sql", "CREATE TABLE t1 (id int PRIMARY KEY);\n")
    _write(tmp_path, "V002__dup.sql", "CREATE TABLE t1 (id int PRIMARY KEY);\n")
    _write(tmp_path, "V003__three.sql", "CREATE TABLE t3 (id int);\n")
    assert migrate.main(["--apply", "--dir", str(tmp_path)]) == 0
    assert _versions(scratch_pg) == [1, 2, 3]

    # Old lineage: V001 recorded, V002 not, and t1 is gone -> refuse, write nothing.
    with psycopg.connect(scratch_pg, autocommit=True) as c:
        c.execute("DELETE FROM schema_migrations WHERE version >= 2")
        c.execute("DROP TABLE t1")
    assert migrate.main(["--apply", "--dir", str(tmp_path)]) == 1
    assert _versions(scratch_pg) == [1]
