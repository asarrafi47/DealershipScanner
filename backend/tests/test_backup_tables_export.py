"""P13B.4a: backup-table census, dump/verify and the dry-run DROP list (never a drop)."""
from __future__ import annotations

import json
import subprocess
import uuid

import pytest

from backend.scripts import backup_tables_export as bte


class FakeConn:
    """Answers the handful of SELECTs the exporter issues from a {table: rows} map."""

    def __init__(self, tables: dict[str, int], db: str = "cars"):
        self.tables, self.db, self.sql = tables, db, []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        s = " ".join(sql.split())
        if s.startswith("SELECT current_database"):
            return _R([(self.db,)])
        if "FROM pg_class" in s:
            return _R([(t,) for t in sorted(self.tables)])
        if s.startswith("SELECT to_regclass"):
            name = params[0].split(".", 1)[1].strip('"')
            return _R([(f"public.{name}" if name in self.tables else None,)])
        if s.startswith("SELECT count(*)"):
            name = s.split("public.", 1)[1].strip('"')
            return _R([(self.tables[name],)])
        if s.startswith("SELECT pg_total_relation_size"):
            return _R([(8192,)])
        raise AssertionError(sql)

    def close(self):
        pass


class _R:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0]


def test_classify_lists_and_protection():
    assert bte.classify("cars_backup_capacity_20260718") == "core"
    assert bte.classify("epa_master_dump_id_map") == "core"
    assert bte.classify("make_canon_log") == "log"
    assert bte.classify("make_canon_log", include_logs=False) is None
    assert bte.classify("cars_retire_backup_20261001") is None
    assert bte.classify("cars_retire_backup_20261001", include_plan_snapshots=True) == "plan_snapshot"
    assert bte.classify("cars_owner_backup_20260930", include_plan_snapshots=True) == "plan_snapshot"
    # live tables never qualify, with or without the flag
    for name in ("cars", "cars_trim_quarantine", "package_values_msrp_quarantine", "epa_master", "cars_backup_x; drop"):
        assert bte.classify(name, include_plan_snapshots=True) is None


def test_every_core_name_matches_ledger():
    ledger = (bte._REPO_ROOT / "docs/db_layer/SCHEMA_LEDGER_2026_10.md").read_text()
    for name in bte.CORE_TABLES + bte.LOG_TABLES:
        assert name in ledger, name


def test_census_reports_candidates_unlisted_and_skips_protected():
    conn = FakeConn({"cars": 5, "cars_backup_vinrepair_20260713": 53, "cars_trim_quarantine": 2,
                     "some_old_bak": 1, "make_canon_log": 59, "cars_retire_backup_t1": 7})
    info = bte.census(conn)
    assert [c["table"] for c in info["candidates"]] == ["cars_backup_vinrepair_20260713", "make_canon_log"]
    assert info["unlisted"] == ["cars_retire_backup_t1", "some_old_bak"]
    assert "cars_trim_quarantine" not in info["unlisted"]
    assert "cars_backup_capacity_20260718" in info["absent"]
    assert bte.census(conn, include_plan_snapshots=True)["candidates"][-1]["table"] == "make_canon_log"
    assert all(s.lstrip().upper().startswith("SELECT") for s in conn.sql)


def test_dsn_with_dbname_forms():
    assert bte.dsn_with_dbname("postgresql:///cars?host=/tmp", "cars_draft_a") == "postgresql:///cars_draft_a?host=/tmp"
    assert bte.dsn_with_dbname("postgresql://u:p@h:5432/cars", "cars_draft_a") == "postgresql://u:p@h:5432/cars_draft_a"
    assert bte.dsn_with_dbname("host=/tmp dbname=cars user=x", "cars_draft_a") == "host=/tmp dbname=cars_draft_a user=x"


@pytest.mark.parametrize("name", ["cars", "cars_cleanroom", "railway", "cars_cr_", "cars_draft_X", "x; drop"])
def test_scratch_guard_refuses(name):
    with pytest.raises(ValueError):
        bte.assert_scratch_db(name)


def test_scratch_guard_accepts_and_refuses_source():
    assert bte.assert_scratch_db("cars_draft_bk") == "cars_draft_bk"
    assert bte.assert_scratch_db("cars_cr_x") == "cars_cr_x"
    with pytest.raises(ValueError):
        bte.assert_scratch_db("cars_rehearsal", source_db="cars_rehearsal")


def _fake_runner(calls, *, fail=False):
    def run(cmd, **kw):
        calls.append(cmd)
        if cmd[0] == "pg_dump" and "-f" in cmd and not fail:
            open(cmd[cmd.index("-f") + 1], "wb").write(b"PGDMP-fake")
        if cmd[0] == "pg_dump" and cmd[1] == "--version":
            return subprocess.CompletedProcess(cmd, 0, "pg_dump (PostgreSQL) 18.6\n", "")
        return subprocess.CompletedProcess(cmd, 1 if fail else 0, "", "boom secret-dsn")
    return run


def test_make_dump_writes_manifest_and_refuses_overwrite(tmp_path):
    conn = FakeConn({"cars": 1, "cars_backup_vinrepair_20260713": 53, "make_canon_log": 59})
    calls: list = []
    out = bte.make_dump(conn, "postgresql:///cars", directory=tmp_path, date="20261027", runner=_fake_runner(calls))
    assert out.name == "backup_tables_cars_20261027.dump"
    cmd = calls[0]
    assert cmd[:3] == ["pg_dump", "-Fc", "--no-owner"]
    assert [cmd[i + 1] for i, a in enumerate(cmd) if a == "-t"] == ['public."cars_backup_vinrepair_20260713"', 'public."make_canon_log"']
    m = json.loads(bte.manifest_path(out).read_text())
    assert m["tables"]["make_canon_log"] == {"category": "log", "rows": 59, "verified": False}
    assert m["sha256"] == bte.sha256_file(out) and m["pg_dump"].startswith("pg_dump")
    with pytest.raises(SystemExit, match="refusing to overwrite"):
        bte.make_dump(conn, "postgresql:///cars", directory=tmp_path, date="20261027", runner=_fake_runner([]))


def test_make_dump_failure_leaves_no_file_and_hides_dsn(tmp_path):
    conn = FakeConn({"make_canon_log": 1})
    with pytest.raises(SystemExit) as ei:
        bte.make_dump(conn, "secret-dsn", directory=tmp_path, date="d", runner=_fake_runner([], fail=True))
    assert "secret-dsn" not in str(ei.value)
    assert not list(tmp_path.iterdir())


def test_make_dump_nothing_to_dump(tmp_path):
    with pytest.raises(SystemExit, match="nothing to dump"):
        bte.make_dump(FakeConn({"cars": 1}), "x", directory=tmp_path, runner=_fake_runner([]))


def _dumped(tmp_path, tables):
    conn = FakeConn(tables)
    return conn, bte.make_dump(conn, "postgresql:///cars", directory=tmp_path, date="d", runner=_fake_runner([]))


def test_verify_marks_only_matching_counts(tmp_path):
    src, dump = _dumped(tmp_path, {"make_canon_log": 59, "model_canon_log": 0})
    scratch = FakeConn({"make_canon_log": 59, "model_canon_log": 3}, db="cars_draft_bk")
    calls: list = []
    res = bte.verify_dump(dump, "postgresql:///cars", "cars_draft_bk", source_db="cars",
                          runner=_fake_runner(calls), connect=lambda dsn: scratch)
    assert calls[0][0] == "pg_restore" and calls[0][-2] == "postgresql:///cars_draft_bk"
    assert res["make_canon_log"]["ok"] and not res["model_canon_log"]["ok"]
    m = bte.load_manifest(dump)
    assert m["tables"]["make_canon_log"]["verified"] and not m["tables"]["model_canon_log"]["verified"]


def test_verify_refuses_non_scratch_and_tampered_dump(tmp_path):
    _, dump = _dumped(tmp_path, {"make_canon_log": 1})
    with pytest.raises(ValueError):
        bte.verify_dump(dump, "postgresql:///cars", "cars", runner=_fake_runner([]))
    dump.write_bytes(b"tampered")
    with pytest.raises(SystemExit, match="sha256"):
        bte.verify_dump(dump, "postgresql:///cars", "cars_draft_bk", runner=_fake_runner([]))


def test_plan_lists_only_verified_unchanged_tables_and_never_executes(tmp_path):
    src, dump = _dumped(tmp_path, {"make_canon_log": 59, "model_canon_log": 0, "image_summary_restore_log": 8})
    m = bte.load_manifest(dump)
    m["tables"]["make_canon_log"]["verified"] = True
    m["tables"]["image_summary_restore_log"]["verified"] = True
    live = FakeConn({"make_canon_log": 59, "image_summary_restore_log": 9, "model_canon_log": 0})
    plan = bte.plan_drops(live, m)
    assert plan["drop"] == ["make_canon_log"]
    assert dict(plan["blocked"])["model_canon_log"] == "dump not verified"
    assert "rows changed" in dict(plan["blocked"])["image_summary_restore_log"]
    text = bte.format_plan(plan)
    assert 'DROP TABLE IF EXISTS public."make_canon_log";' in text and "DRY RUN" in text
    assert all(s.lstrip().upper().startswith(("SELECT", "SET")) for s in live.sql)
    gone = bte.plan_drops(FakeConn({}), m)
    assert gone["drop"] == [] and len(gone["absent"]) == 3


def test_plan_refuses_manifest_of_another_database(tmp_path):
    _, dump = _dumped(tmp_path, {"make_canon_log": 1})
    with pytest.raises(SystemExit, match="manifest is for database"):
        bte.plan_drops(FakeConn({}, db="railway"), bte.load_manifest(dump))


def test_module_has_no_drop_execution():
    src = open(bte.__file__).read()
    assert "DROP DATABASE" not in src and "DROP SCHEMA" not in src
    # the only DROP TABLE text is the printed plan line and prose, never an execute() call
    for ln in src.splitlines():
        if "DROP TABLE" in ln:
            assert "execute" not in ln


@pytest.mark.pg
def test_roundtrip_on_real_postgres():
    from backend.tests import pg_harness as h

    admin = h.admin_dsn_or_skip()
    import shutil

    if not shutil.which("pg_dump") or not shutil.which("pg_restore"):
        pytest.skip("pg_dump/pg_restore not on PATH")
    import tempfile
    from pathlib import Path

    scratch = "cars_draft_bk_" + uuid.uuid4().hex[:8]
    with h.scratch_db(admin) as src_dsn:
        src = bte._connect(src_dsn, readonly=False)
        src.execute("CREATE TABLE cars_backup_vinrepair_20260713 AS SELECT g AS id FROM generate_series(1,53) g")
        src.execute("CREATE TABLE make_canon_log (id serial PRIMARY KEY, v text)")
        src.execute("INSERT INTO make_canon_log (v) SELECT 'x' FROM generate_series(1,5)")
        src.execute("CREATE TABLE cars_trim_quarantine (id int)")
        with tempfile.TemporaryDirectory() as td:
            dump = bte.make_dump(src, src_dsn, directory=Path(td), date="t")
            adm = h.connect_admin(admin)
            adm.execute(f'CREATE DATABASE "{scratch}"')
            try:
                res = bte.verify_dump(dump, src_dsn, scratch, source_db=bte.current_db(src))
                assert {k: v["ok"] for k, v in res.items()} == {"cars_backup_vinrepair_20260713": True, "make_canon_log": True}
                plan = bte.plan_drops(src, bte.load_manifest(dump))
                assert plan["drop"] == ["cars_backup_vinrepair_20260713", "make_canon_log"]
                assert bte.table_stats(src, "make_canon_log")["rows"] == 5  # nothing dropped
                assert bte.table_stats(src, "cars_trim_quarantine") is not None
            finally:
                adm.execute(f'DROP DATABASE IF EXISTS "{scratch}"')
                adm.close()
        src.close()
