"""
backend/scripts/schema_drift_report.py (P4.1).

(a) pure: the snapshot diff, the migration-ledger diff and the classifier over
    dict fixtures; the static migration-object parse.
(b) guards and read-only proof through fake connections: the target opens with
    ``read_only=True``, the server must say so, every statement on it is a
    SELECT / SHOW / ``SET statement_timeout``, and the scratch DB is created and
    dropped only under a ``pytest_chain_*`` name on a local server (D-TC9).
(c) opt-in integration under ``SCHEMA_PARITY_PG_DSN`` (a LOCAL server where
    ``pytest_*`` databases may be created): build a ``pytest_drift_tgt_*`` target
    from the chain, perturb it, and require the report to find each perturbation.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from backend.scripts import schema_drift_report as sdr

REPO_ROOT = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


def _snap(**over: Any) -> dict[str, Any]:
    """A small chain-like schema: dealers + cars, one trigger, one function."""
    s = sdr.empty_snapshot()
    s["tables"] = {"dealers", "cars"}
    s["columns"] = {
        ("dealers", "id"): ("text", "NO", ""),
        ("cars", "id"): ("integer", "NO", "nextval('cars_id_seq'::regclass)"),
        ("cars", "vin"): ("text", "YES", ""),
        ("cars", "price"): ("integer", "YES", ""),
    }
    s["indexes"] = {
        "dealers_pkey": ("dealers", "public.dealers USING btree (id)", True),
        "cars_pkey": ("cars", "public.cars USING btree (id)", True),
        "idx_cars_vin": ("cars", "public.cars USING btree (vin)", False),
    }
    s["constraints"] = {
        ("dealers", "dealers_pkey"): ("p", "PRIMARY KEY (id)"),
        ("cars", "cars_pkey"): ("p", "PRIMARY KEY (id)"),
        ("cars", "cars_price_check"): ("c", "CHECK ((price >= 0))"),
    }
    s["sequences"] = {"cars_id_seq": ("cars.id", "integer")}
    s["functions"] = {"set_updated_at()": "CREATE FUNCTION set_updated_at() ..."}
    s["triggers"] = {("cars", "trg_cars"): "CREATE TRIGGER trg_cars ..."}
    s["extensions"] = {"plpgsql", "vector"}
    for k, v in over.items():
        s[k] = v
    return s


def _add_table(s: dict[str, Any], name: str) -> dict[str, Any]:
    s["tables"] = set(s["tables"]) | {name}
    s["columns"] = {**s["columns"], (name, "id"): ("integer", "NO", f"nextval('{name}_id_seq'::regclass)"),
                    (name, "note"): ("text", "YES", "")}
    s["indexes"] = {**s["indexes"], f"{name}_pkey": (name, f"public.{name} USING btree (id)", True),
                    f"idx_{name}_note": (name, f"public.{name} USING btree (note)", False)}
    s["constraints"] = {**s["constraints"], (name, f"{name}_pkey"): ("p", "PRIMARY KEY (id)")}
    s["sequences"] = {**s["sequences"], f"{name}_id_seq": (f"{name}.id", "integer")}
    s["triggers"] = {**s["triggers"], (name, f"trg_{name}"): f"CREATE TRIGGER trg_{name} ..."}
    return s


def _kinds(items):
    return sorted((d.kind, d.category, d.name) for d in items)


# ---------------------------------------------------------------------------
# (a) pure diff
# ---------------------------------------------------------------------------


def test_identical_snapshots_have_no_drift():
    assert sdr.diff_snapshots(_snap(), _snap()) == []


def test_live_only_table_is_one_item_with_its_objects_folded():
    items = sdr.diff_snapshots(_snap(), _add_table(_snap(), "make_canon_log"))
    assert _kinds(items) == [("live_only", "table", "make_canon_log")]
    kids = items[0].children
    assert set(kids) == {
        "column id", "column note", "constraint make_canon_log_pkey",
        "index idx_make_canon_log_note", "sequence make_canon_log_id_seq", "trigger trg_make_canon_log",
    }
    # the pkey index is compared as the constraint, never listed twice
    assert "index make_canon_log_pkey" not in kids


def test_chain_only_table_folds_the_same_way():
    items = sdr.diff_snapshots(_add_table(_snap(), "review_reports"), _snap())
    assert _kinds(items) == [("chain_only", "table", "review_reports")]
    assert len(items[0].children) == 6


def test_columns_live_only_chain_only_and_mismatch():
    live = _snap()
    live["columns"] = dict(live["columns"])
    live["columns"][("cars", "zip_code")] = ("text", "YES", "")
    del live["columns"][("cars", "price")]
    live["columns"][("cars", "vin")] = ("character varying", "YES", "")
    items = sdr.diff_snapshots(_snap(), live)
    assert _kinds(items) == [
        ("chain_only", "column", "cars.price"),
        ("live_only", "column", "cars.zip_code"),
        ("mismatch", "column", "cars.vin"),
    ]
    mm = next(d for d in items if d.kind == "mismatch")
    assert mm.chain == ("text", "YES", "") and mm.live == ("character varying", "YES", "")
    assert "chain=" in mm.detail and "live=" in mm.detail


def test_column_default_difference_is_a_mismatch():
    live = _snap()
    live["columns"] = {**live["columns"], ("cars", "price"): ("integer", "YES", "0")}
    assert _kinds(sdr.diff_snapshots(_snap(), live)) == [("mismatch", "column", "cars.price")]


def test_index_same_definition_other_name_is_one_mismatch():
    live = _snap()
    live["indexes"] = dict(live["indexes"])
    live["indexes"]["cars_vin_idx"] = live["indexes"].pop("idx_cars_vin")
    items = sdr.diff_snapshots(_snap(), live)
    assert _kinds(items) == [("mismatch", "index", "cars_vin_idx")]
    assert "name differs: chain=idx_cars_vin live=cars_vin_idx" in items[0].detail


def test_index_definition_difference_and_one_sided_indexes():
    live = _snap()
    live["indexes"] = dict(live["indexes"])
    live["indexes"]["idx_cars_vin"] = ("cars", "public.cars USING btree (vin, price)", False)
    live["indexes"]["uq_probe"] = ("cars", "public.cars USING btree (price)", True)
    chain = _snap()
    chain["indexes"] = {**chain["indexes"], "idx_chain_only": ("dealers", "public.dealers USING hash (id)", False)}
    assert _kinds(sdr.diff_snapshots(chain, live)) == [
        ("chain_only", "index", "idx_chain_only"),
        ("live_only", "index", "uq_probe"),
        ("mismatch", "index", "idx_cars_vin"),
    ]


def test_constraint_backed_index_is_reported_once_as_the_constraint():
    live = _snap()
    live["constraints"] = {**live["constraints"], ("cars", "cars_pkey"): ("p", "PRIMARY KEY (id, vin)")}
    live["indexes"] = {**live["indexes"], "cars_pkey": ("cars", "public.cars USING btree (id, vin)", True)}
    items = sdr.diff_snapshots(_snap(), live)
    assert _kinds(items) == [("mismatch", "constraint", "cars_pkey")]


def test_constraint_renamed_and_check_difference():
    live = _snap()
    live["constraints"] = dict(live["constraints"])
    live["constraints"][("cars", "price_nonneg")] = live["constraints"].pop(("cars", "cars_price_check"))
    items = sdr.diff_snapshots(_snap(), live)
    assert _kinds(items) == [("mismatch", "constraint", "price_nonneg")]
    live2 = _snap()
    live2["constraints"] = {**live2["constraints"], ("cars", "cars_price_check"): ("c", "CHECK ((price > 0))")}
    assert _kinds(sdr.diff_snapshots(_snap(), live2)) == [("mismatch", "constraint", "cars_price_check")]


def test_sequences_functions_triggers_views_extensions():
    live = _snap()
    live["sequences"] = {**live["sequences"], "orphan_seq": ("", "bigint"), "cars_id_seq": ("cars.id", "bigint")}
    live["functions"] = {"set_updated_at()": "CREATE FUNCTION set_updated_at() ... v2", "probe(integer)": "x"}
    live["triggers"] = {("cars", "trg_cars"): "CREATE TRIGGER trg_cars ... AFTER"}
    live["views"] = {"v_probe": "SELECT 1"}
    live["extensions"] = {"plpgsql", "vector", "dblink"}
    assert _kinds(sdr.diff_snapshots(_snap(), live)) == [
        ("live_only", "extension", "dblink"),
        ("live_only", "function", "probe(integer)"),
        ("live_only", "sequence", "orphan_seq"),
        ("live_only", "view", "v_probe"),
        ("mismatch", "function", "set_updated_at()"),
        ("mismatch", "sequence", "cars_id_seq"),
        ("mismatch", "trigger", "trg_cars"),
    ]
    chain = _snap(views={"v_chain": "SELECT 2"})
    assert _kinds(sdr.diff_snapshots(chain, _snap(extensions={"plpgsql"}))) == [
        ("chain_only", "extension", "vector"),
        ("chain_only", "view", "v_chain"),
    ]


def test_diff_is_pure():
    chain, live = _snap(), _add_table(_snap(), "probe")
    before = (json.dumps(sdr.snapshot_to_jsonable(chain)), json.dumps(sdr.snapshot_to_jsonable(live)))
    sdr.diff_snapshots(chain, live)
    after = (json.dumps(sdr.snapshot_to_jsonable(chain)), json.dumps(sdr.snapshot_to_jsonable(live)))
    assert before == after


def test_snapshot_json_round_trip():
    s = _add_table(_snap(views={"v": "SELECT 1"}), "probe")
    s["column_types"] = {("cars", "price"): ("numeric(10,2)", "", "", "")}
    assert sdr.snapshot_from_jsonable(json.loads(json.dumps(sdr.snapshot_to_jsonable(s)))) == s
    # a snapshot saved before column_types existed still loads
    old = sdr.snapshot_to_jsonable(s)
    del old["column_types"]
    assert sdr.snapshot_from_jsonable(old)["column_types"] == {}


def test_column_type_difference_hidden_by_information_schema_is_one_mismatch():
    chain, live = _snap(), _snap()
    chain["column_types"] = {("cars", "price"): ("numeric(10,2)", "", "", ""), ("cars", "vin"): ("text", "", "", "")}
    live["column_types"] = {("cars", "price"): ("numeric", "", "", ""), ("cars", "vin"): ("text", "", "", "")}
    items = sdr.diff_snapshots(chain, live)
    assert _kinds(items) == [("mismatch", "column", "cars.price")]
    assert items[0].detail.startswith("type chain=")
    # a data_type mismatch is already listed by the information_schema diff: listed once
    live["columns"] = {**live["columns"], ("cars", "price"): ("numeric", "YES", "")}
    assert _kinds(sdr.diff_snapshots(chain, live)) == [("mismatch", "column", "cars.price")]
    # one-sided column types (one-sided columns) add nothing
    live["column_types"] = {**live["column_types"], ("cars", "zip"): ("text", "", "", "")}
    assert len(sdr.diff_snapshots(chain, live)) == 1


def test_diff_migrations_pending_ahead_and_checksum_drift():
    items = sdr.diff_migrations({1: "a", 2: "b", 3: "c"}, {1: "a", 2: "B", 4: "d"})
    assert _kinds(items) == [
        ("chain_only", "migration", "V003"),
        ("live_only", "migration", "V004"),
        ("mismatch", "migration", "V002"),
    ]
    assert sdr.diff_migrations({1: "a"}, {1: "a"}) == []


# ---------------------------------------------------------------------------
# (a) classification
# ---------------------------------------------------------------------------


def test_known_drift_explains_and_unknown_stays_unexplained():
    items = [
        sdr.Drift("live_only", "table", "cars_epa_link_backup_20260921"),
        sdr.Drift("mismatch", "index", "uq_option_rejections_standing", "option_rejections"),
        sdr.Drift("live_only", "table", "mystery_table"),
    ]
    out, stale = sdr.classify(items, "cars", pending=[26])
    assert [c.explained for c in out] == [True, True, False]
    assert "D-DB1" in out[0].explanation and "D-DB7" in out[1].explanation
    stale_names = {k.name for k in stale}
    assert "cars_owner_backup_20260929" in stale_names and "cars_epa_link_backup_20260921" not in stale_names
    # entries scoped to other targets are never "stale" here
    assert "cars_backup_*" not in stale_names


def test_v026_adopted_objects_are_no_longer_known_live_only():
    # V026 (P4.3) creates them, so a live-only one is real drift again.
    for name in ("car_move_log", "cars_trim_quarantine", "package_values_msrp_quarantine",
                 "image_summary_restore_log", "make_canon_log", "model_canon_log", "package_value_purge_log"):
        out, _ = sdr.classify([sdr.Drift("live_only", "table", name)], "cars", pending=[26])
        assert not out[0].explained, name
    out, _ = sdr.classify([sdr.Drift("live_only", "index", "uq_option_rejections_standing")], "cars", pending=[26])
    assert not out[0].explained


def test_while_pending_entry_explains_only_while_that_migration_is_pending():
    item = sdr.Drift("mismatch", "index", "uq_option_rejections_standing", "option_rejections")
    for db in ("cars", "cars_cleanroom", "railway"):
        out, stale = sdr.classify([item], db, pending=[26])
        assert out[0].explained and "NULLS NOT DISTINCT" in out[0].explanation, db
    # V026 applied: the old index must be gone, so the same mismatch is unexplained ...
    out, stale = sdr.classify([item], "cars", pending=[])
    assert not out[0].explained
    # ... and the entry does not apply to this target, so it is not listed as stale either
    assert "uq_option_rejections_standing" not in {k.name for k in stale}
    out, stale = sdr.classify([], "cars", pending=[26])
    assert "uq_option_rejections_standing" in {k.name for k in stale}
    k = sdr.Known(sdr.MISMATCH, "index", "x", "why", ("cars",), while_pending=27)
    assert k.applies_to("cars", [27]) and not k.applies_to("cars", [26]) and not k.applies_to("other", [27])


def test_cleanroom_exclusions_apply_only_to_cleanroom_style_targets():
    item = sdr.Drift("chain_only", "table", "cars_backup_capacity_20260718")
    bak = sdr.Drift("chain_only", "table", "epa_extended_specs_bak_badhp_20260712")
    for db in ("cars_cleanroom", "cars_cr_n6"):
        out, _ = sdr.classify([item, bak], db)
        assert all(c.explained for c in out), db
    out, _ = sdr.classify([item, bak], "cars")
    assert not any(c.explained for c in out)


def test_kind_and_category_must_match():
    # a chain-only car_move_log is NOT the known live-only V019 object
    out, _ = sdr.classify([sdr.Drift("chain_only", "table", "car_move_log")], "cars")
    assert not out[0].explained
    out, _ = sdr.classify([sdr.Drift("mismatch", "index", "uq_option_rejections_standing")], "cars")
    assert not out[0].explained


def test_pending_migration_explains_its_chain_only_objects():
    objects = {("table", "review_reports"): 26, ("column", "cars.listing_tier"): 26,
               ("index", "idx_review_reports_x"): 26, ("table", "cars"): 1}
    items = [
        sdr.Drift("chain_only", "migration", "V026"),
        sdr.Drift("chain_only", "table", "review_reports"),
        sdr.Drift("chain_only", "column", "cars.listing_tier", "cars"),
        sdr.Drift("chain_only", "index", "idx_review_reports_x", "review_reports"),
        sdr.Drift("chain_only", "index", "idx_unparsed", "review_reports"),  # falls back to its table
        sdr.Drift("chain_only", "column", "cars.other", "cars"),  # cars is V001: not pending
        sdr.Drift("live_only", "table", "review_reports_old"),  # live-only is never "pending"
    ]
    out, _ = sdr.classify(items, "cars", pending=[26], objects=objects, known=())
    assert [c.explained for c in out] == [True, True, True, True, True, False, False]
    assert "pending" in out[1].explanation
    out_cr, _ = sdr.classify(items[:2], "cars_cleanroom", pending=[26], objects=objects, known=())
    assert all("D-N1" in c.explanation for c in out_cr)
    out_none, _ = sdr.classify(items, "cars", pending=[], objects=objects, known=())
    assert not any(c.explained for c in out_none)


def test_known_registry_is_well_formed():
    kinds = {sdr.LIVE_ONLY, sdr.CHAIN_ONLY, sdr.MISMATCH}
    for k in sdr.KNOWN_DRIFT:
        assert k.kind in kinds and k.category in sdr._CATEGORY_ORDER and k.reason.strip()
    names = {k.name for k in sdr.KNOWN_DRIFT}
    # the out-of-chain objects V026 does not adopt (D-DB1 backups) and V026's own pending difference
    for name in ("uq_option_rejections_standing", "cars_epa_link_backup_20260921", "cars_owner_backup_20260929"):
        assert name in names
    uq = [k for k in sdr.KNOWN_DRIFT if k.name == "uq_option_rejections_standing"]
    assert [(k.kind, k.while_pending) for k in uq] == [(sdr.MISMATCH, 26)]


def test_migration_objects_static_parse(tmp_path):
    (tmp_path / "V001__base.sql").write_text(
        "-- CREATE TABLE commented_out (x int);\n"
        "CREATE TABLE public.alpha (id int, CONSTRAINT alpha_named UNIQUE (id));\n"
        "CREATE SEQUENCE IF NOT EXISTS public.alpha_id_seq;\n"
        "CREATE INDEX idx_alpha ON public.alpha USING btree (id);\n", encoding="utf-8")
    (tmp_path / "V002__two.sql").write_text(
        "CREATE TABLE IF NOT EXISTS alpha (id int);\n"
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_beta ON beta (x);\n"
        "ALTER TABLE ONLY public.alpha ADD COLUMN IF NOT EXISTS note text;\n"
        "ALTER TABLE alpha ADD CONSTRAINT alpha_chk CHECK (id > 0);\n", encoding="utf-8")
    (tmp_path / "notes.sql").write_text("CREATE TABLE ignored (x int);", encoding="utf-8")
    objs = sdr.migration_objects(tmp_path)
    assert objs[("table", "alpha")] == 1  # lowest version wins
    assert objs[("sequence", "alpha_id_seq")] == 1
    assert objs[("index", "idx_alpha")] == 1 and objs[("index", "uq_beta")] == 2
    assert objs[("column", "alpha.note")] == 2
    assert objs[("constraint", "alpha_named")] == 1 and objs[("constraint", "alpha_chk")] == 2
    assert ("table", "commented_out") not in objs and ("table", "ignored") not in objs
    assert ("column", "alpha.constraint") not in objs


def test_migration_objects_on_the_repo_chain():
    objs = sdr.migration_objects(REPO_ROOT / "migrations")
    assert objs[("table", "cars")] == 1
    assert objs[("index", "idx_cars_active_zip")] == 25
    assert objs[("table", "vin_owner_conflicts")] == 24
    assert objs[("extension", "vector")] == 1


# ---------------------------------------------------------------------------
# (b) guards
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dsn", [
    "postgresql:///postgres?host=/tmp&port=5432",
    "postgresql://localhost/postgres",
    "postgresql://127.0.0.1:5433/postgres",
    "host=/tmp port=5432 dbname=postgres",
])
def test_scratch_server_accepts_local(dsn, monkeypatch):
    monkeypatch.delenv("PGHOST", raising=False)
    monkeypatch.delenv("PGPORT", raising=False)
    sdr.check_scratch_server(dsn)


@pytest.mark.parametrize("dsn, why", [
    ("postgresql://user:pw@db.proxy.rlwy.net:41234/railway", "local"),
    ("postgresql://10.0.0.5/postgres", "local"),
    ("postgresql://localhost:15432/cars", "15432"),
    ("postgresql:///postgres?host=/tmp&port=15432", "15432"),
    ("postgresql:///postgres?host=localhost&hostaddr=192.168.1.89", "hostaddr"),
])
def test_scratch_server_refuses_non_local_and_tunnel(dsn, why):
    with pytest.raises(sdr.Refused, match=why):
        sdr.check_scratch_server(dsn)


def test_scratch_server_honours_pghost_fallback(monkeypatch):
    monkeypatch.setenv("PGHOST", "db.example.com")
    with pytest.raises(sdr.Refused):
        sdr.check_scratch_server("postgresql:///postgres")
    monkeypatch.setenv("PGHOST", "/tmp")
    monkeypatch.setenv("PGPORT", "15432")
    with pytest.raises(sdr.Refused, match="15432"):
        sdr.check_scratch_server("postgresql:///postgres")


@pytest.mark.parametrize("name, target", [
    ("cars", "x"), ("cars_cleanroom", "x"), ("pytest_other", "x"), ("schema_drift_ab12", "x"),
    ("pytest_chain_UPPER", "x"), ("pytest_chain_ab12", "pytest_chain_ab12"), ("pytest_chain_", "x"),
])
def test_scratch_name_refusals(name, target):
    with pytest.raises(sdr.Refused):
        sdr.check_scratch_name(name, target)


def test_generated_scratch_name_passes_the_guard():
    name = sdr.new_scratch_name()
    assert name.startswith("pytest_chain_")
    sdr.check_scratch_name(name, "cars")


def test_with_dbname_keeps_server_and_is_a_url(monkeypatch):
    from psycopg.conninfo import conninfo_to_dict

    from backend.db import inventory_pg

    out = sdr.with_dbname("postgresql://me:p%40ss%26w@localhost:5433/postgres?sslmode=disable", "pytest_chain_x1")
    info = conninfo_to_dict(out)
    assert info["dbname"] == "pytest_chain_x1" and info["host"] == "localhost" and info["port"] == "5433"
    assert info["password"] == "p@ss&w" and info["sslmode"] == "disable"
    sock = sdr.with_dbname("host=/tmp port=5432 dbname=postgres", "cars")
    assert sock.startswith("postgresql:///cars?")
    assert conninfo_to_dict(sock)["host"] == "/tmp"
    monkeypatch.setenv("INVENTORY_DATABASE_URL", sock)
    assert inventory_pg.inventory_postgres_dsn() == sock  # migrate.py can connect through it


def test_describe_dsn_never_shows_password_or_remote_host():
    text = sdr.describe_dsn("postgresql://postgres:s3cret@abc.proxy.rlwy.net:41234/railway")
    assert "s3cret" not in text and "rlwy" not in text and "db=railway" in text
    assert "host=/tmp" in sdr.describe_dsn("postgresql:///cars?host=/tmp")


# ---------------------------------------------------------------------------
# (b) read-only proof through fake connections
# ---------------------------------------------------------------------------

_READ_ONLY_SQL = re.compile(r"^\s*(SELECT|SHOW|SET statement_timeout)\b", re.I)


class _Cur:
    def __init__(self, conn: "_Conn"):
        self.conn = conn
        self._rows: list[tuple] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.conn.sql.append(sql)
        s = " ".join(sql.split())
        answers = self.conn.answers
        if s.startswith("SHOW default_transaction_read_only"):
            self._rows = [(answers.get("default_ro", "on"),)]
        elif s.startswith("SHOW transaction_read_only"):
            self._rows = [(answers.get("ro", "on"),)]
        elif s.startswith("SHOW server_version"):
            self._rows = [("17.10",)]
        elif s.startswith("SELECT current_database()"):
            self._rows = [(answers.get("dbname", "cars"),)]
        elif "to_regclass" in s:
            self._rows = [("schema_migrations",)]
        elif "FROM public.schema_migrations" in s:
            self._rows = sorted(answers.get("recorded", {1: "sha:1"}).items())
        elif "pg_stat_database" in s:
            self._rows = [(5, 6, 7)]
        elif "FROM pg_database WHERE datname" in s:
            self._rows = [(1,)] if answers.get("scratch_exists") else []
        else:
            self._rows = []  # catalog queries: an empty schema

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def close(self) -> None:
        pass


class _Conn:
    def __init__(self, **answers: Any):
        self.answers = answers
        self.sql: list[str] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> _Cur:
        return _Cur(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


class _FakeConnect:
    def __init__(self, conn: _Conn):
        self.conn = conn
        self.calls: list[dict[str, Any]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> _Conn:
        self.calls.append({"args": args, **kwargs})
        return self.conn


def _run(target: _Conn, admin: _Conn | None = None, build=None, **kw):
    admin = admin or _Conn()
    scratch = _Conn()
    connect = _FakeConnect(target)
    builds: list[str] = []

    def fake_build(dsn, directory, builder):
        builds.append(dsn)
        return "fake", {1: "sha:1"}

    rep = sdr.run_report(
        target_dsn="postgresql:///cars?host=/tmp&port=5432",
        connect_fn=connect,
        admin_connect=lambda dsn: admin,
        scratch_connect=lambda dsn: scratch,
        build_fn=build or fake_build,
        **kw,
    )
    return rep, connect, admin, scratch, builds


def test_target_opens_read_only_and_only_reads():
    target = _Conn()
    rep, connect, admin, scratch, builds = _run(target)
    assert connect.calls == [{"args": (), "read_only": True, "dsn": "postgresql:///cars?host=/tmp&port=5432"}]
    assert target.sql, "the target was never read"
    writes = [s for s in target.sql if not _READ_ONLY_SQL.match(s)]
    assert writes == [], f"non-read statements on the target: {writes}"
    assert target.closed
    # one commit only: the transaction that ran the two SHOWs and SET statement_timeout
    assert target.commits == 1
    assert rep.items == [] and rep.unexplained == []
    assert rep.counters_before == [5, 6, 7] == rep.counters_after


def test_scratch_db_is_created_and_dropped_under_its_own_name():
    rep, _connect, admin, scratch, builds = _run(_Conn())
    creates = [s for s in admin.sql if s.startswith("CREATE DATABASE")]
    drops = [s for s in admin.sql if s.startswith("DROP DATABASE")]
    assert len(creates) == 1 and len(drops) == 1
    name = re.search(r'"([^"]+)"', creates[0]).group(1)
    assert name == rep.scratch and name.startswith("pytest_chain_")
    assert f'"{name}"' in drops[0]
    assert builds and f"/{name}?" in builds[0]
    assert admin.closed and scratch.closed


def test_scratch_db_is_dropped_when_the_build_fails():
    admin = _Conn()

    def broken(dsn, directory, builder):
        raise RuntimeError("V007 failed")

    with pytest.raises(RuntimeError, match="V007"):
        _run(_Conn(), admin=admin, build=broken)
    assert any(s.startswith("DROP DATABASE") for s in admin.sql)


def test_refuses_a_session_that_is_not_read_only_before_creating_anything():
    target = _Conn(ro="off")
    admin = _Conn()
    with pytest.raises(sdr.ReadOnlyRequired):
        _run(target, admin=admin)
    assert admin.sql == [], "no scratch DB may be created for a refused target"
    assert target.closed
    assert not any("information_schema" in s or "pg_catalog" in s for s in target.sql)


def test_existing_scratch_name_is_refused_and_never_dropped():
    admin = _Conn(scratch_exists=True)
    with pytest.raises(sdr.Refused, match="already exists"):
        _run(_Conn(), admin=admin, scratch_name="pytest_chain_taken")
    assert not any(s.startswith(("CREATE DATABASE", "DROP DATABASE")) for s in admin.sql)


def test_scratch_name_equal_to_target_is_refused():
    with pytest.raises(sdr.Refused, match="equals the target"):
        sdr.run_report(target_dsn="postgresql:///pytest_chain_same?host=/tmp",
                       scratch_name="pytest_chain_same", connect_fn=_FakeConnect(_Conn()))


def test_non_local_scratch_server_is_refused_before_the_target_is_touched():
    connect = _FakeConnect(_Conn())
    with pytest.raises(sdr.Refused):
        sdr.run_report(target_dsn="postgresql:///cars?host=/tmp",
                       admin_dsn="postgresql://u:p@remote.example.com/postgres", connect_fn=connect)
    assert connect.calls == []


def test_open_target_uses_backend_db_connect_by_default(monkeypatch):
    from backend.db import connect as connect_mod

    fake = _FakeConnect(_Conn())
    monkeypatch.setattr(connect_mod, "connect", fake)
    conn = sdr.open_target_read_only("postgresql:///cars?host=/tmp")
    assert fake.calls[0]["read_only"] is True and conn is fake.conn


def test_snapshot_issues_selects_only_and_ends_with_rollback():
    conn = _Conn()
    snap = sdr.snapshot(conn)
    assert set(snap) == set(sdr.empty_snapshot())
    assert conn.sql and all(s.lstrip().upper().startswith("SELECT") for s in conn.sql)
    assert conn.rollbacks == 1 and conn.commits == 0


def test_snapshot_queries_are_the_parity_tests_queries(monkeypatch):
    """The four base queries are test_schema_from_migrations._snapshot's, byte for byte."""
    psycopg = pytest.importorskip("psycopg")
    from backend.tests import test_schema_from_migrations as parity

    class _Recorder(_Conn):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class _RecCur(_Cur):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    rec = _Recorder()
    rec.cursor = lambda: _RecCur(rec)  # type: ignore[method-assign]
    monkeypatch.setattr(psycopg, "connect", lambda *a, **k: rec)
    parity._snapshot("postgresql:///never-connected")
    assert rec.sql == [sdr._SQL_TABLES, sdr._SQL_COLUMNS, sdr._SQL_INDEXES, sdr._SQL_CONSTRAINTS]


def test_report_renders_and_serializes():
    items, _ = sdr.classify(
        [sdr.Drift("live_only", "table", "cars_epa_link_backup_20260921", "cars_epa_link_backup_20260921",
                   "2 folded", children=("column id",)),
         sdr.Drift("live_only", "table", "mystery")], "cars")
    rep = sdr.Report(
        generated_at="2026-10-18T00:00:00Z", target="host=/tmp port=5432 db=cars", target_db="cars",
        server_version="17.10", scratch="pytest_chain_x", scratch_server="host=/tmp port=5432 db=postgres",
        builder="fake", chain_versions=[1, 2], recorded_versions=[1, 2], items=items,
        counters_before=[0, 0, 0], counters_after=[0, 0, 0],
    )
    text = sdr.render_text(rep)
    assert "UNEXPLAINED: 1" in text and "[explained] table cars_epa_link_backup_20260921" in text
    assert "folded: column id" in text and "[UNEXPLAINED] table mystery" in text
    data = json.loads(json.dumps(rep.to_jsonable()))
    assert data["summary"]["unexplained"] == 1 and data["summary"]["live_only"] == 2


def test_main_exit_codes(monkeypatch, tmp_path, capsys):
    def fake_run(**kw):
        items, _ = sdr.classify([sdr.Drift("live_only", "table", kw["scratch_name"] or "mystery")], "cars")
        return sdr.Report("t", "host=/tmp port=5432 db=cars", "cars", "17", "pytest_chain_x", "local", "fake",
                          [1], [1], items=items)

    monkeypatch.setattr(sdr, "run_report", fake_run)
    out = tmp_path / "r.json"
    assert sdr.main(["--target-db", "cars", "--json-out", str(out)]) == sdr.EXIT_UNEXPLAINED
    assert json.loads(out.read_text())["summary"]["unexplained"] == 1
    assert sdr.main(["--target-db", "cars", "--scratch-name", "cars_epa_link_backup_20260921", "-q"]) == sdr.EXIT_CLEAN
    assert "UNEXPLAINED 0" in capsys.readouterr().out

    def refuse(**kw):
        raise sdr.Refused("nope")

    monkeypatch.setattr(sdr, "run_report", refuse)
    assert sdr.main(["--target-db", "cars"]) == sdr.EXIT_REFUSED
    assert sdr.main(["--target-db", "pytest_chain_abc"]) == sdr.EXIT_REFUSED


# ---------------------------------------------------------------------------
# (b) chain builder selection (P4.2 hand-over)
# ---------------------------------------------------------------------------


def test_runner_records_v019_detection():
    assert sdr.runner_records_v019(SimpleNamespace(RECORD_ONLY={19: "why"})) is True
    assert sdr.runner_records_v019(SimpleNamespace(RECORD_ONLY={20: "x"})) is False
    assert sdr.runner_records_v019(SimpleNamespace()) is False
    assert sdr.runner_records_v019(SimpleNamespace(RECORD_ONLY=None)) is False


def test_runner_build_pins_the_scratch_dsn_and_restores_env(monkeypatch, tmp_path):
    scratch = "postgresql:///pytest_chain_r1?host=%2Ftmp&port=5432"
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql:///cars?host=/tmp")
    monkeypatch.setenv("DATABASE_URL", "postgresql:///other")
    seen: dict[str, Any] = {}

    def fake_main(argv):
        seen["argv"] = list(argv)
        seen["inv"] = os.environ.get("INVENTORY_DATABASE_URL")
        seen["db"] = os.environ.get("DATABASE_URL")
        return 0

    sdr._build_with_runner(scratch, tmp_path, SimpleNamespace(main=fake_main))
    assert seen == {"argv": ["--apply", "--dir", str(tmp_path)], "inv": scratch, "db": None}
    assert os.environ["INVENTORY_DATABASE_URL"] == "postgresql:///cars?host=/tmp"
    assert os.environ["DATABASE_URL"] == "postgresql:///other"

    with pytest.raises(RuntimeError, match="exited 3"):
        sdr._build_with_runner(scratch, tmp_path, SimpleNamespace(main=lambda argv: 3))
    assert os.environ["INVENTORY_DATABASE_URL"] == "postgresql:///cars?host=/tmp"


def test_runner_build_refuses_when_resolution_is_not_the_scratch_db(monkeypatch, tmp_path):
    from backend.db import inventory_pg

    monkeypatch.setattr(inventory_pg, "inventory_postgres_dsn", lambda: "postgresql:///cars")
    called = []
    with pytest.raises(sdr.Refused):
        sdr._build_with_runner("postgresql:///pytest_chain_r2?host=/tmp", tmp_path,
                               SimpleNamespace(main=lambda argv: called.append(argv) or 0))
    assert called == []


def test_runner_build_checks_the_connect_module_resolver_too(monkeypatch, tmp_path):
    from backend.db import connect as connect_mod

    monkeypatch.setattr(connect_mod, "inventory_dsn", lambda require=True: "postgresql:///cars")
    called = []
    with pytest.raises(sdr.Refused):
        sdr._build_with_runner("postgresql:///pytest_chain_r3?host=/tmp", tmp_path,
                               SimpleNamespace(main=lambda argv: called.append(argv) or 0))
    assert called == []


# ---------------------------------------------------------------------------
# (c) live, opt-in: SCHEMA_PARITY_PG_DSN on a LOCAL server
# ---------------------------------------------------------------------------

_PARITY_DSN_ENV = "SCHEMA_PARITY_PG_DSN"


def _live_snapshot(psycopg: Any, dsn: str) -> dict[str, Any]:
    conn = psycopg.connect(dsn)
    try:
        return sdr.snapshot(conn)
    finally:
        conn.close()


@pytest.mark.integration
def test_live_report_finds_each_perturbation_and_writes_nothing(tmp_path):
    admin_dsn = (os.environ.get(_PARITY_DSN_ENV) or "").strip()
    if not admin_dsn:
        pytest.skip(f"{_PARITY_DSN_ENV} not set (needs a LOCAL server where pytest_* DBs may be created)")
    psycopg = pytest.importorskip("psycopg")
    try:
        sdr.check_scratch_server(admin_dsn)
    except sdr.Refused as exc:
        pytest.skip(f"{_PARITY_DSN_ENV} is not a local scratch server: {exc}")

    tgt = f"pytest_drift_tgt_{uuid.uuid4().hex[:10]}"
    assert tgt not in sdr.PROTECTED_DBS
    tgt_dsn = sdr.with_dbname(admin_dsn, tgt)
    try:
        admin = psycopg.connect(admin_dsn, autocommit=True)
    except Exception as exc:  # pragma: no cover - environment
        pytest.skip(f"cannot reach {_PARITY_DSN_ENV}: {exc}")
    try:
        admin.execute(f'CREATE DATABASE "{tgt}" TEMPLATE template0')
        sdr.build_chain(tgt_dsn, REPO_ROOT / "migrations", "own")
        with psycopg.connect(tgt_dsn, autocommit=True) as c:
            c.execute("CREATE TABLE cars_epa_link_backup_20260921 (id bigint, epa_master_id bigint)")  # known (D-DB1)
            c.execute("CREATE TABLE drift_probe (id int)")                                     # unexplained
            c.execute("ALTER TABLE dealer_recipes ADD COLUMN probe_col text")                  # live-only column
            c.execute("ALTER TABLE dealer_recipes ALTER COLUMN probe_col SET DEFAULT 'x'")
            c.execute("DROP INDEX idx_cars_active_zip")                                         # chain-only index
        before = _live_snapshot(psycopg, tgt_dsn)

        rep = sdr.run_report(target_dsn=tgt_dsn, admin_dsn=admin_dsn, builder="own")
        found = {(c.item.kind, c.item.category, c.item.name): c.explained for c in rep.items}
        assert found.get(("live_only", "table", "cars_epa_link_backup_20260921")) is True
        assert found.get(("live_only", "table", "drift_probe")) is False
        assert found.get(("live_only", "column", "dealer_recipes.probe_col")) is False
        assert found.get(("chain_only", "index", "idx_cars_active_zip")) is False
        assert len(rep.unexplained) == 3, sdr.render_text(rep)
        assert rep.target_db == tgt and rep.scratch.startswith("pytest_chain_")

        after = _live_snapshot(psycopg, tgt_dsn)
        assert before == after, "the report changed the target schema"
        left = admin.execute("SELECT datname FROM pg_database WHERE datname = %s", (rep.scratch,)).fetchall()
        assert left == [], "scratch database was not dropped"
    finally:
        try:
            admin.execute(f'DROP DATABASE IF EXISTS "{tgt}" WITH (FORCE)')
        finally:
            admin.close()
