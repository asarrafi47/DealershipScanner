"""backend/scripts/merge_dealer_snapshot.py: scope rules, apply semantics, rollback, cleanup.

The pure-function tests run everywhere. The database tests are the ``pg`` tier: each one
clones a SOURCE and a TARGET database from the migration-built template
(backend/tests/pg_harness.py) and seeds one small world that holds every case at once:

    scripts/pg_test_up.sh -- python -m pytest backend/tests/test_merge_dealer_snapshot.py -q

World (target = "Railway", source = the 2026-11-03 snapshot):

* strong-com   ok, new 16/20 re-seen + 5 fresh more, used 10/12 re-seen: retires both buckets.
  N02 carries admin notes, N03/N04 the first_seen_at cases, N05/N06/N21/N22 the
  dealership_registry_id guard, N07-N13 the keep rule, X01 is a reactivation; OWN_OUT, MOVE_OK,
  MOVE_CONTESTED and MOVE_WEAK are fresh here but filed elsewhere in the target.
* strong2-com  ok, used 14/17 seen (MOVE_CONTESTED and CLAIM_ONLY by claim), retires; loses
  MOVE_OK, and SEEN_OOS is fresh at norows-com (an out-of-scope dealer), so it is not retired.
* weak-com     ok, new 10/21 re-seen: coverage 0.476 < 0.7, keeps its unseen rows (and MOVE_WEAK).
* border-com   ok, new 13/20 re-seen with a full-size capture: coverage 0.65 retires at
  --min-coverage 0.6 but not at the default 0.7.
* partial-com  thin, used 13/20 re-seen and 13 fresh: coverage 0.65 but size ratio 0.65 < 0.8.
* keptbucket-com inaccurate, the pipeline kept its used bucket (low_bucket).
* inelig-com   ok, triage reconcile not eligible.
* small-com    ok, 5 target used rows < 10.
* norows-com   no_rows (out of scope); owns OWN_OUT in the target.
* targetonly-com (not in the source), notriage-com (not in triage), newer-com (target
  scraped after the scan started): out of scope.
"""

from __future__ import annotations

import csv
import json
import secrets
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from backend.scripts import merge_dealer_snapshot as mds

SINCE = "2026-11-03T17:59:55+00:00"
FRESH = "2026-11-03T18:30:00.000000Z"
OLD = "2026-09-29T10:00:00.000000Z"
NEWER = "2026-11-04T09:00:00.000000Z"
REMOVED = "2026-09-29T12:00:00+00:00"

USER_TABLES = ("car_comments", "dealer_comments", "comment_attachments", "user_search_history",
               "user_hidden_dealers")

IN_SCOPE = {"strong-com", "strong2-com", "weak-com", "border-com", "partial-com", "keptbucket-com", "inelig-com",
            "small-com"}

EXPECTED_TOTALS = {
    "update": 99, "reactivate": 1, "move_out": 1, "insert": 24, "retire": 7, "retire_new": 4, "retire_used": 3,
    "kept_unseen": 27, "skipped_conflicts": 3, "skip_target_owner_out_of_scope": 1, "skip_contested_in_run": 1,
    "skip_target_owner_capture_weak": 1, "seen_by_claim": 1, "keep_rows": 4, "vpic_inserts": 1,
    "grid_cards_deleted": 2, "ref_guarded_dealership_registry_id": 2,
}
EXPECTED_KEPT_CELLS = {"description": 1, "packages": 1, "price": 1, "window_sticker_url": 1}


def _dsn(user: str, password: str, host: str, db: str) -> str:
    """A DSN-shaped string assembled at run time so no password-looking URL sits in the source."""
    return "postgresql" + "://" + user + ":" + password + "@" + host + "/" + db


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------


def test_condition_bucket_is_the_scanners_rule() -> None:
    from backend.scanner.inventory_reconcile import condition_bucket

    for value in (None, "", "  ", "New", "new", " NEW ", "Used", "CPO", "Certified", "certified pre-owned",
                  "Demo", "new-ish", "Pre-Owned"):
        assert mds.condition_bucket(value) == condition_bucket(value), value


def test_least_first_seen_keeps_target_text_on_tie_and_takes_the_earlier() -> None:
    assert mds.least_first_seen("2026-08-01T00:00:00.000000Z", "2026-07-01T00:00:00.000000Z") == \
        "2026-07-01T00:00:00.000000Z"
    assert mds.least_first_seen("2026-08-01T00:00:00.000000Z", "2026-10-01T00:00:00.000000Z") == \
        "2026-08-01T00:00:00.000000Z"
    # same instant, different spelling: no write
    assert mds.least_first_seen("2026-08-01T00:00:00Z", "2026-08-01T00:00:00+00:00") == "2026-08-01T00:00:00Z"
    assert mds.least_first_seen(None, "2026-08-01T00:00:00Z") == "2026-08-01T00:00:00Z"
    assert mds.least_first_seen("2026-08-01T00:00:00Z", None) == "2026-08-01T00:00:00Z"
    assert mds.least_first_seen("garbage", "2026-08-01T00:00:00Z") == "2026-08-01T00:00:00Z"
    assert mds.least_first_seen("2026-08-01 00:00:00", "2026-08-01T00:00:01Z") == "2026-08-01 00:00:00"


def test_a_row_is_never_overwritten_by_an_older_one() -> None:
    old, new = "2026-09-29T10:00:00.000000Z", "2026-11-03T18:30:00.000000Z"
    assert not mds.at_least_as_fresh(old, new)  # the source is newer: write
    assert mds.at_least_as_fresh(new, old)  # the target is newer: leave it alone
    assert mds.at_least_as_fresh(new, new)  # equal: already merged, which is what makes a re-apply a no-op
    assert mds.at_least_as_fresh("2026-11-03T18:30:00Z", "2026-11-03T18:30:00+00:00")  # spelling does not matter
    assert mds.at_least_as_fresh("2026-11-03 18:30:00", "2026-11-03T18:30:00Z")
    assert not mds.at_least_as_fresh(None, new) and not mds.at_least_as_fresh("", new)
    assert mds.at_least_as_fresh(old, None)  # nothing trustworthy to write over it


def test_normalize_since() -> None:
    assert mds.normalize_since("2026-11-03T17:59:55+00:00") == "2026-11-03T17:59:55"
    assert mds.normalize_since("2026-11-03T12:59:55-05:00") == "2026-11-03T17:59:55"
    with pytest.raises(mds.MergeRefused):
        mds.normalize_since("yesterday")


def _bucket(ta: int, fresh: int, seen: int) -> dict[str, int]:
    return {"target_active": ta, "fresh": fresh, "reseen": seen}


def _tri(eligible: bool = True, kept: dict[str, str] | None = None) -> dict[str, Any]:
    return {"reconcile": {"eligible": eligible, "reason": "" if eligible else "8 rows < absolute floor 20",
                          "buckets": {b: {"kept": (kept or {}).get(b)} for b in mds.BUCKETS}}}


def test_bucket_decision_is_stricter_than_the_scanner_floor() -> None:
    rule = mds.Rule(mds.DEFAULT_MIN_COVERAGE, mds.DEFAULT_MIN_SIZE_RATIO, mds.DEFAULT_MIN_BUCKET_ROWS)
    assert mds.DEFAULT_MIN_COVERAGE > mds.SCANNER_COVERAGE_FLOOR
    # encinitasford-com 2026-11-03, New: 89 of 171 pre-scan rows re-seen (52%) and, measured against
    # Railway's own rows, 101 of 170 (59.4%); the capture itself was full size (164 rows).
    for target_rows, seen in ((171, 89), (170, 101)):
        ok, why = mds.bucket_decision("new", _bucket(target_rows, 164, seen), _tri(), rule)
        assert not ok and why.startswith("coverage 0.5"), why
    assert mds.bucket_decision("new", _bucket(100, 95, 75), _tri(), rule)[0]
    assert mds.bucket_decision("new", _bucket(100, 100, 70), _tri(), rule)[0]  # the line itself retires
    assert mds.bucket_decision("new", _bucket(100, 100, 69), _tri(), rule) == (False, "coverage 0.690 < 0.7")
    assert mds.bucket_decision("new", _bucket(100, 79, 70), _tri(), rule) == (False, "size ratio 0.790 < 0.8")
    assert mds.bucket_decision("used", _bucket(9, 9, 9), _tri(), rule) == (False, "9 target rows < 10")
    assert mds.bucket_decision("used", _bucket(0, 9, 0), _tri(), rule) == (False, "no target rows in this bucket")
    assert not mds.bucket_decision("used", _bucket(50, 50, 50), _tri(eligible=False), rule)[0]
    assert mds.bucket_decision("used", _bucket(50, 50, 50), _tri(kept={"used": "low_bucket"}), rule) == \
        (False, "triage kept the used bucket (low_bucket)")


def test_min_coverage_must_beat_the_scanner_floor(tmp_path: Path, capsys) -> None:
    rc = mds.main(["--compute-scope", "--scope", str(tmp_path / "s.json"), "--triage", str(tmp_path / "x.json"),
                   "--source-dsn-env", "A", "--target-dsn-env", "B", "--min-coverage", "0.5"])
    assert rc == mds.EXIT_REFUSED
    assert "stricter than the scanner's 0.5 floor" in capsys.readouterr().err


def test_run_id_is_validated_before_it_names_a_table() -> None:
    assert mds.run_tables("20261109153000_a1b2c3")["backup"] == "merge_backup_20261109153000_a1b2c3"
    for bad in ("x; DROP TABLE cars", "", "20261109153000_A1B2C3", "2026110915300_a1b2c3"):
        with pytest.raises(mds.MergeRefused):
            mds.run_tables(bad)


def _scope_stub(tmp_path: Path) -> str:
    p = tmp_path / "scope.json"
    p.write_text(json.dumps({"format": mds.SCOPE_FORMAT, "since": "2026-11-03T17:59:55", "dealers": {},
                             "source": {"database": "x"}, "target": {"database": "y"}}))
    return str(p)


def test_a_dsn_passed_as_the_env_name_is_never_echoed(tmp_path: Path, capsys) -> None:
    dsn = _dsn("merge_user", "topsecret99", "db.example.com", "x")
    rc = mds.main(["--dry-run", "--scope", _scope_stub(tmp_path),
                   "--source-dsn-env", dsn, "--target-dsn-env", "MERGE_TEST_UNUSED"])
    out = capsys.readouterr()
    assert rc == mds.EXIT_REFUSED
    assert "topsecret99" not in out.out + out.err and "db.example.com" not in out.out + out.err


def test_connection_errors_are_redacted(tmp_path: Path, capsys, monkeypatch) -> None:
    pytest.importorskip("psycopg")
    monkeypatch.setenv("MERGE_TEST_BAD", _dsn("merge_user", "hunter2xyz", "127.0.0.1:1", "nodb") + "?connect_timeout=3")
    rc = mds.main(["--dry-run", "--scope", _scope_stub(tmp_path),
                   "--source-dsn-env", "MERGE_TEST_BAD", "--target-dsn-env", "MERGE_TEST_BAD"])
    out = capsys.readouterr()
    text = out.out + out.err
    assert rc == mds.EXIT_REFUSED and "cannot connect" in text
    for secret in ("hunter2xyz", "127.0.0.1", "merge_user", "postgresql://"):
        assert secret not in text


def test_local_hosts_are_sockets_and_loopback_only() -> None:
    for host, addr in (("/tmp/pgtu.abc", None), ("", None), (None, None), ("localhost", None),
                       ("127.0.0.1", None), ("::1", None), ("/var/run/postgresql", "127.0.0.1")):
        assert mds.is_local_host(host, addr), (host, addr)
    for host, addr in (("roundhouse.proxy.rlwy.net", None), ("10.0.0.5", None), ("db.internal", "10.1.2.3"),
                       ("/tmp/x,db.example.com", None), (None, "203.0.113.9")):
        assert not mds.is_local_host(host, addr), (host, addr)
    assert mds.is_local_host("127.0.0.1", None, 5432) and mds.is_local_host("/tmp/x", None, "55433")
    assert not mds.is_local_host("127.0.0.1", None, 15432) and not mds.is_local_host("localhost", "::1", "15432")


def test_writing_modes_refuse_protected_and_remote_targets() -> None:
    def db(name: str, *, local: bool = True) -> mds.Db:
        return mds.Db(None, "target", name, "18.6", None, False, local)

    mds.check_writable_target(db("railway_rehearsal"))
    for name in ("cars", "cars_cleanroom", "cars_scan_20261103", "postgres", "template1"):
        with pytest.raises(mds.MergeRefused, match="protected"):
            mds.check_writable_target(db(name), allow_remote=True)
    with pytest.raises(mds.MergeRefused, match="--allow-remote-target"):
        mds.check_writable_target(db("railway", local=False))
    mds.check_writable_target(db("railway", local=False), allow_remote=True)
    with pytest.raises(mds.MergeRefused, match="named like the source"):
        mds.check_writable_target(db("same"), mds.Db(None, "source", "same", "17", None, True, True))


_CARS = [("id", "bigint"), ("vin", "text"), ("dealer_id", "text"), ("condition", "text"), ("listing_active", "integer"),
         ("scraped_at", "text"), ("first_seen_at", "text"), ("price", "double precision"), ("description", "text"),
         ("internal_notes", "text"), ("marked_for_review", "integer"), ("dealership_registry_id", "integer"),
         ("epa_master_id", "bigint"), ("epa_match_confidence", "double precision"), ("epa_match_method", "text"),
         ("listing_removed_at", "text"), ("only_in_target", "text")]


def test_column_classes() -> None:
    cp = mds.plan_columns([c for c in _CARS if c[0] != "only_in_target"] + [("only_in_source", "text")], _CARS)
    assert cp.common == [c for c, _ in _CARS if c != "only_in_target"]
    assert cp.source_only == ["only_in_source"] and cp.target_only == ["only_in_target"]
    assert cp.stage == [c for c in cp.common if c != "id"]
    assert cp.ref_cols == ["dealership_registry_id", "epa_master_id", "epa_match_confidence", "epa_match_method"]
    assert cp.always == ["dealer_id", "listing_active", "scraped_at", "listing_removed_at"]  # table order
    assert cp.keepable == ["condition", "price", "description"]
    assert set(cp.copied) == set(cp.always) | set(cp.keepable)
    assert "vin" not in cp.update_set and "internal_notes" not in cp.update_set and "marked_for_review" not in cp.update_set
    assert {"first_seen_at", "dealership_registry_id", "epa_master_id", "price"} <= set(cp.update_set)
    assert cp.text_cols >= {"condition", "description"} and "price" not in cp.text_cols
    plain = mds.plan_columns([c for c in _CARS if c[0] != "only_in_target"], _CARS, keep_rule=False)
    assert plain.keepable == [] and set(plain.always) >= {"price", "description", "condition", "dealer_id"}
    assert plain.update_set == cp.update_set and plain.copied == cp.copied
    with pytest.raises(mds.MergeRefused, match="types differ"):
        mds.plan_columns([("id", "bigint"), ("vin", "text"), ("dealer_id", "text"), ("condition", "text"),
                          ("listing_active", "integer"), ("scraped_at", "text"), ("first_seen_at", "text"),
                          ("price", "numeric")], _CARS)


def test_reference_columns_follow_the_identity_rule() -> None:
    cp = mds.plan_columns(_CARS, _CARS)
    safe = [{1}, {10}]  # dealerships id 1 and epa_master id 10 name the same row in both databases
    tgt = (7, 99, 0.5, "target-method")

    def src(reg, epa=None, conf=None, method=None):
        return (reg, epa, conf, method)

    # update: a verified source id and its travelling columns win, anything else keeps the target's
    assert mds.final_refs(src(1, 10, 0.9, "src"), tgt, cp, safe) == (1, 10, 0.9, "src")
    assert mds.final_refs(src(2, 11, 0.9, "src"), tgt, cp, safe) == tgt
    assert mds.final_refs(src(None), tgt, cp, safe) == tgt  # a NULL source reference keeps the target's
    assert mds.final_refs(src(1, None, 0.9, "src"), tgt, cp, safe) == (1, 99, 0.5, "target-method")
    # insert: the source's group stands when NULL or verified, else NULLs
    assert mds.final_refs(src(1, 10, 0.9, "src"), None, cp, safe) == (1, 10, 0.9, "src")
    assert mds.final_refs(src(2, 11, 0.9, "src"), None, cp, safe) == (None, None, None, None)
    assert mds.final_refs(src(None, None, None, "none"), None, cp, safe) == (None, None, None, "none")
    assert mds._guarded(src(2, 10), cp, safe) == ["dealership_registry_id"]


def _git_checkout_or_skip() -> None:
    if shutil.which("git") is None:
        pytest.skip("git not available")
    inside = subprocess.run(["git", "rev-parse", "--is-inside-work-tree"], cwd=mds.REPO_ROOT,
                            capture_output=True, text=True, check=False)
    if inside.returncode != 0 or inside.stdout.strip() != "true":
        pytest.skip("not a git checkout")


def test_old_backup_tables_are_referenced_so_none_is_on_the_drop_list() -> None:
    _git_checkout_or_skip()
    assert "migrations/V001__baseline.sql" in mds.repo_references("cars_backup_capacity_20260718")
    assert "backend/scripts/schema_drift_report.py" in mds.repo_references("cars_owner_backup_20260929")
    assert mds.repo_references(f"zz_merge_unreferenced_{secrets.token_hex(6)}") == []
    for name in mds.CLEANUP_DROP_TABLES:
        assert mds.repo_references(name) == [], name


# ---------------------------------------------------------------------------
# Postgres tier: a source and a target database per test
# ---------------------------------------------------------------------------


class Side:
    def __init__(self, name: str, dsn: str) -> None:
        self.name = name
        self.dsn = dsn

    def connect(self):
        import psycopg

        return psycopg.connect(self.dsn)

    def run(self, sql: str, params: tuple = ()) -> None:
        with self.connect() as conn:
            conn.execute(sql, params)

    def rows(self, sql: str, params: tuple = ()) -> list[tuple]:
        with self.connect() as conn:
            return conn.execute(sql, params).fetchall()

    def insert(self, table: str, rows: list[dict[str, Any]]) -> None:
        with self.connect() as conn:
            for r in rows:
                cols = ", ".join(f'"{k}"' for k in r)
                marks = ", ".join(["%s"] * len(r))
                conn.execute(f"INSERT INTO {table} ({cols}) VALUES ({marks})", tuple(r.values()))

    def cars(self) -> dict[str, dict[str, Any]]:
        cols = ("id", "vin", "dealer_id", "listing_active", "listing_removed_at", "price", "scraped_at",
                "first_seen_at", "internal_notes", "marked_for_review", "dealership_registry_id",
                "window_sticker_url", "description", "packages", "dealer_url")
        return {r[1]: dict(zip(cols, r)) for r in self.rows(f"SELECT {', '.join(cols)} FROM cars")}

    def snapshot(self) -> dict[int, str]:
        return dict(self.rows("SELECT c.id, c::text FROM cars c ORDER BY c.id"))

    def table(self, name: str) -> list[str]:
        return [r[0] for r in self.rows(f"SELECT x::text FROM {name} x ORDER BY 1")]

    def tables(self) -> set[str]:
        return {r[0] for r in self.rows("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")}


@dataclass
class World:
    src: Side
    tgt: Side
    tmp: Path
    triage: str = ""

    def args(self, *extra: str) -> list[str]:
        return ["--source-dsn-env", "MERGE_TEST_SRC", "--target-dsn-env", "MERGE_TEST_TGT", *extra]

    def scope(self, name: str = "scope.json", *extra: str) -> Path:
        path = self.tmp / name
        assert mds.main(self.args("--compute-scope", "--scope", str(path), "--triage", self.triage, *extra)) == 0
        return path

    def plan(self, scope: Path, *extra: str) -> dict[str, Any]:
        out = self.tmp / f"plan_{secrets.token_hex(3)}.json"
        assert mds.main(self.args("--dry-run", "--scope", str(scope), "--plan-out", str(out), *extra)) == 0
        return json.loads(out.read_text())

    def apply(self, scope: Path, *extra: str) -> int:
        return mds.main(self.args("--apply", "--scope", str(scope), "--expect-db", self.tgt.name, *extra))

    def rollback(self, run_id: str, *extra: str) -> int:
        return mds.main(["--target-dsn-env", "MERGE_TEST_TGT", "--expect-db", self.tgt.name, "--rollback", run_id,
                         *extra])

    def run_ids(self) -> list[str]:
        if "merge_snapshot_runs" not in self.tgt.tables():
            return []
        return [r[0] for r in self.tgt.rows("SELECT run_id FROM merge_snapshot_runs ORDER BY seq")]


@pytest.fixture
def world(pg_template, monkeypatch, tmp_path) -> World:
    from backend.tests import pg_harness as pgh

    made: list[tuple[str, str]] = []
    try:
        made.append(pgh.clone(pg_template))
        made.append(pgh.clone(pg_template))
        (sname, sdsn), (tname, tdsn) = made
        monkeypatch.setenv("MERGE_TEST_SRC", sdsn)
        monkeypatch.setenv("MERGE_TEST_TGT", tdsn)
        w = World(Side(sname, sdsn), Side(tname, tdsn), tmp_path)
        _seed(w)
        yield w
    finally:
        for name, _dsn in made:
            pgh.drop(pg_template.admin, name)


def _seq(prefix: str, a: int, b: int) -> list[str]:
    return [f"{prefix}{i:02d}" for i in range(a, b + 1)]


def _car(vin: str, dealer: str, cond: str, scraped: str, **kw: Any) -> dict[str, Any]:
    row = {"vin": vin, "dealer_id": dealer, "dealer_name": dealer.upper(), "condition": cond, "scraped_at": scraped,
           "first_seen_at": "2026-08-01T00:00:00.000000Z", "listing_active": 1, "title": f"2025 Ford {vin}",
           "year": 2025, "make": "Ford", "model": "F-150", "price": 40000.0}
    row.update(kw)
    return row


def _triage_entry(dealer: str, verdict: str = "ok", *, eligible: bool = True,
                  kept: dict[str, str] | None = None) -> dict[str, Any]:
    return {"dealer_id": dealer, "verdict": verdict, "reason": f"test {verdict}",
            "reconcile": {"eligible": eligible, "reason": "" if eligible else "8 rows < absolute floor 20",
                          "buckets": {b: {"kept": (kept or {}).get(b)} for b in mds.BUCKETS}}}


# Target-side and source-side differences of the strong-com rows that the cases below read back.
TARGET_EXTRAS: dict[str, dict[str, Any]] = {
    "N02": {"internal_notes": "keep me", "marked_for_review": 1},
    "N06": {"dealership_registry_id": 7},
    "N07": {"window_sticker_url": "https://stickers.example.com/N07"},
    "N08": {"description": "target description"},
    "N09": {"packages": '["Tech"]'},
    "N10": {"price": 39999.0},
    "N11": {"description": "old description"},
    "N13": {"dealer_url": "https://old.example.com"},
}
SOURCE_EXTRAS: dict[str, dict[str, Any]] = {
    "N02": {"internal_notes": None, "marked_for_review": 0},
    "N03": {"first_seen_at": "2026-07-01T00:00:00.000000Z"},
    "N04": {"first_seen_at": "2026-10-01T00:00:00.000000Z"},
    "N05": {"dealership_registry_id": 1},
    "N06": {"dealership_registry_id": 2},
    "N08": {"description": ""},
    "N09": {"packages": "[]"},
    "N10": {"price": None},
    "N11": {"description": "fresh description"},
    "N12": {"description": ""},
    "N13": {"dealer_url": None},
}


def _seed(w: World) -> None:
    T: list[dict[str, Any]] = []
    S: list[dict[str, Any]] = []

    def t(vin: str, dealer: str, cond: str, **kw: Any) -> None:
        T.append(_car(vin, dealer, cond, kw.pop("scraped", OLD), **kw))

    def s(vin: str, dealer: str, cond: str, **kw: Any) -> None:
        S.append(_car(vin, dealer, cond, kw.pop("scraped", FRESH), price=kw.pop("price", 41000.0), **kw))

    # strong-com
    for v in _seq("N", 1, 20):
        t(v, "strong-com", "New", **TARGET_EXTRAS.get(v, {}))
    for v in _seq("U", 1, 12):
        t(v, "strong-com", "Used")
    t("X01", "strong-com", "Used", listing_active=0, listing_removed_at=REMOVED)
    for v in _seq("N", 1, 16):
        s(v, "strong-com", "New", **SOURCE_EXTRAS.get(v, {}))
    s("N21", "strong-com", "New", dealership_registry_id=2)
    s("N22", "strong-com", "New", dealership_registry_id=1)
    s("N23", "strong-com", "New")
    s("N24", "strong-com", "New")
    for v in _seq("U", 1, 10):
        s(v, "strong-com", "Used")
    s("X01", "strong-com", "Used")
    for v in ("OWN_OUT", "MOVE_OK", "MOVE_CONTESTED"):
        s(v, "strong-com", "Used")
    s("MOVE_WEAK", "strong-com", "New")
    # strong2-com
    for v in _seq("S2U", 1, 12) + ["MOVE_OK", "MOVE_CONTESTED", "S2GONE", "SEEN_OOS", "CLAIM_ONLY"]:
        t(v, "strong2-com", "Used")
    for v in _seq("S2U", 1, 14):
        s(v, "strong2-com", "Used")
    # weak-com
    for v in _seq("W", 1, 20) + ["MOVE_WEAK"]:
        t(v, "weak-com", "New")
    for v in _seq("W", 1, 10) + _seq("W", 21, 30):
        s(v, "weak-com", "New")
    # border-com
    for v in _seq("B", 1, 20):
        t(v, "border-com", "New")
    for v in _seq("B", 1, 13) + _seq("B", 21, 27):
        s(v, "border-com", "New")
    # norows-com (out of scope)
    for v in _seq("R", 1, 15):
        t(v, "norows-com", "New")
    t("OWN_OUT", "norows-com", "Used")
    s("R01", "norows-com", "New")
    s("R16", "norows-com", "New")
    s("SEEN_OOS", "norows-com", "Used")  # fresh at a dealer that is not merged: strong2-com's row stays
    for v in _seq("R", 2, 15):
        s(v, "norows-com", "New", scraped=OLD)
    # partial-com, keptbucket-com, inelig-com, small-com
    for v in _seq("P", 1, 20):
        t(v, "partial-com", "Used")
    for v in _seq("P", 1, 13):
        s(v, "partial-com", "Used")
    for prefix, dealer in (("KB", "keptbucket-com"), ("IE", "inelig-com")):
        for v in _seq(prefix, 1, 11):
            t(v, dealer, "Used")
        for v in _seq(prefix, 1, 10):
            s(v, dealer, "Used")
    for v in _seq("SM", 1, 5):
        t(v, "small-com", "Used")
    for v in _seq("SM", 1, 4) + ["SM06"]:
        s(v, "small-com", "Used")
    # out of scope for other reasons
    for v in _seq("T", 1, 5):
        t(v, "targetonly-com", "New")
    t("NT01", "notriage-com", "New")
    s("NT01", "notriage-com", "New")
    s("NT02", "notriage-com", "New")
    t("NW01", "newer-com", "New", scraped=NEWER)
    s("NW01", "newer-com", "New")
    s("NW02", "newer-com", "New")

    for i, row in enumerate(S):
        row["id"] = 5001 + i  # source ids never collide with nor leak into the target's
    w.tgt.insert("cars", T)
    w.src.insert("cars", S)
    for side, url2 in ((w.src, "https://a.example.com"), (w.tgt, "https://b.example.com")):
        side.insert("dealerships", [
            {"id": 1, "name": "Strong", "website_url": "https://strong.example.com", "city": "Charlotte", "state": "NC"},
            {"id": 2, "name": "Two", "website_url": url2, "city": "Charlotte", "state": "NC"},
        ])
    w.src.insert("vin_owner_conflicts", [
        {"vin": "MOVE_CONTESTED", "owner_dealer_id": "strong-com", "claimant_dealer_id": "strong2-com", "seen_at": FRESH},
        {"vin": "CLAIM_ONLY", "owner_dealer_id": "strong-com", "claimant_dealer_id": "strong2-com", "seen_at": FRESH},
    ])
    w.src.insert("nhtsa_vpic_cache", [{"vin": "VPA", "response_json": '{"src": 1}'},
                                      {"vin": "VPB", "response_json": '{"src": 2}'}])
    w.tgt.insert("nhtsa_vpic_cache", [{"vin": "VPA", "response_json": '{"tgt": 1}'}])
    ids = {vin: r["id"] for vin, r in w.tgt.cars().items()}
    w.tgt.insert("listings_grid_cards", [
        {"car_id": ids[v], "row_ver": "x", "content_key": "k", "aux_key": "a", "built_day": 1, "card_json": "{}"}
        for v in ("N01", "N17", "T01")
    ])
    w.tgt.insert("car_comments", [{"car_id": ids["N01"], "user_id": 7, "body": "is this one still there"}])
    comment_id = w.tgt.rows("SELECT id FROM car_comments")[0][0]
    w.tgt.insert("comment_attachments", [{"scope": "car", "comment_id": comment_id, "user_id": 7,
                                          "storage_key": "k1", "mime_type": "image/png", "width": 1, "height": 1,
                                          "byte_size": 10}])
    w.tgt.insert("dealer_comments", [{"dealership_id": 1, "user_id": 7, "body": "friendly"}])
    w.tgt.insert("user_search_history", [{"user_id": 7, "filters_json": '{"make": "Ford"}'}])
    w.tgt.insert("user_hidden_dealers", [{"user_id": 7, "dealer_id": "weak-com"}])

    tri = w.tmp / "scan_s0"
    tri.mkdir()
    (tri / "triage.json").write_text(json.dumps({"started": SINCE, "dealers": [
        _triage_entry("strong-com"), _triage_entry("strong2-com"), _triage_entry("weak-com"),
        _triage_entry("border-com"),
        _triage_entry("partial-com", "thin"), _triage_entry("keptbucket-com", "inaccurate", kept={"used": "low_bucket"}),
        _triage_entry("inelig-com", eligible=False), _triage_entry("small-com"),
        _triage_entry("norows-com", "no_rows", eligible=False), _triage_entry("newer-com"),
    ]}))
    w.triage = str(w.tmp / "scan_s*" / "triage.json")


def _user_tables(side: Side) -> dict[str, list[str]]:
    return {t: side.table(t) for t in USER_TABLES}


@pytest.mark.pg
def test_scope_rules(world: World, capsys) -> None:
    path = world.scope()
    scope = json.loads(path.read_text())
    d = scope["dealers"]
    assert scope["since"] == "2026-11-03T17:59:55"
    assert {k for k, v in d.items() if v["in_scope"]} == IN_SCOPE
    assert d["norows-com"]["reason"] == "verdict_no_rows"
    assert d["targetonly-com"]["reason"] == "not_in_source"
    assert d["notriage-com"]["reason"] == "not_in_triage"
    assert d["newer-com"]["reason"] == "target_newer"
    strong = d["strong-com"]["buckets"]
    assert (strong["new"]["target_active"], strong["new"]["fresh"], strong["new"]["reseen"]) == (20, 21, 16)
    assert strong["new"]["coverage"] == 0.8 and strong["new"]["retire_unseen"]
    assert (strong["used"]["target_active"], strong["used"]["reseen"], strong["used"]["unseen"]) == (12, 10, 2)
    assert d["strong-com"]["retire_unseen"] is True
    assert d["strong-com"]["retire_unseen_buckets"] == {"new": True, "used": True, "unknown": True}
    s2 = d["strong2-com"]["buckets"]["used"]
    assert (s2["target_active"], s2["reseen"], s2["moved_elsewhere"], s2["unseen"]) == (17, 14, 2, 1)
    assert d["strong2-com"]["retire_unseen"] is True
    assert d["strong2-com"]["retire_unseen_buckets"] == {"new": False, "used": True, "unknown": False}
    assert d["strong2-com"]["claims_in_run"] == 2
    weak = d["weak-com"]["buckets"]["new"]
    assert not weak["retire_unseen"] and weak["why"] == "coverage 0.476 < 0.7"
    assert d["weak-com"]["retire_unseen"] is False
    assert d["border-com"]["buckets"]["new"]["why"] == "coverage 0.650 < 0.7" and not d["border-com"]["retire_unseen"]
    assert d["partial-com"]["buckets"]["used"]["why"] == "size ratio 0.650 < 0.8"
    assert d["keptbucket-com"]["buckets"]["used"]["why"] == "triage kept the used bucket (low_bucket)"
    assert d["inelig-com"]["buckets"]["used"]["why"].startswith("triage reconcile not eligible")
    assert d["small-com"]["buckets"]["used"]["why"] == "5 target rows < 10"
    assert scope["rule"]["min_coverage"] == 0.7 and scope["rule"]["scanner_coverage_floor"] == 0.5
    # the freshness numbers behind in/out are in the file
    assert d["strong-com"]["source"]["newest"] == FRESH.replace(" ", "T") and d["strong-com"]["target"]["newest"] == OLD
    assert d["newer-com"]["target"]["rows_since_scan"] == 1
    # frozen: never overwritten
    assert mds.main(world.args("--compute-scope", "--scope", str(path), "--triage", world.triage)) == mds.EXIT_REFUSED
    excluded = json.loads(world.scope("scope2.json", "--exclude-dealer", "small-com").read_text())
    assert excluded["dealers"]["small-com"]["reason"] == "excluded_by_operator"
    narrow = json.loads(world.scope("scope4.json", "--verdicts", "ok").read_text())
    assert narrow["rule"]["verdicts_in_scope"] == ["ok"]
    assert narrow["dealers"]["partial-com"]["reason"] == "verdict_thin"
    assert narrow["dealers"]["keptbucket-com"]["reason"] == "verdict_inaccurate"
    assert {k for k, v in narrow["dealers"].items() if v["in_scope"]} == IN_SCOPE - {"partial-com", "keptbucket-com"}
    assert mds.main(world.args("--compute-scope", "--scope", str(world.tmp / "s5.json"), "--triage", world.triage,
                               "--verdicts", "ok,no_rows")) == mds.EXIT_REFUSED
    looser = json.loads(world.scope("scope3.json", "--min-coverage", "0.6").read_text())
    assert looser["dealers"]["border-com"]["retire_unseen"] is True
    assert looser["dealers"]["border-com"]["retire_unseen_buckets"] == {"new": True, "used": False, "unknown": False}
    out = capsys.readouterr()
    assert world.tgt.dsn not in out.out + out.err and world.src.dsn not in out.out + out.err


@pytest.mark.pg
def test_dry_run_counts_exactly_and_writes_nothing(world: World) -> None:
    before, tables = world.tgt.snapshot(), world.tgt.tables()
    src_before = world.src.snapshot()
    detail = world.tmp / "detail.csv"
    plan = world.plan(world.scope(), "--detail-out", str(detail))
    for key, want in EXPECTED_TOTALS.items():
        assert plan["totals"].get(key, 0) == want, key
    assert plan["kept_target_cells"] == EXPECTED_KEPT_CELLS
    assert plan["dealers"]["strong-com"] == {
        "insert": 4, "keep_rows": 4, "move_in": 1, "reactivate": 1, "ref_guarded_dealership_registry_id": 2,
        "retire_new": 4, "retire_used": 2, "skip_contested_in_run": 1, "skip_target_owner_capture_weak": 1,
        "skip_target_owner_out_of_scope": 1, "update": 27,
    }
    assert plan["dealers"]["strong2-com"] == {"insert": 2, "move_out": 1, "retire_used": 1, "seen_by_claim": 1,
                                              "update": 12}
    assert plan["dealers"]["weak-com"] == {"insert": 10, "kept_unseen_new": 10, "update": 10}
    assert plan["dealers"]["border-com"] == {"insert": 7, "kept_unseen_new": 7, "update": 13}
    assert set(plan["dealers"]) == IN_SCOPE
    disk = plan["disk_estimate_bytes"]
    assert set(disk) == {"cars", "backup", "versions", "vpic"} and disk["cars"] > 0 and disk["backup"] > 0
    # the rows a person reads before an apply: moves, retirements and skipped conflicts, with their dealers
    with detail.open(newline="") as fh:
        rows = {(r["kind"], r["vin"]): (r["dealer"], r["other_dealer"], r["bucket"]) for r in csv.DictReader(fh)}
    assert rows == {
        ("move", "MOVE_OK"): ("strong-com", "strong2-com", "used"),
        **{("retire", v): ("strong-com", "", "new") for v in _seq("N", 17, 20)},
        **{("retire", v): ("strong-com", "", "used") for v in ("U11", "U12")},
        ("retire", "S2GONE"): ("strong2-com", "", "used"),
        ("skip_target_owner_out_of_scope", "OWN_OUT"): ("strong-com", "norows-com", "used"),
        ("skip_contested_in_run", "MOVE_CONTESTED"): ("strong-com", "strong2-com", "used"),
        ("skip_target_owner_capture_weak", "MOVE_WEAK"): ("strong-com", "weak-com", "new"),
    }
    assert world.tgt.snapshot() == before and world.tgt.tables() == tables
    assert world.src.snapshot() == src_before


@pytest.mark.pg
def test_dry_run_and_scope_sessions_are_read_only(world: World) -> None:
    import psycopg

    for role in ("source", "target"):
        db = mds.open_db("MERGE_TEST_SRC" if role == "source" else "MERGE_TEST_TGT", role, read_only=True)
        try:
            with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
                db.conn.execute("UPDATE cars SET price = 1")
        finally:
            mds._close(db)
        assert db.local  # the test clusters are unix-socket only


@pytest.mark.pg
def test_apply_semantics(world: World) -> None:
    before = world.tgt.cars()
    snap = world.tgt.snapshot()
    users = _user_tables(world.tgt)
    src_before = world.src.snapshot()
    max_id = max(r["id"] for r in before.values())
    assert world.apply(world.scope()) == 0
    after = world.tgt.cars()
    # update keeps the id and takes the source values; admin columns stay; first_seen_at is the earlier
    assert after["N01"]["id"] == before["N01"]["id"]
    assert (after["N01"]["price"], after["N01"]["scraped_at"]) == (41000.0, FRESH)
    assert (after["N02"]["internal_notes"], after["N02"]["marked_for_review"]) == ("keep me", 1)
    assert after["N03"]["first_seen_at"] == "2026-07-01T00:00:00.000000Z"
    assert after["N04"]["first_seen_at"] == "2026-08-01T00:00:00.000000Z"
    # keep rule: an empty source value never replaces a non-empty target value; a non-empty one wins
    assert after["N07"]["window_sticker_url"] == "https://stickers.example.com/N07"
    assert after["N08"]["description"] == "target description"
    assert after["N09"]["packages"] == '["Tech"]'
    assert after["N10"]["price"] == 39999.0 and after["N10"]["scraped_at"] == FRESH
    assert after["N11"]["description"] == "fresh description"
    assert after["N12"]["description"] == ""  # both empty: the source's spelling stands
    assert after["N13"]["dealer_url"] is None  # state columns take the source value even when empty
    # dealership id 1 means the same row on both sides; id 2 does not, and a NULL source id keeps the target's
    assert after["N05"]["dealership_registry_id"] == 1
    assert after["N06"]["dealership_registry_id"] == 7
    assert after["N21"]["dealership_registry_id"] is None and after["N22"]["dealership_registry_id"] == 1
    # inserts get target ids
    new_vins = (["N21", "N22", "N23", "N24", "S2U13", "S2U14", "SM06"] + _seq("W", 21, 30) + _seq("B", 21, 27))
    assert all(v not in before and after[v]["id"] > max_id for v in new_vins)
    assert all(after[v]["id"] < 5001 for v in new_vins)
    assert len(after) == len(before) + 24
    # retirements only where the scope says retire_unseen
    for v in _seq("N", 17, 20) + ["U11", "U12", "S2GONE"]:
        assert after[v]["listing_active"] == 0 and after[v]["listing_removed_at"], v
    for v in _seq("W", 11, 20) + _seq("B", 14, 20) + _seq("P", 14, 20) + ["KB11", "IE11", "SM05"]:
        assert after[v]["listing_active"] == 1 and snap[after[v]["id"]] == snap[before[v]["id"]], v
    assert (after["X01"]["listing_active"], after["X01"]["listing_removed_at"]) == (1, None)
    # ownership
    assert (after["MOVE_OK"]["id"], after["MOVE_OK"]["dealer_id"]) == (before["MOVE_OK"]["id"], "strong-com")
    for v, owner in (("MOVE_CONTESTED", "strong2-com"), ("MOVE_WEAK", "weak-com"), ("OWN_OUT", "norows-com"),
                     ("SEEN_OOS", "strong2-com"), ("CLAIM_ONLY", "strong2-com")):
        assert after[v]["dealer_id"] == owner and after[v]["listing_active"] == 1, v
    now = world.tgt.snapshot()
    untouched = ("MOVE_CONTESTED", "MOVE_WEAK", "OWN_OUT", "NT01", "NW01", "SEEN_OOS", "CLAIM_ONLY")
    for v in untouched + tuple(_seq("R", 1, 15) + _seq("T", 1, 5)):
        assert now[before[v]["id"]] == snap[before[v]["id"]], v
    assert "R16" not in after and "NT02" not in after and "NW02" not in after
    # side tables
    assert dict(world.tgt.rows("SELECT vin, response_json FROM nhtsa_vpic_cache")) == {"VPA": '{"tgt": 1}',
                                                                                     "VPB": '{"src": 2}'}
    cards = {r[0] for r in world.tgt.rows("SELECT car_id FROM listings_grid_cards")}
    assert cards == {before["T01"]["id"]}
    assert _user_tables(world.tgt) == users
    assert world.src.snapshot() == src_before
    (run_id,) = world.run_ids()
    assert world.tgt.rows(f"SELECT count(*) FROM merge_backup_{run_id}")[0][0] == 99 + 1 + 7
    assert world.tgt.rows(f"SELECT count(*) FROM merge_inserted_{run_id}")[0][0] == 24
    assert world.tgt.rows(f"SELECT vin FROM merge_vpic_{run_id}") == [("VPB",)]
    assert world.tgt.rows("SELECT status FROM merge_snapshot_runs") == [("applied",)]
    recorded = json.loads(world.tgt.rows("SELECT totals_json FROM merge_snapshot_runs")[0][0])
    assert recorded["kept_target_cells"] == EXPECTED_KEPT_CELLS and recorded["update"] == 99


@pytest.mark.pg
def test_second_apply_is_a_no_op(world: World, capsys) -> None:
    scope = world.scope()
    assert world.apply(scope) == 0
    snap, tables = world.tgt.snapshot(), world.tgt.tables()
    vpic = world.tgt.table("nhtsa_vpic_cache")
    capsys.readouterr()
    assert world.apply(scope) == 0
    assert "nothing to apply" in capsys.readouterr().out
    assert world.tgt.snapshot() == snap and world.tgt.tables() == tables
    assert world.tgt.table("nhtsa_vpic_cache") == vpic
    assert len(world.run_ids()) == 1
    second = world.plan(scope)["totals"]
    assert second.get("update", 0) == second.get("insert", 0) == second["retire"] == second["vpic_inserts"] == 0
    assert second.get("keep_rows", 0) == 0
    assert second["unchanged"] == 99 + 1 + 24


@pytest.mark.pg
def test_source_wins_turns_the_keep_rule_off(world: World) -> None:
    scope = world.scope()
    plan = world.plan(scope, "--source-wins")
    assert plan["kept_target_cells"] == {} and plan["totals"].get("keep_rows", 0) == 0
    assert plan["totals"]["update"] == 99 and plan["totals"]["insert"] == 24
    reviewed = world.tmp / "reviewed.json"
    assert mds.main(world.args("--dry-run", "--scope", str(scope), "--plan-out", str(reviewed))) == 0
    # the reviewed plan kept four cells, so an apply that keeps none is not the reviewed apply
    assert world.apply(scope, "--source-wins", "--expect-plan", str(reviewed)) == mds.EXIT_REFUSED
    assert world.run_ids() == []
    assert world.apply(scope, "--source-wins") == 0
    after = world.tgt.cars()
    assert after["N07"]["window_sticker_url"] is None and after["N10"]["price"] is None
    assert after["N08"]["description"] == "" and after["N09"]["packages"] == "[]"
    assert after["N02"]["internal_notes"] == "keep me"  # admin edits are not part of the keep rule
    assert world.plan(scope)["totals"]["unchanged"] == 99 + 1 + 24  # and a re-apply still writes nothing


@pytest.mark.pg
def test_a_target_row_scraped_after_the_source_row_is_never_overwritten(world: World) -> None:
    scope = world.scope()
    world.tgt.run("UPDATE cars SET scraped_at = %s, price = 1 WHERE vin = 'N03'", (NEWER,))
    plan = world.plan(scope)
    assert plan["totals"]["update"] == 98 and plan["totals"]["unchanged"] == 1
    assert world.apply(scope) == 0
    row = world.tgt.cars()["N03"]
    assert (row["price"], row["scraped_at"]) == (1.0, NEWER)


@pytest.mark.pg
def test_expect_db_is_required_and_must_match(world: World) -> None:
    scope = world.scope()
    snap, tables = world.tgt.snapshot(), world.tgt.tables()
    assert mds.main(world.args("--apply", "--scope", str(scope), "--expect-db", "railway")) == mds.EXIT_REFUSED
    assert mds.main(world.args("--apply", "--scope", str(scope))) == mds.EXIT_REFUSED
    assert world.tgt.snapshot() == snap and world.tgt.tables() == tables


@pytest.mark.pg
def test_expect_plan_pins_the_reviewed_counts(world: World) -> None:
    scope = world.scope()
    reviewed = world.tmp / "reviewed.json"
    assert mds.main(world.args("--dry-run", "--scope", str(scope), "--plan-out", str(reviewed))) == 0
    world.tgt.run("UPDATE cars SET listing_active = 0 WHERE vin = 'W11'")  # the target moved after the review
    assert world.apply(scope, "--expect-plan", str(reviewed)) == mds.EXIT_REFUSED
    assert world.run_ids() == []
    world.tgt.run("UPDATE cars SET listing_active = 1 WHERE vin = 'W11'")
    assert world.apply(scope, "--expect-plan", str(reviewed)) == 0


@pytest.mark.pg
def test_a_sequence_behind_the_table_refuses_the_apply(world: World) -> None:
    scope = world.scope()
    snap, tables = world.tgt.snapshot(), world.tgt.tables()
    world.tgt.run("SELECT setval(pg_get_serial_sequence('cars', 'id'), 1)")
    assert world.apply(scope) == mds.EXIT_REFUSED
    assert world.tgt.snapshot() == snap and world.tgt.tables() == tables


@pytest.mark.pg
def test_the_advisory_lock_serialises_applies(world: World) -> None:
    scope = world.scope()
    snap, tables = world.tgt.snapshot(), world.tgt.tables()
    with world.tgt.connect() as holder:
        holder.execute("SELECT pg_advisory_lock(%s)", (mds.ADVISORY_LOCK_KEY,))
        assert world.apply(scope) == mds.EXIT_REFUSED
        assert world.rollback("20261109153000_a1b2c3") == mds.EXIT_REFUSED
    assert world.tgt.snapshot() == snap and world.tgt.tables() == tables
    assert world.apply(scope) == 0  # the lock died with the other session


@pytest.mark.pg
def test_a_failure_midway_leaves_no_trace(world: World) -> None:
    scope = world.scope()
    world.tgt.run("CREATE FUNCTION merge_test_boom() RETURNS trigger LANGUAGE plpgsql AS "
                  "$$ BEGIN IF NEW.vin = 'U05' THEN RAISE EXCEPTION 'boom'; END IF; RETURN NEW; END $$")
    world.tgt.run("CREATE TRIGGER merge_test_boom BEFORE UPDATE ON cars FOR EACH ROW EXECUTE FUNCTION merge_test_boom()")
    snap, tables = world.tgt.snapshot(), world.tgt.tables()
    vpic, cards = world.tgt.table("nhtsa_vpic_cache"), world.tgt.table("listings_grid_cards")
    assert world.apply(scope) == mds.EXIT_ERROR
    # one transaction: no backup tables, no run row, no vpic rows, no deleted cards
    assert world.tgt.snapshot() == snap and world.tgt.tables() == tables
    assert world.tgt.table("nhtsa_vpic_cache") == vpic and world.tgt.table("listings_grid_cards") == cards


@pytest.mark.pg
def test_rollback_restores_byte_identical_rows(world: World) -> None:
    scope = world.scope()
    snap = world.tgt.snapshot()
    vpic = world.tgt.table("nhtsa_vpic_cache")
    users = _user_tables(world.tgt)
    t01_card = world.tgt.rows("SELECT g::text FROM listings_grid_cards g JOIN cars c ON c.id = g.car_id "
                              "WHERE c.vin = 'T01'")
    assert world.apply(scope) == 0
    (run_id,) = world.run_ids()
    assert world.tgt.snapshot() != snap
    assert mds.main(["--target-dsn-env", "MERGE_TEST_TGT", "--rollback", run_id, "--expect-db", "wrong"]) == mds.EXIT_REFUSED
    assert mds.main(["--target-dsn-env", "MERGE_TEST_TGT", "--rollback", run_id]) == mds.EXIT_REFUSED
    assert world.rollback(run_id) == 0
    assert world.tgt.snapshot() == snap
    assert world.tgt.table("nhtsa_vpic_cache") == vpic
    assert _user_tables(world.tgt) == users
    assert world.tgt.rows("SELECT g::text FROM listings_grid_cards g JOIN cars c ON c.id = g.car_id "
                          "WHERE c.vin = 'T01'") == t01_card
    assert world.tgt.rows("SELECT status FROM merge_snapshot_runs") == [("rolled_back",)]
    assert world.rollback(run_id) == mds.EXIT_REFUSED  # only once


@pytest.mark.pg
def test_rollback_refuses_drift_and_user_rows_on_inserted_cars(world: World) -> None:
    scope = world.scope()
    snap = world.tgt.snapshot()
    assert world.apply(scope) == 0
    (run_id,) = world.run_ids()
    world.tgt.run("UPDATE cars SET price = 1 WHERE vin = 'N01'")
    assert world.rollback(run_id) == mds.EXIT_REFUSED
    n23 = world.tgt.cars()["N23"]["id"]
    world.tgt.insert("car_comments", [{"car_id": n23, "user_id": 8, "body": "new car comment"}])
    assert world.rollback(run_id, "--allow-drift") == mds.EXIT_REFUSED  # would cascade-delete the comment
    world.tgt.run("DELETE FROM car_comments WHERE car_id = %s", (n23,))
    assert world.rollback(run_id, "--allow-drift") == 0
    assert world.tgt.snapshot() == snap


@pytest.mark.pg
def test_only_dealers_pilot_touches_only_that_dealer(world: World) -> None:
    scope = world.scope()
    before = world.tgt.cars()
    snap = world.tgt.snapshot()
    assert mds.main(world.args("--only-dealers", "nosuch-com", "--dry-run", "--scope", str(scope))) == mds.EXIT_REFUSED
    assert world.apply(scope, "--only-dealers", "strong2-com") == 0
    after = world.tgt.cars()
    now = world.tgt.snapshot()
    changed = {v for v, r in before.items() if now[r["id"]] != snap[r["id"]]}
    assert changed == set(_seq("S2U", 1, 12)) | {"S2GONE"}
    # MOVE_OK's fresh row belongs to strong-com, which is not part of this run: it stays where it was
    assert (after["MOVE_OK"]["listing_active"], after["MOVE_OK"]["dealer_id"]) == (1, "strong2-com")
    assert {"S2U13", "S2U14"} <= set(after) and "N21" not in after
    # a second pilot cannot move a VIN out of a dealer that is not in its run either
    plan = world.plan(scope, "--only-dealers", "strong-com")
    assert plan["totals"]["skip_target_owner_out_of_scope"] == 4 and plan["totals"].get("move_out", 0) == 0
    assert world.apply(scope, "--only-dealers", "strong-com") == 0
    assert world.tgt.cars()["MOVE_OK"]["dealer_id"] == "strong2-com"
    # the full scope finishes the job: strong2-com is now in the run and its capture was strong
    assert world.apply(scope) == 0
    final = world.tgt.cars()
    assert (final["MOVE_OK"]["dealer_id"], final["MOVE_OK"]["listing_active"]) == ("strong-com", 1)
    assert final["W21"]["dealer_id"] == "weak-com"
    # stacked runs roll back newest first, and then the target is exactly what it was
    first, second, third = world.run_ids()
    assert world.rollback(first) == mds.EXIT_REFUSED
    assert world.rollback(third) == 0
    assert world.rollback(second) == 0
    assert world.rollback(first) == 0
    assert world.tgt.snapshot() == snap


@pytest.mark.pg
def test_schema_tolerant_columns(world: World) -> None:
    world.src.run("ALTER TABLE cars DROP COLUMN interior_pano")
    world.tgt.run("ALTER TABLE cars ADD COLUMN merge_test_only text DEFAULT 'target-only'")
    world.tgt.run("UPDATE cars SET interior_pano = 'pano-' || vin")
    scope = world.scope()
    assert world.apply(scope) == 0
    rows = dict(world.tgt.rows("SELECT vin, interior_pano || '|' || merge_test_only FROM cars"))
    assert rows["N01"] == "pano-N01|target-only"
    assert world.tgt.rows("SELECT interior_pano, merge_test_only FROM cars WHERE vin = 'N21'") == [(None, "target-only")]


@pytest.mark.pg
def test_column_type_mismatch_refuses(world: World) -> None:
    scope = world.scope()
    world.src.run("ALTER TABLE cars ALTER COLUMN mpg_city TYPE bigint")
    assert mds.main(world.args("--dry-run", "--scope", str(scope))) == mds.EXIT_REFUSED


@pytest.mark.pg
def test_the_named_columns_exist_in_the_migrated_schema(world: World) -> None:
    cars = {r[0] for r in world.tgt.rows("SELECT column_name FROM information_schema.columns "
                                         "WHERE table_schema = 'public' AND table_name = 'cars'")}
    assert set(mds.REQUIRED_CAR_COLUMNS) | set(mds.TARGET_OWNED_COLUMNS) | set(mds.ALWAYS_SOURCE_COLUMNS) <= cars
    for guard in mds.REF_GUARDS:
        have = {r[0] for r in world.tgt.rows("SELECT column_name FROM information_schema.columns "
                                             "WHERE table_schema = 'public' AND table_name = %s", (guard.table,))}
        assert {guard.column, *guard.travel} <= cars, guard
        assert {"id", *guard.identity} <= have, guard
    columns = mds.plan_columns(mds.table_columns(mds.open_db("MERGE_TEST_SRC", "source", read_only=True), "cars"),
                               mds.table_columns(mds.open_db("MERGE_TEST_TGT", "target", read_only=True), "cars"))
    assert columns.keepable and "window_sticker_url" in columns.keepable and "price" in columns.keepable
    assert "dealer_id" in columns.always and "internal_notes" not in columns.update_set


@pytest.mark.pg
def test_empty_values_in_sql_match_the_documented_set(world: World) -> None:
    rows = world.tgt.rows(
        "SELECT x, " + mds.empty_sql("x", True) + ", " + mds.empty_sql("n", False) + " "
        "FROM (VALUES (NULL::text, NULL::int), (''::text, 0), ('  ', 1), ('[]', 2), ('{}', 3), ('null', 4), "
        "('a', 5), ('0', 6), ('[1]', 7)) v(x, n)")
    assert {r[0]: r[1] for r in rows} == {None: True, "": True, "  ": True, "[]": True, "{}": True, "null": True,
                                          "a": False, "0": False, "[1]": False}
    assert {r[2] for r in rows if r[0] is not None} == {False} and [r[2] for r in rows if r[0] is None] == [True]


@pytest.mark.pg
def test_cleanup_drops_only_verified_names_and_vacuums(world: World, monkeypatch) -> None:
    _git_checkout_or_skip()
    fresh_name = f"zz_merge_old_backup_{secrets.token_hex(4)}"
    world.tgt.run(f"CREATE TABLE {fresh_name} (id bigint)")
    assert "cars_backup_capacity_20260718" in world.tgt.tables()  # V001 creates it
    base = ["--target-dsn-env", "MERGE_TEST_TGT", "--cleanup"]
    monkeypatch.setattr(mds, "CLEANUP_DROP_TABLES", (fresh_name, "cars_backup_capacity_20260718"))
    assert mds.main([*base, "--expect-db", world.tgt.name]) == mds.EXIT_REFUSED
    assert {fresh_name, "cars_backup_capacity_20260718"} <= world.tgt.tables()
    monkeypatch.setattr(mds, "CLEANUP_DROP_TABLES", (fresh_name,))
    assert mds.main(base) == mds.EXIT_REFUSED  # --expect-db is required
    assert mds.main([*base, "--expect-db", world.tgt.name]) == 0
    tables = world.tgt.tables()
    assert fresh_name not in tables and {"cars", "listings_grid_cards", "cars_backup_capacity_20260718"} <= tables
    analyzed = world.tgt.rows("SELECT last_vacuum IS NOT NULL, last_analyze IS NOT NULL FROM pg_stat_user_tables "
                              "WHERE relname = 'cars'")
    assert analyzed == [(True, True)]
