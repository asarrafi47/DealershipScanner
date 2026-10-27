"""
Tests for the data-quality invariant suite.

Three things are worth testing here and they are not the same thing:

1. The PREDICATES — the pure helpers that decide whether a value is a violation.
   A predicate that quietly stops matching is an invariant that quietly stops
   existing, which is exactly the failure mode this whole suite was built for.

2. The GATE — baseline comparison and exit codes. The suite is only useful if a
   new violation turns into a non-zero exit. This includes the fail-closed rule
   that an invariant with no baseline entry FAILS rather than passes.

3. The SEEDED REGRESSION, end to end. Two versions:
   * a monkeypatched one that always runs, proving main() returns 1 when a count
     rises above baseline;
   * a live-Postgres one (opt-in via ``DQ_INVARIANTS_LIVE_DB=1``) that copies
     ``epa_extended_specs`` into a TEMP table, inserts one impossible row, points
     ``search_path`` at ``pg_temp`` and runs the REAL stored-tier SQL against it.
     The permanent table cannot be reached from that session, so the seeding
     carries no risk of mutating anything, and the SQL under test is the shipped
     SQL rather than a paraphrase of it.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.scripts import data_quality_invariants as dq  # noqa: E402


# ---------------------------------------------------------------------------
# 1. Predicates
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text, expected",
    [
        ("Ram 2500 Laramie", True),
        ("Ford F-250 Super Duty", True),
        ("Silverado 3500HD", True),
        ("Chevrolet Express 2500", True),
        ("Ram 1500 Laramie", False),
        ("Toyota 4Runner TRD Sport", False),
        ("Honda Civic", False),
    ],
)
def test_heavy_duty_pattern(text: str, expected: bool) -> None:
    """The 8,000 lb curb ceiling must not fire on trucks that legitimately exceed it."""
    assert bool(dq._HEAVY_DUTY_RE.search(text)) is expected


@pytest.mark.parametrize(
    "text, expected",
    [
        ("C63 AMG", True),
        ("M3 Competition", True),
        ("Charger SRT Hellcat", True),
        ("Ram 1500 TRX", True),
        ("Civic Type R", True),
        ("Model S Plaid", True),
        ("Accord LX", False),
        ("Camry LE", False),
        ("330i", False),
        ("Grand Cherokee Laredo", False),
    ],
)
def test_performance_badge_pattern(text: str, expected: bool) -> None:
    """Sub-4s is legitimate only on a badged car; the allowlist has to actually match one."""
    assert bool(dq._PERF_BADGE_RE.search(text)) is expected


@pytest.mark.parametrize(
    "fuel, expected",
    [
        ("Electric", True),
        ("EV", True),
        ("Plug-in Hybrid", True),
        ("Plug In Hybrid", True),
        ("PHEV", True),
        ("Gasoline", False),
        ("Hybrid", False),
        ("Gas/Electric Hybrid", False),
        ("Diesel", False),
        ("", False),
    ],
)
def test_plugin_capable(fuel: str, expected: bool) -> None:
    """battery_kwh / ev_range are only real on something with a plug."""
    assert dq._plugin_capable(fuel) is expected


def test_fuel_bucket_normalizes_case_and_spacing_only() -> None:
    """
    The fuel filter matches the stored literal. The card/filter invariant must
    forgive case and whitespace and NOTHING else, or a real relabel slips past.
    """
    assert dq._fuel_bucket("  Gasoline ") == dq._fuel_bucket("GASOLINE")
    assert dq._fuel_bucket("Gas / Electric") == dq._fuel_bucket("gas  /  electric")
    assert dq._fuel_bucket("Gasoline") != dq._fuel_bucket("Regular Gasoline")
    assert dq._fuel_bucket("Gasoline") != dq._fuel_bucket("Hybrid")


def test_close_is_exact_for_the_curb_equals_tow_check() -> None:
    """curb == tow is a collision, not an approximation: 6000/6000 fires, 6000/6001 does not."""
    assert dq._close(6000.0, 6000.0, rel=0.0) is True
    assert dq._close(6000.0, 6001.0, rel=0.0) is False
    assert dq._close(None, 6000.0) is False


def test_close_tolerates_float_noise_for_provenance_matching() -> None:
    """AI-provenance matching compares floats from two tables; exactness would miss them."""
    assert dq._close(3.0999999, 3.1) is True
    assert dq._close(3.1, 3.4) is False


def test_norm_words_makes_trim_matching_punctuation_insensitive() -> None:
    assert dq._norm_words("Grand Touring") == " grand touring "
    assert dq._norm_words("S-Line/quattro").strip() == "s line quattro"


# ---------------------------------------------------------------------------
# 2. The gate
# ---------------------------------------------------------------------------


def _result(iid: str, count: int, tier: str = "stored") -> dq.InvariantResult:
    return dq.InvariantResult(
        id=iid, title=iid, detects="d", impossible_because="w", tier=tier, count=count
    )


def test_compare_flags_only_increases() -> None:
    baseline = {"a": {"count": 10}, "b": {"count": 10}, "c": {"count": 10}}
    verdicts = {
        v["id"]: v["status"]
        for v in dq.compare(
            [_result("a", 10), _result("b", 11), _result("c", 3)], baseline
        )
    }
    assert verdicts == {"a": "OK", "b": "REGRESSION", "c": "IMPROVED"}


def test_compare_fails_closed_when_an_invariant_has_no_baseline() -> None:
    """A brand-new check must force a human to record its count, not pass silently."""
    verdicts = dq.compare([_result("brand_new", 0)], {})
    assert verdicts[0]["status"] == "NO_BASELINE"
    assert verdicts[0]["status"] in dq._FAIL_STATUSES


def test_compare_reports_a_broken_check_as_failing_not_passing() -> None:
    """A check that errored did not pass; it did not run."""
    broken = _result("boom", -1)
    broken.error = "UndefinedColumn: no such column"
    verdicts = dq.compare([broken], {"boom": {"count": 0}})
    assert verdicts[0]["status"] == "ERROR"
    assert verdicts[0]["status"] in dq._FAIL_STATUSES


def test_compare_tags_a_zero_baseline_regression_as_a_new_defect_class() -> None:
    """A brand-new defect class (baseline 0) must be distinguishable from routine
    backlog growth of an existing nonzero baseline -- same REGRESSION-shaped event,
    different severity."""
    verdicts = {
        v["id"]: v["status"]
        for v in dq.compare(
            [_result("fresh_defect", 3), _result("old_backlog", 101)],
            {"fresh_defect": {"count": 0}, "old_backlog": {"count": 100}},
        )
    }
    assert verdicts == {"fresh_defect": "NEW_DEFECT_CLASS", "old_backlog": "REGRESSION"}
    assert "NEW_DEFECT_CLASS" in dq._FAIL_STATUSES


def test_compare_zero_baseline_staying_zero_is_ok_not_a_new_defect_class() -> None:
    verdicts = dq.compare([_result("still_clean", 0)], {"still_clean": {"count": 0}})
    assert verdicts[0]["status"] == "OK"


def test_slack_allows_a_declared_amount_of_drift() -> None:
    baseline = {"a": {"count": 10}}
    assert dq.compare([_result("a", 12)], baseline, slack=2)[0]["status"] == "OK"
    assert dq.compare([_result("a", 13)], baseline, slack=2)[0]["status"] == "REGRESSION"


def test_write_and_load_baseline_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "baseline.json"
    dq.write_baseline(
        path, [_result("a", 7), _result("b", 0, tier="rendered")], note="hi", approver="alice",
    )
    loaded = dq.load_baseline(path)
    assert loaded["a"]["count"] == 7
    assert loaded["b"]["tier"] == "rendered"
    written = json.loads(path.read_text())
    assert written["note"] == "hi"
    assert written["approver"] == "alice"


def test_write_baseline_appends_a_history_line_and_never_overwrites_it(tmp_path: Path) -> None:
    """The history log records every rewrite, not just the latest one."""
    path = tmp_path / "baseline.json"
    dq.write_baseline(path, [_result("a", 7)], note="first cut", approver="alice")
    dq.write_baseline(path, [_result("a", 9)], note="human fixed 2 rows", approver="bob")

    hist_path = tmp_path / "baseline_history.jsonl"
    lines = [json.loads(ln) for ln in hist_path.read_text().splitlines() if ln.strip()]
    assert len(lines) == 2
    assert lines[0]["approver"] == "alice" and lines[0]["note"] == "first cut"
    assert lines[1]["approver"] == "bob" and lines[1]["note"] == "human fixed 2 rows"
    # the second write's diff against the first baseline is recorded
    assert lines[1]["changed"] == [{"id": "a", "old_count": 7, "new_count": 9}]
    # and the baseline FILE itself holds only the latest state
    assert json.loads(path.read_text())["invariants"]["a"]["count"] == 9


def test_main_refuses_write_baseline_without_note_or_approver(wired, tmp_path: Path) -> None:
    base = tmp_path / "b.json"
    assert dq.main(["--baseline", str(base), "--write-baseline", "--approver", "alice"]) == 2
    assert not base.exists()
    assert dq.main(["--baseline", str(base), "--write-baseline", "--note", "why"]) == 2
    assert not base.exists()
    assert dq.main(
        ["--baseline", str(base), "--write-baseline", "--note", "why", "--approver", "alice"]
    ) == 0
    assert base.exists()


def test_report_json_is_tagged_partial_when_render_cohorts_is_set(
    wired, tmp_path: Path
) -> None:
    """Item 6: a sampled run's JSON must self-identify as partial coverage, not look
    like a normal full run to any downstream reader (including the admin hub)."""
    base = tmp_path / "b.json"
    assert dq.main(
        ["--baseline", str(base), "--write-baseline", "--note", "seed", "--approver", "alice"]
    ) == 0
    rep = tmp_path / "r.json"
    dq.main(["--baseline", str(base), "--render-cohorts", "5", "--json", str(rep)])
    payload = json.loads(rep.read_text())
    assert payload["sample_mode"] is True
    assert payload["coverage"] == "partial"

    rep2 = tmp_path / "r2.json"
    dq.main(["--baseline", str(base), "--json", str(rep2)])
    payload2 = json.loads(rep2.read_text())
    assert payload2["sample_mode"] is False
    assert payload2["coverage"] == "full"


def test_write_baseline_omits_errored_invariants(tmp_path: Path) -> None:
    """A baseline of -1 would make the broken check pass forever."""
    broken = _result("boom", -1)
    broken.error = "boom"
    path = tmp_path / "baseline.json"
    dq.write_baseline(path, [_result("ok", 1), broken], note="")
    assert set(dq.load_baseline(path)) == {"ok"}


def test_load_baseline_of_a_missing_or_corrupt_file_is_empty_not_permissive(
    tmp_path: Path,
) -> None:
    assert dq.load_baseline(tmp_path / "nope.json") == {}
    bad = tmp_path / "bad.json"
    bad.write_text("{ not json")
    assert dq.load_baseline(bad) == {}


# ---------------------------------------------------------------------------
# 3a. Seeded regression, end to end (monkeypatched runners)
# ---------------------------------------------------------------------------


class _FakeConn:
    """Records the session statements the script issues, then does nothing."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    class _Cur:
        def __init__(self, owner: "_FakeConn") -> None:
            self.owner = owner

        def execute(self, sql, *a):
            self.owner.statements.append(sql)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def cursor(self):
        return _FakeConn._Cur(self)

    def commit(self):
        pass

    def close(self):
        pass


@pytest.fixture()
def wired(monkeypatch: pytest.MonkeyPatch):
    """main() wired to synthetic counts we control."""
    counts: dict[str, int] = {"curb_equals_tow": 100, "rendered_curb_equals_tow": 5}

    conn = _FakeConn()
    monkeypatch.setattr(dq, "open_readonly_connection", lambda: conn)
    monkeypatch.setattr(
        dq, "run_stored_tier",
        lambda c: [_result("curb_equals_tow", counts["curb_equals_tow"])],
    )
    monkeypatch.setattr(
        dq, "run_rendered_tier",
        lambda c, **kw: (
            [_result("rendered_curb_equals_tow", counts["rendered_curb_equals_tow"], "rendered")],
            0.0,
        ),
    )
    return counts


def test_clean_run_against_its_own_baseline_exits_zero(wired, tmp_path: Path) -> None:
    base = tmp_path / "b.json"
    assert dq.main(["--baseline", str(base), "--write-baseline", "--note", "test baseline", "--approver", "tester"]) == 0
    assert dq.main(["--baseline", str(base)]) == 0


def test_seeded_regression_exits_non_zero_and_names_the_invariant(
    wired, tmp_path: Path
) -> None:
    """The whole point: one new violation since the baseline must fail the run."""
    base = tmp_path / "b.json"
    report = tmp_path / "r.json"
    assert dq.main(["--baseline", str(base), "--write-baseline", "--note", "test baseline", "--approver", "tester"]) == 0

    wired["curb_equals_tow"] += 1  # <- the seeded regression

    assert dq.main(["--baseline", str(base), "--json", str(report)]) == 1
    payload = json.loads(report.read_text())
    assert payload["failing"] == ["curb_equals_tow"]
    row = next(i for i in payload["invariants"] if i["id"] == "curb_equals_tow")
    assert row["baseline"] == 100 and row["count"] == 101 and row["delta"] == 1


def test_regression_in_the_rendered_tier_also_fails(wired, tmp_path: Path) -> None:
    """A backlog that only moves at render time is still a regression."""
    base = tmp_path / "b.json"
    assert dq.main(["--baseline", str(base), "--write-baseline", "--note", "test baseline", "--approver", "tester"]) == 0
    wired["rendered_curb_equals_tow"] += 1
    assert dq.main(["--baseline", str(base)]) == 1


def test_sql_only_run_cannot_pass_on_a_rendered_tier_regression(
    wired, tmp_path: Path
) -> None:
    """
    --sql-only skips the rendered tier. It must not therefore report the rendered
    invariants as OK; they are simply absent from the report.
    """
    base = tmp_path / "b.json"
    assert dq.main(["--baseline", str(base), "--write-baseline", "--note", "test baseline", "--approver", "tester"]) == 0
    wired["rendered_curb_equals_tow"] += 99
    rep = tmp_path / "r.json"
    assert dq.main(["--baseline", str(base), "--sql-only", "--json", str(rep)]) == 0
    payload = json.loads(rep.read_text())
    assert payload["rendered_tier_ran"] is False
    assert [i["id"] for i in payload["invariants"]] == ["curb_equals_tow"]


def test_write_baseline_is_refused_when_the_counts_would_be_partial(
    wired, tmp_path: Path
) -> None:
    """
    A baseline recorded from a sampled or stored-only run is worse than no
    baseline: it would silently license a real regression.
    """
    base = tmp_path / "b.json"
    assert dq.main(["--baseline", str(base), "--write-baseline", "--sql-only"]) == 2
    assert not base.exists()
    assert dq.main(
        ["--baseline", str(base), "--write-baseline", "--render-cohorts", "10"]
    ) == 2
    assert not base.exists()


def test_missing_baseline_file_fails_the_run(wired, tmp_path: Path) -> None:
    assert dq.main(["--baseline", str(tmp_path / "absent.json")]) == 1


def test_connection_failure_exits_two_not_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    """A suite that could not run must never be mistaken for a suite that passed."""

    def _boom():
        raise RuntimeError("no database")

    monkeypatch.setattr(dq, "open_readonly_connection", _boom)
    assert dq.main([]) == 2


def test_the_session_is_put_in_read_only_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Read-only is enforced by the server, not by convention. Assert the statement
    that does it is actually issued.
    """
    import backend.scripts.data_quality_invariants as mod

    conn = _FakeConn()

    class _FakePsycopg:
        @staticmethod
        def connect(url):
            return conn

    monkeypatch.setitem(sys.modules, "psycopg", _FakePsycopg)
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql://x/y")
    assert mod.open_readonly_connection() is conn
    assert any(
        "TRANSACTION READ ONLY" in s.upper() for s in conn.statements
    ), conn.statements


# ---------------------------------------------------------------------------
# 3b. Seeded regression against the real SQL, on a real Postgres
# ---------------------------------------------------------------------------


@pytest.mark.pg
def test_live_seeded_regression_in_a_temp_table(pg_db) -> None:
    """
    Runs on the harness's throwaway clone (``pg_db`` points INVENTORY_DATABASE_URL at
    it), so the permanent table is a migration-built empty one.

    Copy ``epa_extended_specs`` into ``pg_temp``, seed ONE impossible row, and run
    the shipped stored-tier SQL against it.

    ``search_path = pg_temp, public`` makes every unqualified reference resolve to
    the temp copy, so the permanent table is unreachable for the duration and the
    seeding cannot touch it. The temp table dies with the session.
    """
    import psycopg

    # A migration-built clone has an empty table, and a baseline of 0 reads as a
    # NEW_DEFECT_CLASS, not a REGRESSION: seed one standing defect of each kind first.
    with pg_db.connect() as seed:
        seed.execute("INSERT INTO public.epa_master (id, year, make, model) VALUES (1, 2023, 'Honda', 'Accord')")
        seed.execute(
            "INSERT INTO public.epa_extended_specs "
            "(epa_master_id, year, make, model, trim, curb_weight_lb, tow_capacity_lb, zero_to_60_sec) "
            "VALUES (1, 2023, 'Honda', 'Accord', 'EX', 9998, 9998, 3.2)"
        )
    conn = psycopg.connect(dq._inventory_url())
    try:
        with conn.cursor() as cur:
            cur.execute("SET search_path = pg_temp, public")
            cur.execute(
                "CREATE TEMP TABLE epa_extended_specs AS "
                "SELECT * FROM public.epa_extended_specs"
            )
        conn.commit()

        before = dq.run_stored_tier(conn)
        by_id = {r.id: r for r in before}
        assert by_id["curb_equals_tow"].error is None

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pg_temp.epa_extended_specs "
                "(year, make, model, trim, curb_weight_lb, tow_capacity_lb, zero_to_60_sec) "
                "VALUES (2024, 'Honda', 'Accord', 'LX', 9999, 9999, 3.1)"
            )
        conn.commit()

        after = {r.id: r for r in dq.run_stored_tier(conn)}
        # One row, three separate invariants: the tow figure in the curb column,
        # the four-ton light-duty sedan, and the 3.1 s Accord LX.
        assert after["curb_equals_tow"].count == by_id["curb_equals_tow"].count + 1
        assert (
            after["curb_over_light_duty_ceiling"].count
            == by_id["curb_over_light_duty_ceiling"].count + 1
        )
        assert (
            after["zero_to_60_sub_four_no_badge"].count
            == by_id["zero_to_60_sub_four_no_badge"].count + 1
        )

        baseline = {r.id: {"count": r.count} for r in before}
        verdicts = {v["id"]: v["status"] for v in dq.compare(list(after.values()), baseline)}
        assert verdicts["curb_equals_tow"] == "REGRESSION"
        assert verdicts["curb_over_light_duty_ceiling"] == "REGRESSION"
        assert verdicts["zero_to_60_sub_four_no_badge"] == "REGRESSION"

        # ...and the permanent table never saw any of it.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM public.epa_extended_specs "
                "WHERE make='Honda' AND model='Accord' AND curb_weight_lb=9999"
            )
            assert cur.fetchone()[0] == 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 4. Precedence invariants (P8A.1)
# ---------------------------------------------------------------------------

_PRECEDENCE_IDS = (
    "vpic_electrification_fuel_conflict",
    "vpic_drive_wheels_conflict",
    "vpic_cylinders_conflict",
    "provenance_stale_drivetrain",
    "sticker_vs_vpic_conflict",
    "engine_text_vs_vpic_displacement",
    "catalog_tiebreak_over_explicit_end",
)


def test_every_sql_invariant_has_a_baseline_entry() -> None:
    """The nightly gate fails closed on an id with no baseline: shipping one without
    its entry would turn the next nightly run red."""
    baseline = dq.load_baseline(dq.DEFAULT_BASELINE_PATH)
    # The catalog invariants wait for the P10B.4 provenance run: a baseline of 0 written before
    # the backfill would be a false clean. Remove each id from the exemption as its baseline lands.
    missing = [s.id for s in dq.SQL_INVARIANTS
               if s.id not in baseline and s.id not in dq.CATALOG_INVARIANT_IDS]
    assert missing == []
    assert set(_PRECEDENCE_IDS) <= {s.id for s in dq.SQL_INVARIANTS}


def test_precedence_invariants_are_read_only_and_use_postgres_word_boundaries() -> None:
    import re

    specs = {s.id: s for s in dq.SQL_INVARIANTS}
    for iid in _PRECEDENCE_IDS:
        spec = specs[iid]
        assert spec.example_sql and spec.denominator_sql, iid
        for sql in (spec.count_sql, spec.example_sql, spec.denominator_sql):
            assert not re.search(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE)\b", sql, re.I), iid
            assert "\\b" not in sql, f"{iid}: \\b is a backspace in Postgres; use \\y, \\m, \\M"
        assert "LIMIT 20" in spec.example_sql, iid


def test_only_the_tiebreak_report_is_informational() -> None:
    info = [s.id for s in dq.SQL_INVARIANTS if s.informational]
    assert info == ["catalog_tiebreak_over_explicit_end"]


def test_informational_invariant_never_fails_the_gate() -> None:
    def info(count: int) -> dq.InvariantResult:
        r = _result("report", count)
        r.informational = True
        return r

    for baseline in ({}, {"report": {"count": 1}}):
        v = dq.compare([info(500)], baseline)[0]
        assert v["status"] == "INFO"
        assert v["status"] not in dq._FAIL_STATUSES
    assert dq.compare([info(500)], {"report": {"count": 1}})[0]["delta"] == 499
    # a broken informational check is still an ERROR: it must not hide
    broken = info(-1)
    broken.error = "boom"
    assert dq.compare([broken], {})[0]["status"] == "ERROR"


@pytest.mark.pg
def test_precedence_invariants_count_seeded_rows(pg_db) -> None:
    import json as _json

    def decode(**kw) -> str:
        row = {"ElectrificationLevel": "", "DriveType": "", "FuelTypePrimary": "Gasoline",
               "FuelTypeSecondary": "", "EngineCylinders": "", "DisplacementL": ""}
        row.update(kw)
        return _json.dumps({"Results": [row]})

    def prov(**fields) -> str:
        return _json.dumps({k: {"source": s, **({"detail": d} if d else {})} for k, (s, d) in fields.items()})

    cars = [
        # id, vin, fuel, drive, cyl, engine text, provenance, catalog id
        # (a) PHEV decode on a plain Hybrid label; a BEV decode on a matching label is fine
        (1, "1" * 17, "Hybrid", "AWD", None, None, None, None),
        (2, "2" * 17, "Electric", "AWD", None, None, None, None),
        # (b) FWD against a RWD decode; AWD against a 4x2 decode; AWD 4MATIC against 4WD is the same wheels
        (3, "3" * 17, "Gasoline", "FWD", None, None, None, None),
        (4, "4" * 17, "Gasoline", "AWD", None, None, None, None),
        (5, "5" * 17, "Gasoline", "AWD 4MATIC", None, None, None, None),
        # (c) 8 stored vs 6 decoded; 6 vs 6 agrees
        (6, "6" * 17, "Gasoline", "RWD", 8, "3.0L I6", None, None),
        (7, "7" * 17, "Gasoline", "RWD", 6, "3.0L I6", None, None),
        # (d) tiebreak label on a column the feed reverted to 2WD, catalog says rear-wheel
        (8, "8" * 17, "Gasoline", "2WD", None, None,
         prov(drivetrain=("epa_catalog_tiebreak", "DecodeVINValuesBatch; feed said '2WD'")), 1),
        # (e) a sticker-sourced end the decode contradicts
        (9, "9" * 17, "Gasoline", "FWD", None, None, prov(drivetrain=("oem_window_sticker", None)), None),
        # (f) 4.0L text against a 5.5L decode; 2.0L against 2.05L is rounding
        (10, "A" * 17, "Gasoline", "RWD", None, "4.0L V8", None, None),
        (11, "B" * 17, "Gasoline", "RWD", None, "2.0L", None, None),
        # (g) an explicit dealer FWD replaced by the catalog's RWD
        (12, "C" * 17, "Gasoline", "RWD", None, None,
         prov(drivetrain=("epa_catalog_tiebreak", "DecodeVINValuesBatch; feed said 'FWD'")), 1),
    ]
    decodes = {
        1: decode(ElectrificationLevel="PHEV (Plug-in Hybrid Electric Vehicle)", DriveType="AWD/All-Wheel Drive"),
        2: decode(ElectrificationLevel="BEV (Battery Electric Vehicle)", DriveType="AWD/All-Wheel Drive",
                  FuelTypePrimary="Electric"),
        3: decode(DriveType="RWD/Rear-Wheel Drive"),
        4: decode(DriveType="4x2"),
        5: decode(DriveType="4WD/4-Wheel Drive/4x4"),
        6: decode(DriveType="RWD/Rear-Wheel Drive", EngineCylinders="6", DisplacementL="3.0"),
        7: decode(DriveType="RWD/Rear-Wheel Drive", EngineCylinders="6", DisplacementL="3.0"),
        8: decode(),
        9: decode(DriveType="RWD/Rear-Wheel Drive"),
        10: decode(DriveType="RWD/Rear-Wheel Drive", DisplacementL="5.5"),
        11: decode(DriveType="RWD/Rear-Wheel Drive", DisplacementL="2.05"),
        12: decode(),
    }
    with pg_db.connect() as seed:
        seed.execute("INSERT INTO epa_master (id, year, make, model, drive) VALUES (1, 2023, 'BMW', 'X5', 'Rear-Wheel Drive')")
        for cid, vin, fuel, drive, cyl, text, pv, cat in cars:
            seed.execute(
                "INSERT INTO cars (id, vin, listing_active, fuel_type, drivetrain, cylinders, engine_description, "
                "spec_source_json, epa_master_id) VALUES (%s, %s, 1, %s, %s, %s, %s, %s, %s)",
                (cid, vin, fuel, drive, cyl, text, pv, cat),
            )
            seed.execute("INSERT INTO nhtsa_vpic_cache (vin, response_json) VALUES (%s, %s)", (vin, decodes[cid]))
    conn = dq.open_readonly_connection()
    try:
        by_id = {r.id: r for r in dq.run_stored_tier(conn) if r.id in _PRECEDENCE_IDS}
    finally:
        conn.close()
    assert {k: (r.error, r.count) for k, r in by_id.items()} == {
        "vpic_electrification_fuel_conflict": (None, 1),
        "vpic_drive_wheels_conflict": (None, 3),   # cars 3, 4 and 9 (the sticker row is also a drive conflict)
        "vpic_cylinders_conflict": (None, 1),
        "provenance_stale_drivetrain": (None, 1),
        "sticker_vs_vpic_conflict": (None, 1),
        "engine_text_vs_vpic_displacement": (None, 1),
        "catalog_tiebreak_over_explicit_end": (None, 1),
    }
    assert by_id["vpic_cylinders_conflict"].examples[0]["engine_text_split"] == "agrees_with_vpic"
    assert by_id["catalog_tiebreak_over_explicit_end"].informational is True


def test_catalog_invariants_are_registered_read_only_and_awaiting_baseline() -> None:
    import re

    specs = {s.id: s for s in dq.SQL_INVARIANTS}
    assert set(dq.CATALOG_INVARIANT_IDS) <= set(specs)
    baseline = dq.load_baseline(dq.DEFAULT_BASELINE_PATH)
    for iid in dq.CATALOG_INVARIANT_IDS:
        spec = specs[iid]
        assert not spec.informational, iid
        assert spec.example_sql and spec.denominator_sql and "LIMIT 20" in spec.example_sql, iid
        for sql in (spec.count_sql, spec.example_sql, spec.denominator_sql):
            assert not re.search(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE)\b", sql, re.I), iid
        # never recorded as 0 before the P10B.4 run; a recorded value is a reviewed edit
        assert iid not in baseline or baseline[iid]["count"] >= 0


def test_catalog_invariant_without_baseline_fails_closed() -> None:
    v = dq.compare([_result("epa_master_missing_provenance", 0)], {})[0]
    assert v["status"] == "NO_BASELINE"
    assert v["status"] in dq._FAIL_STATUSES


def test_catalog_invariant_regression_through_the_monkeypatched_runner(wired, tmp_path: Path) -> None:
    # same harness as the seeded-regression tests: an increase over a recorded baseline exits 1
    base = [_result(i, 0) for i in dq.CATALOG_INVARIANT_IDS]
    bp = tmp_path / "b.json"
    dq.write_baseline(bp, base, note="test", approver="test")
    verdicts = dq.compare([_result("epa_master_cross_source_twin", 3)] + base[:3], dq.load_baseline(bp))
    assert [v["status"] for v in verdicts][0] in ("REGRESSION", "NEW_DEFECT_CLASS")


@pytest.mark.pg
def test_catalog_invariants_count_seeded_defects(pg_db) -> None:
    import psycopg

    with pg_db.connect() as c:
        c.execute("INSERT INTO public.epa_master (id, year, make, model, source, natural_key) "
                  "VALUES (1, 2023, 'Honda', 'Accord', 'dictionary_epa', 'vid:1'), "
                  "(2, 2023, 'Honda', 'Accord', 'epa_vehicles_csv', 'vid:2'), "
                  "(3, 2023, 'Honda', 'Civic', NULL, NULL)")
    conn = psycopg.connect(dq._inventory_url())
    try:
        got = {r.id: r.count for r in dq.run_stored_tier(conn) if r.id in dq.CATALOG_INVARIANT_IDS}
    finally:
        conn.close()
    assert got["epa_master_missing_provenance"] == 1
    assert got["epa_master_cross_source_twin"] == 1
    assert got["epa_extended_specs_orphan"] == 0
    assert got["epa_link_dangling_all_listings"] == 0
