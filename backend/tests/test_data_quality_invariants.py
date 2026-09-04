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
import os
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


LIVE = os.environ.get("DQ_INVARIANTS_LIVE_DB") == "1"


@pytest.mark.skipif(not LIVE, reason="set DQ_INVARIANTS_LIVE_DB=1 to run against Postgres")
def test_live_seeded_regression_in_a_temp_table() -> None:
    """
    Copy ``epa_extended_specs`` into ``pg_temp``, seed ONE impossible row, and run
    the shipped stored-tier SQL against it.

    ``search_path = pg_temp, public`` makes every unqualified reference resolve to
    the temp copy, so the permanent table is unreachable for the duration and the
    seeding cannot touch it. The temp table dies with the session.
    """
    import psycopg

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
