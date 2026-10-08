"""Destructive enrichment scripts stay disarmed (remediation plan P1A.5).

1. ``backend/scripts/backfill_forced_induction_pg.py`` is deleted. One run re-guessed
   ``forced_induction`` from listing text for every NULL row (~125k active cars locally),
   and nothing executable may still point at it.
2. ``enrich_from_dictionary.py --all`` overwrites filled values (vPIC drivetrain and
   cylinders, the dealer's engine text) with the EPA CSV pick. It refuses unless
   ``ALLOW_DICTIONARY_OVERWRITE=1`` is exported, and it refuses before ``.env`` is loaded
   and before any DB connection. The gap-filling default is unchanged.
3. ``heal_cylinders_from_vpic.py`` Phase B (forced_induction recompute) clears EPA- and
   VIN-backed labels. It runs only with ``--phase-b``; Phase A (cylinders) is unchanged.
"""
from __future__ import annotations

import importlib
import importlib.util
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from backend.dictionary import enrich_from_dictionary as efd

REPO_ROOT = Path(__file__).resolve().parents[2]
DELETED_SCRIPT = "backend/scripts/backfill_forced_induction_pg.py"
DELETED_STEM = "backfill_forced_induction"


# ---------------------------------------------------------------------------
# 1. backfill_forced_induction_pg.py is gone and unreferenced
# ---------------------------------------------------------------------------


def test_backfill_forced_induction_script_is_deleted():
    assert not (REPO_ROOT / DELETED_SCRIPT).exists()
    assert importlib.util.find_spec("backend.scripts.backfill_forced_induction_pg") is None


def test_no_executable_reference_to_deleted_backfill_script():
    """Only history may name it: docs/ and CHANGELOG.md record what was removed and why.

    Code, scripts, deploy files, CI, Dockerfiles and plists must not, or a cron line or an
    importlib string would fail at run time (or, worse, someone would restore the file).
    """
    if shutil.which("git") is None:
        pytest.skip("git not available")
    inside = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=REPO_ROOT, capture_output=True, text=True, check=False,
    )
    if inside.returncode != 0 or inside.stdout.strip() != "true":
        pytest.skip("not a git checkout")
    hits = subprocess.run(
        [
            "git", "grep", "-n", "-I", "-F", DELETED_STEM, "--", ".",
            ":(exclude)docs/**",
            ":(exclude)CHANGELOG.md",
            ":(exclude)backend/tests/test_destructive_script_guards.py",
        ],
        cwd=REPO_ROOT, capture_output=True, text=True, check=False,
    )
    # git grep: 0 = matches, 1 = no match, anything else = error.
    assert hits.returncode in (0, 1), hits.stderr
    assert hits.stdout == "", f"references to the deleted script remain:\n{hits.stdout}"


# ---------------------------------------------------------------------------
# 2. enrich_from_dictionary --all needs ALLOW_DICTIONARY_OVERWRITE=1
# ---------------------------------------------------------------------------


_CARS_DDL = """
CREATE TABLE cars (
    id INTEGER PRIMARY KEY, vin TEXT, year INTEGER, make TEXT, model TEXT, trim TEXT,
    transmission TEXT, drivetrain TEXT, fuel_type TEXT, cylinders INTEGER, engine_l REAL,
    mpg_city INTEGER, mpg_highway INTEGER, body_style TEXT, engine_description TEXT,
    exterior_color TEXT, forced_induction TEXT, spec_source_json TEXT, description TEXT,
    title TEXT
)
"""


def _one_car_conn() -> sqlite3.Connection:
    """In-memory ``cars`` with one row that has a gap (transmission NULL)."""
    conn = sqlite3.connect(":memory:")
    conn.execute(_CARS_DDL)
    conn.execute(
        "INSERT INTO cars (id, vin, year, make, model, trim, drivetrain, cylinders) "
        "VALUES (1, '1FTFW1ET5EFA00001', 2014, 'Ford', 'F-150', 'Lariat', '4WD', 6)"
    )
    conn.commit()
    return conn


@pytest.fixture
def no_db_no_dotenv(monkeypatch):
    """Fail the test if main() loads .env / chdirs or opens an inventory connection."""
    import backend.db.inventory_db as inv_db

    calls: list[str] = []

    def _no_configure():
        calls.append("configure")
        raise AssertionError("_configure_cli_process ran before the --all guard")

    def _no_conn(*_a, **_k):
        calls.append("get_conn")
        raise AssertionError("an inventory connection was opened before the --all guard")

    monkeypatch.setattr(efd, "_configure_cli_process", _no_configure)
    monkeypatch.setattr(inv_db, "get_conn", _no_conn)
    return calls


@pytest.mark.parametrize(
    "argv",
    [["--all"], ["--all", "--dry-run"], ["--all", "--no-vpic"], ["--dry-run", "--all", "--no-vpic"]],
)
def test_enrich_all_refuses_without_env(monkeypatch, capsys, no_db_no_dotenv, argv):
    monkeypatch.setenv(efd.ALLOW_OVERWRITE_ENV, "")
    monkeypatch.setattr(sys, "argv", ["enrich_from_dictionary.py", *argv])
    cwd = os.getcwd()

    with pytest.raises(SystemExit) as exc:
        efd.main()

    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "--all is disabled" in err
    assert "ALLOW_DICTIONARY_OVERWRITE=1" in err
    assert no_db_no_dotenv == []
    assert os.getcwd() == cwd


@pytest.mark.parametrize("value", ["0", "true", "yes", "on", "2", "11", "1 x"])
def test_enrich_all_refuses_unless_env_is_exactly_one(monkeypatch, capsys, no_db_no_dotenv, value):
    monkeypatch.setenv(efd.ALLOW_OVERWRITE_ENV, value)
    monkeypatch.setattr(sys, "argv", ["enrich_from_dictionary.py", "--all"])

    with pytest.raises(SystemExit) as exc:
        efd.main()

    assert exc.value.code == 2
    assert "--all is disabled" in capsys.readouterr().err
    assert no_db_no_dotenv == []


def _record_enrich_runs(monkeypatch) -> list[bool]:
    """Stub get_conn (in-memory cars) and enrich_car (records fill_all, no EPA/vPIC I/O)."""
    import backend.db.inventory_db as inv_db

    seen: list[bool] = []

    def _fake_enrich_car(car, dry_run=False, fill_all=False, *, use_vpic=True):
        seen.append(bool(fill_all))
        return {}

    monkeypatch.setattr(efd, "_configure_cli_process", lambda: None)
    monkeypatch.setattr(inv_db, "get_conn", _one_car_conn)
    monkeypatch.setattr(efd, "enrich_car", _fake_enrich_car)
    return seen


def test_enrich_all_runs_with_env_opt_in(monkeypatch):
    monkeypatch.setenv(efd.ALLOW_OVERWRITE_ENV, "1")
    monkeypatch.setattr(sys, "argv", ["enrich_from_dictionary.py", "--all", "--dry-run", "--no-vpic"])
    seen = _record_enrich_runs(monkeypatch)

    efd.main()

    assert seen == [True]


def test_enrich_default_gap_fill_unchanged_without_env(monkeypatch):
    """The non-destructive default (no --all) needs no opt-in and never sets fill_all."""
    monkeypatch.setenv(efd.ALLOW_OVERWRITE_ENV, "")
    monkeypatch.setattr(sys, "argv", ["enrich_from_dictionary.py", "--dry-run", "--no-vpic"])
    seen = _record_enrich_runs(monkeypatch)

    efd.main()

    assert seen == [False]


# ---------------------------------------------------------------------------
# 3. heal_cylinders_from_vpic: Phase B only with --phase-b
# ---------------------------------------------------------------------------


@pytest.fixture
def heal_mod():
    return importlib.import_module("backend.scripts.heal_cylinders_from_vpic")


@pytest.fixture
def heal_calls(monkeypatch, heal_mod):
    calls: list[tuple[str, dict]] = []

    def _fake_a(**kw):
        calls.append(("A", kw))
        return {"rows": 0}

    def _fake_b(**kw):
        calls.append(("B", kw))
        return {"rows": 0}

    monkeypatch.setattr(heal_mod, "heal_cylinders", _fake_a)
    monkeypatch.setattr(heal_mod, "heal_forced_induction", _fake_b)
    return calls


@pytest.mark.parametrize("argv", [[], ["--dry-run"], ["--limit", "5"], ["--skip-fi"], ["--dry-run", "--skip-fi"]])
def test_heal_phase_b_off_by_default(monkeypatch, capsys, heal_mod, heal_calls, argv):
    monkeypatch.setattr(sys, "argv", ["heal_cylinders_from_vpic.py", *argv])

    heal_mod.main()

    assert [name for name, _ in heal_calls] == ["A"]
    assert "--phase-b" in capsys.readouterr().out


def test_heal_phase_a_defaults_unchanged(monkeypatch, heal_mod, heal_calls):
    monkeypatch.setattr(sys, "argv", ["heal_cylinders_from_vpic.py", "--dry-run", "--limit", "7"])

    heal_mod.main()

    assert heal_calls == [("A", {"dry_run": True, "limit": 7})]


@pytest.mark.parametrize("dry_run", [True, False])
def test_heal_phase_b_runs_only_with_flag(monkeypatch, heal_mod, heal_calls, dry_run):
    argv = ["heal_cylinders_from_vpic.py", "--phase-b"] + (["--dry-run"] if dry_run else [])
    monkeypatch.setattr(sys, "argv", argv)

    heal_mod.main()

    assert heal_calls == [("A", {"dry_run": dry_run, "limit": None}), ("B", {"dry_run": dry_run})]


def test_heal_phase_b_and_skip_fi_conflict_runs_nothing(monkeypatch, heal_mod, heal_calls):
    monkeypatch.setattr(sys, "argv", ["heal_cylinders_from_vpic.py", "--phase-b", "--skip-fi"])

    with pytest.raises(SystemExit) as exc:
        heal_mod.main()

    assert exc.value.code == 2
    assert heal_calls == []


def test_heal_default_run_keeps_label_phase_b_would_clear(monkeypatch, sqlite_inventory, heal_mod):
    """SQLite end to end: a 2014 F-150 3.5L V6 (an EcoBoost) labelled Turbocharged by the
    catalog has no turbo text, so Phase B would clear it. A default live run must not."""
    vin = "1FTFW1ET5EFA00001"
    sqlite_inventory.add_cars([{
        "vin": vin, "year": 2014, "make": "Ford", "model": "F-150", "trim": "Lariat",
        "cylinders": 6, "engine_l": 3.5, "engine_description": "3.5L V6",
        "fuel_type": "Gasoline", "title": "2014 Ford F-150 Lariat",
        "forced_induction": "Turbocharged", "listing_active": 1,
    }])
    # Precondition: this row is one Phase B damages (dry run, no write).
    assert heal_mod.heal_forced_induction(dry_run=True)["cleared"] == 1

    # Phase A runs for real against the SQLite row; only the vPIC HTTP call is stubbed.
    monkeypatch.setattr(heal_mod, "_decode_batch", lambda vins: {v: 6 for v in vins})
    monkeypatch.setattr(sys, "argv", ["heal_cylinders_from_vpic.py"])

    heal_mod.main()

    conn = sqlite3.connect(str(sqlite_inventory.path))
    try:
        row = conn.execute("SELECT forced_induction, cylinders FROM cars WHERE vin = ?", (vin,)).fetchone()
    finally:
        conn.close()
    assert row == ("Turbocharged", 6)
