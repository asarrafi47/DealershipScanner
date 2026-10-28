"""Destructive enrichment scripts stay disarmed (remediation plan P1A.5).

1. ``backend/scripts/backfill_forced_induction_pg.py`` is deleted. One run re-guessed
   ``forced_induction`` from listing text for every NULL row (~125k active cars locally),
   and nothing executable may still point at it.
2. ``enrich_from_dictionary.py --all`` used to overwrite filled values (vPIC drivetrain and
   cylinders, the dealer's engine text) with the EPA CSV pick, and sat behind an
   ``ALLOW_DICTIONARY_OVERWRITE=1`` guard. P8B.5 replaced that guard with fact precedence
   (tests/test_dictionary_enrich_precedence.py); only the CLI plumbing is checked here.
3. ``heal_cylinders_from_vpic.py`` Phase B (forced_induction recompute) clears EPA- and
   VIN-backed labels. P10C.6 limits it to heuristic-sourced labels and drops the
   ``--phase-b`` opt-in (``--skip-fi`` turns it off); Phase A (cylinders) is unchanged.
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
# 2. enrich_from_dictionary --all runs through fact precedence (P8B.5; was an env guard)
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


def _record_enrich_runs(monkeypatch) -> list[bool]:
    """Stub get_conn (in-memory cars) and enrich_car (records fill_all, no EPA/vPIC I/O)."""
    import backend.db.inventory_db as inv_db

    seen: list[bool] = []

    def _fake_enrich_car(car, dry_run=False, fill_all=False, *, use_vpic=True, vpic_ctx=None):
        seen.append(bool(fill_all))
        return {}

    monkeypatch.setattr(efd, "_configure_cli_process", lambda: None)
    monkeypatch.setattr(inv_db, "get_conn", _one_car_conn)
    monkeypatch.setattr(efd, "enrich_car", _fake_enrich_car)
    return seen


def test_enrich_all_runs_without_env_opt_in(monkeypatch):
    """P8B.5: the P1A.5 env guard is gone; per-field precedence decides what --all may refill."""
    monkeypatch.setattr(sys, "argv", ["enrich_from_dictionary.py", "--all", "--dry-run", "--no-vpic"])
    seen = _record_enrich_runs(monkeypatch)

    efd.main()

    assert seen == [True]


def test_enrich_default_gap_fill_never_sets_fill_all(monkeypatch):
    """The default (no --all) never sets fill_all."""
    monkeypatch.setattr(sys, "argv", ["enrich_from_dictionary.py", "--dry-run", "--no-vpic"])
    seen = _record_enrich_runs(monkeypatch)

    efd.main()

    assert seen == [False]


# ---------------------------------------------------------------------------
# 3. heal_cylinders_from_vpic: Phase B skips backed labels (P10C.6)
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


@pytest.mark.parametrize("dry_run", [True, False])
def test_heal_phase_b_runs_by_default_and_flag_is_gone(monkeypatch, heal_mod, heal_calls, dry_run):
    monkeypatch.setattr(sys, "argv", ["heal_cylinders_from_vpic.py"] + (["--dry-run"] if dry_run else []))

    heal_mod.main()

    assert heal_calls == [("A", {"dry_run": dry_run, "limit": None}), ("B", {"dry_run": dry_run})]
    monkeypatch.setattr(sys, "argv", ["heal_cylinders_from_vpic.py", "--phase-b"])
    with pytest.raises(SystemExit):
        heal_mod.main()


def test_heal_skip_fi_runs_phase_a_only(monkeypatch, capsys, heal_mod, heal_calls):
    monkeypatch.setattr(sys, "argv", ["heal_cylinders_from_vpic.py", "--limit", "7", "--skip-fi"])

    heal_mod.main()

    assert heal_calls == [("A", {"dry_run": False, "limit": 7})]
    assert "--skip-fi" in capsys.readouterr().out


def test_heal_default_run_keeps_label_phase_b_would_clear(monkeypatch, sqlite_inventory, heal_mod):
    """SQLite end to end: a 2014 F-150 3.5L V6 (an EcoBoost) labelled Turbocharged by the
    catalog has no turbo text, so the classifier alone would clear it. The label has no
    heuristic provenance, so a default live run (Phase B included) must keep it."""
    vin = "1FTFW1ET5EFA00001"
    sqlite_inventory.add_cars([{
        "vin": vin, "year": 2014, "make": "Ford", "model": "F-150", "trim": "Lariat",
        "cylinders": 6, "engine_l": 3.5, "engine_description": "3.5L V6",
        "fuel_type": "Gasoline", "title": "2014 Ford F-150 Lariat",
        "forced_induction": "Turbocharged", "listing_active": 1,
    }])
    # Phase B skips it as not heuristic-sourced (dry run, no write).
    stats = heal_mod.heal_forced_induction(dry_run=True)
    assert stats["backed"] == 1 and stats["cleared"] == 0

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
