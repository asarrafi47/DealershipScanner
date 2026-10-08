"""synthesize_recipes CLI: the set gate records only what the run saves (P1B.8).

The gate (``recipe_validation.gate_recipes``) used to run, and write its record,
before the CLI asked whether it would save anything: ``--dry-run`` appended a
discovery.md entry and set ``scan_hints.recipe_status``, and a dealer that keeps
its healthy recipe had that status overwritten by a verdict on a set nobody
saved. ``gate_recipes(record=False)`` returns the same verdict and writes nothing.

Every place the CLI can write points under ``tmp_path``: the recipe cache
(``recipes.RECIPES_DIR``), the dealer-log root (``recipe_validation.LOG_ROOT``)
and a per-test SQLite ``dealer_recipes`` store wired through ``recipe_store._conn``.
Fetching, fingerprinting, synthesis and the set validation are stubs; the gate,
its discovery.md writer, ``record_recipe_status`` and the store are real.
"""
from __future__ import annotations

import importlib
import json
import signal
import sqlite3
from pathlib import Path

import pytest

import backend.scanner.recipes as rec
from backend.scanner import recipe_store
from backend.scanner import recipe_validation as rv
from backend.scanner.recipes import PAGINATION_CARSCOMMERCE, EndpointRecipe

DEALER = "austintoyota-com"
NAME = "Austin Toyota"
ORIGIN = "https://www.austintoyota.com"
CC_URL = "https://websites-search.api.carscommerce.inc/api/v1/listings/1/search"
STAMP = "2026-10-08T12:00:00Z"


def _cc_recipe(**kw) -> EndpointRecipe:
    body = {"page": 1, "perPage": 100, "filters": {"status": ["publish"]}, "facets": ["year"]}
    return EndpointRecipe(dealer_id=DEALER, url=CC_URL, method="POST", content_type="application/json",
                          post_template=json.dumps(body), pagination=PAGINATION_CARSCOMMERCE,
                          provider_hint="carscommerce", auth_headers={"x-api-key": "k"}, **kw)


def _report(verdict: str) -> rv.RecipeValidationReport:
    if verdict == "reject":
        return rv.RecipeValidationReport(
            dealer_id=DEALER, verdict="reject", reasons=["one_condition: only new rows while the site sells used"],
            vins_total=50, per_condition={"new": 50}, site_total=90, coverage=0.56,
            flags={"one_condition": True}, stamp=STAMP)
    return rv.RecipeValidationReport(dealer_id=DEALER, verdict="ok", vins_total=50, per_condition={"new": 30, "used": 20},
                                     site_total=50, coverage=1.0, stamp=STAMP)


@pytest.fixture
def sr(monkeypatch):
    """The script module. It chdirs to the repo root at import and main() installs
    a SIGALRM handler; both are put back after the test."""
    monkeypatch.chdir(Path.cwd())
    previous_alarm = signal.getsignal(signal.SIGALRM)
    module = importlib.import_module("backend.scripts.synthesize_recipes")
    yield module
    signal.alarm(0)
    signal.signal(signal.SIGALRM, previous_alarm)


@pytest.fixture
def world(tmp_path, monkeypatch, sr):
    """Stubs the network half of the CLI and points every writer under tmp_path.

    Returns a namespace: ``verdict`` (set it before running), ``validated``
    (dealer ids the set validation saw), ``hint_writes`` (every
    ``set_scan_hints`` call), and the tmp paths."""
    recipes_dir = tmp_path / "recipes"
    logs = tmp_path / "dealer_logs"
    store_db = tmp_path / "recipe_store.db"

    monkeypatch.setattr(rec, "RECIPES_DIR", recipes_dir)
    monkeypatch.setattr(rv, "LOG_ROOT", logs)
    monkeypatch.setenv("RECIPES_DB_DISABLED", "")
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "")
    monkeypatch.setattr(recipe_store, "_conn", lambda: sqlite3.connect(store_db))
    monkeypatch.setattr(recipe_store, "_table_ready", False)

    class W:
        verdict = "ok"
        validated: list[str] = []
        hint_writes: list[tuple[str, dict]] = []

    W.recipes_dir, W.logs, W.store_db = recipes_dir, logs, store_db

    real_set_scan_hints = recipe_store.set_scan_hints

    def spy_set_scan_hints(dealer_id, hints, **kw):
        W.hint_writes.append((dealer_id, dict(hints)))
        return real_set_scan_hints(dealer_id, hints, **kw)

    monkeypatch.setattr(recipe_store, "set_scan_hints", spy_set_scan_hints)

    def fake_validate_recipe_set(dealer_id, recipes, fetch=None, **kw):
        W.validated.append(dealer_id)
        return _report(W.verdict)

    monkeypatch.setattr(rv, "validate_recipe_set", fake_validate_recipe_set)
    monkeypatch.setattr(sr, "classify_dealer", lambda *a, **k: {})
    monkeypatch.setattr(sr, "_gather_html", lambda url: ("<html>carscommerce</html>", "carscommerce"))
    monkeypatch.setattr(sr, "is_synthesizable", lambda platform: True)
    monkeypatch.setattr(sr, "synthesize_recipes", lambda did, url, html, platform: [_cc_recipe()])
    monkeypatch.setattr(sr, "validate_recipe", lambda *a, **k: 50)
    return W


def _run(sr, *extra: str) -> int:
    return sr.main(["--dealer-id", DEALER, "--url", ORIGIN, "--name", NAME, *extra])


def _files_under(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()) if root.exists() else []


def _stored_hints(world) -> dict | None:
    """scan_hints straight from the tmp store file (None: no row / no table)."""
    if not world.store_db.exists():
        return None
    con = sqlite3.connect(world.store_db)
    try:
        row = con.execute("SELECT scan_hints FROM dealer_recipes WHERE dealer_id = ?", (DEALER,)).fetchone()
    except sqlite3.OperationalError:
        return None
    finally:
        con.close()
    return json.loads(row[0]) if row and row[0] else None


def _seed_healthy_recipe(world) -> bytes:
    """A live recipe that replayed with rich coverage, plus the status its own
    validation left: the state a sweep must not disturb without --force."""
    healthy = _cc_recipe(vehicle_rows=180, total_count=180, last_ok_at=1_790_000_000.0, saved_at=1_790_000_000.0,
                         field_coverage={"price": 0.98, "trim": 0.97, "exterior_color": 0.95})
    rec.save_recipes(DEALER, [healthy])
    assert recipe_store.set_scan_hints(DEALER, {"recipe_status": "ok",
                                                "recipe_validation": {"verdict": "ok", "stamp": "2026-09-28T09:00:00Z",
                                                                      "context": "dealer_pipeline"}})
    world.hint_writes.clear()
    return (world.recipes_dir / f"{DEALER}.json").read_bytes()


# ── gate_recipes(record=) ──────────────────────────────────────────────────────

@pytest.mark.parametrize("verdict", ["ok", "reject"])
def test_gate_without_record_returns_the_verdict_and_writes_nothing(world, verdict):
    world.verdict = verdict
    recipes = [_cc_recipe()]
    kept, report = rv.gate_recipes(DEALER, recipes, base_url=ORIGIN, dealer_name=NAME, context="synthesize_recipes",
                                   log_root=world.logs, record=False)
    assert report.verdict == verdict
    assert kept == ([] if verdict == "reject" else recipes)
    assert _files_under(world.logs) == []
    assert world.hint_writes == [] and _stored_hints(world) is None


@pytest.mark.parametrize("verdict", ["ok", "reject"])
def test_gate_records_by_default(world, verdict):
    world.verdict = verdict
    kept, report = rv.gate_recipes(DEALER, [_cc_recipe()], base_url=ORIGIN, dealer_name=NAME,
                                   context="synthesize_recipes", log_root=world.logs)
    assert report.verdict == verdict
    assert f"recipe validation (synthesize_recipes) — {verdict.upper()}" in \
        (world.logs / DEALER / "discovery.md").read_text(encoding="utf-8")
    assert (world.logs / "_learning" / "errors_index.md").exists() is (verdict == "reject")
    assert [d for d, _ in world.hint_writes] == [DEALER]
    assert _stored_hints(world)["recipe_status"] == report.status


# ── the CLI ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("verdict, note", [
    ("ok", "dry-run (would save 1 recipe(s))"),
    ("reject", "rejected by validation: one_condition"),
])
def test_dry_run_writes_no_discovery_log_and_never_sets_scan_hints(world, sr, capsys, verdict, note):
    world.verdict = verdict
    assert _run(sr, "--dry-run") == 0
    out = capsys.readouterr().out
    assert world.validated == [DEALER], "the dry run still gates the set"
    assert note in out and "(dry-run: no recipes written)" in out
    assert _files_under(world.logs) == []
    assert world.hint_writes == []
    assert _stored_hints(world) is None
    assert _files_under(world.recipes_dir) == []


@pytest.mark.parametrize("extra", [[], ["--dry-run"]], ids=["save-run", "dry-run"])
@pytest.mark.parametrize("verdict, note", [
    ("ok", "healthy recipe already exists"),
    ("reject", "rejected by validation: one_condition"),
])
def test_healthy_recipe_without_force_leaves_recipe_status_unchanged(world, sr, capsys, extra, verdict, note):
    recipe_file = _seed_healthy_recipe(world)
    before = recipe_store.get_scan_hints(DEALER)
    assert before["recipe_status"] == "ok"
    world.verdict = verdict

    assert _run(sr, *extra) == 0
    assert note in capsys.readouterr().out
    assert world.validated == [DEALER]
    assert recipe_store.get_scan_hints(DEALER) == before
    assert world.hint_writes == []
    assert _files_under(world.logs) == []
    assert (world.recipes_dir / f"{DEALER}.json").read_bytes() == recipe_file


def test_real_save_writes_the_recipe_discovery_log_and_status(world, sr, capsys):
    world.verdict = "ok"
    assert _run(sr) == 0
    assert "browser-free" in capsys.readouterr().out
    (saved,) = rec.load_recipes(DEALER)
    assert saved.url == CC_URL and saved.vehicle_rows == 50 and saved.last_ok_at > 0
    log = (world.logs / DEALER / "discovery.md").read_text(encoding="utf-8")
    assert "recipe validation (synthesize_recipes) — OK" in log
    assert [d for d, _ in world.hint_writes] == [DEALER]
    hints = _stored_hints(world)
    assert hints["recipe_status"] == "ok"
    assert hints["recipe_validation"]["context"] == "synthesize_recipes"


def test_real_run_reject_still_records_and_saves_nothing(world, sr, capsys):
    world.verdict = "reject"
    assert _run(sr) == 0
    assert "rejected by validation" in capsys.readouterr().out
    assert _files_under(world.recipes_dir) == []
    assert "— REJECT" in (world.logs / DEALER / "discovery.md").read_text(encoding="utf-8")
    assert f"recipe_rejected:one_condition -> {DEALER}" in \
        (world.logs / "_learning" / "errors_index.md").read_text(encoding="utf-8")
    assert _stored_hints(world)["recipe_status"] == "rejected:one_condition"


def test_force_over_a_healthy_recipe_records_and_saves(world, sr, capsys):
    _seed_healthy_recipe(world)
    world.verdict = "ok"
    assert _run(sr, "--force") == 0
    assert "browser-free" in capsys.readouterr().out
    assert "— OK" in (world.logs / DEALER / "discovery.md").read_text(encoding="utf-8")
    assert [d for d, _ in world.hint_writes] == [DEALER]
    hints = _stored_hints(world)
    assert hints["recipe_status"] == "ok" and hints["recipe_validation"]["context"] == "synthesize_recipes"
    (saved,) = rec.load_recipes(DEALER)
    assert saved.vehicle_rows == 50
