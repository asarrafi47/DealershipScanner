"""Real effects for ``ops_tick``: the recipe store, the validation gate and the scan child process.

Kept apart from ``ops_tick`` so the orchestration imports nothing heavy and the tests never touch a database. Nothing
here runs unless ``ops_tick.main`` is started on Railway with the data repository configured.
"""

from __future__ import annotations

import subprocess
import sys
from dataclasses import asdict
from pathlib import Path

SCAN_TIMEOUT_S = 25 * 60


class RealIO:
    """``apply_proposal`` effects against the production recipe store."""

    def __init__(self, env):
        self.env = env

    def current_rows(self, dealer_id):
        from backend.scanner import recipes

        return [asdict(r) for r in recipes.load_recipes(dealer_id)]

    def _recipes(self, rows):
        from backend.scanner import recipes

        return recipes._rows_to_recipes(rows)

    def gate(self, dealer_id, rows):
        from backend.scanner import recipe_validation as rv

        _kept, report = rv.gate_recipes(dealer_id, self._recipes(rows), record=False)
        return report.verdict

    def save(self, dealer_id, rows):
        from backend.scanner import recipes

        recipes.save_recipes(dealer_id, self._recipes(rows))

    def canary(self, dealer_id):
        """Rescan the dealer, then replay the saved set live: ``(verdict, ratio)``; ratio is coverage or 1.0."""
        ok, _detail = scan(self.env, "rescan", [dealer_id])
        if not ok:
            return "reject", 0.0
        from backend.scanner import recipe_validation as rv

        _kept, report = rv.gate_recipes(dealer_id, self._recipes(self.current_rows(dealer_id)), record=False)
        if report.vins_total <= 0:
            return "reject", 0.0
        return report.verdict, report.coverage if report.coverage is not None else 1.0


def scan(env, kind, dealers):
    """Run ``fleet_scan`` for exactly these dealers (one shard, no post steps, no reconcile). ``(ok, detail)``."""
    child = dict(env)
    child.update({"SCAN_DEALERS": ",".join(dealers), "SCAN_SHARDS": "1", "SCAN_BATCH": str(min(len(dealers), 6)),
                  "SCAN_POST_STEPS": "0", "SCAN_PIPELINE_ARGS": "--no-reconcile", "SCAN_FLEET": "0"})
    try:
        proc = subprocess.run([sys.executable, "-m", "backend.scripts.fleet_scan"], env=child, timeout=SCAN_TIMEOUT_S,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    except subprocess.TimeoutExpired:
        return False, kind + " timed out"
    return proc.returncode == 0, kind + " exit " + str(proc.returncode)


def build(env, config, clone: Path):
    """``(io, probe_rescan, ctx)`` for ``ops_tick.main``."""
    from backend.scanner import manifest, recipes

    roster = manifest.load_manifest(explicit_dealer_ids=True)
    ids = {str(d["dealer_id"]) for d in roster if d.get("dealer_id")}
    blocked = set()
    for dealer in ids:
        try:
            hints = recipes.get_scan_hints(dealer) if hasattr(recipes, "get_scan_hints") else {}
        except Exception:  # noqa: BLE001
            hints = {}
        if recipes.blocked_on_this_host(str((hints or {}).get("recipe_status") or "")):
            blocked.add(dealer)
    ctx = {"manifest_ids": ids, "do_not_scan": manifest.load_do_not_scan_ids(),
           "recipe_only_not_revived": set(), "blocked": blocked}
    return RealIO(env), (lambda kind, dealers: scan(env, kind, dealers)), ctx
