"""Fleet resume (N9.15): which of a shard's dealers a restarted pass still has to scan.

A pass killed midway (a Mac sleep, a SIGKILL) used to restart from dealer one: ``fleet_scan``
made a fresh run dir every time and the pipeline had no way to skip finished dealers. With
``--resume`` a dealer is done when

  * the shard's ``triage.json`` already has an entry for it (the pipeline writes the file at
    the end of a shard, so this covers shards, or earlier resumes, that ran to the end), or
  * ``scan_runs`` has a row for it finished at or after the run start. scan_runs has no
    ``started_at`` column; every run in the pass finishes after the pass began, so
    ``finished_at >= run start`` is the same test.

A dealer in the second group has no triage entry (its shard died before writing one): it is
not scanned again but still assessed, so the merged triage keeps one entry per dealer.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def prior_triage_dealers(out_dir: Path) -> list[dict[str, Any]]:
    """The dealer entries already in ``out_dir/triage.json`` (empty when absent or unreadable)."""
    try:
        dealers = json.loads((out_dir / "triage.json").read_text(encoding="utf-8")).get("dealers") or []
    except (OSError, ValueError, AttributeError):
        return []
    return [d for d in dealers if isinstance(d, dict) and d.get("dealer_id")]


def dealers_with_run_since(conn, dealer_ids: list[str], since_iso: str) -> set[str]:
    """Dealers with a scan_runs row finished at or after ``since_iso``."""
    if not dealer_ids:
        return set()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT dealer_id FROM scan_runs WHERE finished_at >= ? AND dealer_id IN ("
                    + ",".join("?" * len(dealer_ids)) + ")", (since_iso, *dealer_ids))
        return {str(r[0]) for r in cur.fetchall()}
    except Exception:  # noqa: BLE001 - no table / column: nothing is known to be done
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        return set()


def plan_resume(ids: list[str], out_dir: Path, conn, run_start_iso: str) -> dict[str, Any]:
    """Split ``ids`` (in roster order) into ``todo`` (scan + assess), ``assess_only`` (a scan_runs
    row exists, no triage entry) and the ``prior`` triage entries kept as they are."""
    prior = prior_triage_dealers(out_dir)
    done = {str(d["dealer_id"]) for d in prior}
    prior = [d for d in prior if str(d["dealer_id"]) in set(ids)]
    rest = [d for d in ids if d not in done]
    ran = dealers_with_run_since(conn, rest, run_start_iso)
    return {"prior": prior,
            "todo": [d for d in rest if d not in ran],
            "assess_only": [d for d in rest if d in ran]}
