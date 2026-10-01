"""Run the HTTP-only dealer pipeline for the dev / operator tools.

The dev dashboard's "Import & scan" (smart import, bulk import, test scanner) and the
/dev/manifest console's "Scan" button used to spawn ``node backend/scanner/scanner.js``
and ``backend/scanner.py``; neither exists any more. The current per-dealer loop is
``backend/scripts/dealer_pipeline.py`` (docs/NETWORK_SCAN_PROCESS.md): recipe
(synthesized from the homepage when missing) -> HTTP-only scan -> NHTSA vPIC ->
assess -> dealer logs, with a discovery capture when the HTTP recipe finds nothing.
Every helper here builds or reads that one command, so the tools and the fleet run
the same code.
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
DEV_PIPELINE_OUT_ROOT = REPO_ROOT / "workspace" / "pipeline" / "dev"
_TAG_SAFE_RE = re.compile(r"[^A-Za-z0-9_.-]+")

# Verdicts dealer_pipeline.assess() writes; only these two mean rows landed.
PIPELINE_SUCCESS_VERDICTS = frozenset({"ok", "thin"})


def pipeline_out_dir(tag: str) -> Path:
    """Fresh output directory for one dev-triggered run (triage.json lands here)."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe = _TAG_SAFE_RE.sub("-", tag or "run").strip("-")[:80] or "run"
    return DEV_PIPELINE_OUT_ROOT / f"{stamp}_{safe}"


def pipeline_command(dealer_id: str, out_dir: Path, *, manifest_path: Path | None = None) -> list[str]:
    """``python -m backend.scripts.dealer_pipeline --dealers <id> --out <dir>`` for one dealer."""
    cmd = [
        sys.executable,
        "-m",
        "backend.scripts.dealer_pipeline",
        "--dealers",
        dealer_id,
        "--out",
        str(out_dir),
        "--batch",
        "1",
    ]
    if manifest_path is not None:
        cmd += ["--manifest", str(manifest_path)]
    return cmd


def read_triage_row(out_dir: Path, dealer_id: str) -> dict[str, Any] | None:
    """The pipeline's assessment of ``dealer_id`` from ``<out_dir>/triage.json``, or None."""
    path = out_dir / "triage.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    for row in data.get("dealers") or []:
        if isinstance(row, dict) and row.get("dealer_id") == dealer_id:
            return row
    return None


def summarize_triage_row(row: dict[str, Any] | None) -> dict[str, Any]:
    """Small JSON-safe view of a triage row for the dev job poller."""
    if not row:
        return {"verdict": None, "reason": "pipeline wrote no triage row for this dealer", "rows": 0}
    return {
        "verdict": row.get("verdict"),
        "reason": row.get("reason"),
        "rows": int(row.get("rows") or 0),
        "active_after": row.get("active_after"),
        "lifecycle": row.get("lifecycle"),
    }
