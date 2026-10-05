"""Paths and thresholds of the dealer pipeline. Moved verbatim from backend/scripts/dealer_pipeline.py (audit F11)."""
from __future__ import annotations

import os
from pathlib import Path

from backend.utils.project_env import load_project_dotenv

# The pre-split module loaded .env before computing LOG_ROOT (DEALER_LOGS_ROOT);
# keep that order for any importer that reaches this module first.
load_project_dotenv()

ROOT = Path(__file__).resolve().parents[3]  # backend/scanner/pipeline/ -> repo root
# DEALER_LOGS_ROOT redirects every per-dealer log (tests; a scratch run). The
# discovery probe and the recipe validator honour the same variable.
LOG_ROOT = Path(os.environ.get("DEALER_LOGS_ROOT") or (ROOT / "workspace" / "dealer_logs"))
PROCESS_DOC = "docs/NETWORK_SCAN_PROCESS.md"
INCOMPLETE_FLOOR = 0.05   # share of rows with a missing spec-sheet field after the NHTSA fill
DISCREPANCY_FLOOR = 0.05  # share of rows with a hard dictionary-vs-dealer discrepancy
FIELD_FLOOR = 0.90
BASELINE_DAYS = 30            # "listed before" = rows seen this many days before the run
RECONCILE_MIN_SHARE = 0.60    # a run that returned fewer rows than this share of the baseline is partial: never retire on it
ROW_FLOOR = 0.50
MIN_ROWS_UNKNOWN = 20
KEY_FIELDS = ("price", "trim", "exterior_color")
SECONDARY_FIELDS = ("interior_color", "engine_description", "transmission", "drivetrain", "fuel_type",
                    "body_style", "description", "stock_number", "msrp", "gallery_8plus")

# Scans are HTTP-only by policy (backend/scanner/browser_gate.py): no cap juggling
# needed any more — without SCANNER_ALLOW_BROWSER the scanner never launches
# Chromium, opens no page and the browser VDP pool does not exist.
HTTP_ONLY_ENV = {"SCANNER_HTTP_ONLY": "1", "SCANNER_VDP_DB_MERGE": "0"}
