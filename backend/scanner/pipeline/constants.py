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
# N21.5: a dealer above this share of incomplete rows with no open item is alarmed in the triage.
INCOMPLETE_ALARM_SHARE = 0.25
INCOMPLETE_ALARM_MIN_ROWS = 20
DISCREPANCY_FLOOR = 0.05  # share of rows with a hard dictionary-vs-dealer discrepancy
FIELD_FLOOR = 0.90
BASELINE_DAYS = 30            # "listed before" = rows seen this many days before the run
RECONCILE_MIN_SHARE = 0.60    # a run that returned fewer rows than this share of the baseline is partial: never retire on it
# P5A.7 / D-N21: alarm when one run's retirements exceed this share of the dealer's prior active
# rows. Nightly keeps 10%; the first live retirement and its rehearsal use
# min(60%, 10% + 1.5% x days since the retired rows were last scraped).
RETIRE_ALARM_NIGHTLY = 0.10
RETIRE_ALARM_GAP_BASE = 0.10
RETIRE_ALARM_GAP_PER_DAY = 0.015
RETIRE_ALARM_GAP_CAP = 0.60
ROW_FLOOR = 0.50
MIN_ROWS_UNKNOWN = 20
KEY_FIELDS = ("price", "trim", "exterior_color")
SECONDARY_FIELDS = ("interior_color", "engine_description", "transmission", "drivetrain", "fuel_type",
                    "body_style", "description", "stock_number", "msrp", "gallery_8plus")

# N6.9: used-row mileage coverage (gated at FIELD_FLOOR) and its report-only counters.
MILEAGE_COVERAGE_KEYS = ("used_mileage", "used_n", "used_zero_mi", "used_zero_mi_my2026")

# Scans are HTTP-only by policy (backend/scanner/browser_gate.py): no cap juggling
# needed any more — without SCANNER_ALLOW_BROWSER the scanner never launches
# Chromium, opens no page and the browser VDP pool does not exist.
HTTP_ONLY_ENV = {"SCANNER_HTTP_ONLY": "1", "SCANNER_VDP_DB_MERGE": "0"}
