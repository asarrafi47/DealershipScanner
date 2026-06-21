"""Repository paths for the dealership inventory scanner package."""
from __future__ import annotations

from pathlib import Path

SCANNER_ROOT = Path(__file__).resolve().parent

INTERCEPT_POLICY_PATH = SCANNER_ROOT / "config" / "scanner_intercept_policy.json"
