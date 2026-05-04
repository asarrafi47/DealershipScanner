#!/usr/bin/env python3
"""
Remove legacy dummy inventory rows (VIN001, VINXXX-style placeholders) from ``inventory.db``.

Uses ``backend.db.inventory_db.delete_cars_with_dummy_placeholder_vins``.
Run from project root:  PYTHONPATH=. python scripts/remove_dummy_vin_cars.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

from backend.db.inventory_db import delete_cars_with_dummy_placeholder_vins  # noqa: E402


def main() -> int:
    out = delete_cars_with_dummy_placeholder_vins()
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
