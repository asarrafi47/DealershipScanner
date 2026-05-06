#!/usr/bin/env python3
"""
Compatibility entrypoint at repo root; implementation in ``backend.scanner.cli``.

Run: ``python scanner.py`` (same as ``python -m backend.scanner.cli``).
"""
from pathlib import Path

import os
import sys

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

from backend.scanner.cli import (
    DEBUG_DIR,
    MANIFEST_PATH,
    _apply_gallery_vision_filter_to_vehicles,
    _apply_monroney_vision_to_vehicles,
    filter_manifest_by_dealer_id,
    filter_manifest_by_shard,
    load_manifest,
    main,
    run_cli_entry,
)

__all__ = [
    "DEBUG_DIR",
    "MANIFEST_PATH",
    "_apply_gallery_vision_filter_to_vehicles",
    "_apply_monroney_vision_to_vehicles",
    "filter_manifest_by_dealer_id",
    "filter_manifest_by_shard",
    "load_manifest",
    "main",
    "run_cli_entry",
]

if __name__ == "__main__":
    run_cli_entry()
