#!/usr/bin/env python3
"""
Mac Mini (16 GB RAM) scanner entrypoint — scaled-down duplicate of ``scanner.py``.

Uses the same Playwright pipeline as production but applies memory-safe defaults
via ``backend.scanner.mac_mini_lite`` (serial dealers, fewer pages, no image downloads).

Scope: local 92694 / 25 mi manifest only (``workspace/manifest_92694_25mi.json``).

Run:
  python scanner_mac_mini.py
  python scanner_mac_mini.py --manifest workspace/manifest_92694_25mi.json --scan-only

Production ``scanner.py`` is unchanged.
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

from backend.scanner.mac_mini_lite import run_continue_mac_mini_scan, run_mac_mini_cli_entry

if __name__ == "__main__":
    if "--continue" in sys.argv:
        sys.argv = [a for a in sys.argv if a != "--continue"]
        raise SystemExit(run_continue_mac_mini_scan())
    run_mac_mini_cli_entry()
