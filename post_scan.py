#!/usr/bin/env python3
"""
Post-scan repair and enrichment (decoupled from inventory capture).

Run: ``python post_scan.py`` (same as ``python -m backend.scanner.post_scan.job``).
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

from backend.scanner.post_scan.job import run_cli_entry

if __name__ == "__main__":
    run_cli_entry()
