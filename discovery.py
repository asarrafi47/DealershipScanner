#!/usr/bin/env python3
"""
Root-level entry point for dealership discovery.

Discovers dealerships near a ZIP code via DMV records, OpenStreetMap (Overpass),
and DuckDuckGo URL gap-fill, then merges results into dealers.json.

Run:
  python discovery.py --zip 28210 --radius 25 --dmv-state NC --merge-manifest -v
  python discovery.py --zip 90210 --radius 50 --json
  python discovery.py --help
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

from backend.discovery.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
