#!/usr/bin/env python3
"""
Operator CLI: tiered dealership discovery (DMV → OSM → DDG URL gap-fill).

Examples::

  python scripts/discover_dealerships.py --zip 28210 --radius 25 --dmv-state NC --json
  python scripts/discover_dealerships.py --zip 28210 --radius 25 --merge-manifest --json

``--merge-manifest`` appends/updates ``dealers.json`` (HTTPS ``url`` required per row). Then::

  python scanner.py

Adds repository root to ``sys.path`` — same behavior as ``python -m backend.discovery.cli``.
"""
from __future__ import annotations

import warnings

# warnings.filterwarnings uses re.match() on the full message — anchor with .* (see stdlib warnings.py).
warnings.filterwarnings(
    "ignore",
    message=r".*doesn't match a supported version.*",
)

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.discovery.cli import main


if __name__ == "__main__":
    raise SystemExit(main(project_root=REPO_ROOT))
