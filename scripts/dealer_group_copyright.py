#!/usr/bin/env python3
"""
CLI wrapper — implementation lives under SCRAPING/.

  python scripts/dealer_group_copyright.py --test
  python -m SCRAPING.cli --test
  python -m SCRAPING --test
  python -m SCRAPING.cli --fixture-test

Env: INVENTORY_DB_PATH — default <repo>/inventory.db
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from SCRAPING.cli import main

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
