"""Shared paths and scanner constants (repo root, debug dirs, inventory URL paths)."""
from __future__ import annotations

import os
from pathlib import Path

# Repo root (contains dealers.json, scanner.py shim)
ROOT = Path(__file__).resolve().parents[2]

MANIFEST_PATH = Path(os.environ.get("DEALERS_MANIFEST_PATH") or (ROOT / "dealers.json"))
DEBUG_DIR = ROOT / "debug"
WORKSPACE_DEBUG_DIR = ROOT / "workspace" / "debug"

SCANNER_HYDRATION_SELECTORS = '[data-vin], a[href*="/inventory/"]'

KNOWN_HAR_PROVIDERS = frozenset({"dealer_dot_com", "dealer_on"})

NEXT_SELECTORS = [
    'button:has-text("Next")',
    'button:has-text("Load More")',
    'a:has-text("Next")',
    '[data-action="next"]',
    ".pagination-next",
    ".load-more",
    'a:has-text("Load More")',
    '[aria-label="Next page"]',
    '[aria-label="Go to next page"]',
    '[data-testid*="next"]',
    ".page-next",
    "li.next > a",
    'a[rel="next"]',
    'button[data-action="page-next"]',
]
MAX_PAGINATION_CLICKS = 15

OEM_MANUFACTURER_DOMAINS = frozenset({
    "www.chevrolet.com",
    "www.chrysler.com",
    "www.ford.com",
    "www.gm.com",
    "www.nissan.com",
    "nissan-global.com",
    "www.honda.com",
    "automobiles.honda.com",
    "www.toyota.com",
    "www.hyundai.com",
    "www.kia.com",
    "www.bmw.com",
    "www.audi.com",
    "www.volkswagen.com",
    "www.mercedes.com",
    "www.jeep.com",
    "www.ram.com",
    "www.dodge.com",
})
