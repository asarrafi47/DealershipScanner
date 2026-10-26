#!/usr/bin/env python3
"""Import every module the scanner image runs and report missing third-party packages.

Run inside the scanner image (Dockerfile.scanner) after changing
requirements-scanner.txt:

    python scripts/scanner_import_sweep.py

``--profile web`` sweeps the web entry point against the web image's packages
(requirements-web.txt: no Playwright); ``--profile discovery`` expects the scanner
set plus Playwright (requirements-scanner.txt + requirements-browser.txt). The default profile is ``scanner`` (P15B.2).

Exit 1 when an entry module fails to import, or when any module under the swept
packages fails on a package that is not in the allowed-missing set (the browser /
ML / web stacks the scan never touches).
"""
from __future__ import annotations

import argparse
import importlib
import pkgutil
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

ENTRY = [
    "backend.scripts.dealer_pipeline",
    "backend.scanner.cli",
    "backend.scanner.orchestrator",
    "backend.enrichment.vpic_facts",
    "backend.scripts.compute_market_stats",
    "backend.scripts.build_listings_grid_cards",
    "backend.db.repositories.grid_cards_repo",
    "backend.intelligence.market_pricing",
    "backend.scripts.discovery_probe",
    "backend.scripts.platform_candidates",
    "backend.scanner.recipe_validation",
    "backend.scanner.recipe_totals",
    "backend.scanner.recipe_synth",
    "backend.scripts.heal_from_vpic",
]
SWEEP = ["backend.scanner", "backend.db", "backend.enrichment", "backend.utils"]
# Packages the scan path must never need (browser, ML, web, LLM stacks).
# Every entry is still imported somewhere under backend/ or scripts/
# (backend/tests/test_requirements_hygiene.py checks it). P15B.1 (D-DEP1) dropped the
# names nothing imports any more: crawl4ai, patchright, ollama, pyotp, segno,
# langchain_core, langchain_openai, openai, torch, transformers, cv2, pytesseract.
ALLOWED_MISSING = {
    "playwright", "playwright_stealth", "sentence_transformers",
    "flask", "werkzeug", "anthropic",
    "pdfplumber", "fitz", "pypdf", "PIL", "pgvector", "sqlcipher3", "redis", "stripe",
    "resend", "bcrypt", "jwt", "duckdb", "httpx", "pytest",
}

# Per-image profiles (P15B.2): entry modules, swept packages, and the packages
# that image may lack. "scanner" is the default and uses the lists above.
# Playwright is browser-only (requirements-browser.txt); web must import without it.
PROFILES = {
    "scanner": {"entry": ENTRY, "sweep": SWEEP, "allowed": ALLOWED_MISSING},
    "web": {
        "entry": ["backend.main"],
        "sweep": ["backend.web", "backend.routes", "backend.auth", "backend.db"],
        "allowed": {"playwright", "playwright_stealth", "pytest"},
    },
    "discovery": {
        "entry": ENTRY + ["backend.scanner.discovery_capture"],
        "sweep": SWEEP,
        # Slim image (P15B.5): scanner + browser files, so only Playwright is present
        # beyond what the scanner profile has; the rest stays allowed-missing.
        "allowed": ALLOWED_MISSING - {"playwright", "playwright_stealth"},
    },
}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--profile", choices=sorted(PROFILES), default="scanner")
    prof = PROFILES[ap.parse_args(argv).profile]
    entry, sweep, allowed = prof["entry"], prof["sweep"], prof["allowed"]
    bad = 0
    for mod in entry:
        try:
            importlib.import_module(mod)
            print(f"ok      {mod}")
        except Exception as exc:  # noqa: BLE001
            bad += 1
            print(f"ENTRY   {mod}: {type(exc).__name__}: {exc}")
            traceback.print_exc(limit=3)
    missing: dict[str, list[str]] = {}
    for pkg in sweep:
        p = importlib.import_module(pkg)
        for info in pkgutil.walk_packages(p.__path__, pkg + "."):
            if ".tests" in info.name or info.name.endswith("__main__"):
                continue
            try:
                importlib.import_module(info.name)
            except ModuleNotFoundError as exc:
                top = (exc.name or "?").split(".")[0]
                missing.setdefault(top, []).append(info.name)
            except SystemExit:
                pass
            except Exception as exc:  # noqa: BLE001
                print(f"other   {info.name}: {type(exc).__name__}: {str(exc)[:120]}")
    for top, mods in sorted(missing.items()):
        tag = "allowed" if top in allowed or top == "backend" else "MISSING"
        if tag == "MISSING":
            bad += 1
        print(f"{tag:8s}{top}: {len(mods)} module(s), e.g. {', '.join(mods[:4])}")
    print(f"sweep done: {bad} problem(s)")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
