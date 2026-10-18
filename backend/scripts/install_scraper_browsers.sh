#!/usr/bin/env bash
# Install Playwright's Chromium (the playwright package in requirements.txt).
# Run after: pip install -r requirements.txt (cwd does not matter; script cds to repo root).
# Patchright's Chromium is no longer installed: its only consumer was crawl4ai, removed
# from requirements.txt in P15B.1 (D-DEP1, 2026-10-18).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
PY="${PYTHON:-${PYTHON3:-python3}}"
echo "Using: $PY"
"$PY" -m playwright install chromium
echo "Done. Playwright's Chromium is installed (PLAYWRIGHT_BROWSERS_PATH, when set, picks the location)."
