#!/usr/bin/env bash
# Install Chromium for Puppeteer (playwright package) and for Crawl4ai (patchright).
# Run from repo root after: pip install -r requirements.txt
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY="${PYTHON:-${PYTHON3:-python3}}"
echo "Using: $PY"
"$PY" -m playwright install chromium
"$PY" -m patchright install chromium
echo "Done. Puppeteer uses PLAYWRIGHT_BROWSERS_PATH; Crawl4ai uses patchright's Chromium."
