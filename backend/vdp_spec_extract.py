"""Playwright fetch of a single VDP URL for spec backfill (optional tier)."""
from __future__ import annotations

import logging
import time
from typing import Any

from backend.utils.vdp_spec_parse import parse_html_for_vehicle_specs

log = logging.getLogger(__name__)


def extract_specs_from_vdp_url(url: str, *, timeout_ms: int = 45000) -> dict[str, Any]:
    """
    Load *url* with headless Chromium and parse cylinders / MPG / trans / drive from HTML.

    Requires Playwright browsers (``python -m playwright install chromium``).
    """
    if not url or not str(url).strip().lower().startswith("http"):
        return {}
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("playwright not installed; VDP spec extract skipped")
        return {}
    u = str(url).strip()
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.goto(u, wait_until="domcontentloaded", timeout=timeout_ms)
                time.sleep(0.85)
                html = page.content()
            finally:
                browser.close()
    except Exception as e:
        log.info("VDP spec extract failed url=%s err=%s", u[:120], e)
        return {}
    return parse_html_for_vehicle_specs(html)
