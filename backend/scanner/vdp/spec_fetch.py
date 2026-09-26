"""HTTP fetch of a single VDP URL for spec backfill (optional tier).

Was a headless-Chromium fetch until 2026-09-26; scans and post-scan enrichment
are HTTP-only now (docs/HTTP_ONLY_SCANS_PLAN.md), so the page is read with the
same impersonated client the HTTP-first VDP pass uses.
"""
from __future__ import annotations

import logging
from typing import Any

from backend.scanner.utils.vdp_spec_parse import parse_html_for_vehicle_specs

log = logging.getLogger(__name__)


def extract_specs_from_vdp_url(url: str, *, timeout_ms: int = 45000) -> dict[str, Any]:
    """Fetch *url* over HTTP and parse cylinders / MPG / trans / drive from the HTML."""
    if not url or not str(url).strip().lower().startswith("http"):
        return {}
    from backend.scanner.vdp.prefetch import _fetch_html

    u = str(url).strip()
    try:
        html = _fetch_html(u)
    except Exception as e:  # noqa: BLE001
        log.info("VDP spec extract failed url=%s err=%s", u[:120], e)
        return {}
    if not html:
        return {}
    return parse_html_for_vehicle_specs(html)
