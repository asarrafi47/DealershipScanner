"""
autoWALL (gratis solutions; server-rendered /gs-vehicle/list) platform template.
"""
from __future__ import annotations

import logging
import re

from backend.scanner.recipes import EndpointRecipe, PAGINATION_HTML_PAGE
from backend.scanner.synth.common import _origin
from backend.scanner.synth.http import _dep_fetch_html

logger = logging.getLogger("scanner")


# ── Platform: autoWALL (gratis solutions; server-rendered /gs-vehicle/list) ─────

# autoWALL sites (Long Chevrolet Buick GMC of Athens, 2026-09-24) answer plain
# HTTP: GET /gs-vehicle/list?filter=All&page=N returns an HTML SRP with one
# ``.vehicle-inventory-container[data-vin]`` card per car, 25 per page, and a
# "<N> Vehicles for Sale" title (309). The homepage does not carry the card
# markup (every discovery path 404s with the "Powered By autoWALL" title), so
# detection reads the nav links / badge and the SRP is fetched to confirm.
_AUTOWALL_LIST_PATH = "/gs-vehicle/list?filter=All"
_AUTOWALL_TOTAL_RE = re.compile(r"<title>\s*(\d{1,5})\s+Vehicles for Sale", re.I)


def _detect_autowall(html: str, dealer_url: str) -> bool:
    low = (html or "").lower()
    return "/gs-vehicle/list" in low or "powered_by_autowall" in low or "powered by autowall" in low


def _synth_autowall(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    from backend.scanner.scrapers.autowall import _is_autowall_html, parse_autowall_inventory_html

    origin = _origin(dealer_url)
    url = origin + _AUTOWALL_LIST_PATH
    page = _dep_fetch_html(url)
    if not page or not _is_autowall_html(page):
        logger.info("autowall [%s]: %s did not return the card markup", dealer_id, url)
        return None
    rows = parse_autowall_inventory_html(page, base_url=origin, dealer_id=dealer_id, dealer_name=dealer_id, dealer_url=origin)
    if not rows:
        return None
    m = _AUTOWALL_TOTAL_RE.search(page)
    total = int(m.group(1)) if m else None
    logger.info("autowall [%s]: %d cards on page 1, title total %s", dealer_id, len(rows), total)
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="GET",
        content_type="text/html",
        post_template=None,
        auth_headers={},
        pagination=PAGINATION_HTML_PAGE,
        provider_hint="autowall",
        vehicle_rows=len(rows),
        total_count=total,
    )
