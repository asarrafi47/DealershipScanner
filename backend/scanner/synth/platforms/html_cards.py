"""
Server-rendered card pages (data-vin) platform template.
"""
from __future__ import annotations

import logging

from backend.scanner.recipes import _unique_vins, EndpointRecipe, PAGINATION_HTML_PAGE
from backend.scanner.synth.common import _origin
from backend.scanner.synth.http import _dep_fetch_html, _fetch_impersonated

logger = logging.getLogger("scanner")


# ── Platform: server-rendered card pages (data-vin) ────────────────────────────

# Sites that render the whole lot into an HTML page with ``data-vin`` cards
# (Quantum Auto Sales, a "Responsive Automotive" Next.js site: 208 cars in
# /inventory/, 2026-09-26). The last template tried: it needs no fingerprint,
# only an SRP path that holds enough cards. The card parser
# (backend/parsers/html_cards.py) takes identity from JSON-LD or the detail
# slug; the HTTP-first detail pass fills the rest.
_HTML_CARDS_PATHS = ("/inventory/", "/inventory", "/cars-for-sale/", "/used-vehicles/", "/vehicles/", "/all-inventory/", "/used-cars/", "/inventory/used/", "/inventory/new/")
_HTML_CARDS_MIN = 10


def _detect_html_cards(html: str, dealer_url: str) -> bool:
    from backend.parsers.html_cards import detect

    if detect(html, _HTML_CARDS_MIN):
        return True
    low = (html or "").lower()
    return "data-vin=" in low and any(p in low for p in ("/inventory", "cars-for-sale", "/vehicles"))


def _synth_html_cards(dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    from backend.parsers.html_cards import detect, parse

    origin = _origin(dealer_url)
    best: list[tuple[int, str, str]] = []
    for path in _HTML_CARDS_PATHS:
        page = _fetch_impersonated(origin + path, timeout=40.0) or _dep_fetch_html(origin + path)
        if not page or not detect(page, _HTML_CARDS_MIN):
            continue
        rows = parse(page, base_url=origin, dealer_id=dealer_id, dealer_name=dealer_id, dealer_url=origin)
        vins = _unique_vins(rows)
        if vins:
            best.append((len(vins), path, page))
    if not best:
        return []
    best.sort(reverse=True)
    out: list[EndpointRecipe] = []
    seen_vins: set[str] = set()
    for n, path, page in best[:2]:
        rows = parse(page, base_url=origin, dealer_id=dealer_id, dealer_name=dealer_id, dealer_url=origin)
        vins = _unique_vins(rows)
        if len(vins - seen_vins) < max(3, n // 5):
            continue  # same lot under another path
        seen_vins |= vins
        logger.info("html_cards [%s]: %s holds %d card(s)", dealer_id, path, n)
        out.append(EndpointRecipe(
            dealer_id=dealer_id, url=origin + path, method="GET", content_type="text/html", post_template=None, auth_headers={},
            pagination=PAGINATION_HTML_PAGE, provider_hint="html_cards", vehicle_rows=n, total_count=None,
        ))
    return out
