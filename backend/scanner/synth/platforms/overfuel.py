"""
Overfuel (Next.js SSR, __NEXT_DATA__ inventory) platform template.
"""
from __future__ import annotations

from backend.scanner.recipes import _unique_vins, EndpointRecipe, PAGINATION_HTML_PAGE
from backend.scanner.synth.common import _origin
from backend.scanner.synth.http import _dep_fetch_html


# ── Platform: Overfuel (Next.js SSR, __NEXT_DATA__ inventory) ──────────────────

# Overfuel SSR SRP embeds full per-vehicle records in <script id="__NEXT_DATA__">
# at props.pageProps.inventory.results (25/page, ?page=N). Parameterized by the
# dealer DOMAIN alone. The recipe is an HTML page-walk whose parser (overfuel)
# extracts __NEXT_DATA__ and walks inventory.results.
_OVERFUEL_SRP_PATH = "/inventory"


def _detect_overfuel(html: str, dealer_url: str) -> bool:
    return "overfuel" in html.lower()


def _synth_overfuel(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    origin = _origin(dealer_url)
    url = origin + _OVERFUEL_SRP_PATH
    page1 = _dep_fetch_html(url)
    if not page1:
        return None
    from backend.parsers.overfuel import parse as _of_parse
    from backend.scanner.scrapers.next_data_inventory import parse_next_data_json_from_html

    vins = _unique_vins(_of_parse(page1, base_url=origin, dealer_id=dealer_id, dealer_url=origin))
    if not vins:
        return None
    total: int | None = None
    nd = parse_next_data_json_from_html(page1)
    try:
        meta = nd["props"]["pageProps"]["inventory"]["meta"]
        total = int(meta.get("total")) or None
    except (KeyError, TypeError, ValueError):
        total = None
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="GET",
        content_type="text/html",
        post_template=None,
        auth_headers={},
        pagination=PAGINATION_HTML_PAGE,
        total_count=total,
        provider_hint="overfuel",
    )
