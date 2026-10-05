"""
nabthat (Angular SSR SRP + schema.org Vehicle JSON-LD) platform template.
"""
from __future__ import annotations

from backend.scanner.recipes import _unique_vins, EndpointRecipe, PAGINATION_HTML_PAGE
from backend.scanner.synth.common import _origin
from backend.scanner.synth.http import _dep_fetch_html


# ── Platform: nabthat (Angular SSR SRP + schema.org Vehicle JSON-LD) ───────────

# nabthat SSR SRP returns text/html with 16 schema.org @type:Vehicle JSON-LD
# nodes per page, ?page=N honored server-side. Parameterized by the dealer DOMAIN
# alone. Same JSON-LD shape as Dealer eProcess, so it reuses the dealer_eprocess
# parser; the /inventory.json feed the platform advertises is a broken (502)
# Lambda route, so the SRP page-walk is the real path. Full lot = used + new.
_NABTHAT_SRP_PATHS = ("/inventory/used", "/inventory/new")


def _detect_nabthat(html: str, dealer_url: str) -> bool:
    return "nabthat.com" in html.lower()


def _nabthat_page_vins(html: str, dealer_id: str, origin: str) -> set[str]:
    from backend.parsers.dealer_eprocess import parse as _dep_parse

    return _unique_vins(_dep_parse(html, base_url=origin, dealer_id=dealer_id, dealer_url=origin))


def _nabthat_recipe(dealer_id: str, origin: str, path: str) -> tuple[EndpointRecipe, set[str]] | None:
    url = origin + path
    html = _dep_fetch_html(url)
    if not html:
        return None
    vins = _nabthat_page_vins(html, dealer_id, origin)
    if not vins:
        return None
    recipe = EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="GET",
        content_type="text/html",
        post_template=None,
        auth_headers={},
        pagination=PAGINATION_HTML_PAGE,
        provider_hint="dealer_eprocess",
    )
    return recipe, vins


def _synth_nabthat(dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    origin = _origin(dealer_url)
    built: list[tuple[EndpointRecipe, set[str]]] = [
        r for p in _NABTHAT_SRP_PATHS if (r := _nabthat_recipe(dealer_id, origin, p)) is not None
    ]
    if not built:
        return []
    if len(built) == 2:
        (r_used, v_used), (r_new, v_new) = built
        overlap = len(v_used & v_new)
        smaller = min(len(v_used), len(v_new)) or 1
        if overlap / smaller >= 0.5:
            return [r_used]
    return [r for r, _ in built]
