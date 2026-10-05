"""
Chapman (apiv2.chapmanapps.com flat JSON arrays) platform template.
"""
from __future__ import annotations

import re

from backend.scanner.recipes import EndpointRecipe, PAGINATION_NONE
from backend.scanner.synth.http import _cosmos_get_json


# ── Platform: Chapman (apiv2.chapmanapps.com flat JSON arrays) ─────────────────

# Chapman Auto Group runs an in-house Nuxt SSR SPA whose inventory is served by a
# clean REST API at apiv2.chapmanapps.com. GET /inventory/{arkona}/new and
# /inventory/{arkona}/used each return the dealer's full inventory as a single
# flat JSON array (no pagination). The only per-dealer input is the lowercase
# 'arkona' store code, extracted from the homepage HTML. Full lot = new + used.
_CHAPMAN_API_HOST = "apiv2.chapmanapps.com"
_CHAPMAN_ARKONA_RES = (
    re.compile(r'assets\.chapmanchoice\.com/img/dealers/([a-z0-9]+)\.webp', re.IGNORECASE),
    re.compile(r'\\?["\']arkona\\?["\']\s*:\s*\\?["\']([A-Za-z0-9]{2,8})\\?["\']'),
)
_CHAPMAN_CONDITIONS = ("new", "used")


def _detect_chapman(html: str, dealer_url: str) -> bool:
    low = html.lower()
    if "chapmanapps.com" not in low and "chapmanchoice.com" not in low:
        return False
    return bool(_extract_chapman_arkona(html))


def _extract_chapman_arkona(html: str) -> str | None:
    for rx in _CHAPMAN_ARKONA_RES:
        m = rx.search(html)
        if m:
            return m.group(1).lower()
    return None


def _chapman_recipe(dealer_id: str, arkona: str, condition: str) -> EndpointRecipe | None:
    url = f"https://{_CHAPMAN_API_HOST}/inventory/{arkona}/{condition}"
    body = _cosmos_get_json(url)
    if not isinstance(body, list) or not body:
        return None
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="GET",
        content_type="application/json",
        post_template=None,
        auth_headers={},
        pagination=PAGINATION_NONE,
        total_count=len(body),
        provider_hint="chapman",
    )


def _synth_chapman(dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    arkona = _extract_chapman_arkona(html)
    if not arkona:
        return []
    return [r for c in _CHAPMAN_CONDITIONS
            if (r := _chapman_recipe(dealer_id, arkona, c)) is not None]
