"""
CarsCommerce (websites-search.api) platform template. The group-account store
scoping lives in :mod:`backend.scanner.synth.platforms.carscommerce_scope`.
"""
from __future__ import annotations

import json
import logging
import re

from backend.scanner.recipes import EndpointRecipe, PAGINATION_CARSCOMMERCE
from backend.scanner.synth.common import _load_reference_recipe
from backend.scanner.synth.platforms.carscommerce_scope import _carscommerce_store_filter

logger = logging.getLogger("scanner")


# ── Platform: CarsCommerce (websites-search.api) ──────────────────────────────

_CARSCOMMERCE_REF_ID = "courtesychev-com"
_CARSCOMMERCE_HOST = "websites-search.api.carscommerce.inc"

_CCID_RES = (
    re.compile(r'["\']ccid["\']\s*:\s*["\'](\d{3,})["\']'),
    re.compile(r'/api/v1/listings\\?/(\d{3,})'),
    re.compile(r'var\s+account\s*=\s*["\'](\d{3,})["\']'),
)
_APIKEY_RE = re.compile(r'["\']apiKey["\']\s*:\s*["\']([A-Za-z0-9]{16,})["\']')


def _detect_carscommerce(html: str, dealer_url: str) -> bool:
    low = html.lower()
    if _CARSCOMMERCE_HOST not in low and "carscommerce" not in low:
        return False
    return bool(_extract_ccid(html))


def _extract_ccid(html: str) -> str | None:
    for rx in _CCID_RES:
        m = rx.search(html)
        if m:
            return m.group(1)
    return None


def _extract_carscommerce_key(html: str) -> str | None:
    """apiKey embedded in the dealer HTML (CarsCommerce ships it client-side)."""
    m = _APIKEY_RE.search(html)
    return m.group(1) if m else None


def _synth_carscommerce(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    ccid = _extract_ccid(html)
    if not ccid:
        return None
    ref = _load_reference_recipe(_CARSCOMMERCE_REF_ID, "/listings/")
    if not ref or not ref.post_template:
        logger.warning("recipe_synth: CarsCommerce reference template unavailable (%s)", _CARSCOMMERCE_REF_ID)
        return None
    # Prefer the key the dealer's own page ships; the CarsCommerce search key is
    # shared across all their dealers, so the reference recipe's key is a safe
    # fallback if the page doesn't expose it.
    api_key = _extract_carscommerce_key(html) or (ref.auth_headers or {}).get("x-api-key")
    if not api_key:
        return None
    # Full-lot body: (1) drop the reference recipe's Used/CPO type restriction so
    # new inventory is included (mirrors carscommerce_harvest include_new — the
    # delta replay honors facetFilters and would silently exclude every new car);
    # (2) raise perPage to 100 (the harvester's _PER_PAGE; API max is 200) so the
    # bounded page walk reaches large accounts — at perPage 20 the 40-page cap
    # tops out at 800, short of dealers like Bill Luke (~1,979).
    try:
        body = json.loads(ref.post_template)
    except ValueError:
        body = None
    if isinstance(body, dict):
        body.pop("facetFilters", None)
        body["perPage"] = 100
        post_template = json.dumps(body)
    else:
        post_template = ref.post_template
    url = f"https://{_CARSCOMMERCE_HOST}/api/v1/listings/{ccid}/search"
    recipe = EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="POST",
        content_type="application/json; charset=utf-8",
        post_template=post_template,
        auth_headers={"x-api-key": api_key},
        pagination=PAGINATION_CARSCOMMERCE,
        provider_hint="dealer_dot_com",  # CarsCommerce payloads route through the generic dealer_dot_com mapper
    )
    try:
        extras = _carscommerce_store_filter(recipe, dealer_id, dealer_url, html)
    except Exception as exc:  # noqa: BLE001 - the unfiltered recipe still works for single-store accounts
        logger.debug("carscommerce store filter probe failed [%s]: %s", dealer_id, exc)
        extras = []
    return [recipe, *extras] if extras else recipe
