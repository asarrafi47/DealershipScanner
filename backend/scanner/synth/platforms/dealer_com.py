"""
Dealer.com (ws-inv-data) platform template.
"""
from __future__ import annotations

import logging
import re

from backend.scanner.recipes import EndpointRecipe, PAGINATION_DEALER_COM
from backend.scanner.synth.common import _load_reference_recipe, _origin

logger = logging.getLogger("scanner")


# ── Platform: Dealer.com (ws-inv-data) ────────────────────────────────────────

# Reference recipe supplying the Dealer.com POST body template.
_DEALER_COM_REF_ID = "camelbacktoyota-com"
_DEALER_COM_REF_SITEID = "camelbacktoyotavtg"
_DEALER_COM_INVENTORY_PATH = "/api/widget/ws-inv-data/getInventory"

_SITEID_RE = re.compile(r'["\']siteId["\']\s*:\s*["\']([\w-]+)["\']')


def _detect_dealer_com(html: str, dealer_url: str) -> bool:
    low = html.lower()
    if "carscommerce" in low or "teamvelocityportal" in low:
        return False  # disambiguate from other platforms that also embed a siteId
    markers = ("data-widget-name", "ddc", "ws-inv-data", _DEALER_COM_INVENTORY_PATH)
    if sum(1 for m in markers if m in low) < 2:
        return False
    return bool(_extract_dealer_com_siteid(html))


def _extract_dealer_com_siteid(html: str) -> str | None:
    m = _SITEID_RE.search(html)
    if m:
        return m.group(1)
    return None


def _synth_dealer_com(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    site_id = _extract_dealer_com_siteid(html)
    if not site_id:
        return None
    ref = _load_reference_recipe(_DEALER_COM_REF_ID, "getInventory")
    if not ref or not ref.post_template:
        logger.warning("recipe_synth: Dealer.com reference template unavailable (%s)", _DEALER_COM_REF_ID)
        return None
    # The reference dealer's siteId appears verbatim in siteId + pageId; swap it
    # to the target dealer's siteId. The ws-inv-data endpoint is lenient about
    # the pageId version suffix (verified against live dealers), so this single
    # substitution is enough to make the body dealer-correct.
    post_template = ref.post_template.replace(_DEALER_COM_REF_SITEID, site_id)
    url = _origin(dealer_url) + _DEALER_COM_INVENTORY_PATH
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="POST",
        content_type="application/json; charset=utf-8",
        post_template=post_template,
        auth_headers={},
        pagination=PAGINATION_DEALER_COM,
        provider_hint="dealer_dot_com",
    )
