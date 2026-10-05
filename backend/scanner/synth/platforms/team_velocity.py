"""
Team Velocity (same-origin JSON inventory feed) platform template.
"""
from __future__ import annotations

from backend.scanner.recipes import EndpointRecipe, PAGINATION_PAGE_QUERY
from backend.scanner.synth.common import _origin
from backend.scanner.synth.http import _cosmos_get_json


# ── Platform: Team Velocity (same-origin JSON inventory feed) ──────────────────

# Team Velocity (Right Honda, Right Toyota, Mark Kia) hydrates its SRP from a Vue
# SPA whose XHR endpoint (/api/Inventory/getinventorymultiselectionfilters) only
# returns filter FACETS — NOT the vehicle list. BUT the platform also publishes
# plain same-origin paginated JSON feeds (linked from inventorysitemap.xml), one
# per condition:
#   https://{domain}/inventory-used.json   (used; CPO is a subset of used)
#   https://{domain}/inventory-new.json    (new — a separate feed, disjoint VINs)
# shape: {totalVehicles, totalPages, nextPage, pageSize, vehicles:[...]}, walked
# via ?page=N. There is NO combined feed, so we emit one recipe per feed to cover
# the FULL lot (used + new). Parameterized by the dealer DOMAIN alone; the dedicated
# team_velocity parser maps its vehicle objects (and owns VDP image/carfax
# completion); validate_recipe walks ?page=N.
_TEAM_VELOCITY_FEED = "/inventory-used.json"
# Used already contains CPO (verified: cpo VINs ⊆ used), so used + new = full lot.
_TEAM_VELOCITY_FEEDS = ("/inventory-used.json", "/inventory-new.json")


def _detect_team_velocity(html: str, dealer_url: str) -> bool:
    low = html.lower()
    return "teamvelocityportal" in low or "inventoryapibaseurl" in low


def _tv_feed_recipe(dealer_id: str, origin: str, feed_path: str) -> EndpointRecipe | None:
    """Build one Team Velocity feed recipe, probing page 1 for existence + total."""
    url = origin + feed_path
    first = _cosmos_get_json(url)
    if not isinstance(first, dict) or not first.get("vehicles"):
        return None
    try:
        total = int(first.get("totalVehicles") or 0) or None
    except (TypeError, ValueError):
        total = None
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="GET",
        content_type="application/json",
        post_template=None,
        auth_headers={},
        # GET ?page=N feed — replay + delta walk every page (see PAGINATION_PAGE_QUERY).
        pagination=PAGINATION_PAGE_QUERY,
        total_count=total,
        provider_hint="team_velocity",
    )


def _synth_team_velocity(dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    """Emit a recipe per condition feed (used + new) — no combined feed exists."""
    origin = _origin(dealer_url)
    recipes = [r for fp in _TEAM_VELOCITY_FEEDS
               if (r := _tv_feed_recipe(dealer_id, origin, fp)) is not None]
    return recipes
