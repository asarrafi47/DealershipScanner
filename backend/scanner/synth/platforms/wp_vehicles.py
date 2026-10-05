"""
WordPress dealer sites with /wp-json/v1/vehicles platform template.
"""
from __future__ import annotations

from backend.scanner.recipes import EndpointRecipe, PAGINATION_NONE
from backend.scanner.synth.common import _origin


# ── Platform: WordPress dealer sites with /wp-json/v1/vehicles ─────────────────
# Burns Honda, Honda of Cleveland, Honda of Pasadena, Kia of Chattanooga (2026-09-24):
# WordPress (WP Rocket, wpforms, an ADF lead plugin) exposing a REST index of every
# car as {title, link, search}. The index is the whole lot in one GET; identity
# comes from the title, everything else from the detail page's JSON-LD.
_WP_VEHICLES_PATH = "/wp-json/v1/vehicles"


def _detect_wp_vehicles_index(html: str, dealer_url: str) -> bool:
    low = html.lower()
    if "/wp-json/" not in low:
        return False
    return any(m in low for m in ("adf_lead_nonce", "asc_datalayer", "favorites_data", "wpforms_settings"))


def _fetch_wp_vehicles(origin: str) -> list[dict] | None:
    try:
        from curl_cffi import requests as cr

        r = cr.get(origin + _WP_VEHICLES_PATH, impersonate="chrome", timeout=30,
                   headers={"Accept": "application/json", "Referer": origin + "/"})
        if r.status_code != 200:
            return None
        js = r.json()
    except Exception:  # noqa: BLE001
        return None
    v = js.get("vehicles") if isinstance(js, dict) else None
    return v if isinstance(v, list) else None


def _synth_wp_vehicles_index(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    origin = _origin(dealer_url)
    items = _fetch_wp_vehicles(origin)
    if not items or len(items) < 5:
        return None
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=origin + _WP_VEHICLES_PATH,
        method="GET",
        content_type="application/json",
        post_template=None,
        auth_headers={},
        pagination=PAGINATION_NONE,
        provider_hint="wp_vehicles_index",
        vehicle_rows=len(items),
        total_count=len(items),
    )
