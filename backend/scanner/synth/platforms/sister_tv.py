"""
sister.tv (Elasticsearch): detected, not synthesizable.
"""
from __future__ import annotations


# ── Platform: sister.tv (Elasticsearch) — detected, not synthesizable ──────────

# TODO(sister_tv): recognized but NOT synthesizable from current dealer HTML.
# The es-data-v2.sister.tv/vehicles/inventory/_search GET replays fine (a
# library-scoped q=library_id:<id> returns vehicles), but the library_id is not
# in the live HTML of the two known dealers (audiofcostamesa/audifletcherjones,
# fjmercedes) — both have MIGRATED to CarsCommerce (ccid 5379783 / 2924) and are
# already browser-free via the CarsCommerce template. No current dealer embeds a
# sister.tv library_id to extract/validate against, so per the guardrail
# ("only register a template that REPLAYS and yields VINs") sister.tv stays
# synth=None. If a dealer resurfaces exposing library_id, add extraction here.
def _detect_sister_tv(html: str, dealer_url: str) -> bool:
    low = html.lower()
    return "es-data-v2.sister.tv" in low or "sister.tv/vehicles" in low
