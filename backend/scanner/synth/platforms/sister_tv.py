"""
sister.tv (Elasticsearch): detected, not synthesizable.
"""
from __future__ import annotations


# ── Platform: sister.tv (Elasticsearch) — detected, not synthesizable ──────────

# sister.tv is recognized but not synthesizable from current dealer HTML.
# The es-data-v2.sister.tv/vehicles/inventory/_search GET replays fine (a
# library-scoped q=library_id:<id> returns vehicles), but no live dealer HTML
# exposes the library_id to extract and validate against, so per the guardrail
# ("only register a template that REPLAYS and yields VINs") sister.tv stays
# synth=None. Observed state: 3 dealers (audiofcostamesa-com,
# audifletcherjones-com, fjmercedes-com) carry a captured recipe with the same
# library_id 202405220922, replayed alongside their carscommerce recipes;
# ownership is under investigation by the attribution owner.
def _detect_sister_tv(html: str, dealer_url: str) -> bool:
    low = html.lower()
    return "es-data-v2.sister.tv" in low or "sister.tv/vehicles" in low
