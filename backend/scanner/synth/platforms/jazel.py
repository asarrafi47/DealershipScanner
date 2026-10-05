"""
Jazel (SSR Angular SRP, jzlSetVehicleInfoContext) platform template.
"""
from __future__ import annotations

import re

from backend.scanner.recipes import _unique_vins, EndpointRecipe, PAGINATION_JAZEL_SRP
from backend.scanner.synth.common import _origin
from backend.scanner.synth.http import _dep_fetch_html


# ── Platform: Jazel (SSR Angular SRP, jzlSetVehicleInfoContext) ────────────────

# Jazel serves a fully server-rendered SRP whose per-card vehicle JSON is embedded
# as window.jzlSetVehicleInfoContext('VIN', {...}) inline calls. Parameterized by
# the dealer DOMAIN alone; paginated by a path segment (.../srp-page-N/). The
# recipe is an HTML page-walk whose parser (jazel) regexes the calls. Note: Jazel
# is a dealer-GROUP platform (one SRP can mix rooftops), like mazdariverside.
_JAZEL_SRP_PATH = "/inventory/all-vehicles/"


def _detect_jazel(html: str, dealer_url: str) -> bool:
    low = html.lower()
    return (
        "jzlsetvehicleinfocontext" in low
        or "window.jzla5p" in low
        or "jazelc.com" in low
        or "jazel-cdn" in low
    )


def _synth_jazel(dealer_id: str, dealer_url: str, html: str) -> EndpointRecipe | None:
    origin = _origin(dealer_url)
    url = origin + _JAZEL_SRP_PATH
    page1 = _dep_fetch_html(url)
    if not page1:
        return None
    from backend.parsers.jazel import parse as _jazel_parse

    vins = _unique_vins(_jazel_parse(page1, base_url=origin, dealer_id=dealer_id, dealer_url=origin))
    if not vins:
        return None
    total: int | None = None
    m = re.search(r'([\d,]{1,7})\s+vehicles', page1, re.IGNORECASE)
    if m:
        try:
            total = int(m.group(1).replace(",", "")) or None
        except ValueError:
            total = None
    return EndpointRecipe(
        dealer_id=dealer_id,
        url=url,
        method="GET",
        content_type="text/html",
        post_template=None,
        auth_headers={},
        pagination=PAGINATION_JAZEL_SRP,
        total_count=total,
        provider_hint="jazel",
    )
