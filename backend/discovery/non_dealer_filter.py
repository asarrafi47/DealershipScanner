"""
Non-dealer filter for the discovery pipeline.

Google Places (and other tiers) mislabel many businesses as ``car_dealer``:
self-service salvage / u-pull yards, car-rental outfits, RV dealers, the odd
restaurant, direct-to-consumer EV brands with no franchise dealer network, and
the auction / wholesale platforms we never want to scan. Those inflate the
denominator when we report franchise-dealer coverage.

``is_probable_non_dealer(name, url, ...)`` is a conservative NAME + URL keyword
predicate (plus an optional Google Places ``primaryType`` / ``types`` check).
It is deliberately biased toward *keeping* anything ambiguous so real franchise
dealers (gardenahonda.com, toyotasunnyvale.com, ...) are never dropped.
"""
from __future__ import annotations

import re
from urllib.parse import urlparse

# --- NAME keyword patterns (matched against the lowercased business name) ------
# Each entry is a regex fragment. Word boundaries are used where a bare substring
# would risk clipping a real dealer name (e.g. "rv" inside "Irvine").
_NAME_PATTERNS: tuple[str, ...] = (
    # self-service salvage / u-pull / junk yards
    r"\bsalvage\b",
    r"\bjunk\s*yard\b",
    r"\bjunkyard\b",
    r"\bu[\s\-]*pull\b",
    r"\bpull[\s\-]*a[\s\-]*part\b",
    r"\bpull[\s\-]*n[\s\-]*save\b",
    r"\bpick[\s\-]*n[\s\-]*pull\b",
    r"\bpick[\s\-]*n[\s\-]*save\b",
    r"\bpull[\s\-]*it\b",
    r"\bpull[\s\-]*&[\s\-]*pay\b",
    r"\bpull[\s\-]*and[\s\-]*pay\b",
    r"\bwrench[\s\-]*a[\s\-]*part\b",
    r"\byou[\s\-]*pull[\s\-]*it\b",
    # car rental
    r"\brental\b",
    r"\brent[\s\-]*a[\s\-]*car\b",
    # RV / camping
    r"\brv\b",
    r"\brvs\b",
    r"\bmotor\s*home\b",
    r"\bmotorhome\b",
    r"\bcamping\s+world\b",
    # restaurants (real hit: North Italia)
    r"\brestaurant\b",
    r"\bnorth\s+italia\b",
    # direct-to-consumer brands with no franchise dealers
    r"\brivian\b",
    r"\btesla\b",
    # auctions / wholesale platforms we never scan
    r"\bauction\b",
    r"\bmanheim\b",
    r"\bcopart\b",
    r"\bcarvana\b",
    r"\bcarmax\b",
    r"\badesa\b",
)

# --- URL host substrings (matched against the lowercased hostname) -------------
# Substring match is safe here because these are specific brand domains.
_URL_HOST_SUBSTRINGS: tuple[str, ...] = (
    "picknpull",
    "pullapart",
    "u-pull",
    "upullit",
    "gopullit",
    "wrenchapart",
    "-pull-",
    "uwrench",
    "rvworld",
    "campingworld",
    "rivian.com",
    "tesla.com",
    "manheim.com",
    "copart.com",
    "carvana.com",
    "carmax.com",
    "adesa.com",
)

# --- Google Places primaryType / types values that are never a franchise dealer
_NON_DEALER_PLACE_TYPES: frozenset[str] = frozenset(
    {
        "restaurant",
        "car_rental",
        "rv_park",
        "campground",
        "used_car_dealer_auction",  # defensive; not a real Places type
        "auto_parts_store",
    }
)

_NAME_RE = re.compile("|".join(_NAME_PATTERNS), re.IGNORECASE)


def _host_of(url: str | None) -> str:
    if not url:
        return ""
    s = str(url).strip()
    if not s:
        return ""
    if "://" not in s:
        s = "https://" + s
    try:
        host = urlparse(s).netloc.lower()
    except ValueError:
        return ""
    return host.removeprefix("www.")


def is_probable_non_dealer(
    name: str | None,
    url: str | None = None,
    *,
    place_primary_type: str | None = None,
    place_types: list[str] | None = None,
) -> bool:
    """
    True when a discovered ``car_dealer`` candidate is probably NOT a franchise
    dealer and should be dropped / flagged (``is_dealer: false``).

    Conservative by design: only fires on high-confidence salvage / rental / RV /
    restaurant / direct-to-consumer / auction signals in the NAME or URL host, or
    on an explicit non-dealer Google Places ``primaryType`` / ``types`` value.
    """
    # Google Places primaryType is the strongest signal when present.
    pt = (place_primary_type or "").strip().lower()
    if pt and pt in _NON_DEALER_PLACE_TYPES:
        return True
    if place_types:
        typeset = {str(t).strip().lower() for t in place_types}
        # Drop only when a non-dealer type is present AND car_dealer is absent,
        # so a legit dealer that also lists e.g. auto_parts_store is kept.
        if typeset & _NON_DEALER_PLACE_TYPES and "car_dealer" not in typeset:
            return True

    name_s = (name or "").strip()
    if name_s and _NAME_RE.search(name_s):
        return True

    host = _host_of(url)
    if host and any(sub in host for sub in _URL_HOST_SUBSTRINGS):
        return True

    return False
