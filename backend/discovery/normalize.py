"""
Shared normalization helpers for the discovery pipeline.
"""
from __future__ import annotations

import re
from urllib.parse import urlparse

_ZIP_RE = re.compile(r"^\d{5}$")

# Full US state / DC names → USPS-like 2-letter codes (OSM often uses full names in addr:state).
_US_STATE_FULL_TO_CODE = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "district of columbia": "DC",
}


def normalize_us_state_to_code(raw: str | None) -> str:
    """Return two-letter state code, or '' if unknown."""
    if not raw:
        return ""
    s = str(raw).strip()
    if len(s) == 2 and s.isalpha():
        return s.upper()
    key = re.sub(r"\s+", " ", s.lower()).strip()
    return _US_STATE_FULL_TO_CODE.get(key, "")


def state_code_for_geocode(raw: str | None) -> str:
    """Prefer normalized full-name mapping; fall back to two-letter input."""
    z = normalize_us_state_to_code(raw)
    if len(z) == 2:
        return z
    t = (raw or "").strip().upper()
    return t[:2] if len(t) == 2 else ""

# Domains that are aggregators / search engines / directories, not dealer sites.
_AGGREGATOR_HOSTS = frozenset(
    {
        # Search engines
        "duckduckgo.com",
        "google.com",
        "bing.com",
        "yahoo.com",
        # Car listing aggregators
        "cars.com",
        "cargurus.com",
        "autotrader.com",
        "carfax.com",
        "truecar.com",
        "edmunds.com",
        "kbb.com",
        # carmax.com / carvana.com / vroom.com intentionally NOT listed — they are
        # actual dealers whose own websites are valid discovery targets.
        # Dealer directory sites
        "cardealersnc.com",
        "cardealerdb.com",
        "dealerrater.com",
        "dealers.com",
        "dealer.com",
        "dealer-socket.com",
        "dealersocket.com",
        "dealerfire.com",
        "dealeron.com",
        # Generic business directories
        "superpages.com",
        "yellowpages.com",
        "yellowpages.ca",
        "whitepages.com",
        "bbb.org",
        "bizapedia.com",
        "loc8nearme.com",
        "manta.com",
        "chamberofcommerce.com",
        "mapquest.com",
        "foursquare.com",
        # Maps / geo
        "openstreetmap.org",
        "osm.org",
        "nominatim.openstreetmap.org",
        "overpass-api.de",
        "maps.google.com",
        "goo.gl",
        "maps.app.goo.gl",
        # Social / reference
        "wikipedia.org",
        "yelp.com",
        "facebook.com",
        "instagram.com",
        "tiktok.com",
        "linkedin.com",
        "twitter.com",
        "x.com",
        "nextdoor.com",
        # News / review
        "reddit.com",
        "tripadvisor.com",
        # Review / directory platforms (not dealer sites)
        "birdeye.com",
        "dealeressential.com",
        "businessyab.com",
        # Financial / payments (not dealer sites)
        "capitalone.com",
    }
)


def normalize_zip(raw: str | None) -> str | None:
    """Return zero-padded 5-digit ZIP or None."""
    if not raw:
        return None
    s = re.sub(r"\D", "", str(raw).strip())
    if not s:
        return None
    padded = s.zfill(5)[:5]
    return padded if _ZIP_RE.match(padded) else None


def normalize_url(raw: str | None) -> str | None:
    """
    Return a clean https:// URL or None.

    Rules:
    - Must be http or https scheme.
    - Must not be an aggregator / search engine domain.
    - Normalizes http → https and strips trailing slash.
    """
    if not raw:
        return None
    s = str(raw).strip()
    if not s.startswith(("http://", "https://")):
        s = "https://" + s
    try:
        p = urlparse(s)
    except Exception:
        return None
    if p.scheme not in ("http", "https"):
        return None
    host = (p.netloc or "").lower().removeprefix("www.")
    if not host or host in _AGGREGATOR_HOSTS:
        return None
    # Rebuild with https
    normalized = f"https://{p.netloc}{p.path}".rstrip("/")
    return normalized


def is_aggregator_url(url: str | None) -> bool:
    if not url:
        return True
    try:
        host = urlparse(str(url)).netloc.lower().removeprefix("www.")
    except Exception:
        return True
    return host in _AGGREGATOR_HOSTS or not host


def normalize_address(raw: str | None) -> str | None:
    """Collapse whitespace; return None for blank/placeholder."""
    if not raw:
        return None
    s = " ".join(str(raw).split())
    return s if len(s) >= 5 else None


def looks_like_dealer_website(url: str | None) -> bool:
    """True if *url* is plausibly a dealer-operated site (not search/social/listings)."""
    nu = normalize_url(url)
    if not nu:
        return False
    try:
        host = urlparse(nu).netloc.lower().removeprefix("www.")
    except Exception:
        return False
    if not host or "." not in host:
        return False
    parts = host.split(".")
    if len(parts) < 2:
        return False
    # Reject ISP / parking pages with generic second-level only
    if host.endswith(".wordpress.com") or host.endswith(".wixsite.com"):
        return False
    return True
