"""Keyword and host heuristics for BMW locator discovery (noise vs signal)."""
from __future__ import annotations

import re
from typing import Any

# Flag responses / scripts that may carry dealer or geo state
BODY_KEYWORDS: tuple[str, ...] = (
    "dealer",
    "retailer",
    "center",
    "dealership",
    "location",
    "latitude",
    "longitude",
    "postal",
    "zip",
    "dealerlocator",
    "dealerLocator",
    "stores",
    "features",
    "map marker",
    "marker",
    "bmw center",
    "outlet",
    "geojson",
    "featurecollection",
)

SCRIPT_KEYWORDS: tuple[str, ...] = BODY_KEYWORDS + (
    "window.",
    "__INITIAL",
    "__NEXT_DATA__",
    "hydrat",
    "redux",
    "apollo",
    "graphql",
)

# Host/path substrings almost always unrelated to dealer roster
NOISE_URL_SUBSTRINGS: tuple[str, ...] = (
    "demdex.net",
    "doubleclick",
    "google-analytics",
    "googletagmanager",
    "facebook.net",
    "scorecardresearch",
    "adservice",
    "adsystem",
    "adnxs",
    "rubicon",
    "hotjar",
    "mpulse",
    "go-mpulse",
    "newrelic",
    "optimizely",
    "adobedc.net",
    "adobe.com",
    "target.",
    "everestjs.net",
    "autointel.ai",
    "segment.com",
    "segment.io",
    "mparticle",
    "launchdarkly",
    "cookie",
    "onetrust",
    "trustarc",
    "cdn.cookielaw.org",
)

# Semi-interesting but often not dealer JSON — downrank unless keywords hit
MAPS_NOISE_UNLESS_KEYWORDS: tuple[str, ...] = (
    "maps.googleapis.com/$rpc",
    "GetViewportInfo",
    "maps.gstatic.com",
)


def keyword_hits_in_text(text: str, *, keywords: tuple[str, ...] = BODY_KEYWORDS) -> list[str]:
    if not text:
        return []
    low = text.lower()
    hits: list[str] = []
    for kw in keywords:
        if kw.lower() in low:
            hits.append(kw)
    return hits


def is_likely_noise_url(url: str) -> bool:
    u = url.lower()
    return any(s in u for s in NOISE_URL_SUBSTRINGS)


def is_maps_rpc_noise_unless_dealer(url: str, body_lower: str) -> bool:
    u = url.lower()
    if not any(m in u for m in MAPS_NOISE_UNLESS_KEYWORDS):
        return False
    return "dealer" not in body_lower and "retailer" not in body_lower and "bmw" not in body_lower


def classify_response_bucket(
    url: str,
    content_type: str,
    body_preview: str,
) -> tuple[str, str]:
    """
    Returns (bucket, reason) where bucket is:
    likely_noise | likely_relevant | ambiguous
    """
    ct = (content_type or "").lower()
    bl = (body_preview or "").lower()
    u = url.lower()

    if is_likely_noise_url(url):
        return "likely_noise", "host_matches_known_tracking_or_consent_pattern"

    if is_maps_rpc_noise_unless_dealer(url, bl):
        return "likely_noise", "google_maps_internal_rpc_without_dealer_keywords"

    hits = keyword_hits_in_text(body_preview, keywords=BODY_KEYWORDS)
    if len(hits) >= 3:
        return "likely_relevant", f"multiple_body_keyword_hits:{','.join(hits[:8])}"

    if len(hits) >= 1:
        if "json" in ct or "graphql" in u or "/api/" in u or "bmw" in u:
            return "likely_relevant", f"keyword_hits_plus_structured_or_bmw_url:{','.join(hits[:6])}"
        return "ambiguous", f"keyword_hits_need_context:{','.join(hits[:6])}"

    if "graphql" in u or ("graphql" in ct and "json" in ct):
        return "ambiguous", "graphql_response_no_dealer_keywords_in_preview"

    if "protobuf" in ct or "json+protobuf" in ct:
        return "ambiguous", "protobuf_json_shape_may_encode_map_data"

    if "/api/" in u and "bmw" in u:
        return "ambiguous", "bmw_path_api_without_keyword_hits_in_preview"

    return "likely_noise", "no_dealer_keywords_and_no_structured_signal_in_preview"


_PHONE_RE = re.compile(r"\(\d{3}\)\s*\d{3}[-\s]?\d{4}|\d{3}[-.\s]?\d{3}[-.\s]?\d{4}")
_ZIP_RE = re.compile(r"\b\d{5}(?:-\d{4})?\b")
_CITY_STATE_ZIP_RE = re.compile(r"\b([A-Za-z .'-]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\b")


def extract_dom_card_guess(text: str) -> dict[str, Any]:
    """Heuristic extraction from a visible block of text."""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    phone = ""
    m = _PHONE_RE.search(text)
    if m:
        phone = m.group(0)
    zips = _ZIP_RE.findall(text)
    zip_ = zips[0] if zips else ""
    city = ""
    state = ""
    m2 = _CITY_STATE_ZIP_RE.search(text)
    if m2:
        city = m2.group(1).strip()
        state = m2.group(2).strip()
        if not zip_:
            zip_ = m2.group(3).strip()
    address_line = ""
    for ln in lines[1:8]:
        if re.search(r"\d", ln) and ("," in ln or re.search(r"\b[A-Z]{2}\b", ln)):
            address_line = ln
            break
    name = lines[0] if lines else ""
    return {
        "dealer_name_guess": name[:200],
        "lines": lines[:12],
        "phone_guess": phone,
        "address_guess": address_line[:220],
        "city_guess": city[:120],
        "state_guess": state[:8],
        "zip_guess": zip_[:12],
    }
