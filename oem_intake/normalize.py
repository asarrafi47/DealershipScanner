from __future__ import annotations

import hashlib
import re
from typing import Any
from urllib.parse import urlparse

from SCRAPING.text_utils import collapse_ws, normalize_root

from oem_intake.models import NormalizedDealer

NON_DEALER_DOMAINS = {
    "google.com",
    "maps.google.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "yelp.com",
    "linkedin.com",
    "waze.com",
}


def normalize_dealer_name(s: str) -> str:
    t = collapse_ws(s).lower()
    return re.sub(r"[^a-z0-9\s]", "", t)


def digits_phone(s: str) -> str:
    return re.sub(r"\D", "", s or "")[:10]


def domain_from_url(url: str) -> str:
    r = normalize_root(url)
    if not r:
        return ""
    host = urlparse(r).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def zip5(z: str) -> str:
    z = (z or "").strip()
    m = re.match(r"^(\d{5})", z)
    return m.group(1) if m else z[:5]


def _host_from_url(url: str) -> str:
    r = normalize_root(url)
    if not r:
        return ""
    host = urlparse(r).netloc.lower()
    return host[4:] if host.startswith("www.") else host


def is_map_or_reference_url(url: str) -> bool:
    if not url:
        return False
    p = urlparse(url.strip())
    host = (p.netloc or "").lower()
    path = (p.path or "").lower()
    q = (p.query or "").lower()
    if "google." in host and ("/maps" in path or "maps." in host):
        return True
    if "maps.apple.com" in host or "waze.com" in host:
        return True
    if "directions" in path or "destination=" in q:
        return True
    return False


def is_plausible_dealer_website(url: str) -> bool:
    host = _host_from_url(url)
    if not host:
        return False
    if host in NON_DEALER_DOMAINS:
        return False
    if any(host == d or host.endswith("." + d) for d in NON_DEALER_DOMAINS):
        return False
    return not is_map_or_reference_url(url)


def _pick_best_root_and_map(row: dict[str, Any]) -> tuple[str, str]:
    candidates: list[str] = []
    main = (row.get("website") or "").strip()
    if main:
        candidates.append(main)
    map_ref = (row.get("map_reference_url") or "").strip()
    if map_ref:
        candidates.append(map_ref)
    for u in row.get("candidate_websites") or []:
        if isinstance(u, str) and u.strip():
            candidates.append(u.strip())
    seen: set[str] = set()
    root = ""
    map_url = ""
    for u in candidates:
        n = normalize_root(u)
        if not n or n in seen:
            continue
        seen.add(n)
        if is_plausible_dealer_website(n) and not root:
            root = n
        elif is_map_or_reference_url(u) and not map_url:
            map_url = u.strip()
    return root, map_url


def classify_row_quality(row: dict[str, Any], root_website: str) -> tuple[str, list[str]]:
    reasons: list[str] = []
    name = (row.get("dealer_name") or "").strip()
    city = (row.get("city") or "").strip()
    state = (row.get("state") or "").strip()
    street = (row.get("street") or "").strip()
    if not name:
        reasons.append("missing_dealer_name")
    if not ((city and state) or street):
        reasons.append("missing_location_context")
    if not root_website:
        reasons.append("missing_plausible_dealer_website")
    if not reasons:
        return "usable", []
    if len(reasons) <= 2 and "missing_dealer_name" not in reasons:
        return "partial", reasons
    return "insufficient", reasons


def intake_dict_to_normalized(row: dict[str, Any], *, last_verified_at: str) -> NormalizedDealer:
    name = (row.get("dealer_name") or "").strip()
    brand = (row.get("brand") or "BMW").strip() or "BMW"
    root_web, map_ref = _pick_best_root_and_map(row)
    dom = domain_from_url(root_web)
    quality, reasons = classify_row_quality(row, root_web)
    nd = NormalizedDealer(
        dealer_name=name,
        normalized_dealer_name=normalize_dealer_name(name),
        brand=brand,
        street=(row.get("street") or "").strip(),
        city=(row.get("city") or "").strip(),
        state=(row.get("state") or "").strip(),
        zip=zip5(str(row.get("zip") or "")),
        latitude=row.get("latitude"),
        longitude=row.get("longitude"),
        phone=(row.get("phone") or "").strip(),
        root_website=root_web,
        normalized_root_domain=dom,
        map_reference_url=map_ref,
        new_inventory_url=None,
        used_inventory_url=None,
        dealer_group_canonical=None,
        confidence_score=None,
        row_quality=quality,
        row_rejection_reasons=reasons,
        enrichment_ready=(quality == "usable" and bool(root_web)),
        partial_group_key="",
        source_oem="bmw_usa",
        source_locator_url=(row.get("source_locator_url") or "").strip(),
        last_verified_at=last_verified_at,
    )
    nd.partial_group_key = compute_partial_group_key(nd, row=row)
    nd.extra["source_of_each_field"] = row.get("source_of_each_field") or {}
    rsp = row.get("raw_source_payload") or {}
    if isinstance(rsp, dict):
        nd.extra["zip_seed_hint"] = rsp.get("zip_seed") or rsp.get("zip_seed_hint") or ""
    nd.dedupe_key = compute_dedupe_key(nd)
    return nd


def compute_dedupe_key(d: NormalizedDealer) -> str:
    brand = d.brand.upper()
    dom = d.normalized_root_domain
    z = zip5(d.zip)
    if dom and z:
        return f"d:{dom}|z:{z}|{brand}"
    # Avoid over-merging weak/partial rows across runs.
    if d.row_quality != "usable":
        raw = f"{d.normalized_dealer_name}|{d.street}|{d.city}|{d.state}|{z}|{digits_phone(d.phone)}|{brand}"
        return "p:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    nm = d.normalized_dealer_name
    if nm and z:
        return f"n:{nm}|z:{z}|{brand}"
    ph = digits_phone(d.phone)
    if nm and ph:
        return f"n:{nm}|p:{ph}|{brand}"
    raw = f"{nm}|{d.street}|{d.city}|{brand}"
    return "h:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def missing_address_fields(d: NormalizedDealer) -> bool:
    return not (d.street and d.city and d.state and d.zip)


def compute_partial_group_key(d: NormalizedDealer, *, row: dict[str, Any]) -> str:
    """Stable key for promoting partial rows across runs without over-merging."""
    brand = d.brand.upper()
    nm = d.normalized_dealer_name
    z = zip5(d.zip)
    ph = digits_phone(d.phone)
    dom = d.normalized_root_domain
    if nm and z:
        return f"ps:nz:{nm}|{z}|{brand}"
    if nm and ph:
        return f"ps:np:{nm}|{ph}|{brand}"
    if dom:
        return f"ps:d:{dom}|{brand}"
    raw = f"{nm}|{d.street}|{d.city}|{d.state}|{row.get('source_locator_url','')}|{brand}"
    return "ps:h:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
