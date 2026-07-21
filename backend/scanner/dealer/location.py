"""
Detect sister-store / off-lot inventory listed on a dealer's website.

Many dealer groups publish another store's vehicles on a storefront with low local
stock. When we can read a per-vehicle location (inventory JSON or VDP DOM), we
drop rows that do not match the dealership being scanned.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import urlparse

log = logging.getLogger(__name__)

_STOPWORDS = frozenset(
    {
        "the",
        "and",
        "of",
        "at",
        "in",
        "llc",
        "inc",
        "co",
        "company",
        "motor",
        "motors",
        "automotive",
        "auto",
        "cars",
        "car",
        "dealer",
        "dealership",
        "group",
    }
)

_LOCATION_KEY_RE = re.compile(
    r"dealer|location|lot|store|account|selling|physical|located",
    re.I,
)

_LOCATED_AT_RE = re.compile(
    r"(?:located\s+at|location\s*:)\s*(.+?)(?:\.|\||$|\n|·)",
    re.I,
)

_CITY_STATE_RE = re.compile(
    r"\b([A-Za-z][A-Za-z .'-]{2,28}),\s*([A-Z]{2})\b"
)


@dataclass(frozen=True)
class DealerSiteProfile:
    """Canonical identity for the storefront being scanned."""

    dealer_id: str
    name: str
    url: str
    city: str = ""
    state: str = ""


def sister_store_filter_enabled() -> bool:
    raw = (os.environ.get("SCANNER_SISTER_STORE_FILTER") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def build_dealer_site_profile(dealer: dict[str, Any]) -> DealerSiteProfile:
    name = (dealer.get("name") or "").strip()
    url = (dealer.get("url") or "").strip()
    city = (dealer.get("city") or "").strip()
    state = _norm_state(dealer.get("state") or "")
    reg_id = dealer.get("dealership_registry_id")
    if reg_id and (not city or not state):
        try:
            from backend.db.dealerships_db import get_dealership_by_id

            row = get_dealership_by_id(int(reg_id))
            if row:
                city = city or (row.get("city") or "").strip()
                state = state or _norm_state(row.get("state") or "")
        except Exception:
            pass
    return DealerSiteProfile(
        dealer_id=(dealer.get("dealer_id") or "").strip(),
        name=name,
        url=url,
        city=city,
        state=state,
    )


def _norm_state(st: str) -> str:
    s = re.sub(r"[^A-Za-z]", "", (st or "").strip().upper())
    return s[:2] if len(s) >= 2 else ""


def _normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").lower().replace("&", " and ")).strip()


def _name_tokens(name: str) -> set[str]:
    out: set[str] = set()
    for part in re.split(r"[^a-z0-9]+", _normalize_text(name)):
        if len(part) >= 3 and part not in _STOPWORDS:
            out.add(part)
    return out


def _host_stem(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host.split(".")[0] if host else ""
    except Exception:
        return ""


def _pick_str(*vals: Any) -> str:
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() not in ("n/a", "na", "null", "none"):
            return s
    return ""


_URL_LIKE_RE = re.compile(r"^\s*(?:https?://|www\.)|\.(?:com|net|org)\b", re.I)


def _is_url_like(value: str) -> bool:
    """
    A URL is not a lot label.

    Group feeds carry the storefront domain on every row (Dealer.com's
    ``dealerDomain``), so sweeping it into the location text made the host check
    in ``classify_vehicle_location`` fire for every vehicle -- a sister store's
    car read as a match for the store hosting the feed.
    """
    return bool(_URL_LIKE_RE.search(value or ""))


def extract_location_from_inventory_object(obj: dict[str, Any]) -> str:
    """Best-effort lot / selling dealer label from a raw inventory vehicle object."""
    if not isinstance(obj, dict):
        return ""
    parts: list[str] = []
    for key in (
        "locatedAt",
        "located_at",
        "locationName",
        "location_name",
        "vehicleLocation",
        "vehicle_location",
        "dealerName",
        "dealer_name",
        "sellingDealerName",
        "selling_dealer_name",
        "accountName",
        "account_name",
        "physicalLocation",
        "lotName",
        "lot_name",
        "storeName",
        "store_name",
    ):
        v = _pick_str(obj.get(key))
        if v:
            parts.append(v)
    city = _pick_str(obj.get("dealerCity"), obj.get("dealer_city"), obj.get("city"))
    state = _norm_state(_pick_str(obj.get("dealerState"), obj.get("dealer_state"), obj.get("state")))
    if city and state:
        parts.append(f"{city}, {state}")
    elif city:
        parts.append(city)

    arr = obj.get("trackingAttributes") or obj.get("tracking_attributes")
    if isinstance(arr, list):
        try:
            from backend.parsers.base import find_tracking_attr

            for attr in (
                "dealerName",
                "dealer_name",
                "location",
                "locationName",
                "vehicleLocation",
                "sellingDealer",
                "lotName",
                "accountName",
            ):
                hit = find_tracking_attr(arr, attr, "value")
                if hit is not None:
                    s = _pick_str(hit)
                    if s:
                        parts.append(s)
        except ImportError:
            pass

    for k, v in obj.items():
        if not isinstance(k, str) or not _LOCATION_KEY_RE.search(k):
            continue
        if isinstance(v, str) and len(v) > 2 and not _is_url_like(v):
            parts.append(v.strip())

    seen: set[str] = set()
    merged: list[str] = []
    for p in parts:
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        merged.append(p)
    return " | ".join(merged)[:500]


def extract_location_from_vehicle_row(v: dict[str, Any]) -> str:
    """Location hints already stored on a scanner vehicle dict."""
    for key in ("_lot_location", "_inventory_location", "lot_location"):
        s = _pick_str(v.get(key))
        if s:
            return s
    return ""


def extract_location_from_vdp_bundle(bundle: dict[str, Any]) -> str:
    """Merge DOM / spec snippets from ``PAGE_EXTRACT_JS`` capture."""
    if not isinstance(bundle, dict):
        return ""
    parts: list[str] = []
    for sn in bundle.get("domLocationSnippets") or []:
        if isinstance(sn, str) and sn.strip():
            parts.append(sn.strip())
    specs = bundle.get("domSpecs")
    if isinstance(specs, dict):
        for k, val in specs.items():
            if not isinstance(k, str) or not isinstance(val, str):
                continue
            if _LOCATION_KEY_RE.search(k) or _LOCATION_KEY_RE.search(val):
                parts.append(f"{k}: {val}".strip())
    for badge in bundle.get("domBadges") or []:
        if isinstance(badge, str) and _LOCATION_KEY_RE.search(badge):
            parts.append(badge.strip())
    body = _pick_str(bundle.get("pageTextSample"))
    if body:
        for m in _LOCATED_AT_RE.finditer(body):
            parts.append(m.group(1).strip())
    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        k = p.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(p)
    return " | ".join(out)[:500]


def classify_vehicle_location(
    location_text: str,
    profile: DealerSiteProfile,
) -> str:
    """
    Return ``match``, ``mismatch``, or ``unknown``.

    ``unknown`` means we could not decide — caller should keep the vehicle.
    """
    loc = (location_text or "").strip()
    if not loc or len(loc) < 4:
        return "unknown"

    m = _LOCATED_AT_RE.search(loc)
    if m:
        loc = m.group(1).strip()
        if not loc:
            return "unknown"

    loc_n = _normalize_text(loc)
    name_n = _normalize_text(profile.name)
    city_n = _normalize_text(profile.city)
    state = profile.state

    if name_n and name_n in loc_n:
        if not city_n or city_n in loc_n:
            return "match"
        if state and f", {state.lower()}" in loc_n:
            return "match"

    if city_n and state:
        has_city = city_n in loc_n
        has_state = state.lower() in loc_n or f", {state.lower()}" in loc_n
        if has_city and has_state:
            return "match"
        if has_state and not has_city:
            for city_part, st in _CITY_STATE_RE.findall(loc):
                c_norm = _normalize_text(city_part)
                if st.upper() == state and c_norm and c_norm != city_n:
                    return "mismatch"

    host = _host_stem(profile.url)
    if host and len(host) >= 5 and host in loc_n.replace("-", "").replace(" ", ""):
        return "match"

    if name_n:
        sim = SequenceMatcher(None, name_n, loc_n).ratio()
        if sim >= 0.72:
            return "match"
        prof_tok = _name_tokens(profile.name)
        loc_tok = _name_tokens(loc)
        if prof_tok:
            overlap = len(prof_tok & loc_tok) / max(1, len(prof_tok))
            if overlap >= 0.65 and (not city_n or city_n in loc_n):
                return "match"
            if overlap >= 0.2 and city_n and city_n not in loc_n:
                if any(t in loc_n for t in loc_tok if t not in prof_tok and len(t) >= 5):
                    return "mismatch"
            if overlap < 0.35 and sim < 0.5 and len(loc_n) >= 12:
                if city_n and city_n not in loc_n:
                    return "mismatch"

    return "unknown"


def filter_sister_store_vehicles(
    vehicles: list[dict[str, Any]],
    profile: DealerSiteProfile,
    *,
    source: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Drop vehicles whose known lot location does not match ``profile``.

    Returns (kept_vehicles, stats).
    """
    stats: dict[str, Any] = {
        "source": source,
        "input": len(vehicles),
        "kept": 0,
        "excluded": 0,
        "unknown_location": 0,
        "matched": 0,
        "samples_excluded": [],
    }
    if not sister_store_filter_enabled():
        stats["skipped"] = True
        stats["kept"] = len(vehicles)
        return vehicles, stats

    kept: list[dict[str, Any]] = []
    for v in vehicles:
        loc = extract_location_from_vehicle_row(v)
        verdict = classify_vehicle_location(loc, profile)
        if verdict == "unknown":
            stats["unknown_location"] += 1
            kept.append(v)
            continue
        if verdict == "match":
            stats["matched"] += 1
            kept.append(v)
            continue
        stats["excluded"] += 1
        if len(stats["samples_excluded"]) < 5:
            stats["samples_excluded"].append(
                {"vin": (v.get("vin") or "")[:17], "location": loc[:120]}
            )
    stats["kept"] = len(kept)
    # Safety: pooled-inventory feeds often tag sister rooftops — do not zero the whole lot.
    min_keep = 8
    try:
        import os

        min_keep = max(1, int((os.environ.get("SCANNER_SISTER_STORE_MIN_KEEP") or "8").strip()))
    except ValueError:
        pass
    safe_raw = (os.environ.get("SCANNER_SISTER_STORE_SAFE") or "1").strip().lower()
    safe_enabled = safe_raw not in ("0", "false", "no", "off")
    if (
        safe_enabled
        and stats["input"] >= min_keep
        and len(kept) == 0
        and stats["excluded"] > 0
        and stats["matched"] == 0
    ):
        log.warning(
            "Sister-store filter [%s] %s: would exclude entire lot (%d rows) — "
            "keeping all (set SCANNER_SISTER_STORE_SAFE=0 to allow empty result)",
            profile.name,
            source,
            stats["input"],
        )
        stats["aborted_empty"] = True
        stats["kept"] = stats["input"]
        stats["excluded"] = 0
        return vehicles, stats

    if stats["excluded"]:
        log.info(
            "Sister-store filter [%s] %s: excluded %d / %d (unknown=%d matched=%d) samples=%s",
            profile.name,
            source,
            stats["excluded"],
            stats["input"],
            stats["unknown_location"],
            stats["matched"],
            stats["samples_excluded"],
        )
    return kept, stats


def apply_vdp_location_verdict(
    vehicle: dict[str, Any],
    bundle: dict[str, Any],
    profile: DealerSiteProfile,
) -> str:
    """
    If VDP capture shows an off-lot location, mark ``vehicle`` for exclusion.

    Returns verdict string (``match`` / ``mismatch`` / ``unknown``).
    """
    if not sister_store_filter_enabled():
        return "unknown"
    loc = extract_location_from_vdp_bundle(bundle)
    if loc:
        vehicle["_lot_location"] = loc
    verdict = classify_vehicle_location(loc, profile)
    if verdict == "mismatch":
        vehicle["_sister_store_exclude"] = True
    return verdict
