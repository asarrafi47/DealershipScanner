"""
Inventory JSON intercept gating for ``scanner.py`` (pure helpers — unit-tested).

Reject third-party JSON (e.g. payment widgets) while still capturing SPA/API payloads via
``payload_qualifies_for_inventory_intercept`` (classic ≥3 VIN rows in a nested list, or
inventory-shaped JSON keys + ≥1 VIN row). Default URL policy: same hostname / subdomain as
dealer ``dealers.json`` URL, OR URL contains a substring from ``SCANNER_INTERCEPT_URL_ALLOW``.
Substrings in ``SCANNER_INTERCEPT_URL_DENY`` always reject (e.g. ``carnow.com``, ``payments``).
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from backend.parsers.base import find_vehicle_list, get_total_count

# Baseline allow substrings (merged with config/scanner_intercept_policy.json when present).
_FALLBACK_ALLOW_SUBSTRINGS = frozenset(
    (
        "getinventory",
        "getinventoryandfacets",
        "ws-inv-data",
        "/inventory",
        "algolia",
        "algolianet",
        "dealer.com",
        "dealerinspire",
        "typesense",
        "cdk",
    )
)

# Built-in deny substrings — rejected before same-site / allow-list checks.
_FALLBACK_DENY_SUBSTRINGS = frozenset(
    (
        "calc/payment",
        "/api/calc/",
        "payment-calculator",
        "paymentcalculator",
        "/checkout/",
        "finance/calculator",
        "finance-calculator",
        "/trade-in",
        "/tradein",
        "creditapp",
        "credit-app",
        "apply-for-credit",
    )
)

_POLICY_MERGED_ALLOWS: frozenset[str] | None = None


def _policy_file_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "scanner_intercept_policy.json"


def _default_allow_substrings() -> frozenset[str]:
    """File defaults (shared with scanner.js) unioned with built-in fallback."""
    global _POLICY_MERGED_ALLOWS
    if _POLICY_MERGED_ALLOWS is not None:
        return _POLICY_MERGED_ALLOWS
    extra: set[str] = set()
    try:
        with open(_policy_file_path(), encoding="utf-8") as f:
            data = json.load(f)
        for x in data.get("default_allow_url_substrings") or []:
            s = str(x).strip().lower()
            if s:
                extra.add(s)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        pass
    _POLICY_MERGED_ALLOWS = frozenset(_FALLBACK_ALLOW_SUBSTRINGS | extra)
    return _POLICY_MERGED_ALLOWS


def reload_scanner_intercept_policy() -> None:
    """Clear cached policy (tests or hot edits to JSON)."""
    global _POLICY_MERGED_ALLOWS
    _POLICY_MERGED_ALLOWS = None


def _env_csv_substrings(name: str) -> frozenset[str]:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return frozenset()
    parts = [x.strip() for x in raw.split(",") if x.strip()]
    return frozenset(parts)


def _host_key(url: str) -> str:
    try:
        h = (urlparse(url).hostname or "").strip().lower()
    except ValueError:
        return ""
    if h.startswith("www."):
        h = h[4:]
    return h


def same_dealer_site(response_url: str, dealer_base_url: str) -> bool:
    """True when response host equals dealer host or is a subdomain of it (or reverse)."""
    rh = _host_key(response_url)
    dh = _host_key(dealer_base_url)
    if not rh or not dh:
        return False
    if rh == dh:
        return True
    if rh.endswith("." + dh):
        return True
    if dh.endswith("." + rh):
        return True
    return False


def response_content_type_looks_json(content_type: str | None) -> bool:
    """True for ``application/json``, ``application/*+json``, etc."""
    if not content_type:
        return False
    base = content_type.split(";")[0].strip().lower()
    return base == "application/json" or base.endswith("+json")


_STRONG_INVENTORY_KEYS = frozenset(
    {
        "inventory",
        "vehicles",
        "vehiclelist",
        "vehicleinventory",
        "inventoryvehicles",
        "inventorylist",
        "inventoryresults",
        "usedinventory",
        "newinventory",
        "certifiedinventory",
        "vehicleresults",
        "hits",
    }
)

_INVENTORY_META_KEYS = frozenset(
    {
        "inventorytype",
        "inventorystatus",
        "inventorytypecode",
        "inventorycondition",
        "inventoryage",
        "inventorysource",
        "inventoryid",
    }
)

_VEHICLE_META_KEYS = frozenset(
    {
        "vehicletype",
        "vehicletypes",
        "vehiclecondition",
        "vehiclestatus",
        "vehicleid",
        "vehiclespec",
    }
)


def _normalize_json_key(k: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(k).strip().lower())


def _dict_key_suggests_inventory_container(key: object) -> bool:
    nk = _normalize_json_key(key)
    if nk in _STRONG_INVENTORY_KEYS:
        return True
    if nk in _INVENTORY_META_KEYS or nk in _VEHICLE_META_KEYS:
        return False
    if "inventory" in nk:
        return True
    if "vehicle" in nk and any(
        frag in nk for frag in ("search", "list", "result", "offer", "browse", "stock")
    ):
        return True
    return False


def payload_has_inventory_json_structure_signal(
    body: Any,
    *,
    max_depth: int = 8,
    _depth: int = 0,
) -> bool:
    """
    True when nested JSON uses typical SPA/API container keys (``inventory``, ``vehicles``, …).
    Used with ``find_vehicle_list(..., min_vin_count=1)`` to widen interception without relying
    on HTML-first extraction.
    """
    if body is None or _depth > max_depth:
        return False
    if isinstance(body, dict):
        for key in body.keys():
            if _dict_key_suggests_inventory_container(key):
                return True
        for v in body.values():
            if isinstance(v, (dict, list)) and payload_has_inventory_json_structure_signal(
                v, max_depth=max_depth, _depth=_depth + 1
            ):
                return True
    elif isinstance(body, list):
        for item in body[:40]:
            if isinstance(item, (dict, list)) and payload_has_inventory_json_structure_signal(
                item, max_depth=max_depth, _depth=_depth + 1
            ):
                return True
    return False


def payload_qualifies_for_inventory_intercept(body: Any) -> bool:
    """
    Structured-data interception: classic feed (≥3 VIN rows) OR SPA-shaped payload with ≥1 VIN row.

    Stricter than ``find_vehicle_list(..., 1)`` alone — weak single-VIN widgets still need an
    inventory-like key path (see ``payload_has_inventory_json_structure_signal``).
    """
    if body is None:
        return False
    if find_vehicle_list(body, min_vin_count=3) is not None:
        return True
    if find_vehicle_list(body, min_vin_count=1) is None:
        return False
    return payload_has_inventory_json_structure_signal(body)


def intercept_url_allowed(response_url: str, dealer_base_url: str) -> bool:
    """
    True if this response URL may carry dealer inventory JSON for *dealer_base_url*.

    Order: deny-list → then (same-site OR allow-list substring match).
    """
    if not response_url or not str(response_url).strip().lower().startswith("http"):
        return False
    low = response_url.lower()
    for sub in _FALLBACK_DENY_SUBSTRINGS | _env_csv_substrings("SCANNER_INTERCEPT_URL_DENY"):
        if sub in low:
            return False
    if same_dealer_site(response_url, dealer_base_url):
        return True
    for sub in _default_allow_substrings() | _env_csv_substrings("SCANNER_INTERCEPT_URL_ALLOW"):
        if sub in low:
            return True
    return False


def vehicle_list_len(body: Any, *, min_vin_count: int = 3) -> int:
    lst = find_vehicle_list(body, min_vin_count=min_vin_count) if body is not None else None
    return len(lst) if lst else 0


def max_algolia_nb_hits_from_intercepts(
    records: list[tuple[str, Any]],
    dealer_base_url: str,
) -> int | None:
    """
    Largest Algolia ``nbHits`` among intercept blocks that include a vehicle hit list.

    Used when ``pick_total_count_from_intercepts`` returns a page-local total (e.g. 53)
    while another block reports the full index size (e.g. 387).
    """
    best: int | None = None
    for resp_url, body in records:
        if not intercept_url_allowed(resp_url, dealer_base_url):
            continue
        if not isinstance(body, dict):
            continue
        blocks: list[dict[str, Any]] = []
        results = body.get("results")
        if isinstance(results, list):
            blocks.extend(b for b in results if isinstance(b, dict))
        else:
            blocks.append(body)
        for block in blocks:
            hits = block.get("hits")
            if not isinstance(hits, list) or _vin_dict_count(hits) < 3:
                continue
            nb = block.get("nbHits") or block.get("nb_hits")
            if nb is not None and isinstance(nb, (int, float)):
                n = int(nb)
                if n > 0 and (best is None or n > best):
                    best = n
    return best


def _vin_dict_count(lst: list) -> int:
    return sum(
        1
        for i in lst
        if isinstance(i, dict) and ("vin" in i or "VIN" in i or "item_id" in i)
    )


def effective_lot_total_from_intercepts(
    records: list[tuple[str, Any]],
    dealer_base_url: str,
) -> int | None:
    """Best estimate of full-lot size for pagination / sufficiency (max of page + Algolia hints)."""
    picked = pick_total_count_from_intercepts(records, dealer_base_url)
    algolia_max = max_algolia_nb_hits_from_intercepts(records, dealer_base_url)
    if picked is None and algolia_max is None:
        return None
    if picked is None:
        return algolia_max
    if algolia_max is None:
        return picked
    return max(picked, algolia_max)


def max_vehicle_list_len_from_intercepts(
    records: list[tuple[str, Any]],
    dealer_base_url: str,
) -> int:
    best = 0
    for resp_url, body in records:
        if not intercept_url_allowed(resp_url, dealer_base_url):
            continue
        best = max(best, vehicle_list_len(body, min_vin_count=1))
    return best


def pick_total_count_from_intercepts(
    records: list[tuple[str, Any]],
    dealer_base_url: str,
) -> int | None:
    """
    Choose ``totalCount`` from the best matching intercept: prefer payloads with a
    numeric totalCount / pageInfo total, tie-break by larger vehicle-list length.
    *records* are ``(response_url, parsed_json_body)`` (typically URL-gated on append).
    """
    best_tc: int | None = None
    best_key: tuple[int, int, int] = (-1, -1, -1)

    for resp_url, body in records:
        if not intercept_url_allowed(resp_url, dealer_base_url):
            continue
        if not isinstance(body, dict):
            continue
        tc = get_total_count(body)
        n = vehicle_list_len(body)
        has_tc = 1 if tc is not None and int(tc) > 0 else 0
        # Prefer the tightest total for this page (Algolia facet blocks often report index-wide nbHits).
        tc_i = int(tc) if tc is not None else 0
        key = (has_tc, n, -tc_i)
        if key > best_key:
            best_key = key
            best_tc = tc_i if tc_i > 0 else None

    if best_tc is not None and best_tc > 0:
        return best_tc
    for resp_url, body in reversed(records):
        if not intercept_url_allowed(resp_url, dealer_base_url):
            continue
        if isinstance(body, dict):
            return get_total_count(body)
        return None
    return None
