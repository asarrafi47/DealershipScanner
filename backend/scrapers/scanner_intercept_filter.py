"""
Inventory JSON intercept gating for ``scanner.py`` (pure helpers — unit-tested).

Reject third-party JSON (e.g. payment widgets) that still matches ``find_vehicle_list``
heuristically. Default: same hostname / subdomain as dealer ``dealers.json`` URL, OR URL
contains a substring from ``SCANNER_INTERCEPT_URL_ALLOW``. Substrings in
``SCANNER_INTERCEPT_URL_DENY`` always reject (e.g. ``carnow.com``, ``payments``).
"""
from __future__ import annotations

import json
import os
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
        "cdk",
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


def intercept_url_allowed(response_url: str, dealer_base_url: str) -> bool:
    """
    True if this response URL may carry dealer inventory JSON for *dealer_base_url*.

    Order: deny-list → then (same-site OR allow-list substring match).
    """
    if not response_url or not str(response_url).strip().lower().startswith("http"):
        return False
    low = response_url.lower()
    for sub in _env_csv_substrings("SCANNER_INTERCEPT_URL_DENY"):
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
        key = (has_tc, n, int(tc) if tc is not None else 0)
        if key > best_key:
            best_key = key
            best_tc = int(tc) if tc is not None else None

    if best_tc is not None and best_tc > 0:
        return best_tc
    for resp_url, body in reversed(records):
        if not intercept_url_allowed(resp_url, dealer_base_url):
            continue
        if isinstance(body, dict):
            return get_total_count(body)
        return None
    return None
