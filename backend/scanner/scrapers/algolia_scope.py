"""
Algolia inventory scoping for DealerInspire (and similar) production indexes.

Group/production indexes often include in-transit pipeline stock or sister rooftops.
Manifest overrides win; otherwise we infer safe filters from a sample of hits.
"""
from __future__ import annotations

import logging
import os
import re
from collections import Counter
from typing import Any
from urllib.parse import quote

logger = logging.getLogger(__name__)

_KNOWN_SCOPES = frozenset({"full", "on_lot", "primary_rooftop"})

# DealerInspire production indexes (e.g. crevierbmw-sbm0125_production_inventory).
_PRODUCTION_INDEX_RE = re.compile(r"_production_inventory\b", re.I)


def _auto_scope_enabled() -> bool:
    raw = (os.environ.get("SCANNER_ALGOLIA_AUTO_SCOPE") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def manifest_algolia_filters(dealer: dict[str, Any] | None) -> str | None:
    if not isinstance(dealer, dict):
        return None
    raw = dealer.get("algolia_filters")
    if raw is None:
        return None
    filt = str(raw).strip()
    return filt or None


def manifest_algolia_scope(dealer: dict[str, Any] | None) -> str | None:
    if not isinstance(dealer, dict):
        return None
    raw = dealer.get("algolia_scope")
    if raw is None:
        return None
    scope = str(raw).strip().lower()
    return scope if scope in _KNOWN_SCOPES else None


def _filter_for_scope(scope: str, sample_hits: list[dict[str, Any]]) -> str | None:
    if scope == "full":
        return ""
    if scope == "on_lot":
        # Trust manifest when probe is empty (e.g. Cloudflare shell before config load).
        if not sample_hits or _field_has_values(sample_hits, "in_transit", {"On Lot"}):
            return 'in_transit:"On Lot"'
        return None
    if scope == "primary_rooftop":
        api_id = _primary_on_lot_api_id(sample_hits)
        if api_id:
            return f"api_id:{api_id}"
        return None
    return None


def _field_has_values(hits: list[dict[str, Any]], field: str, want: set[str]) -> bool:
    seen: set[str] = set()
    for h in hits:
        if not isinstance(h, dict):
            continue
        val = str(h.get(field) or "").strip()
        if val:
            seen.add(val)
    return bool(seen & want)


def _primary_on_lot_api_id(hits: list[dict[str, Any]]) -> str | None:
    """Dominant api_id among on-lot rows (DealerInspire rooftop code)."""
    counts: Counter[str] = Counter()
    for h in hits:
        if not isinstance(h, dict):
            continue
        if str(h.get("in_transit") or "").strip() not in ("", "On Lot"):
            continue
        api_id = str(h.get("api_id") or "").strip()
        if api_id:
            counts[api_id] += 1
    if not counts:
        return None
    top_id, top_n = counts.most_common(1)[0]
    total_on_lot = sum(counts.values())
    if total_on_lot and top_n / total_on_lot >= 0.55:
        return top_id
    return None


def infer_algolia_filters(
    dealer: dict[str, Any] | None,
    *,
    index_name: str,
    sample_hits: list[dict[str, Any]],
) -> str:
    """
    Resolve Algolia ``filters`` param for a dealer/index.

    Priority: manifest ``algolia_filters`` → manifest ``algolia_scope`` → auto-infer.
    """
    explicit = manifest_algolia_filters(dealer)
    if explicit is not None:
        return explicit

    scope = manifest_algolia_scope(dealer)
    if scope:
        filt = _filter_for_scope(scope, sample_hits)
        if filt is not None:
            return filt
        logger.warning(
            "Algolia scope %r requested but could not build filter for index %s",
            scope,
            index_name,
        )
        return ""

    if not _auto_scope_enabled():
        return ""
    if not _PRODUCTION_INDEX_RE.search(index_name or ""):
        return ""
    if not sample_hits:
        return ""

    in_transit_vals = Counter(
        str(h.get("in_transit") or "").strip() for h in sample_hits if isinstance(h, dict)
    )
    in_transit_vals.pop("", None)
    if "In-Transit" in in_transit_vals and "On Lot" in in_transit_vals:
        in_transit_n = in_transit_vals.get("In-Transit", 0)
        on_lot_n = in_transit_vals.get("On Lot", 0)
        total = in_transit_n + on_lot_n
        if total and in_transit_n / total >= 0.08:
            logger.info(
                "Algolia auto-scope: production index %s — applying on-lot filter "
                "(sample in-transit %d / on-lot %d)",
                index_name,
                in_transit_n,
                on_lot_n,
            )
            return 'in_transit:"On Lot"'

    api_ids = Counter(str(h.get("api_id") or "").strip() for h in sample_hits if isinstance(h, dict))
    api_ids.pop("", None)
    if len(api_ids) >= 2:
        primary = _primary_on_lot_api_id(sample_hits)
        if primary:
            logger.info(
                "Algolia auto-scope: production index %s — applying primary api_id:%s",
                index_name,
                primary,
            )
            return f"api_id:{primary}"

    return ""


def encode_algolia_filter_param(filters: str) -> str:
    """URL-encode Algolia filter string for the ``params`` query segment."""
    return quote((filters or "").strip(), safe="")


def post_filter_algolia_hits(
    hits: list[dict[str, Any]],
    dealer: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """
    Optional post-query row filter (e.g. OEM make guard for BMW storefronts).
    """
    if not hits or not isinstance(dealer, dict):
        return hits

    raw = dealer.get("algolia_make_guard")
    if raw is False:
        return hits
    if raw is True or str(raw or "").strip().lower() in ("1", "true", "yes", "on"):
        allowed = {"BMW", "MINI"}
    elif isinstance(raw, list):
        allowed = {str(x).strip().upper() for x in raw if str(x).strip()}
    else:
        name = str(dealer.get("name") or "").lower()
        url = str(dealer.get("url") or "").lower()
        if "bmw" not in name and "bmw" not in url:
            return hits
        allowed = {"BMW", "MINI"}

    kept: list[dict[str, Any]] = []
    dropped = 0
    for h in hits:
        make = str(h.get("make") or "").strip().upper()
        if make and make not in allowed:
            dropped += 1
            continue
        kept.append(h)
    if dropped:
        logger.info(
            "Algolia make guard: kept %d, dropped %d non-OEM row(s) for %s",
            len(kept),
            dropped,
            dealer.get("name") or dealer.get("dealer_id"),
        )
    return kept
