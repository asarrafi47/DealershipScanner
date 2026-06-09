"""
Scanner throughput helpers: inventory path sets, intercept sufficiency gates, fast-mode defaults.
"""

from __future__ import annotations

import os
from typing import Any

from backend.scanner.scrapers.scanner_intercept_filter import (
    effective_lot_total_from_intercepts,
    max_algolia_nb_hits_from_intercepts,
    max_vehicle_list_len_from_intercepts,
)

# Dealer.com-style sites: three category entry points (avoid duplicate /inventory/ aliases).
INVENTORY_PATHS_CORE: tuple[str, ...] = (
    "/new-inventory/index.htm",
    "/used-inventory/index.htm",
    "/certified-inventory/index.htm",
)

# DealerInspire / Maven Algolia storefronts.
INVENTORY_PATHS_DEALER_INSPIRE: tuple[str, ...] = (
    "/new-vehicles/",
    "/used-vehicles/",
    "/certified-pre-owned/",
)

INVENTORY_PATHS_EXTENDED: tuple[str, ...] = (
    *INVENTORY_PATHS_CORE,
    "/new-inventory/",
    "/used-inventory/",
    "/inventory/",
    "/new-vehicles/",
    "/used-vehicles/",
)


def scanner_fast_mode_enabled() -> bool:
    raw = (os.environ.get("SCANNER_FAST_MODE") or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def scanner_scan_only_enabled() -> bool:
    """When true, inventory capture runs without inline post-scan (use post_scan.py)."""
    raw = (os.environ.get("SCANNER_SCAN_ONLY") or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def apply_fast_mode_env_defaults() -> None:
    """Set conservative defaults when SCANNER_FAST_MODE=1 (only unset vars)."""
    if not scanner_fast_mode_enabled():
        return
    completeness = (os.environ.get("SCANNER_VDP_COMPLETENESS_PASS") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    defaults: dict[str, str] = {
        "SCANNER_VDP_EP_MAX": "0",
        "SCANNER_VDP_PRICE_MAX": "80",
        "SCANNER_POST_INTERIOR_VISION": "0",
        "SCANNER_GALLERY_VISION_INLINE": "0",
        "SCANNER_SCAN_ONLY": "1",
    }
    if completeness:
        defaults.pop("SCANNER_VDP_EP_MAX", None)
        defaults["SCANNER_VDP_PRICE_MAX"] = "0"
    for key, val in defaults.items():
        os.environ.setdefault(key, val)


def apply_scan_only_env_defaults() -> None:
    """Disable post-scan stages when SCANNER_SCAN_ONLY=1 (only unset vars)."""
    if not scanner_scan_only_enabled():
        return
    for key in (
        "SCANNER_POST_REPAIR",
        "SCANNER_POST_LISTING_DESCRIPTION",
        "SCANNER_POST_INTERIOR_VISION",
        "SCANNER_POST_WINDOW_STICKER",
        "SCANNER_POST_GAS_PRICES",
        "SCANNER_POST_DEALER_RATINGS",
        "SCANNER_POST_LISTING_GAP_FILL",
        "SCANNER_POST_DICT_ENRICH",
        "SCANNER_POST_ENRICH",
        "SCANNER_POST_ENRICH_VISION",
        "SCANNER_GALLERY_VISION_POST",
        "SCANNER_GALLERY_VISION_INLINE",
        "SCANNER_VDP_DOWNLOAD_IMAGES",
    ):
        os.environ.setdefault(key, "0")


def inventory_paths_for_dealer(dealer: dict[str, Any] | None = None) -> list[str]:
    """
    Paths opened in parallel per dealer. Default: three category pages.
    Set ``SCANNER_INVENTORY_PATHS=extended`` for the legacy nine-path sweep.
    """
    mode = (os.environ.get("SCANNER_INVENTORY_PATHS") or "core").strip().lower()
    if mode in ("extended", "full", "all", "legacy"):
        return list(INVENTORY_PATHS_EXTENDED)
    dealer_paths = dealer.get("inventory_paths") if isinstance(dealer, dict) else None
    if isinstance(dealer_paths, list) and dealer_paths:
        out = [str(p).strip() for p in dealer_paths if str(p).strip()]
        if out:
            return out
    provider = ""
    if isinstance(dealer, dict):
        provider = str(dealer.get("provider") or "").strip().lower()
    if provider == "dealer_inspire":
        return list(INVENTORY_PATHS_DEALER_INSPIRE)
    return list(INVENTORY_PATHS_CORE)


def _intercept_coverage_ratio() -> float:
    try:
        return max(0.5, min(1.0, float((os.environ.get("SCANNER_INTERCEPT_COVERAGE_RATIO") or "0.92").strip())))
    except ValueError:
        return 0.92


def _intercept_min_rows() -> int:
    try:
        return max(1, int((os.environ.get("SCANNER_INTERCEPT_MIN_ROWS") or "8").strip()))
    except ValueError:
        return 8


def intercept_feed_is_sufficient(
    intercept_records: list[tuple[str, Any]],
    dealer_base_url: str,
    vehicle_row_count: int,
    *,
    unique_vin_count: int | None = None,
) -> bool:
    """
    True when JSON intercepts likely captured the full lot (skip DealerOn/Venom/Inspire augment).

    Prefer *unique_vin_count* when supplied (deduped); raw row counts can overstate coverage.
    """
    rows = unique_vin_count if unique_vin_count is not None else vehicle_row_count
    if rows < _intercept_min_rows():
        return False
    if not intercept_records:
        return False
    total = effective_lot_total_from_intercepts(intercept_records, dealer_base_url)
    if total is not None and total > 0:
        if rows < int(total * _intercept_coverage_ratio()):
            return False
        # Algolia page-local totalCount can match one page while another block shows the real index.
        algolia_max = max_algolia_nb_hits_from_intercepts(intercept_records, dealer_base_url)
        if algolia_max is not None and algolia_max > total and rows < int(algolia_max * _intercept_coverage_ratio()):
            return False
        return True
    # No totalCount: require a healthy intercept payload with many VIN rows.
    best_n = max_vehicle_list_len_from_intercepts(intercept_records, dealer_base_url)
    return best_n >= _intercept_min_rows() and rows >= _intercept_min_rows()


def effective_vdp_ep_max(deduped_rows: int) -> int:
    """
    Cap EP VDP visits per dealer. When SCANNER_VDP_COMPLETENESS_PASS=1, visit all rows
    (up to SCANNER_VDP_COMPLETENESS_MAX, default 400) so public-incomplete lots get filled.
    """
    completeness = (os.environ.get("SCANNER_VDP_COMPLETENESS_PASS") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    if completeness:
        try:
            cap = int((os.environ.get("SCANNER_VDP_COMPLETENESS_MAX") or "400").strip())
        except ValueError:
            cap = 400
        return min(max(0, deduped_rows), max(1, cap))
    raw = (os.environ.get("SCANNER_VDP_EP_MAX") or "").strip()
    if raw:
        try:
            return max(0, min(5000, int(raw)))
        except ValueError:
            pass
    if scanner_fast_mode_enabled():
        return min(10, max(0, deduped_rows))
    return min(10, max(0, deduped_rows))


def effective_vdp_price_max(deduped_rows: int) -> int:
    """Cap price VDP visits by lot size unless SCANNER_VDP_PRICE_MAX overrides."""
    raw = (os.environ.get("SCANNER_VDP_PRICE_MAX") or "").strip()
    if raw:
        try:
            return max(0, min(5000, int(raw)))
        except ValueError:
            pass
    if scanner_fast_mode_enabled():
        return min(80, max(0, deduped_rows // 4))
    return min(400, max(0, deduped_rows))


def effective_vdp_spec_gap_max(deduped_rows: int) -> int:
    """
    Extra VDP visits for rows missing engine/transmission/etc. after inventory JSON.

    Set ``SCANNER_VDP_SPEC_GAP_MAX`` (e.g. 40) for scan-only runs that skip full EP caps.
  """
    raw = (os.environ.get("SCANNER_VDP_SPEC_GAP_MAX") or "").strip()
    if not raw:
        return 0
    try:
        cap = max(0, min(500, int(raw)))
    except ValueError:
        return 0
    return min(cap, max(0, deduped_rows))


def gallery_vision_inline_enabled() -> bool:
    """Claude gallery filter during ``run_dealer`` (blocks upsert). Default off."""
    if scanner_fast_mode_enabled():
        return False
    raw = (os.environ.get("SCANNER_GALLERY_VISION_INLINE") or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def inventory_json_wait_ms(
    *,
    json_already_captured: bool,
    inv_wait_ms: int,
    pag_wait_ms: int,
) -> int:
    """
    After hydration, avoid waiting the full inventory window when JSON already landed.
    Saves up to ~15s per inventory path on typical Dealer.com SPA loads.
    """
    if json_already_captured:
        try:
            tail = int((os.environ.get("SCANNER_INVENTORY_JSON_TAIL_MS") or "2500").strip())
        except ValueError:
            tail = 2500
        return max(300, min(tail, pag_wait_ms, inv_wait_ms))
    return inv_wait_ms


def inventory_idle_loop_sec(json_captured: bool) -> int:
    """Seconds for post-navigation idle polling (1s steps)."""
    if json_captured:
        raw = (os.environ.get("SCANNER_INVENTORY_IDLE_SEC_FOUND") or "2").strip()
    else:
        raw = (os.environ.get("SCANNER_INVENTORY_IDLE_SEC_MISSING") or "6").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 2 if json_captured else 6


def effective_vdp_concurrency() -> int:
    """
    Parallel VDP browser workers per dealer.

    Default ``4`` (was ``12``) — gallery carousel harvest is heavy per page; override with
    ``SCANNER_MAX_VDP_CONCURRENCY``.
    """
    raw = (os.environ.get("SCANNER_MAX_VDP_CONCURRENCY") or "").strip()
    if raw:
        try:
            return max(1, min(64, int(raw)))
        except ValueError:
            pass
    return 4


def vdp_gallery_carousel_only() -> bool:
    """
    When true (default), VDP gallery merge uses carousel/DOM harvest only — not page-wide
    network image capture or dataLayer JSON walks (avoids badges, logos, marketing tiles).
    Set ``SCANNER_VDP_GALLERY_CAROUSEL_ONLY=0`` to restore the wider harvest.
    """
    raw = (os.environ.get("SCANNER_VDP_GALLERY_CAROUSEL_ONLY") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def vdp_gallery_url_max() -> int | None:
    """
    Max HTTPS gallery URLs stored per VDP visit.

    Default ``None`` (no cap — harvest every carousel photo). Set ``SCANNER_VDP_GALLERY_MAX_URLS``
    to a positive integer to cap storage.
    """
    raw = (os.environ.get("SCANNER_VDP_GALLERY_MAX_URLS") or "").strip()
    if not raw or raw.lower() in ("0", "unlimited", "none", "off"):
        return None
    try:
        return max(4, int(raw))
    except ValueError:
        return None


def gallery_vision_post_enabled() -> bool:
    """Post-scan gallery cleanup for touched VINs. On with ``--enable-gallery-vision``."""
    raw = (os.environ.get("SCANNER_GALLERY_VISION_POST") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    return False
