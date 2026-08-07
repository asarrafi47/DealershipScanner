"""
VDP env-knob helpers (process-lifetime tunables).

Every function reads ``os.environ`` at call time — never at import time — because the
scanner CLI (``cli.py``) sets several of these via ``os.environ`` as IPC after import.
Keep this module a leaf: stdlib only (plus lazy imports of scan_efficiency).
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def _vdp_max_per_dealer(override: int | None = None) -> int:
    if override is not None:
        return max(0, int(override))
    raw = (os.environ.get("SCANNER_VDP_EP_MAX") or "10").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 10


def _vdp_price_max_per_dealer(override: int | None = None) -> int:
    """Extra unique listing URLs for rows still missing price after inventory JSON (aligned with Node ``scanner.js``)."""
    if override is not None:
        return max(0, min(5000, int(override)))
    raw = (os.environ.get("SCANNER_VDP_PRICE_MAX") or "400").strip()
    try:
        return max(0, min(5000, int(raw)))
    except ValueError:
        return 400


def _vdp_spec_gap_max_per_dealer() -> int:
    """Extra VDP visits for inventory rows missing key specs (engine, transmission, …)."""
    from backend.scanner.scan_efficiency import effective_vdp_spec_gap_max

    return effective_vdp_spec_gap_max(10_000)


def _vdp_description_max_per_dealer(override: int | None = None) -> int:
    if override is not None:
        return max(0, min(2000, int(override)))
    raw = (os.environ.get("SCANNER_VDP_DESCRIPTION_MAX") or "120").strip()
    try:
        return max(0, min(2000, int(raw)))
    except ValueError:
        return 120


def _nav_timeout_ms() -> int:
    raw = (os.environ.get("SCANNER_VDP_NAV_TIMEOUT_MS") or "32000").strip()
    try:
        return max(5000, int(raw))
    except ValueError:
        return 32000


def _settle_ms() -> int:
    raw = (os.environ.get("SCANNER_VDP_SETTLE_MS") or "2200").strip()
    try:
        return max(200, int(raw))
    except ValueError:
        return 2200


def _vdp_js_timeout_ms() -> int:
    """Cap Playwright ``evaluate`` calls (gallery harvest can hang on huge DOM)."""
    raw = (os.environ.get("SCANNER_VDP_JS_TIMEOUT_MS") or "12000").strip()
    try:
        return max(2000, min(120000, int(raw)))
    except ValueError:
        return 12000


def _vdp_response_text_timeout_sec() -> float:
    raw = (os.environ.get("SCANNER_VDP_RESPONSE_TEXT_TIMEOUT_SEC") or "8").strip()
    try:
        return max(1.0, min(60.0, float(raw)))
    except ValueError:
        return 8.0


def _vdp_drain_pending_timeout_sec() -> float:
    raw = (os.environ.get("SCANNER_VDP_DRAIN_PENDING_TIMEOUT_SEC") or "12").strip()
    try:
        return max(2.0, min(120.0, float(raw)))
    except ValueError:
        return 12.0


def _vdp_gallery_loop_max_sec(site_profile: Any = None) -> float:
    """Wall-clock cap per VDP gallery carousel harvest (URL count stays uncapped)."""
    opt = ""
    if isinstance(site_profile, dict):
        opt = str(site_profile.get("optimize_for") or "").strip().lower()
    if opt == "bmw":
        raw = (os.environ.get("SCANNER_VDP_GALLERY_MAX_SEC_BMW") or "300").strip()
    else:
        raw = (os.environ.get("SCANNER_VDP_GALLERY_MAX_SEC") or "150").strip()
    try:
        return max(30.0, min(600.0, float(raw)))
    except ValueError:
        return 300.0 if opt == "bmw" else 150.0


def _max_vdp_concurrency() -> int:
    from backend.scanner.scan_efficiency import effective_vdp_concurrency

    return effective_vdp_concurrency()


def _vdp_gallery_min_https() -> int:
    try:
        return max(1, int((os.environ.get("SCANNER_VDP_GALLERY_MIN_HTTPS") or "3").strip()))
    except ValueError:
        return 3


def _vdp_gallery_skip_if_feed_ge() -> int:
    """
    Skip the per-VDP carousel interaction loop when the listing feed already supplied
    at least this many HTTPS gallery images for the vehicle.

    The carousel harvest loop is the dominant VDP cost (up to the wall-clock cap per
    vehicle). On platforms whose listing feed already returns full galleries
    (DealerOn cosmos, Dealer.com, eProcess results API, Algolia), that harvest only
    *extends* an already-good gallery, so it can be skipped while still doing the cheap
    nav + EP/spec/price extraction.

    Default ``0`` = disabled (always harvest — current behaviour preserved exactly).
    Set e.g. ``SCANNER_VDP_GALLERY_SKIP_IF_FEED_GE=8`` to enable.
    """
    raw = (os.environ.get("SCANNER_VDP_GALLERY_SKIP_IF_FEED_GE") or "0").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _vdp_gallery_priority_enabled() -> bool:
    return (os.environ.get("SCANNER_VDP_GALLERY_PRIORITY") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _gallery_max_rounds() -> int:
    try:
        return max(4, min(120, int((os.environ.get("SCANNER_VDP_GALLERY_MAX_ROUNDS") or "80").strip())))
    except ValueError:
        return 80


def _gallery_idle_rounds() -> int:
    try:
        return max(1, min(20, int((os.environ.get("SCANNER_VDP_GALLERY_IDLE_ROUNDS") or "3").strip())))
    except ValueError:
        return 3


def _vdp_gallery_open_lightbox_enabled() -> bool:
    """When truthy, try to open the dealer photo lightbox before scoped DOM gallery harvest (default: on)."""
    return (os.environ.get("SCANNER_VDP_GALLERY_OPEN_LIGHTBOX") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _vdp_download_images_enabled() -> bool:
    """Default: skip the byte download; set ``SCANNER_VDP_DOWNLOAD_IMAGES=1`` to opt in.

    Nothing reads these files -- the site and the iOS app render the dealer's remote URLs from
    ``cars.gallery`` -- so storing copies buys no product value.
    """
    raw = (os.environ.get("SCANNER_VDP_DOWNLOAD_IMAGES") or "0").strip().lower()
    return raw not in ("0", "false", "no", "off", "")


def _vdp_image_download_dir() -> Path:
    raw = (os.environ.get("SCANNER_VDP_IMAGE_DOWNLOAD_DIR") or "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path(__file__).resolve().parents[2] / "vdp_images"


def _vdp_spin_capture_enabled() -> bool:
    """Default: capture 360-spin assets (Impel/SpinCar/WebRotate); ``SCANNER_VDP_SPIN_CAPTURE=0`` to skip."""
    raw = (os.environ.get("SCANNER_VDP_SPIN_CAPTURE") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off", "")


def _vdp_spin_max_sec() -> float:
    try:
        return max(2.0, float(os.environ.get("SCANNER_VDP_SPIN_MAX_SEC") or 12.0))
    except (TypeError, ValueError):
        return 12.0
