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


def _nav_timeout_ms() -> int:
    raw = (os.environ.get("SCANNER_VDP_NAV_TIMEOUT_MS") or "32000").strip()
    try:
        return max(5000, int(raw))
    except ValueError:
        return 32000


def _vdp_response_text_timeout_sec() -> float:
    raw = (os.environ.get("SCANNER_VDP_RESPONSE_TEXT_TIMEOUT_SEC") or "8").strip()
    try:
        return max(1.0, min(60.0, float(raw)))
    except ValueError:
        return 8.0


def _vdp_gallery_loop_max_sec(site_profile: Any = None) -> float:
    """Wall-clock cap per VDP gallery carousel harvest (URL count stays uncapped)."""
    opt = ""
    if isinstance(site_profile, dict):
        opt = str(site_profile.get("optimize_for") or "").strip().lower()
    if opt == "bmw":
        raw = (os.environ.get("SCANNER_VDP_GALLERY_MAX_SEC_BMW") or "300").strip()
    else:
        raw = (os.environ.get("SCANNER_VDP_GALLERY_MAX_SEC") or "45").strip()
    try:
        return max(30.0, min(600.0, float(raw)))
    except ValueError:
        return 300.0 if opt == "bmw" else 45.0


def _max_vdp_concurrency() -> int:
    from backend.scanner.scan_efficiency import effective_vdp_concurrency

    return effective_vdp_concurrency()


def _vdp_gallery_min_https() -> int:
    try:
        return max(1, int((os.environ.get("SCANNER_VDP_GALLERY_MIN_HTTPS") or "3").strip()))
    except ValueError:
        return 3


def _vdp_gallery_priority_enabled() -> bool:
    return (os.environ.get("SCANNER_VDP_GALLERY_PRIORITY") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


