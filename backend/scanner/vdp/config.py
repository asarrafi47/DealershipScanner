"""
VDP env-knob helpers (process-lifetime tunables).

Reads env at call time — never at import time — because the scanner CLI (``cli.py``)
sets knobs via ``os.environ`` after import. The browser gallery/nav knobs
(``_nav_timeout_ms``, ``_vdp_response_text_timeout_sec``, ``_vdp_gallery_loop_max_sec``,
``_vdp_gallery_min_https``, ``_vdp_gallery_priority_enabled``) were deleted 2026-10-01:
no readers after the browser VDP pool went away.
"""
from __future__ import annotations


def _max_vdp_concurrency() -> int:
    from backend.scanner.scan_efficiency import effective_vdp_concurrency

    return effective_vdp_concurrency()
