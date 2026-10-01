"""
VDP (vehicle detail page) helpers for HTTP-only scans.

- prefetch       — per-car detail-page HTML pass + DB carry-forward (scan time)
- vdp_recipes    — per-dealer VDP JSON recipe capture/replay
- extract        — capture analysis / EP fragments used by vdp_recipes
- html_recovery  — gallery/description recovery from listing HTML (post-scan)
- spec_fetch     — single-URL spec fetch (enrichment)
- queue          — per-row gap predicates (spec gap, HTTPS gallery count)
- config         — env knobs (VDP concurrency)
- core           — re-export surface (see its docstring)
"""
from backend.scanner.vdp.core import (  # noqa: F401
    _count_https_gallery_urls,
    _is_generic_vhr_vin_only_url,
    _max_vdp_concurrency,
    _merge_vdp_vehicle_history_url,
    _pick_best_vehicle_history_url,
    _vehicle_needs_spec_gap_vdp,
)
