"""
VDP (Vehicle Detail Page) enrichment — gallery, price, packages, specs, Claude extraction.

- core           — main enrich_vehicles_vdp orchestrator
- html_recovery  — thin gallery recovery from listing HTML
- packages       — structured options/packages parsing
- spec_fetch     — single-URL spec fetch (optional tier)
- specs          — spec-sheet extraction from PAGE_EXTRACT_JS
- claude_extract — Claude Haiku inline VDP extraction
"""
from backend.scanner.vdp.core import *  # noqa: F401, F403
from backend.scanner.vdp.core import (
    GALLERY_COLLECT_URLS_JS,
    enrich_vehicles_vdp,
    _drain_pending_tasks,
    _is_generic_vhr_vin_only_url,
    _max_vdp_concurrency,
    _merge_vdp_vehicle_history_url,
    _pick_best_vehicle_history_url,
    _vehicle_needs_description_vdp,
    _vehicle_needs_spec_gap_vdp,
    _vdp_gallery_loop_max_sec,
    _vdp_queue_sort_key,
    _vdp_response_text_timeout_sec,
    _vdp_visit_one,
)
