"""
Compatibility surface for ``from backend.scanner.vdp import X`` / ``vdp.core import X``.

Scans are HTTP-only (docs/HTTP_ONLY_SCANS_PLAN.md): the per-car layer is
``vdp/prefetch.py`` (detail-page HTML pass, DB carry-forward) and ``vdp/vdp_recipes.py``
(per-dealer VDP JSON recipe capture/replay, using ``vdp/extract.py``).
``vdp/html_recovery.py`` recovers gallery/description from listing HTML after the scan;
``vdp/spec_fetch.py`` is the single-URL spec fetch used by enrichment.

The browser VDP stack (visit / gallery / spin_capture / dispatch / browser_js) was deleted
2026-09-26; its leftovers (queue scoring, price_hints, packages, specs, claude_extract and
the gallery/nav knobs) were deleted 2026-10-01. Only names with a live consumer are
re-exported here:
- ``_max_vdp_concurrency`` — orchestrator
- ``_vehicle_needs_spec_gap_vdp`` — vdp/prefetch.py
- ``_count_https_gallery_urls`` — vdp/prefetch.py, vdp/vdp_recipes.py (import vdp.queue)
- vehicle-history URL helpers — tests (test_monroney_merge)
"""
from __future__ import annotations

from backend.scanner.vdp.config import _max_vdp_concurrency  # noqa: F401
from backend.scanner.vdp.extract import (  # noqa: F401
    _is_generic_vhr_vin_only_url,
    _merge_vdp_vehicle_history_url,
    _pick_best_vehicle_history_url,
)
from backend.scanner.vdp.queue import (  # noqa: F401
    _count_https_gallery_urls,
    _vehicle_needs_spec_gap_vdp,
)
