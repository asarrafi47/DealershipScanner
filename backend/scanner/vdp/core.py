"""
VDP (vehicle detail page) enrichment for the Playwright scanner (scanner.py).

This module does not start Playwright; the ``page`` passed in is the same as the main scanner
browser, which (when ``playwright_stealth`` is installed) is created via
``Stealth().use_async(async_playwright())`` in ``scanner.py``.

Runs during the main scan when SCANNER_VDP_EP_MAX > 0 or SCANNER_VDP_PRICE_MAX > 0: visits up to N
unique listing (VDP) URLs per phase — EP/gallery budget plus optional extra URLs for rows missing price.
per dealer, extracts analytics ep.*, network JSON, JSON-LD, inline JSON, and DOM heuristics,
then merges into vehicle rows via merge_analytics_ep_into_vehicle (conservative fallback).
Gallery URLs from network JSON and in-page extraction are merged separately (see
backend.utils.gallery_merge.merge_vdp_gallery_into_vehicle) after EP merge.

Gallery interaction (after load + ``PAGE_EXTRACT_JS`` settle): ``GALLERY_COLLECT_URLS_JS`` runs
on the main document and in each frame (SpinCar/Impel iframes, etc.; cross-origin frames are
skipped) in a loop while advancing the carousel (ArrowRight → next/chevron locators → thumbnails)
until no new unique HTTPS URLs appear for ``SCANNER_VDP_GALLERY_IDLE_ROUNDS`` rounds or
``SCANNER_VDP_GALLERY_MAX_ROUNDS``. A short randomized pointer move runs before the gallery loop
to nudge React/lazy clients. Network image URLs use ``Content-Type`` (e.g. ``image/webp``) not only
URL extensions; JSON bodies are parsed when ``Content-Type`` is JSON-like or ``text/plain`` (e.g. GraphQL).
Image ``response`` URLs merge with DOM harvest.

``SCANNER_VDP_GALLERY_OPEN_LIGHTBOX`` (default ``1``): before the gallery URL harvest loop, try
Playwright clicks to open the same full-screen photo modal a user would (e.g. “N of M Photos”,
“View all photos”); when the modal is open, ``GALLERY_COLLECT_URLS_JS`` prefers ``[role=dialog]`` /
``[aria-modal]`` and large image regions over harvesting every ``img`` on the page. Set to ``0`` to
use legacy whole-document harvest only. Validate manually on a DMS with a lightbox, or with
``python -c "from scanner_vdp import GALLERY_COLLECT_URLS_JS; assert 'pickGalleryRoot' in GALLERY_COLLECT_URLS_JS"``.

Local VDP image download (default **off**): gallery bytes are saved under
``SCANNER_VDP_IMAGE_DOWNLOAD_DIR`` (default ``vdp_images`` in the process cwd), keyed by VIN with
optional ``SCANNER_VDP_IMAGE_DOWNLOAD_KEY=vin|stock`` (default ``vin``). A ``manifest.json`` is
written per vehicle folder; a compact summary is merged into ``spec_source_json`` (``vdp_gallery_local``).
Serving and enrichment read the dealer's remote URLs from ``cars.gallery``, never these bytes, so
copies are kept only for one-off local analysis: set ``SCANNER_VDP_DOWNLOAD_IMAGES=1`` to opt in.
Reuses ``SCANNER_VDP_NAV_TIMEOUT_MS``, ``SCANNER_VDP_SETTLE_MS``, ``SCANNER_MAX_VDP_CONCURRENCY``.

VDP price hints (JSON-LD ``offers``, ``dataLayer`` keys like ``internetPrice`` / ``salePrice``, light
DOM) merge into ``price`` only when the listing has no positive price; provenance is stored under
``spec_source_json`` key ``vdp_price`` when applied (see ``backend.scanner.database.upsert_vehicles``).

Split (2026-08): env knobs live in ``vdp.config``, queue scoring in ``vdp.queue``, capture
analysis / EP fragments in ``vdp.extract``, gallery interaction in ``vdp.gallery``, page JS in
``vdp.browser_js``. Split (2026-09): price-hint merge/ripple live in ``vdp.price_hints``, the
360-spin capture step in ``vdp.spin_capture`` (alongside its pure URL helpers), the per-VDP visit
worker in ``vdp.visit``, and the ``enrich_vehicles_vdp`` dispatcher in ``vdp.dispatch``. Everything
is re-imported here so the historical import surface (``from backend.scanner.vdp.core import X``
and package-level ``from backend.scanner.vdp import X``) is unchanged.
"""
from __future__ import annotations

from backend.scanner.vdp.browser_js import (  # noqa: F401
    GALLERY_COLLECT_URLS_JS,
    GALLERY_MODAL_NUDGE_JS,
    PAGE_EXTRACT_JS,
)

# Re-exports: these moved into sibling leaf modules in the 2026-08/2026-09 splits but stay
# importable from core (and, via __init__, from the package) for compatibility.
# Env-var reads inside them stay lazy (functions read os.environ at call time).
from backend.scanner.vdp.config import (  # noqa: F401
    _gallery_idle_rounds,
    _gallery_max_rounds,
    _max_vdp_concurrency,
    _nav_timeout_ms,
    _settle_ms,
    _vdp_description_max_per_dealer,
    _vdp_download_images_enabled,
    _vdp_drain_pending_timeout_sec,
    _vdp_gallery_loop_max_sec,
    _vdp_gallery_min_https,
    _vdp_gallery_open_lightbox_enabled,
    _vdp_gallery_priority_enabled,
    _vdp_gallery_skip_if_feed_ge,
    _vdp_image_download_dir,
    _vdp_js_timeout_ms,
    _vdp_max_per_dealer,
    _vdp_price_max_per_dealer,
    _vdp_response_text_timeout_sec,
    _vdp_spec_gap_max_per_dealer,
    _vdp_spin_capture_enabled,
    _vdp_spin_max_sec,
)
from backend.scanner.vdp.extract import (  # noqa: F401
    PRIORITY,
    VEHICLE_SIGNAL_KEYS,
    _GENERIC_VHR_VIN_ONLY,
    _analyze_json_signals,
    _build_fragments_from_vdp_capture,
    _combine_ep_fragments,
    _dom_specs_to_ep,
    _is_generic_vhr_vin_only_url,
    _ld_to_ep,
    _looks_like_vin17,
    _merge_vdp_sticker_url,
    _merge_vdp_vehicle_history_url,
    _pick_best_sticker_url,
    _pick_best_vehicle_history_url,
    _pick_vehicle_like_object,
    _response_maybe_gallery_image_url,
    _response_origin,
    _string_quality,
    _vdp_count_gallery_signals,
    _vdp_wants_json_network_capture,
)
from backend.scanner.vdp.gallery import (  # noqa: F401
    _VDP_LIGHTBOX_OPEN_TIMEOUT_MS,
    _download_vdp_gallery_images,
    _drain_pending_tasks,
    _vdp_evaluate_gallery_all_frames,
    _vdp_gallery_interaction_loop,
    _vdp_gallery_step_advance,
    _vdp_image_download_key,
    _vdp_mouse_jitter,
    _vdp_page_evaluate,
    _vdp_try_open_photo_lightbox,
)
from backend.scanner.vdp.queue import (  # noqa: F401
    _count_https_gallery_urls,
    _vdp_field_gap_score,
    _vdp_gallery_thin_boost,
    _vdp_public_incomplete_gap_score,
    _vdp_queue_sort_key,
    _vdp_rotation_enabled,
    _vdp_rotation_seed,
    _vdp_rotation_tie_hash,
    _vdp_visit_priority_tuple,
    _vehicle_needs_description_vdp,
    _vehicle_needs_spec_gap_vdp,
)
from backend.scanner.vdp.price_hints import (  # noqa: F401
    _apply_vdp_price_hints,
    _ripple_vdp_price_same_detail_url,
)
from backend.scanner.vdp.spin_capture import _vdp_spin_capture  # noqa: F401
from backend.scanner.vdp.visit import (  # noqa: F401
    MAX_JSON_BYTES,
    MAX_NETWORK_ROWS,
    _detach_response_handler,
    _vdp_visit_one,
    log,
)
from backend.scanner.vdp.dispatch import enrich_vehicles_vdp  # noqa: F401
