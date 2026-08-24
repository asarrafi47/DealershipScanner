"""Car detail (VDP), compare page, window sticker, and AI chat routes.

This is the most monkeypatched cluster in the test suite: ``get_car_by_id``,
``prepare_car_detail_context``, ``_session_has_paid_access``,
``_viewer_sees_premium_features``, ``listings_geo_kwargs_from_session``,
``run_car_page_chat`` and friends are all patched on ``backend.main``.  Every
main-owned helper is therefore resolved through the module object at request
time (see ``backend.routes._shared``).  ``backend.main`` re-exports
``_build_car_detail_view_context`` for ``backend.dev.scan_lab_routes``.
"""

from __future__ import annotations

import logging
import os
import threading
import time

from flask import abort, jsonify, make_response, render_template, request, send_from_directory, session, url_for
from werkzeug.exceptions import HTTPException

from backend.billing.catalog import (
    FEATURE_AI_CAR_CHAT,
    FEATURE_AI_COMPARE_CHAT,
    FEATURE_PACKAGES_ENSURE,
    FEATURE_VEHICLE_HISTORY,
    FEATURE_WINDOW_STICKER,
)
from backend.routes._shared import _client_ip, main_module
from backend.utils.car_chat_policy import (
    car_chat_listing_daily_limit,
    car_chat_rate_limits,
    car_chat_user_daily_limit,
    web_research_playwright_allowed,
)
from backend.utils.csrf import validate_csrf_header
from backend.utils.ip_rate_limit import allow_request
from backend.utils.listing_completeness import INCOMPLETE_FIELD_LABELS


logger = logging.getLogger(__name__)


def _car_window_sticker_preview_url(car_id: int) -> str:
    return f"/car/{car_id}/window-sticker-preview.png"


# --- window sticker: honest state + a deadline on the on-demand fetch --------
#
# ``ensure_listing_packages_for_car`` is the on-demand OEM/listing sticker path
# and must keep working exactly as it does (see the project rule: no batch
# harvesting, no new brands).  What it must NOT do is hold an HTTP request open
# while it re-scrapes a dealer VDP.  Measured on this branch, car 136451 (a 2024
# Altima with no sticker source at all): the inner call takes 28.0s cold / 12.2s
# warm and stores nothing, *every single time* — nothing about the row changes,
# so the next view pays it again.
#
# Five separate things were wrong; all five are fixed here.  (1)-(3) came first;
# (4)-(5) are the user-visible regressions that first round introduced.
#
# 1. Only the inner call had a budget, not the request.  After ``join(budget)``
#    the route still did ``get_car_by_id`` + ``prepare_car_detail_context`` +
#    the sticker probes *while the worker kept running*, and that worker starves
#    them: the same tail measured 0.04s idle and 10.5s alongside a live worker
#    (window_sticker_available alone: 0.00s -> 5.57s).  So a 6s budget produced
#    13-17s responses.  Fix: build the panel payload BEFORE starting the worker,
#    and on timeout return that snapshot without touching anything contended.
# 2. Repeat views re-paid the whole budget forever, because the single flight
#    only covers a worker that is still alive.  Fix: a per-car cooldown — one
#    pipeline run per car per ``CAR_PACKAGES_ENSURE_COOLDOWN_SECONDS`` — plus a
#    short TTL on the read-only panel build.  Later views answer from stored
#    state and launch nothing; when a worker finishes it drops both, so anything
#    it stored shows up on the very next view.
# 3. The status flipped between calls on the same car, because the payload was
#    read off a row a background worker was concurrently rewriting.  Fix: while
#    a worker is in flight for a car, replay the panel *content* captured before
#    it started, field for field.  (The fetch-state fields are deliberately not
#    replayed — see (5); they are re-derived on every response.)
#
# 4. The budget silently *dropped* the sticker for the shopper.  The response
#    said ``window_sticker_status="fetching"`` and nothing else, and
#    ``car_packages.js`` calls this endpoint exactly once per page load — so the
#    server-side guarantee "it shows on the very next view" had no next view.
#    Measured on this branch before the fix, car 91583 (fresh process, premium
#    session, GET /car/91583 then the ensure XHR): the XHR answered at 6.01s with
#    ``fetching`` and the worker stored a real Monroney PDF 18.9s later that the
#    shopper never saw.  Fix: the response now carries an explicit, machine-
#    readable retry contract (``window_sticker_fetch_in_flight`` /
#    ``window_sticker_should_retry`` / ``window_sticker_retry_after_seconds``)
#    so the client can poll, and stops the moment the server says to.
# 5. ``window_sticker_status`` lied about work being in flight.  It said
#    "fetching" whenever a sticker *source* was known and nothing was stored —
#    including when the pipeline had already run and found nothing (car 125964,
#    measured: ``window_sticker_ensure_skipped="cooldown"`` together with
#    ``window_sticker_status="fetching"``, no thread alive).  Fix: "fetching"
#    now means exactly "a worker thread for this car is alive as this response
#    is built"; the "a source exists but nothing is stored and nothing is
#    running" case is its own state, ``pending``.
_PACKAGES_ENSURE_DEFAULT_BUDGET = 6.0
_PACKAGES_ENSURE_DEFAULT_COOLDOWN = 900.0
_PACKAGES_ENSURE_DEFAULT_PANEL_TTL = 60.0
# How soon a client should re-POST while a fetch is genuinely in flight.
_PACKAGES_ENSURE_DEFAULT_POLL = 2.0
# Bound the bookkeeping dicts; a fleet-sized cap with a coarse prune is enough
# (correctness never depends on a hit — a miss just re-runs the pipeline).
_PACKAGES_ENSURE_TRACK_MAX = 20_000

_packages_ensure_inflight: dict[int, threading.Thread] = {}
# Panel payload captured just before the in-flight worker for that car started.
_packages_ensure_snapshot: dict[int, dict] = {}
# Short-lived cache of the read-only panel build, per car: ``{car_id: (t, payload)}``.
# Building it is 0.01-0.05s idle but was measured at 10.7s while the listings
# prewarm/rebuild saturates the process, and that rebuild fires on a 60s token —
# so a repeat view must not have to build it again. Invalidated as soon as a
# worker for that car finishes, so a sticker it stored shows up on the next view.
_packages_ensure_panel_cache: dict[int, tuple[float, dict]] = {}
# Monotonic timestamp of the last pipeline run per car (set at launch, refreshed
# on completion so the cooldown is measured from when the work actually ended).
_packages_ensure_attempted_at: dict[int, float] = {}
_packages_ensure_lock = threading.Lock()

# Outcome of an ensure attempt, as seen by the request thread. Reported to the
# client as ``window_sticker_ensure_outcome`` — diagnostics, not a UI signal.
ENSURE_COMPLETED = "completed"
ENSURE_TIMEOUT = "timeout"
ENSURE_INFLIGHT = "inflight"
ENSURE_COOLDOWN = "cooldown"

# Panel states the client can render without guessing.
#   ready       — something is stored, show it.
#   fetching    — a worker thread for this car is alive right now. This is the
#                 ONLY state that means work is happening; it is never returned
#                 unless ``window_sticker_fetch_in_flight`` is true.
#   pending     — a sticker source exists but nothing is stored and nothing is
#                 running. Either the pipeline has not run for this car yet, or
#                 it ran and came back empty. Not a spinner.
#   unavailable — this vehicle has no sticker source at all.
#   hidden      — the panel is not shown for this car.
STICKER_STATUS_READY = "ready"
STICKER_STATUS_FETCHING = "fetching"
STICKER_STATUS_PENDING = "pending"
STICKER_STATUS_UNAVAILABLE = "unavailable"
STICKER_STATUS_HIDDEN = "hidden"

# Panel *content*: a function of (row, stored sticker files) alone. For an
# unchanged row every call returns the same values for these, including while a
# background worker is rewriting the row (that window replays a snapshot).
PACKAGES_ENSURE_PANEL_FIELDS = (
    "show_window_sticker_ui",
    "window_sticker_available",
    "window_sticker_visual_available",
    "window_sticker_source_known",
    "window_sticker_oem_url",
    "window_sticker_view_url",
    "window_sticker_preview_url",
    "packages_panel_has_content",
    "listing_photo_detected_equipment",
)

# Fetch *state*: deliberately live, re-derived on every response. These describe
# what is happening right now, so they may differ between two calls for the same
# car — that is the point.
PACKAGES_ENSURE_FETCH_FIELDS = (
    "window_sticker_status",
    "window_sticker_fetch_in_flight",
    "window_sticker_should_retry",
    "window_sticker_retry_after_seconds",
    "window_sticker_ensure_outcome",
    "window_sticker_fetch_pending",
)

# Every 200 from ``/packages/ensure`` carries all of these. (The rest of the
# payload is diagnostics from the pipeline run — ``stored``, ``analyzed``,
# ``listing_description_reason``, … — only present on the view that actually ran
# it. Don't drive UI off those.)
PACKAGES_ENSURE_RESPONSE_FIELDS = PACKAGES_ENSURE_PANEL_FIELDS + PACKAGES_ENSURE_FETCH_FIELDS


def _packages_ensure_budget_seconds() -> float:
    """Wall-clock budget for the whole ``/packages/ensure`` request (0 disables the deadline)."""
    raw = (os.getenv("CAR_PACKAGES_ENSURE_BUDGET_SECONDS") or "").strip()
    if not raw:
        return _PACKAGES_ENSURE_DEFAULT_BUDGET
    try:
        return max(0.0, float(raw))
    except ValueError:
        return _PACKAGES_ENSURE_DEFAULT_BUDGET


def _packages_ensure_cooldown_seconds() -> float:
    """How long after a pipeline run for a car before another view may launch one (0 disables)."""
    raw = (os.getenv("CAR_PACKAGES_ENSURE_COOLDOWN_SECONDS") or "").strip()
    if not raw:
        return _PACKAGES_ENSURE_DEFAULT_COOLDOWN
    try:
        return max(0.0, float(raw))
    except ValueError:
        return _PACKAGES_ENSURE_DEFAULT_COOLDOWN


def _packages_ensure_panel_ttl_seconds() -> float:
    """How long a read-only panel build may be reused for a car (0 disables the cache)."""
    raw = (os.getenv("CAR_PACKAGES_ENSURE_PANEL_TTL_SECONDS") or "").strip()
    if not raw:
        return _PACKAGES_ENSURE_DEFAULT_PANEL_TTL
    try:
        return max(0.0, float(raw))
    except ValueError:
        return _PACKAGES_ENSURE_DEFAULT_PANEL_TTL


def _packages_ensure_poll_seconds() -> float:
    """How long the client should wait before re-POSTing while a fetch is in flight."""
    raw = (os.getenv("CAR_PACKAGES_ENSURE_POLL_SECONDS") or "").strip()
    if not raw:
        return _PACKAGES_ENSURE_DEFAULT_POLL
    try:
        return max(0.25, float(raw))
    except ValueError:
        return _PACKAGES_ENSURE_DEFAULT_POLL


def _prune_packages_ensure_tracking() -> None:
    """Caller holds ``_packages_ensure_lock``."""
    if len(_packages_ensure_attempted_at) > _PACKAGES_ENSURE_TRACK_MAX:
        _packages_ensure_attempted_at.clear()
    if len(_packages_ensure_snapshot) > _PACKAGES_ENSURE_TRACK_MAX:
        _packages_ensure_snapshot.clear()
    if len(_packages_ensure_panel_cache) > _PACKAGES_ENSURE_TRACK_MAX:
        _packages_ensure_panel_cache.clear()


def _packages_ensure_inflight_snapshot(car_id: int) -> dict | None:
    """Payload captured before the still-running worker for *car_id* started, if any."""
    with _packages_ensure_lock:
        t = _packages_ensure_inflight.get(int(car_id))
        if t is None or not t.is_alive():
            return None
        snap = _packages_ensure_snapshot.get(int(car_id))
    return dict(snap) if snap is not None else None


def _packages_ensure_worker_alive(car_id: int) -> bool:
    """True iff a pipeline thread for *car_id* is alive at this instant."""
    with _packages_ensure_lock:
        t = _packages_ensure_inflight.get(int(car_id))
    return bool(t is not None and t.is_alive())


def _packages_ensure_cooldown_remaining(car_id: int, cooldown: float) -> float:
    """Seconds until another pipeline run may be launched for *car_id* (0 = now)."""
    if cooldown <= 0:
        return 0.0
    with _packages_ensure_lock:
        last = _packages_ensure_attempted_at.get(int(car_id))
    if last is None:
        return 0.0
    return max(0.0, cooldown - (time.monotonic() - last))


def _packages_ensure_in_cooldown(car_id: int, cooldown: float) -> bool:
    return _packages_ensure_cooldown_remaining(car_id, cooldown) > 0.0


def _window_sticker_status(
    *,
    show_sticker_ui: bool,
    sticker_ready: bool,
    sticker_visual: bool,
    has_source: bool,
    fetch_in_flight: bool = False,
) -> str:
    """
    Collapse the sticker flags into one state string for the panel.

    ``fetch_in_flight`` is the only thing that can produce
    :data:`STICKER_STATUS_FETCHING`: a known sticker source with nothing stored
    and no worker running is :data:`STICKER_STATUS_PENDING`, not "fetching".
    """
    if not show_sticker_ui:
        return STICKER_STATUS_HIDDEN
    if sticker_ready or sticker_visual:
        return STICKER_STATUS_READY
    if fetch_in_flight:
        return STICKER_STATUS_FETCHING
    if has_source:
        return STICKER_STATUS_PENDING
    return STICKER_STATUS_UNAVAILABLE


def _run_packages_ensure_with_budget(
    car_id: int,
    *,
    allow_vision: bool,
    budget: float | None = None,
    snapshot: dict | None = None,
) -> tuple[dict | None, str]:
    """
    Run the on-demand packages/sticker pipeline, but wait at most *budget* seconds.

    Returns ``(status, outcome)``; ``status`` is only non-``None`` for
    :data:`ENSURE_COMPLETED`. On :data:`ENSURE_TIMEOUT` the worker keeps running
    so a later view finds the result already stored; *snapshot* (the panel
    payload as it looked before the worker started) is registered for that
    window so concurrent views get a consistent answer instead of a torn read.
    """
    from backend.enrichment.listing_packages_service import ensure_listing_packages_for_car

    car_id = int(car_id)
    if budget is None:
        budget = _packages_ensure_budget_seconds()
    if budget <= 0:
        # Deadline disabled — run it inline (dev/debug and the unit tests).
        try:
            return ensure_listing_packages_for_car(
                car_id, allow_vision_fallback=allow_vision, refetch_description=True
            ), ENSURE_COMPLETED
        finally:
            with _packages_ensure_lock:
                _packages_ensure_panel_cache.pop(car_id, None)
                _packages_ensure_attempted_at[car_id] = time.monotonic()
                _prune_packages_ensure_tracking()

    box: dict[str, dict] = {}

    def _work() -> None:
        try:
            box["status"] = ensure_listing_packages_for_car(
                car_id, allow_vision_fallback=allow_vision, refetch_description=True
            )
        except Exception:
            logger.exception("packages ensure failed for car %s", car_id)
        finally:
            with _packages_ensure_lock:
                _packages_ensure_inflight.pop(car_id, None)
                _packages_ensure_snapshot.pop(car_id, None)
                # Whatever it stored must be visible to the very next view.
                _packages_ensure_panel_cache.pop(car_id, None)
                # Measure the cooldown from when the work ended, not started.
                _packages_ensure_attempted_at[car_id] = time.monotonic()

    with _packages_ensure_lock:
        existing = _packages_ensure_inflight.get(car_id)
        if existing is not None and existing.is_alive():
            # Someone is already fetching this VIN — don't start a second one.
            return None, ENSURE_INFLIGHT
        t = threading.Thread(target=_work, name=f"packages-ensure-{car_id}", daemon=True)
        _packages_ensure_inflight[car_id] = t
        if snapshot is not None:
            _packages_ensure_snapshot[car_id] = dict(snapshot)
        _packages_ensure_attempted_at[car_id] = time.monotonic()
        _prune_packages_ensure_tracking()
        t.start()

    t.join(budget)
    if t.is_alive():
        return None, ENSURE_TIMEOUT
    return box.get("status"), ENSURE_COMPLETED


def _serve_car_window_sticker_preview(car_id: int):
    """Render page 1 of stored Monroney PDF as PNG (same gate as chat/packages; SEC-072/073)."""
    main = main_module()
    car = main.get_car_by_id(car_id, include_inactive=False)
    if not car:
        abort(404)
    ok, _err = main._require_feature(FEATURE_WINDOW_STICKER)
    if not ok:
        abort(403)
    from backend.enrichment.window_sticker_service import (
        ensure_sticker_preview_png,
        ensure_window_sticker_for_car,
        window_sticker_visual_local_path,
    )

    ensure_window_sticker_for_car(car_id, allow_vision_fallback=False)
    path = window_sticker_visual_local_path(
        car.get("vin"),
        dealer_id=car.get("dealer_id"),
        dealership_registry_id=car.get("dealership_registry_id"),
    )
    if not path:
        abort(404)
    png = ensure_sticker_preview_png(path)
    if not png or not png.is_file():
        abort(503)
    mime = "image/png"
    suffix = png.suffix.lower()
    if suffix in (".jpg", ".jpeg"):
        mime = "image/jpeg"
    elif suffix == ".webp":
        mime = "image/webp"
    return send_from_directory(
        str(png.parent),
        png.name,
        mimetype=mime,
        as_attachment=False,
        download_name=f"window-sticker-{car_id}.png",
    )


def car_window_sticker_preview_png(car_id: int):
    return _serve_car_window_sticker_preview(car_id)


def compare_page():
    """Side-by-side specs; public (ids in query string), history recorded for signed-in users."""
    main = main_module()
    from backend.utils.compare_specs import build_compare_context, parse_compare_car_ids

    ids = parse_compare_car_ids(request.args.get("ids"))
    if ids and session.get("user_id"):
        try:
            main.record_compare_session(int(session["user_id"]), ids)
            main._invalidate_reco_cache(int(session["user_id"]))
        except (TypeError, ValueError):
            pass
    raw_cars = main.get_cars_by_ids(ids)
    ctx = build_compare_context(raw_cars)
    show_compare_chat = bool(
        ctx["cars"]
        and (
            main._session_has_paid_access()
            or (session.get("user_id") and not main._billing_enabled())
        )
    )
    return render_template(
        "compare.html",
        cars=ctx["cars"],
        compare_rows=ctx["compare_rows"],
        compare_ids=ctx["compare_car_ids"],
        show_compare_chat=show_compare_chat,
    )


def _build_car_detail_view_context(car_id: int, car_raw: dict) -> dict:
    main = main_module()
    uid = session.get("user_id")
    car_is_saved = False
    if uid:
        try:
            main.record_car_view(int(uid), car_id)
            main._invalidate_reco_cache(int(uid))
        except Exception:
            pass
        try:
            car_is_saved = main.is_car_saved(int(uid), car_id)
        except Exception:
            pass
    ctx = main.prepare_car_detail_context(car_raw)
    from backend.enrichment.window_sticker_service import (
        window_sticker_available,
        window_sticker_has_visual,
    )
    from backend.scanner.window_sticker import (
        car_listing_may_have_sticker,
        car_listing_sticker_urls,
        cdjr_oem_window_sticker_eligible,
        get_window_sticker_url,
        is_cdjr_stellantis_car,
        show_window_sticker_panel,
        sticker_embed_preview_url,
    )

    cdjr_sticker_eligible = cdjr_oem_window_sticker_eligible(car_raw)
    show_sticker_ui = show_window_sticker_panel(car_raw, ctx)
    sticker_ready = show_sticker_ui and window_sticker_available(car_raw)
    sticker_visual = show_sticker_ui and window_sticker_has_visual(car_raw)
    listing_sticker_urls = car_listing_sticker_urls(car_raw) if car_listing_may_have_sticker(car_raw) else []
    listing_sticker_image_url = listing_sticker_urls[0] if listing_sticker_urls else None
    premium_viewer = main._viewer_sees_premium_features()
    sticker_preview_api = _car_window_sticker_preview_url(car_id)
    # Window sticker fetch runs async via car_packages.js — avoid blocking VDP render.
    sticker_preview_embed_url = (
        sticker_embed_preview_url(
            car_raw,
            preview_api_url=sticker_preview_api,
            has_paid_access=premium_viewer,
        )
        if show_sticker_ui
        else None
    )
    sticker_fetch_pending = bool(
        show_sticker_ui and not sticker_ready and (listing_sticker_image_url or cdjr_sticker_eligible)
    )

    window_sticker_oem_url = None
    window_sticker_pdf_url = None
    if cdjr_sticker_eligible:
        window_sticker_oem_url = get_window_sticker_url(str(car_raw.get("vin") or ""))
    if show_sticker_ui and premium_viewer:
        if sticker_visual or cdjr_sticker_eligible:
            window_sticker_pdf_url = url_for("api_car_window_sticker", car_id=car_id)
        if not window_sticker_oem_url and listing_sticker_urls:
            window_sticker_oem_url = listing_sticker_urls[0]
    if show_sticker_ui and premium_viewer and not sticker_preview_embed_url:
        sticker_preview_embed_url = sticker_preview_api if sticker_visual else None

    car = main.serialize_car_for_api(
        car_raw,
        include_verified=False,
        verified_specs=ctx.get("verified_specs") or {},
    )
    from backend.db.incomplete_listings_db import get_missing_field_codes_for_car_id

    _missing_codes = get_missing_field_codes_for_car_id(car_id)
    listing_incomplete_fields = [
        {"code": c, "label": INCOMPLETE_FIELD_LABELS.get(c, c.replace("_", " ").title())}
        for c in _missing_codes
    ]
    dealer_info = None
    reg_id = car_raw.get("dealership_registry_id")
    if reg_id:
        try:
            from backend.db.dealerships_db import get_dealership_by_id
            from backend.utils.field_clean import normalize_optional_url

            raw_dealer = get_dealership_by_id(int(reg_id))
            if raw_dealer:
                dealer_info = dict(raw_dealer)
                for url_key in ("dealer_website_url", "website_url"):
                    if url_key in dealer_info:
                        dealer_info[url_key] = normalize_optional_url(dealer_info.get(url_key))
        except Exception:
            pass
    # What this listing's own photographs say about where the car actually is. One
    # batched read keyed by car id (see cars_repo.car_attribution_states); a miss —
    # the common case — leaves every dealer surface on this page exactly as it was.
    from backend.db.repositories.cars_repo import car_attribution_states
    from backend.utils.car_serialize.attribution import attribution_public_fields

    attribution = car_attribution_states([car_id]).get(car_id)
    attribution_fields = attribution_public_fields(attribution)
    # Onto the serialized car too, so GET /api/cars/<id> and the compare/save flows
    # that read it carry the same caveat the rendered page shows.
    car.update(attribution_fields)

    # When the trusted-MSRP resolver produced nothing (car.msrp is None), two
    # sidecar stores may still have something honest to say: a Monroney total an
    # agent read off a sticker photographed in this listing's gallery
    # (car_image_text), or the range this trim has been observed to sticker at
    # (trim_msrp_bands). Same batched-read shape as the attribution overlay
    # above; the wording lives in car_serialize.msrp_overlay so no surface can
    # present either as the feed's MSRP. car_raw carries the identity because
    # the band keys match cars.model/cars.trim verbatim, not the BMW
    # display-normalized pair the serialized dict may hold.
    from backend.db.repositories.cars_repo import car_sticker_msrp_values
    from backend.utils.car_serialize.color_overlay import color_overlay_public_fields
    from backend.utils.car_serialize.msrp_overlay import msrp_overlay_public_fields

    sticker_facts = car_sticker_msrp_values([car_id]).get(car_id)
    car.update(msrp_overlay_public_fields(car, sticker_facts, identity=car_raw))
    # Colors the sticker document PRINTS supersede the feed's in the display
    # (policy 2026-08-18: the window sticker is the single source of truth), with
    # provenance shown and the cars columns untouched. Same batched read as the
    # MSRP overlay; photo-observed colors never qualify (see color_overlay).
    car.update(color_overlay_public_fields(sticker_facts))

    from backend.listings.dealer_map import build_dealer_map_for_car

    dealer_map = build_dealer_map_for_car(car_raw, dealer_info, attribution=attribution)
    market_intel = None
    trim_ladder = None
    deal_score_detail = None
    if main._session_has_paid_access():
        from backend.utils.market_price import market_price_for_car

        geo = main.listings_geo_kwargs_from_session(session)
        market_intel = market_price_for_car(
            car_raw,
            zip_code=geo.get("zip_code"),
            radius_miles=geo.get("radius_miles"),
        )
        # Detailed market-price-band breakdown (median/p25/p75, N listings across
        # M dealers) — gated behind FEATURE_MARKET_INTEL like the market intel above.
        try:
            from backend.intelligence.deal_score_cache import detailed_deal_score

            deal_score_detail = detailed_deal_score(car_raw)
        except Exception:
            deal_score_detail = None
    if main._viewer_sees_premium_features():
        try:
            ladder_year = int(car_raw.get("year") or 0)
        except (TypeError, ValueError):
            ladder_year = 0
        if not ladder_year or ladder_year >= 2010:
            from backend.enrichment.trim_ladder import resolve_trim_ladder

            trim_ladder = resolve_trim_ladder(
                make=car_raw.get("make"),
                model=car_raw.get("model"),
                year=car_raw.get("year"),
                trim=car_raw.get("trim"),
            )
    # Inventory rarity — scarcity within our own active fleet plus visible-
    # option evidence from the vision scan. Ungated: it is our own data, and
    # "one of 2 in our inventory" is a purchase nudge for every viewer.
    rarity = None
    try:
        from backend.utils.rarity_score import rarity_for_car, vision_summary_for_car

        rarity = rarity_for_car(
            car_raw, vision_summary=vision_summary_for_car(car_raw.get("id"))
        )
    except Exception:
        rarity = None

    listings_geo = main.listings_geo_kwargs_from_session(session)
    # Synthesized build sheet from the data we hold (listing row + verified EPA
    # specs + best-effort catalog options) — but ONLY when this car has no real
    # window sticker. When a genuine OEM/listing sticker exists (or is being
    # fetched), that is authoritative and we never fabricate our own alongside it.
    has_real_sticker = bool(
        show_sticker_ui
        and (
            sticker_ready
            or sticker_visual
            or sticker_preview_embed_url
            or window_sticker_pdf_url
            or window_sticker_oem_url
            or listing_sticker_urls
            or cdjr_sticker_eligible
            or sticker_fetch_pending
            or ctx.get("listing_sticker_options")
            or ctx.get("listing_sticker_option_sections")
        )
    )
    generated_spec_sheet = None
    if not has_real_sticker:
        try:
            from backend.enrichment.generated_spec_sheet import build_generated_spec_sheet

            generated_spec_sheet = build_generated_spec_sheet(
                car_raw, ctx.get("verified_specs") or {}
            )
        except Exception:
            generated_spec_sheet = None
    return {
        "car": car,
        "generated_spec_sheet": generated_spec_sheet,
        "market_intel": market_intel,
        "deal_score_detail": deal_score_detail,
        "rarity": rarity,
        "trim_ladder": trim_ladder,
        "car_is_saved": car_is_saved,
        "logged_in": bool(uid),
        "listings_geo_zip": listings_geo.get("zip_code") or "",
        "listing_incomplete_fields": listing_incomplete_fields,
        "gallery_images": ctx.get("gallery_images") or [],
        "verified_specs": ctx.get("verified_specs") or {},
        "listing_packages_sections": ctx.get("listing_packages_sections") or [],
        "listing_standalone_features": ctx.get("listing_standalone_features") or [],
        "listing_observed_features": ctx.get("listing_observed_features") or [],
        "listing_monroney_options": ctx.get("listing_monroney_options") or [],
        "listing_monroney_standard": ctx.get("listing_monroney_standard") or [],
        "listing_sticker_options": ctx.get("listing_sticker_options") or [],
        "listing_sticker_option_groups": ctx.get("listing_sticker_option_groups") or [],
        "listing_sticker_option_sections": ctx.get("listing_sticker_option_sections") or {},
        "listing_possible_packages": ctx.get("listing_possible_packages") or [],
        "listing_photo_detected_equipment": ctx.get("listing_photo_detected_equipment") or [],
        # Sticker-printed colors: the fetched OEM sticker's parse (ctx) and the
        # photographed-sticker read (car overlay) are the same kind of fact —
        # printed on the document — so either fills the template slot; the
        # photographed read wins only because it was verified against THIS car's
        # own gallery. Both supersede the feed color in the display.
        "sticker_exterior_color": car.get("sticker_exterior_color")
        or ctx.get("sticker_exterior_color"),
        "sticker_interior_color": car.get("sticker_interior_color")
        or ctx.get("sticker_interior_color"),
        "sticker_interior_material": ctx.get("sticker_interior_material"),
        "sticker_spec_lines": ctx.get("sticker_spec_lines") or [],
        "interior_from_listing_description": bool(ctx.get("interior_from_listing_description")),
        "interior_from_llava_vision": bool(ctx.get("interior_from_llava_vision")),
        "packages_panel_has_content": bool(ctx.get("packages_panel_has_content")),
        "llava_interior_section": ctx.get("llava_interior_section"),
        "window_sticker_available": sticker_ready,
        "window_sticker_visual_available": sticker_visual,
        "show_window_sticker_ui": show_sticker_ui,
        "listing_sticker_image_url": listing_sticker_image_url,
        "sticker_fetch_pending": sticker_fetch_pending,
        "is_cdjr_stellantis": is_cdjr_stellantis_car(car_raw),
        "cdjr_window_sticker_eligible": cdjr_sticker_eligible,
        "window_sticker_preview_api_url": sticker_preview_api,
        "window_sticker_preview_url": sticker_preview_embed_url,
        # Rendered state for the sticker card so it can settle before (and
        # independently of) the /packages/ensure XHR. Same vocabulary as the
        # endpoint: "unavailable" = no sticker source at all, "pending" = a
        # source exists but nothing is stored and nothing is running yet.
        # Rendering this page starts no fetch, so "fetching" only appears when a
        # worker from an earlier ensure call happens to still be alive.
        "window_sticker_status": _window_sticker_status(
            show_sticker_ui=show_sticker_ui,
            sticker_ready=sticker_ready,
            sticker_visual=sticker_visual,
            has_source=bool(listing_sticker_urls or cdjr_sticker_eligible),
            fetch_in_flight=_packages_ensure_worker_alive(car_id),
        ),
        "window_sticker_source_known": bool(listing_sticker_urls or cdjr_sticker_eligible),
        "hide_photo_analysis": bool(ctx.get("hide_photo_analysis")),
        "window_sticker_oem_url": window_sticker_oem_url,
        "window_sticker_pdf_url": window_sticker_pdf_url,
        "dealer_info": dealer_info,
        "dealer_map": dealer_map,
        "location_attribution": attribution_fields or None,
    }


def car_detail(car_id):
    main = main_module()
    car_raw = main.get_car_by_id(car_id, include_inactive=False)
    if not car_raw:
        abort(404)
    embed = request.args.get("embed") in ("1", "true", "yes")
    ctx = main._build_car_detail_view_context(car_id, car_raw)
    ctx["car_embed"] = embed
    resp = make_response(render_template("car.html", **ctx))
    resp.headers["Cache-Control"] = "private, max-age=180"
    return resp


def api_car_detail(car_id):
    main = main_module()
    car_raw = main.get_car_by_id(car_id, include_inactive=False)
    if not car_raw:
        return jsonify({"ok": False, "error": "not_found"}), 404
    return jsonify({"ok": True, **main._build_car_detail_view_context(car_id, car_raw)})


def api_car_window_sticker(car_id: int):
    """Serve stored OEM window sticker PDF (premium only)."""
    main = main_module()
    ok, err = main._require_feature(FEATURE_WINDOW_STICKER)
    if not ok:
        return jsonify(main._feature_denied_json(FEATURE_WINDOW_STICKER, err)), 403
    car = main.get_car_by_id(car_id, include_inactive=False)
    if not car:
        return jsonify({"ok": False, "error": "not_found"}), 404
    from backend.enrichment.window_sticker_service import (
        ensure_window_sticker_for_car,
        window_sticker_local_path,
        window_sticker_visual_local_path,
    )

    path = window_sticker_visual_local_path(
        car.get("vin"),
        dealer_id=car.get("dealer_id"),
        dealership_registry_id=car.get("dealership_registry_id"),
    )
    if not path:
        ensure_window_sticker_for_car(car_id, allow_vision_fallback=False)
        path = window_sticker_visual_local_path(
            car.get("vin"),
            dealer_id=car.get("dealer_id"),
            dealership_registry_id=car.get("dealership_registry_id"),
        )
    if not path:
        return jsonify({"ok": False, "error": "sticker_not_available"}), 404
    mime = "application/pdf"
    suffix = path.suffix.lower()
    if suffix in (".jpg", ".jpeg"):
        mime = "image/jpeg"
    elif suffix == ".png":
        mime = "image/png"
    elif suffix == ".webp":
        mime = "image/webp"
    elif suffix == ".txt":
        mime = "text/plain; charset=utf-8"
    return send_from_directory(
        str(path.parent),
        path.name,
        mimetype=mime,
        as_attachment=False,
        download_name=f"window-sticker-{car_id}{path.suffix.lower()}",
    )


def api_car_window_sticker_preview(car_id: int):
    """PNG preview of page 1 (legacy API path; prefer /car/<id>/window-sticker-preview.png)."""
    return _serve_car_window_sticker_preview(car_id)


def api_car_nhtsa_recalls(car_id: int):
    """NHTSA recall campaigns for a listing VIN (public JSON)."""
    main = main_module()
    car = main.get_car_by_id(car_id, include_inactive=False)
    if not car:
        return jsonify({"ok": False, "error": "not_found"}), 404
    payload, status = main._nhtsa_recalls_lookup_payload(
        vin_raw=str(car.get("vin") or "").strip(),
        make=str(car.get("make") or "").strip() or None,
        model=str(car.get("model") or "").strip() or None,
        year=str(car.get("year") or "").strip() or None,
        rate_key=f"nhtsa-recalls-api:{_client_ip()}",
    )
    return jsonify(payload), status


def api_car_vehicle_history_intelligence(car_id: int):
    """NHTSA recalls + vPIC validation + listing title flags (premium; no Carfax)."""
    main = main_module()
    ok, err = main._require_feature(FEATURE_VEHICLE_HISTORY)
    if not ok:
        return jsonify(main._feature_denied_json(FEATURE_VEHICLE_HISTORY, err)), 403
    car = main.get_car_by_id(car_id, include_inactive=False)
    if not car:
        return jsonify({"ok": False, "error": "not_found"}), 404
    from backend.enrichment.vehicle_history_intelligence import build_vehicle_history_intelligence

    payload = build_vehicle_history_intelligence(car)
    status = 200 if payload.get("ok") else 400
    return jsonify(payload), status


def _packages_ensure_payload(car_id: int, base: dict | None = None, car: dict | None = None) -> dict:
    """
    The panel payload for *car_id*, read off the row as it stands right now.

    Pure read + derive: it starts no fetch and writes nothing, so it is a
    function of (row, stored sticker files) alone — call it twice on an
    unchanged row and you get the same dict.  *base* is the result of an ensure
    run when one completed inside the request; otherwise just ``ok``/``car_id``.
    """
    main = main_module()
    status: dict = dict(base) if base else {}
    status.setdefault("ok", True)
    status.setdefault("car_id", int(car_id))
    car2 = car or main.get_car_by_id(car_id, include_inactive=False) or {}
    ctx = main.prepare_car_detail_context(car2)
    status["window_sticker_available"] = bool(status.get("window_sticker_available"))
    from backend.enrichment.window_sticker_service import (
        sticker_panel_payload,
        window_sticker_available,
        window_sticker_has_visual,
    )
    from backend.scanner.window_sticker import (
        car_listing_sticker_urls,
        cdjr_oem_window_sticker_eligible,
        get_window_sticker_url,
        show_window_sticker_panel,
        sticker_embed_preview_url,
    )

    status.update(sticker_panel_payload(ctx, car2))
    show_sticker = show_window_sticker_panel(car2, ctx)
    status["show_window_sticker_ui"] = show_sticker
    status["window_sticker_available"] = bool(
        show_sticker and (status.get("window_sticker_available") or window_sticker_available(car2))
    )
    status["window_sticker_visual_available"] = bool(
        show_sticker and window_sticker_has_visual(car2)
    )
    cdjr_eligible = cdjr_oem_window_sticker_eligible(car2)
    listing_urls = car_listing_sticker_urls(car2)
    vin = str(car2.get("vin") or "")
    if show_sticker and vin:
        if cdjr_eligible:
            status["window_sticker_oem_url"] = get_window_sticker_url(vin)
        elif listing_urls:
            status["window_sticker_oem_url"] = listing_urls[0]
    if show_sticker and cdjr_eligible:
        status["window_sticker_view_url"] = url_for("api_car_window_sticker", car_id=car_id)
    elif status.get("window_sticker_visual_available"):
        status["window_sticker_view_url"] = url_for("api_car_window_sticker", car_id=car_id)
    if status.get("window_sticker_visual_available") or status.get("window_sticker_view_url"):
        status["window_sticker_preview_url"] = sticker_embed_preview_url(
            car2,
            preview_api_url=_car_window_sticker_preview_url(car_id),
            has_paid_access=True,
        ) or _car_window_sticker_preview_url(car_id)
    status["listing_photo_detected_equipment"] = ctx.get("listing_photo_detected_equipment") or []
    status["packages_panel_has_content"] = bool(ctx.get("packages_panel_has_content"))
    # Whether this vehicle has anywhere a sticker could come from. Carried in the
    # payload because the fetch-state fields are re-derived later (possibly off a
    # replayed snapshot) and need it without re-reading the row.
    status["window_sticker_source_known"] = bool(
        status.get("window_sticker_oem_url")
        or status.get("window_sticker_view_url")
        or cdjr_eligible
        or listing_urls
    )
    for field in PACKAGES_ENSURE_PANEL_FIELDS:
        status.setdefault(field, None)
    return status


def _finalize_ensure_payload(
    payload: dict,
    *,
    car_id: int,
    outcome: str,
    in_flight: bool,
    cooldown: float,
) -> dict:
    """
    Stamp the live fetch-state fields onto a panel payload.

    Panel *content* may come from a snapshot taken before a worker started;
    everything this adds is derived from the state of the world right now, so
    ``window_sticker_status`` never reports "fetching" unless a thread for this
    car is actually alive, and the client is told whether re-POSTing can change
    the answer and how long to wait first.
    """
    out = dict(payload)
    out.setdefault("ok", True)
    out["car_id"] = int(car_id)
    status = _window_sticker_status(
        show_sticker_ui=bool(out.get("show_window_sticker_ui")),
        sticker_ready=bool(out.get("window_sticker_available")),
        sticker_visual=bool(out.get("window_sticker_visual_available")),
        has_source=bool(out.get("window_sticker_source_known")),
        fetch_in_flight=bool(in_flight),
    )
    out["window_sticker_status"] = status
    out["window_sticker_fetch_in_flight"] = bool(in_flight)
    # Legacy alias kept for anything still reading it; same meaning as
    # ``window_sticker_fetch_in_flight``. New code should use that.
    out["window_sticker_fetch_pending"] = bool(in_flight)
    out["window_sticker_ensure_outcome"] = outcome
    if in_flight:
        # A worker is running; the answer will change when it lands.
        should_retry, retry_after = True, round(_packages_ensure_poll_seconds(), 2)
    elif status == STICKER_STATUS_PENDING:
        remaining = _packages_ensure_cooldown_remaining(car_id, cooldown)
        if remaining > 0:
            # Nothing is running and nothing may be started until the cooldown
            # expires, so polling now cannot change anything. Report when it
            # could, and let the client decide it is not worth waiting for.
            should_retry, retry_after = False, round(remaining, 1)
        else:
            should_retry, retry_after = True, round(_packages_ensure_poll_seconds(), 2)
    else:
        # ready / unavailable / hidden — settled, stop asking.
        should_retry, retry_after = False, None
    out["window_sticker_should_retry"] = should_retry
    out["window_sticker_retry_after_seconds"] = retry_after
    return out


def _packages_ensure_panel_snapshot(car_id: int, car: dict | None = None) -> dict:
    """:func:`_packages_ensure_payload` for the stored state, reused for a short TTL."""
    ttl = _packages_ensure_panel_ttl_seconds()
    now = time.monotonic()
    if ttl > 0:
        with _packages_ensure_lock:
            cached = _packages_ensure_panel_cache.get(int(car_id))
        if cached is not None and (now - cached[0]) < ttl:
            return dict(cached[1])
    payload = _packages_ensure_payload(car_id, car=car)
    if ttl > 0:
        with _packages_ensure_lock:
            _packages_ensure_panel_cache[int(car_id)] = (now, dict(payload))
            _prune_packages_ensure_tracking()
    return payload


def api_car_packages_ensure(car_id: int):
    """
    Fetch/analyze window sticker and merge packages (premium only).

    Wall-clock contract: after the read-only panel build, the on-demand pipeline
    gets at most what is left of ``CAR_PACKAGES_ENSURE_BUDGET_SECONDS``
    (default 6s); past that it keeps running in the background and the request
    returns.  The panel build itself is not interruptible — measured 0.01-0.40s
    once the process is warm (8 real cars, premium session), but it is starved by
    the listings-grid prewarm/rebuild, so the *first* request a fresh worker
    process handles can still be tens of seconds regardless of this budget.

    Retry contract (this is what ``car_packages.js`` drives off — see
    :data:`PACKAGES_ENSURE_RESPONSE_FIELDS`):

    * ``window_sticker_status`` — ``ready`` | ``fetching`` | ``pending`` |
      ``unavailable`` | ``hidden``. ``fetching`` is returned if and only if
      ``window_sticker_fetch_in_flight`` is true.
    * ``window_sticker_fetch_in_flight`` — a pipeline thread for this car was
      alive when this response was built.
    * ``window_sticker_should_retry`` — re-POSTing can change the answer.
    * ``window_sticker_retry_after_seconds`` — wait this long first. Present
      (with ``should_retry`` false) when the only thing blocking a new attempt is
      the per-car cooldown, so the client can see it is not worth waiting.

    A client that loops while ``should_retry`` is true, sleeping
    ``retry_after_seconds`` between calls, terminates: once the worker finishes,
    the car is in cooldown and ``should_retry`` goes false.
    """
    t0 = time.perf_counter()
    main = main_module()
    ok, err = main._require_feature(FEATURE_PACKAGES_ENSURE)
    if not ok:
        return jsonify(main._feature_denied_json(FEATURE_PACKAGES_ENSURE, err)), 403
    try:
        validate_csrf_header()
    except HTTPException:
        return jsonify({"ok": False, "error": "csrf_required"}), 403
    car = main.get_car_by_id(car_id, include_inactive=False)
    if not car:
        return jsonify({"ok": False, "error": "not_found"}), 404

    cooldown = _packages_ensure_cooldown_seconds()

    # A worker is already fetching this car: reading the row now would race its
    # writes (that is how the same car answered "unavailable" then "hidden").
    # Replay the panel *content* from just before it started — but the fetch
    # state is re-derived, so this answer says "fetching" and asks for a poll.
    inflight = _packages_ensure_inflight_snapshot(car_id)
    if inflight is not None and _packages_ensure_worker_alive(car_id):
        inflight["window_sticker_ensure_skipped"] = ENSURE_INFLIGHT
        return jsonify(
            _finalize_ensure_payload(
                inflight,
                car_id=car_id,
                outcome=ENSURE_INFLIGHT,
                in_flight=True,
                cooldown=cooldown,
            )
        )
    # (If the worker died between those two checks, fall through and rebuild:
    # answering from the pre-worker snapshot *and* reporting "not fetching"
    # would tell the client to stop right as the result landed.)

    allow_vision = request.args.get("vision", "0").strip().lower() in ("1", "true", "yes")
    budget = _packages_ensure_budget_seconds()
    # Read-only panel state, built BEFORE any worker exists so it runs
    # uncontended (0.04s warm) instead of alongside a fetch (10.5s measured).
    payload = _packages_ensure_panel_snapshot(car_id, car=car)

    if _packages_ensure_in_cooldown(car_id, cooldown):
        # This car ran the pipeline recently. The payload above already reflects
        # anything that run stored, so answer from it and launch nothing. Note
        # the in-flight flag is still read live: it is false in the normal case,
        # which is what stops this answer claiming a fetch is happening.
        payload["window_sticker_ensure_skipped"] = ENSURE_COOLDOWN
        return jsonify(
            _finalize_ensure_payload(
                payload,
                car_id=car_id,
                outcome=ENSURE_COOLDOWN,
                in_flight=_packages_ensure_worker_alive(car_id),
                cooldown=cooldown,
            )
        )

    if budget > 0:
        # Whatever the panel build cost comes out of the SAME budget — the
        # deadline belongs to the request, not to the inner call. Never floor to
        # 0: that is the "no deadline" sentinel, which would block forever.
        remaining = max(budget - (time.perf_counter() - t0), 0.01)
    else:
        remaining = 0.0
    status, outcome = _run_packages_ensure_with_budget(
        car_id,
        allow_vision=allow_vision,
        budget=remaining,
        snapshot=payload,
    )
    if outcome == ENSURE_COMPLETED and status is not None:
        # The worker is done, so a rebuild is both fresh and uncontended.
        payload = _packages_ensure_payload(car_id, status)
    elif outcome == ENSURE_INFLIGHT:
        payload["window_sticker_ensure_skipped"] = ENSURE_INFLIGHT
    elif outcome == ENSURE_TIMEOUT and not _packages_ensure_worker_alive(car_id):
        # It landed in the gap between join() giving up and this check. The
        # worker dropped the panel cache on its way out, so rebuild rather than
        # answer from the pre-worker snapshot and then tell the client to stop.
        payload = _packages_ensure_payload(car_id)
    return jsonify(
        _finalize_ensure_payload(
            payload,
            car_id=car_id,
            outcome=outcome,
            in_flight=_packages_ensure_worker_alive(car_id),
            cooldown=cooldown,
        )
    )


def api_car_chat(car_id: int):
    main = main_module()
    ok, err = main._require_feature(FEATURE_AI_CAR_CHAT)
    if not ok:
        return jsonify(main._feature_denied_json(FEATURE_AI_CAR_CHAT, err)), 403

    ip = _client_ip()
    rpm_pair, rpm_ip, rpm_global = car_chat_rate_limits()

    if rpm_global > 0 and not allow_request(
        "chat:global",
        max_events=rpm_global,
        window_seconds=60.0,
    ):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    if not allow_request(f"chat:ip:{ip}", max_events=rpm_ip, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    if not allow_request(f"chat:{ip}:{car_id}", max_events=rpm_pair, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    uid = session.get("user_id")

    daily_limit = car_chat_user_daily_limit()
    if daily_limit > 0:
        if uid:
            daily_key = f"chat:daily:user:{int(uid)}"
        else:
            daily_key = f"chat:daily:ip:{ip}"
        if not allow_request(
            daily_key,
            max_events=daily_limit,
            window_seconds=86400.0,
        ):
            return jsonify({"ok": False, "error": "user_chat_limit_reached"}), 429

    # Second tier: per-(user, listing). Keeps one car's thread from draining
    # the whole-account budget sideways and vice versa — both tiers must pass.
    listing_limit = car_chat_listing_daily_limit()
    if listing_limit > 0:
        if uid:
            listing_key = f"chat:daily:user:{int(uid)}:car:{car_id}"
        else:
            listing_key = f"chat:daily:ip:{ip}:car:{car_id}"
        if not allow_request(
            listing_key,
            max_events=listing_limit,
            window_seconds=86400.0,
        ):
            return jsonify({"ok": False, "error": "listing_chat_limit_reached"}), 429

    if request.content_length is not None and request.content_length > main._CHAT_MAX_BODY:
        return jsonify({"ok": False, "error": "payload_too_large"}), 413

    car_raw = main.get_car_by_id(car_id, include_inactive=False)
    if not car_raw:
        return jsonify({"ok": False, "error": "not_found"}), 404

    # Enrich car dict with dealer location from dealerships table for map link
    car_dict = dict(car_raw)
    try:
        reg_id = car_dict.get("dealership_registry_id")
        if reg_id:
            from backend.db.inventory_db import get_conn
            with get_conn() as _conn:
                _row = _conn.execute(
                    "SELECT street_address, city, state, zip_code, latitude, longitude FROM dealerships WHERE id=?",
                    (reg_id,)
                ).fetchone()
                if _row:
                    addr_parts = [p for p in [_row["street_address"], _row["city"], _row["state"], _row["zip_code"]] if p]
                    car_dict["dealer_address"] = ", ".join(addr_parts) or None
                    car_dict["dealer_lat"] = _row["latitude"]
                    car_dict["dealer_lon"] = _row["longitude"]
    except Exception:
        pass

    # The address above is fetched by the car row's own registry id — the exact field
    # group-feed misattribution corrupts. Carry the verdict into the prompt so the
    # reply caveats the location instead of asserting it.
    try:
        from backend.db.repositories.cars_repo import car_attribution_states
        from backend.utils.car_serialize.attribution import attribution_public_fields

        car_dict.update(attribution_public_fields(car_attribution_states([car_id]).get(car_id)))
    except Exception:
        pass

    body = request.get_json() or {}

    message = (body.get("message") or body.get("q") or "").strip()
    if not message:
        return jsonify({"ok": False, "error": "message_required"}), 400
    if len(message) > main._CHAT_MAX_MESSAGE:
        return jsonify({"ok": False, "error": "message_too_long"}), 400

    allow_playwright = web_research_playwright_allowed(session.get("user_id"))
    out = main.run_car_page_chat(car_dict, message, allow_web_research=allow_playwright)
    err = out.get("error")
    return jsonify(
        {
            "ok": err is None,
            "reply": out.get("reply") or "",
            "error": err,
        }
    )


def api_compare_chat():
    """Premium compare assistant — side-by-side listing Q&A (up to 4 cars)."""
    main = main_module()
    ok, err = main._require_feature(FEATURE_AI_COMPARE_CHAT)
    if not ok:
        return jsonify(main._feature_denied_json(FEATURE_AI_COMPARE_CHAT, err)), 403

    ip = _client_ip()
    rpm_pair, rpm_ip, rpm_global = car_chat_rate_limits()

    if rpm_global > 0 and not allow_request(
        "chat:global",
        max_events=rpm_global,
        window_seconds=60.0,
    ):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    if not allow_request(f"chat:ip:{ip}", max_events=rpm_ip, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    if not allow_request(f"chat:compare:ip:{ip}", max_events=rpm_pair, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    # Compare chat has its own daily namespace ('cmp:') so comparing cars does
    # not drain the car-chat budget. Same per-user tier value; no per-listing
    # tier — a compare has no single car_id.
    daily_limit = car_chat_user_daily_limit()
    if daily_limit > 0:
        uid = session.get("user_id")
        if uid:
            daily_key = f"cmp:daily:user:{int(uid)}"
        else:
            daily_key = f"cmp:daily:ip:{ip}"
        if not allow_request(
            daily_key,
            max_events=daily_limit,
            window_seconds=86400.0,
        ):
            return jsonify({"ok": False, "error": "user_chat_limit_reached"}), 429

    if request.content_length is not None and request.content_length > main._CHAT_MAX_BODY:
        return jsonify({"ok": False, "error": "payload_too_large"}), 413

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return jsonify({"ok": False, "error": "json_object"}), 400

    from backend.utils.compare_specs import parse_compare_car_ids

    raw_ids = body.get("car_ids") or body.get("ids") or []
    if isinstance(raw_ids, str):
        id_list = parse_compare_car_ids(raw_ids)
    elif isinstance(raw_ids, list):
        id_list = parse_compare_car_ids(",".join(str(x) for x in raw_ids))
    else:
        id_list = []

    if not id_list:
        return jsonify({"ok": False, "error": "car_ids_required"}), 400

    message = (body.get("message") or body.get("q") or "").strip()
    if not message:
        return jsonify({"ok": False, "error": "message_required"}), 400
    if len(message) > main._CHAT_MAX_MESSAGE:
        return jsonify({"ok": False, "error": "message_too_long"}), 400

    raw_cars = main.get_cars_by_ids(id_list)
    if not raw_cars:
        return jsonify({"ok": False, "error": "not_found"}), 404

    allow_playwright = web_research_playwright_allowed(session.get("user_id"))
    out = main.run_compare_chat(raw_cars, message, allow_web_research=allow_playwright)
    err = out.get("error")
    return jsonify(
        {
            "ok": err is None,
            "reply": out.get("reply") or "",
            "error": err,
        }
    )


def register(app) -> None:
    """Attach routes to ``app`` keeping the original bare endpoint names."""
    app.add_url_rule(
        "/car/<int:car_id>/window-sticker-preview.png", view_func=car_window_sticker_preview_png
    )
    app.add_url_rule("/compare", view_func=compare_page)
    app.add_url_rule("/car/<int:car_id>", view_func=car_detail)
    app.add_url_rule("/api/cars/<int:car_id>", view_func=api_car_detail, methods=["GET"])
    app.add_url_rule("/api/cars/<int:car_id>/window-sticker", view_func=api_car_window_sticker)
    app.add_url_rule(
        "/api/cars/<int:car_id>/window-sticker-preview", view_func=api_car_window_sticker_preview
    )
    app.add_url_rule(
        "/api/cars/<int:car_id>/nhtsa-recalls", view_func=api_car_nhtsa_recalls, methods=["GET"]
    )
    app.add_url_rule(
        "/api/cars/<int:car_id>/vehicle-history-intelligence",
        view_func=api_car_vehicle_history_intelligence,
        methods=["GET"],
    )
    app.add_url_rule(
        "/api/cars/<int:car_id>/packages/ensure",
        view_func=api_car_packages_ensure,
        methods=["POST"],
    )
    app.add_url_rule("/api/car/<int:car_id>/chat", view_func=api_car_chat, methods=["POST"])
    app.add_url_rule("/api/compare/chat", view_func=api_compare_chat, methods=["POST"])
