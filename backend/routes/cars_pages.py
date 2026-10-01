"""Car detail (VDP), compare page, window sticker, and AI chat routes.

This is the most monkeypatched cluster in the test suite: ``get_car_by_id``,
``prepare_car_detail_context``, ``listings_geo_kwargs_from_session``,
``run_car_page_chat`` and friends are all patched on ``backend.main``.  Every
main-owned helper is therefore resolved through the module object at request
time (see ``backend.routes._shared``).  ``backend.main`` re-exports
``_build_car_detail_view_context`` for ``backend.dev.scan_lab_routes``.
"""

from __future__ import annotations

import logging
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
from backend.billing import access as paid_access
from backend.routes._shared import _client_ip, main_module
from backend.utils.car_chat_policy import (
    car_chat_listing_daily_limit,
    car_chat_rate_limits,
    car_chat_user_daily_limit,
    web_research_playwright_allowed,
)
from backend.utils.csrf import validate_csrf_header
from backend.utils.ip_rate_limit import allow_request

# The packages/sticker fetch job manager moved to car_detail/packages_ensure.py
# (monolith audit 2026-10-01, W5). Every name is re-exported here: tests and
# the ensure route below use them as ``cars_pages`` globals, and the dicts and
# lock are the very same objects in both modules.
from backend.routes.car_detail.packages_ensure import (  # noqa: F401
    ENSURE_COMPLETED,
    ENSURE_COOLDOWN,
    ENSURE_INFLIGHT,
    ENSURE_TIMEOUT,
    PACKAGES_ENSURE_FETCH_FIELDS,
    PACKAGES_ENSURE_PANEL_FIELDS,
    PACKAGES_ENSURE_RESPONSE_FIELDS,
    STICKER_STATUS_FETCHING,
    STICKER_STATUS_HIDDEN,
    STICKER_STATUS_PENDING,
    STICKER_STATUS_READY,
    STICKER_STATUS_UNAVAILABLE,
    _PACKAGES_ENSURE_DEFAULT_BUDGET,
    _PACKAGES_ENSURE_DEFAULT_COOLDOWN,
    _PACKAGES_ENSURE_DEFAULT_PANEL_TTL,
    _PACKAGES_ENSURE_DEFAULT_POLL,
    _PACKAGES_ENSURE_TRACK_MAX,
    _packages_ensure_attempted_at,
    _packages_ensure_budget_seconds,
    _packages_ensure_cooldown_remaining,
    _packages_ensure_cooldown_seconds,
    _packages_ensure_in_cooldown,
    _packages_ensure_inflight,
    _packages_ensure_inflight_snapshot,
    _packages_ensure_lock,
    _packages_ensure_panel_cache,
    _packages_ensure_panel_ttl_seconds,
    _packages_ensure_poll_seconds,
    _packages_ensure_snapshot,
    _packages_ensure_worker_alive,
    _prune_packages_ensure_tracking,
    _run_packages_ensure_with_budget,
    _window_sticker_status,
)
from backend.routes.car_detail import assemble as _cd_assemble
from backend.routes.car_detail import insights as _cd_insights
from backend.routes.car_detail import listing as _cd_listing
from backend.routes.car_detail import location as _cd_location
from backend.routes.car_detail import options as _cd_options
from backend.routes.car_detail import sticker as _cd_sticker
from backend.routes.car_detail import viewer as _cd_viewer
from backend.routes.car_detail.sticker import _car_window_sticker_preview_url  # noqa: F401


logger = logging.getLogger(__name__)


def _serve_car_window_sticker_preview(car_id: int):
    """Render page 1 of stored Monroney PDF as PNG (same gate as chat/packages; SEC-072/073)."""
    main = main_module()
    car = main.get_car_by_id(car_id, include_inactive=False)
    if not car:
        abort(404)
    ok, _err = paid_access.check_feature(FEATURE_WINDOW_STICKER)
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
        and paid_access.shows(FEATURE_AI_COMPARE_CHAT)
    )
    return render_template(
        "compare.html",
        cars=ctx["cars"],
        compare_rows=ctx["compare_rows"],
        compare_ids=ctx["compare_car_ids"],
        show_compare_chat=show_compare_chat,
    )


def _build_car_detail_view_context(car_id: int, car_raw: dict) -> dict:
    """Context for the car detail page; shared by ``car_detail``, ``api_car_detail``
    and the dev scan lab. Each step lives in ``backend/routes/car_detail/``; the
    order below is the historical one (only the first step writes anything)."""
    main = main_module()
    uid = session.get("user_id")
    car_is_saved = _cd_viewer.record_view_and_saved_state(main, uid, car_id)
    ctx = main.prepare_car_detail_context(car_raw)
    sticker = _cd_sticker.resolve_window_sticker_view(car_id, car_raw, ctx)
    car = _cd_listing.serialize_car(main, car_raw, ctx)
    listing_incomplete_fields = _cd_listing.listing_incomplete_fields(car_id)
    dealer_info = _cd_listing.registry_dealer_info(car_raw)
    attribution, attribution_fields = _cd_listing.apply_attribution_overlay(car_id, car)
    _cd_listing.apply_sticker_fact_overlays(car_id, car, car_raw)
    dealer_map = _cd_location.dealer_map_for_car(car_raw, dealer_info, attribution)
    market_intel, deal_score_detail = _cd_insights.market_intel_for_viewer(main, car_raw)
    trim_ladder = _cd_insights.trim_ladder_for_viewer(car_raw)
    rarity = _cd_insights.inventory_rarity(car_raw)
    listings_geo = main.listings_geo_kwargs_from_session(session)
    hero_location = _cd_location.hero_location(dealer_map, listings_geo)
    generated_spec_sheet = _cd_options.generated_spec_sheet_for(
        car_raw, ctx, has_real_sticker=_cd_sticker.has_real_sticker(sticker, ctx)
    )
    unified_options_list = _cd_options.unified_options_list_for(
        ctx, show_sticker_ui=sticker.show_ui, generated_spec_sheet=generated_spec_sheet
    )
    return _cd_assemble.assemble_view_context(
        car_raw=car_raw,
        ctx=ctx,
        car=car,
        sticker=sticker,
        window_sticker_status=_cd_sticker.rendered_window_sticker_status(
            sticker, fetch_in_flight=_packages_ensure_worker_alive(car_id)
        ),
        uid=uid,
        car_is_saved=car_is_saved,
        listing_incomplete_fields=listing_incomplete_fields,
        dealer_info=dealer_info,
        attribution_fields=attribution_fields,
        dealer_map=dealer_map,
        market_intel=market_intel,
        deal_score_detail=deal_score_detail,
        trim_ladder=trim_ladder,
        rarity=rarity,
        listings_geo=listings_geo,
        hero_location=hero_location,
        generated_spec_sheet=generated_spec_sheet,
        unified_options_list=unified_options_list,
    )


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
    ok, err = paid_access.check_feature(FEATURE_WINDOW_STICKER)
    if not ok:
        return jsonify(paid_access.denied_json(FEATURE_WINDOW_STICKER, err)), 403
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
    ok, err = paid_access.check_feature(FEATURE_VEHICLE_HISTORY)
    if not ok:
        return jsonify(paid_access.denied_json(FEATURE_VEHICLE_HISTORY, err)), 403
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
    ok, err = paid_access.check_feature(FEATURE_PACKAGES_ENSURE)
    if not ok:
        return jsonify(paid_access.denied_json(FEATURE_PACKAGES_ENSURE, err)), 403
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
    ok, err = paid_access.check_feature(FEATURE_AI_CAR_CHAT)
    if not ok:
        return jsonify(paid_access.denied_json(FEATURE_AI_CAR_CHAT, err)), 403

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
    ok, err = paid_access.check_feature(FEATURE_AI_COMPARE_CHAT)
    if not ok:
        return jsonify(paid_access.denied_json(FEATURE_AI_COMPARE_CHAT, err)), 403

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
