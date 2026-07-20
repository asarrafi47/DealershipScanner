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
    car_chat_rate_limits,
    car_chat_user_daily_limit,
    web_research_playwright_allowed,
)
from backend.utils.csrf import validate_csrf_header
from backend.utils.ip_rate_limit import allow_request
from backend.utils.listing_completeness import INCOMPLETE_FIELD_LABELS


def _car_window_sticker_preview_url(car_id: int) -> str:
    return f"/car/{car_id}/window-sticker-preview.png"


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
    from backend.listings.dealer_map import build_dealer_map_for_car

    dealer_map = build_dealer_map_for_car(car_raw, dealer_info)
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
        "sticker_exterior_color": ctx.get("sticker_exterior_color"),
        "sticker_interior_color": ctx.get("sticker_interior_color"),
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
        "hide_photo_analysis": bool(ctx.get("hide_photo_analysis")),
        "window_sticker_oem_url": window_sticker_oem_url,
        "window_sticker_pdf_url": window_sticker_pdf_url,
        "dealer_info": dealer_info,
        "dealer_map": dealer_map,
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


def api_car_packages_ensure(car_id: int):
    """Fetch/analyze window sticker and merge packages (premium only)."""
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

    from backend.enrichment.listing_packages_service import ensure_listing_packages_for_car

    allow_vision = request.args.get("vision", "0").strip().lower() in ("1", "true", "yes")
    status = ensure_listing_packages_for_car(
        car_id,
        allow_vision_fallback=allow_vision,
        refetch_description=True,
    )
    car2 = main.get_car_by_id(car_id, include_inactive=False) or car
    ctx = main.prepare_car_detail_context(car2)
    status["packages_panel_has_content"] = bool(ctx.get("packages_panel_has_content"))
    status["window_sticker_available"] = bool(status.get("window_sticker_available"))
    from backend.enrichment.window_sticker_service import sticker_panel_payload, window_sticker_available, window_sticker_has_visual
    from backend.scanner.window_sticker import (
        car_listing_sticker_urls,
        cdjr_oem_window_sticker_eligible,
        get_window_sticker_url,
        is_cdjr_stellantis_car,
        show_window_sticker_panel,
        sticker_embed_preview_url,
    )

    status.update(sticker_panel_payload(ctx, car2))
    show_sticker = show_window_sticker_panel(car2, ctx)
    status["show_window_sticker_ui"] = show_sticker
    status["window_sticker_available"] = bool(
        show_sticker
        and (
            status.get("window_sticker_available")
            or window_sticker_available(car2)
        )
    )
    status["window_sticker_visual_available"] = bool(
        show_sticker and window_sticker_has_visual(car2)
    )
    vin = str(car2.get("vin") or "")
    if show_sticker and vin:
        if cdjr_oem_window_sticker_eligible(car2):
            status["window_sticker_oem_url"] = get_window_sticker_url(vin)
        else:
            listing_urls = car_listing_sticker_urls(car2)
            if listing_urls:
                status["window_sticker_oem_url"] = listing_urls[0]
    if show_sticker and cdjr_oem_window_sticker_eligible(car2):
        status["window_sticker_view_url"] = url_for("api_car_window_sticker", car_id=car_id)
    elif status.get("window_sticker_visual_available"):
        status["window_sticker_view_url"] = url_for(
            "api_car_window_sticker", car_id=car_id
        )
    if status.get("window_sticker_visual_available") or status.get("window_sticker_view_url"):
        status["window_sticker_preview_url"] = sticker_embed_preview_url(
            car2,
            preview_api_url=_car_window_sticker_preview_url(car_id),
            has_paid_access=True,
        ) or _car_window_sticker_preview_url(car_id)
    status["listing_photo_detected_equipment"] = ctx.get("listing_photo_detected_equipment") or []
    status["packages_panel_has_content"] = bool(ctx.get("packages_panel_has_content"))
    return jsonify(status)


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

    daily_limit = car_chat_user_daily_limit()
    if daily_limit > 0:
        uid = session.get("user_id")
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

    daily_limit = car_chat_user_daily_limit()
    if daily_limit > 0:
        uid = session.get("user_id")
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
