"""Listings pages + listings/search/geo/saved-cars JSON APIs.

Inventory accessors that tests monkeypatch on ``backend.main``
(``get_saved_car_ids``, ``get_cars_by_ids``, ``serialize_car_for_listings_grid``,
``get_car_by_id``, ``listings_geo_kwargs_from_session``) and the env-derived
rate-limit globals are resolved through the module object at request time.
See ``backend.routes._shared``.
"""

from __future__ import annotations

import os

from flask import jsonify, make_response, redirect, render_template, request, session, url_for

from backend.billing.catalog import FEATURE_MARKET_INTEL
from backend.db.inventory_db import (
    get_filter_options,
    listings_geo_coords_maps,
    listings_grid_cache_etag,
    listings_grid_serialized_cars,
)
from backend.listings.geo_session import apply_listings_geo_to_session
from backend.listings.routes import listings_page
from backend.routes._shared import _client_ip, main_module
from backend.utils.ip_rate_limit import allow_request
from backend.utils.query_parser import parse_natural_query


def _listings_client_poll_ms() -> int:
    """Optional client refresh of ``/api/listings/cars`` (0 = off)."""
    raw = (os.environ.get("LISTINGS_CLIENT_POLL_MS") or "0").strip() or "0"
    try:
        return max(0, int(raw.split()[0]))
    except (TypeError, ValueError, IndexError):
        return 0


def search():
    """Backward-compatible alias: inventory search lives at ``/listings``."""
    dest = url_for("listings")
    qs = request.query_string.decode("utf-8")
    if qs:
        dest = f"{dest}?{qs}"
    return redirect(dest, code=302)


def listings():
    return listings_page(listings_poll_ms=_listings_client_poll_ms())


def premium_page():
    from backend.billing.catalog import plan_display_list
    from backend.billing.entitlements import FEATURE_LABELS
    from backend.billing.stripe_billing import billing_enabled as stripe_billing_enabled

    welcome_source = session.pop("auth_welcome_source", None)
    embed = request.args.get("embed") in ("1", "true", "yes")
    return render_template(
        "premium.html",
        auth_welcome_source=welcome_source,
        premium_embed=embed,
        subscription_plans=plan_display_list(),
        billing_enabled=stripe_billing_enabled(),
        current_plan_id=(session.get("subscription_plan_id") or "").strip().lower() or None,
        feature_labels=FEATURE_LABELS,
    )


def api_session_listings_geo():
    """Remember ZIP + radius for dashboard recommendations (session cookie)."""
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return jsonify({"ok": False, "error": "json_object"}), 400
    zip_code = str(body.get("zip_code") or "").strip()
    try:
        radius_mi = float(body.get("radius"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "bad_radius"}), 400
    if not apply_listings_geo_to_session(session, zip_code, radius_mi):
        return jsonify({"ok": False, "error": "invalid_zip_or_radius"}), 400
    return jsonify({"ok": True})


def api_listings_filter_options():
    """Facet metadata for listings filters (same source as the listings page sidebar)."""
    opts = get_filter_options(include_all_cars=False)
    return jsonify(
        {
            "ok": True,
            "makes": opts.get("makes") or [],
            "model_rows": opts.get("model_rows") or [],
            "trim_rows": opts.get("trim_rows") or [],
            "fuel_types": opts.get("fuel_types") or [],
            "cylinders": opts.get("cylinders") or [],
            "transmissions": opts.get("transmissions") or [],
            "drivetrains": opts.get("drivetrains") or [],
            "forced_inductions": opts.get("forced_inductions") or [],
            "body_styles": opts.get("body_styles") or [],
            "exterior_colors": opts.get("exterior_colors") or [],
            "interior_colors": opts.get("interior_colors") or [],
            "package_rows": opts.get("package_rows") or [],
            "all_package_names": opts.get("all_package_names") or [],
        }
    )


def api_listings_geo_coords():
    """Lazy ZIP + dealer coordinate maps for listings radius filtering."""
    maps = listings_geo_coords_maps()
    resp = make_response(
        jsonify(
            {
                "ok": True,
                "zip_coords": maps.get("zip_coords") or {},
                "dealer_coords": maps.get("dealer_coords") or {},
                # registry_coords is built server-side and read by the client
                # (carGeoCoords → REGISTRY_COORDS[regId]); it was omitted here, so
                # the by-registry-id coordinate path was dead and those dealers'
                # cars were dropped from radius search.
                "registry_coords": maps.get("registry_coords") or {},
                "registry_id_by_host": maps.get("registry_id_by_host") or {},
            }
        )
    )
    resp.headers["Cache-Control"] = "private, max-age=300"
    return resp


def api_listings_cars():
    """Read-only JSON for the listings grid; supports client refresh while a scan is running."""
    etag = listings_grid_cache_etag()
    inm = (request.headers.get("If-None-Match") or "").strip()
    if inm and inm == etag:
        resp = make_response("", 304)
        resp.headers["ETag"] = etag
        resp.headers["Cache-Control"] = "private, no-cache"
        return resp
    cars = listings_grid_serialized_cars()
    etag = listings_grid_cache_etag()
    resp = make_response(jsonify({"ok": True, "cars": cars}))
    resp.headers["ETag"] = etag
    resp.headers["Cache-Control"] = "private, no-cache"
    return resp


def api_listings_market_stats():
    """Trim-level average prices for premium listings grid (cached server-side)."""
    main = main_module()
    ok, err = main._require_feature(FEATURE_MARKET_INTEL)
    if not ok:
        return jsonify(main._feature_denied_json(FEATURE_MARKET_INTEL, err)), 403
    from backend.listings.geo_session import listings_geo_kwargs_from_session
    from backend.utils.market_price import trim_price_stats_for_client

    zip_code = (request.args.get("zip_code") or "").strip()
    radius_s = (request.args.get("radius") or "").strip()
    geo = listings_geo_kwargs_from_session(session)
    if not zip_code and geo.get("zip_code"):
        zip_code = str(geo["zip_code"])
    if not radius_s and geo.get("radius_miles") is not None:
        radius_s = str(geo["radius_miles"])
    radius_mi = None
    if radius_s:
        try:
            radius_mi = float(radius_s)
        except (TypeError, ValueError):
            radius_mi = None

    payload = trim_price_stats_for_client(zip_code=zip_code or None, radius_miles=radius_mi)
    return jsonify({"ok": True, **payload})


def api_zip_coords():
    """Return {lat, lon} for a US zip code via pgeocode."""
    zip_code = request.args.get("zip", "").strip()
    if not zip_code:
        return jsonify({"error": "zip required"}), 400
    try:
        from backend.db.geo import zip_to_coords
        coords = zip_to_coords(zip_code)
        if coords is None:
            return jsonify({"error": "not found"}), 404
        import math
        if math.isnan(coords[0]) or math.isnan(coords[1]):
            return jsonify({"error": "not found"}), 404
        return jsonify({"lat": float(coords[0]), "lon": float(coords[1])})
    except Exception:
        return jsonify({"error": "lookup failed"}), 500


def api_coords_to_zip():
    """Return nearest US ZIP for lat/lon (browser geolocation → listings ZIP)."""
    try:
        lat = float(request.args.get("lat", ""))
        lon = float(request.args.get("lon", ""))
    except (TypeError, ValueError):
        return jsonify({"error": "lat and lon required"}), 400
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return jsonify({"error": "invalid coordinates"}), 400
    try:
        from backend.db.geo import nearest_us_postal_meta

        meta = nearest_us_postal_meta(lat, lon)
        if not meta or not meta.get("postal_code"):
            return jsonify({"error": "not found"}), 404
        return jsonify({"zip_code": meta["postal_code"], "lat": lat, "lon": lon})
    except Exception:
        return jsonify({"error": "lookup failed"}), 500


def _highlight_params_from_filters(filters: dict) -> list[str]:
    """UI filter control keys for styling (matches data-param / form names)."""
    keys = []
    for k in filters:
        if k == "exterior_color":
            keys.append("exterior_color")
        elif k == "drivetrain":
            keys.append("drivetrain")
        elif k == "body_style":
            keys.append("body_style")
        elif k == "max_price":
            keys.append("max_price")
        elif k == "max_mileage":
            keys.append("max_mileage")
        elif k in ("min_year", "max_year"):
            if "year" not in keys:
                keys.append("year")
        elif k in ("make", "model", "vehicle_or"):
            if "make" not in keys:
                keys.append("make")
            if "model" not in keys:
                keys.append("model")
        elif k == "interior_color":
            keys.append("interior_color")
        elif k in ("engine_displacement_l_min", "engine_displacement_l_max", "engine_l_min", "engine_l_max"):
            if "engine_l_min" not in keys:
                keys.append("engine_l_min")
            if "engine_l_max" not in keys:
                keys.append("engine_l_max")
        elif k in ("packages_json_contains", "packages_json_contains_list", "package_contains"):
            if "package" not in keys:
                keys.append("package")
    return keys


def api_search_smart_parse():
    """Fast parse-only for listings instant preview (no DB search)."""
    ip = _client_ip()
    if not allow_request(f"smart:{ip}", max_events=main_module()._SMART_SEARCH_RPM, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429
    q = (request.args.get("query") or request.args.get("q") or "").strip()
    filters = parse_natural_query(q)
    return jsonify(
        {
            "ok": True,
            "filters": filters,
            "highlight": _highlight_params_from_filters(filters),
        }
    )


def api_search_smart():
    """Listings search bar: local ``parse_natural_query`` + SQL/pgvector only (no Claude)."""
    main = main_module()
    ip = _client_ip()
    if not allow_request(f"smart:{ip}", max_events=main._SMART_SEARCH_RPM, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    if request.content_length is not None and request.content_length > main._CHAT_MAX_BODY:
        return jsonify({"ok": False, "error": "payload_too_large"}), 413

    data = request.get_json() or {}
    q = (data.get("query") or data.get("q") or "").strip()
    filters = parse_natural_query(q)
    from backend.utils.hybrid_search import hybrid_smart_search

    geo_kw = {}
    zc = str(data.get("zip_code") or data.get("zip") or "").strip()
    rad_raw = data.get("radius")
    if zc and rad_raw is not None and str(rad_raw).strip() != "":
        try:
            rm = float(rad_raw)
            if rm > 0:
                geo_kw = {"zip_code": zc, "radius_miles": rm}
        except (TypeError, ValueError):
            pass

    from backend.utils.hybrid_search import NO_PARSE_MATCH_MESSAGE, public_search_meta

    results, search_meta = hybrid_smart_search(
        q, filters, vector_top_k=50, listing_geo_kwargs=geo_kw if geo_kw else None
    )
    safe_results = [main.serialize_car_for_listings_grid(c) for c in results]
    try:
        from backend.db.search_analytics_db import analytics_session_key, record_search_event

        uid_raw = session.get("user_id")
        user_id = int(uid_raw) if uid_raw is not None else None
        record_search_event(
            source="smart_search",
            query_text=q or None,
            filters={**filters, **geo_kw},
            result_count=len(safe_results),
            user_id=user_id,
            session_key=analytics_session_key(session),
            geo_zip=geo_kw.get("zip_code") if geo_kw else None,
        )
    except Exception:
        pass
    empty_message = None
    if not safe_results and search_meta.get("mode") == "no_parse_match":
        empty_message = NO_PARSE_MATCH_MESSAGE
    return jsonify(
        {
            "ok": True,
            "filters": filters,
            "results": safe_results,
            "highlight": _highlight_params_from_filters(filters),
            "search_meta": public_search_meta(search_meta),
            "empty_message": empty_message,
        }
    )


def api_saved_cars():
    """Saved inventory for the signed-in user (native clients)."""
    main = main_module()
    uid = session.get("user_id")
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    saved_ids = main.get_saved_car_ids(int(uid))
    raw_saved = main.get_cars_by_ids(saved_ids)
    cars = [main.serialize_car_for_listings_grid(c) for c in raw_saved]
    return jsonify({"ok": True, "cars": cars})


def api_toggle_save(car_id):
    main = main_module()
    try:
        uid = session["user_id"]
    except KeyError:
        uid = None
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    uid = int(uid)
    if not main.get_car_by_id(car_id, include_inactive=False):
        return jsonify({"ok": False, "error": "not_found"}), 404
    currently_saved = main.is_car_saved(uid, car_id)
    if currently_saved:
        main.unsave_car(uid, car_id)
    else:
        main.save_car(uid, car_id)
    return jsonify({"ok": True, "saved": not currently_saved})


def register(app) -> None:
    """Attach routes to ``app`` keeping the original bare endpoint names."""
    app.add_url_rule("/search", view_func=search)
    app.add_url_rule("/listings", view_func=listings)
    app.add_url_rule("/premium", view_func=premium_page)
    app.add_url_rule(
        "/api/session/listings-geo", view_func=api_session_listings_geo, methods=["POST"]
    )
    app.add_url_rule("/api/listings/filter-options", view_func=api_listings_filter_options)
    app.add_url_rule("/api/listings/geo-coords", view_func=api_listings_geo_coords)
    app.add_url_rule("/api/listings/cars", view_func=api_listings_cars)
    app.add_url_rule("/api/listings/market-stats", view_func=api_listings_market_stats)
    app.add_url_rule("/api/zip-coords", view_func=api_zip_coords)
    app.add_url_rule("/api/coords-to-zip", view_func=api_coords_to_zip)
    app.add_url_rule("/api/search/smart/parse", view_func=api_search_smart_parse, methods=["GET"])
    app.add_url_rule("/api/search/smart", view_func=api_search_smart, methods=["POST"])
    app.add_url_rule("/api/saved-cars", view_func=api_saved_cars, methods=["GET"])
    app.add_url_rule("/api/cars/<int:car_id>/save", view_func=api_toggle_save, methods=["POST"])
