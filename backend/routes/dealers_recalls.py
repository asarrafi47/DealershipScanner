"""Dealer locator + NHTSA recall lookup routes (HTML page and JSON APIs).

``_nhtsa_recalls_lookup_payload`` stays in ``backend.main`` (tests monkeypatch
it there); views resolve it and the feature-gate helpers through the module
object at request time. See ``backend.routes._shared``.
"""

from __future__ import annotations

import os

from flask import jsonify, render_template, request, session

from backend.billing.catalog import FEATURE_NEARBY_DEALERS
from backend.routes._shared import _client_ip, main_module
from backend.utils.ip_rate_limit import allow_request
from backend.utils.runtime_env import is_production_env


def api_nearby_dealers():
    """Return dealerships within radius of a ZIP code (max 50 mi). Premium when billing enabled."""
    main = main_module()
    ok, err = main._require_feature(FEATURE_NEARBY_DEALERS)
    if not ok:
        return jsonify(main._feature_denied_json(FEATURE_NEARBY_DEALERS, err, dealers=[])), 403
    from backend.listings.nearby_dealers import resolve_nearby_dealers_for_listings

    zip_code = (request.args.get("zip_code") or "").strip()
    try:
        radius = min(float(request.args.get("radius") or 50), 50.0)
    except (ValueError, TypeError):
        radius = 50.0
    if not zip_code:
        return jsonify({"ok": False, "dealers": []})
    q = (request.args.get("q") or request.args.get("search") or "").strip()
    payload = resolve_nearby_dealers_for_listings(
        zip_code=zip_code,
        radius_miles=radius,
        search_query=q or None,
    )
    return jsonify(payload)


def find_dealers_page():
    from backend.db.inventory_pg import is_inventory_postgres
    from backend.utils.roles import is_admin_role

    return render_template(
        "find_dealers.html",
        site_admin_onboard=is_admin_role(session.get("user_role")),
        dealer_onboard_enabled=is_inventory_postgres(),
    )


def api_nhtsa_recalls_lookup():
    """JSON NHTSA recall lookup for inline VDP (same inputs as /nhtsa-recalls)."""
    payload, status = main_module()._nhtsa_recalls_lookup_payload(
        vin_raw=(request.args.get("vin") or "").strip(),
        make=request.args.get("make"),
        model=request.args.get("model"),
        year=request.args.get("year") or request.args.get("modelYear"),
        rate_key=f"nhtsa-recalls-api:{_client_ip()}",
    )
    return jsonify(payload), status


def nhtsa_recalls_lookup():
    """VIN recall lookup using NHTSA public API (auto-runs on page load)."""
    vin_raw = (request.args.get("vin") or "").strip()
    payload, status = main_module()._nhtsa_recalls_lookup_payload(
        vin_raw=vin_raw,
        make=request.args.get("make"),
        model=request.args.get("model"),
        year=request.args.get("year") or request.args.get("modelYear"),
        rate_key=f"nhtsa-recalls:{_client_ip()}",
    )
    return render_template(
        "nhtsa_recalls.html",
        vin=payload.get("vin"),
        vin_raw=vin_raw,
        recalls=payload.get("recalls") or [],
        lookup_error=payload.get("error"),
        rate_limited=payload.get("error") == "rate_limited" or status == 429,
        vehicle_label=payload.get("vehicle_label"),
    )


def api_dealer_locator():
    """Nearby car dealers: registry + optional Google Places (server-side)."""
    ip = _client_ip()
    if not allow_request(
        f"dealer-locator:{ip}",
        max_events=main_module()._DEALER_LOCATOR_RPM,
        window_seconds=60.0,
    ):
        return jsonify({"ok": False, "error": "rate_limited", "dealers": []}), 429

    from backend.listings.dealer_locator import find_nearby_dealers

    lat = lon = None
    try:
        if request.args.get("lat") not in (None, "") and request.args.get("lon") not in (None, ""):
            lat = float(request.args.get("lat"))
            lon = float(request.args.get("lon"))
    except (TypeError, ValueError):
        lat = lon = None

    zip_code = (request.args.get("zip") or request.args.get("zip_code") or "").strip()
    city = (request.args.get("city") or "").strip()
    state = (request.args.get("state") or "").strip()
    try:
        radius = min(max(float(request.args.get("radius") or 25), 1.0), 50.0)
    except (TypeError, ValueError):
        radius = 25.0
    include_google = (request.args.get("include_google") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
    )

    google_key = (os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip()
    require_login = (os.environ.get("DEALER_LOCATOR_REQUIRE_LOGIN") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    if (
        require_login
        and is_production_env()
        and include_google
        and google_key
        and not session.get("user_id")
    ):
        return jsonify({"ok": False, "error": "login_required", "dealers": []}), 403

    payload = find_nearby_dealers(
        lat=lat,
        lon=lon,
        zip_code=zip_code or None,
        city=city or None,
        state=state or None,
        radius_miles=radius,
        include_google=include_google,
    )
    status = 200 if payload.get("ok") else 400
    return jsonify(payload), status


def register(app) -> None:
    """Attach routes to ``app`` keeping the original bare endpoint names."""
    app.add_url_rule("/api/nearby-dealers", view_func=api_nearby_dealers)
    app.add_url_rule("/find-dealers", view_func=find_dealers_page)
    app.add_url_rule("/api/nhtsa-recalls", view_func=api_nhtsa_recalls_lookup, methods=["GET"])
    app.add_url_rule("/nhtsa-recalls", view_func=nhtsa_recalls_lookup)
    app.add_url_rule("/api/dealer-locator", view_func=api_dealer_locator)
