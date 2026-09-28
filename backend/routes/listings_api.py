"""Listings pages + listings/search/geo/saved-cars JSON APIs.

Inventory accessors that tests monkeypatch on ``backend.main``
(``get_saved_car_ids``, ``get_cars_by_ids``, ``serialize_cars_for_listings_grid``,
``get_car_by_id``, ``listings_geo_kwargs_from_session``) and the env-derived
rate-limit globals are resolved through the module object at request time.
See ``backend.routes._shared``.
"""

from __future__ import annotations

import gzip
import logging
import os
import threading
from collections import OrderedDict
from typing import Any, NamedTuple

from flask import jsonify, make_response, redirect, render_template, request, session, url_for

from backend.billing.catalog import FEATURE_MARKET_INTEL, FEATURE_SAVED_SEARCHES
from backend.db.inventory_db import (
    get_filter_options,
    listings_geo_coords_maps,
)
from backend.listings.geo_session import apply_listings_geo_to_session
from backend.listings.routes import listings_page
from backend.routes._shared import _client_ip, main_module
from backend.utils.ip_rate_limit import allow_request
from backend.utils.query_parser import parse_natural_query

# Hard bound on free-text search input. The parser fans out over every distinct
# make/model/trim in inventory and the remainder feeds the embedding model, so
# an unbounded string amplifies both CPU and paid-API cost. Matches the
# parser's own internal truncation (query_parser.py).
_SMART_QUERY_MAX_LEN = 200

_logger = logging.getLogger(__name__)


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
    opts = get_filter_options()
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


LISTINGS_RADIUS_MIN_MI = 5.0
LISTINGS_RADIUS_MAX_MI = 250.0
LISTINGS_RADIUS_DEFAULT_MI = 50.0


def clamp_listings_radius(raw) -> float:
    """Radius for the scoped grid: 5..250 mi, 50 when absent or unparseable."""
    try:
        r = float(raw)
    except (TypeError, ValueError):
        return LISTINGS_RADIUS_DEFAULT_MI
    if r != r or r in (float("inf"), float("-inf")):
        return LISTINGS_RADIUS_DEFAULT_MI
    return max(LISTINGS_RADIUS_MIN_MI, min(LISTINGS_RADIUS_MAX_MI, r))


# The radii the listings page offers. The scoped grid snaps a requested radius UP to
# the nearest of these (a superset the client narrows by the exact radius), so the
# scope cache / build-lock key space is five values per ZIP, not every float a
# client can send (radius=49.99, 50.01, ... would each build and cache a body).
LISTINGS_RADIUS_OPTIONS_MI = (10.0, 25.0, 50.0, 100.0, 250.0)


def snap_listings_radius(raw) -> float:
    """Clamp *raw* (see :func:`clamp_listings_radius`), then snap it up to the
    nearest :data:`LISTINGS_RADIUS_OPTIONS_MI` value (max 250)."""
    r = clamp_listings_radius(raw)
    for opt in LISTINGS_RADIUS_OPTIONS_MI:
        if r <= opt:
            return opt
    return LISTINGS_RADIUS_OPTIONS_MI[-1]


def _normalize_zip(raw: str) -> str:
    digits = "".join(ch for ch in (raw or "") if ch.isdigit())
    return digits[:5] if len(digits) >= 5 else ""


class _CarsScopeEntry(NamedTuple):
    token: Any
    store_gen: int | None  # set while the body contains stale cards awaiting refresh
    etag: str
    gz_body: bytes
    count: int


# Gzipped scoped bodies, keyed by (zip, radius, hidden-dealer set). Bounded by entry
# count AND total bytes: the largest metro is ~3-4 MB gzipped, so 64 entries of that
# would be ~250 MB; the byte cap keeps the worst case well under that.
_CARS_SCOPE_MAX_ENTRIES = 64
_CARS_SCOPE_MAX_BYTES = 96 * 1024 * 1024
_cars_scope_cache: "OrderedDict[tuple, _CarsScopeEntry]" = OrderedDict()
_cars_scope_bytes = 0
_cars_scope_lock = threading.Lock()
# One build per scope key at a time: concurrent shoppers in the same metro wait for
# the first build instead of all running it.
_cars_scope_build_locks: dict[tuple, threading.Lock] = {}


def _cars_scope_get(key: tuple) -> _CarsScopeEntry | None:
    with _cars_scope_lock:
        entry = _cars_scope_cache.get(key)
        if entry is not None:
            _cars_scope_cache.move_to_end(key)
        return entry


def _cars_scope_put(key: tuple, entry: _CarsScopeEntry) -> None:
    global _cars_scope_bytes
    with _cars_scope_lock:
        old = _cars_scope_cache.pop(key, None)
        if old is not None:
            _cars_scope_bytes -= len(old.gz_body)
        _cars_scope_cache[key] = entry
        _cars_scope_bytes += len(entry.gz_body)
        while _cars_scope_cache and (
            len(_cars_scope_cache) > _CARS_SCOPE_MAX_ENTRIES
            or _cars_scope_bytes > _CARS_SCOPE_MAX_BYTES
        ):
            _, dropped = _cars_scope_cache.popitem(last=False)
            _cars_scope_bytes -= len(dropped.gz_body)


def clear_cars_scope_cache() -> None:
    global _cars_scope_bytes
    with _cars_scope_lock:
        _cars_scope_cache.clear()
        _cars_scope_bytes = 0


def _cars_scope_entry_valid(entry: _CarsScopeEntry | None, token: Any) -> bool:
    if entry is None or entry.token != token:
        return False
    if entry.store_gen is not None:
        from backend.db.repositories.grid_cards_repo import store_generation

        return entry.store_gen == store_generation()
    return True


def _build_cars_scope_entry(
    zip_code: str, origin: tuple[float, float], radius: float, hidden: tuple, token: Any
) -> _CarsScopeEntry:
    import hashlib
    import json as _json

    from backend.db.repositories.grid_cards_repo import cards_near, store_generation

    gen_before = store_generation()
    res = cards_near(origin[0], origin[1], radius, exclude_dealer_ids=hidden)
    head = _json.dumps(
        {
            "ok": True,
            "zip": zip_code,
            "radius": radius,
            "count": len(res.entries),
            "missing_coords": res.missing_coords,
        },
        separators=(",", ":"),
    )
    body = (head[:-1] + ',"cars":[' + ",".join(res.cards_json()) + "]}").encode("utf-8")
    gz_body = gzip.compress(body, compresslevel=6)
    digest = hashlib.blake2b(gz_body, digest_size=10).hexdigest()
    etag = f'W/"lc-{zip_code}-{int(radius)}-{digest}"'
    return _CarsScopeEntry(
        token=token,
        store_gen=gen_before if res.partial else None,
        etag=etag,
        gz_body=gz_body,
        count=len(res.entries),
    )


def _cars_json_response(entry: _CarsScopeEntry, *, private: bool):
    cache_control = (
        "private, no-cache" if private
        else "public, max-age=0, s-maxage=60, stale-while-revalidate=30"
    )
    inm = (request.headers.get("If-None-Match") or "").strip()
    if inm and inm == entry.etag:
        resp = make_response("", 304)
    elif "gzip" in (request.headers.get("Accept-Encoding") or "").lower():
        resp = make_response(entry.gz_body)
        resp.headers["Content-Encoding"] = "gzip"
        resp.headers["Content-Type"] = "application/json"
    else:
        resp = make_response(gzip.decompress(entry.gz_body))
        resp.headers["Content-Type"] = "application/json"
    resp.headers["ETag"] = entry.etag
    resp.headers["Cache-Control"] = cache_control
    resp.headers["Vary"] = "Accept-Encoding, Cookie" if private else "Accept-Encoding"
    return resp


def api_listings_cars():
    """Grid cards for the cars within ``radius`` miles of ``zip`` (listings page).

    Owner decision 2026-09-28: the listings page no longer downloads the whole
    fleet (214k rows, 32 MB gzipped, ~9.5 GB peak in the web process). It asks for
    the shopper's area once a search starts and filters that subset client-side.

    * ``zip`` (or ``zip_code``) is required: without it this is a 400
      ``zip_required``, never the fleet.
    * ``radius`` is clamped to 5..250 mi (default 50) and snapped up to one of
      10/25/50/100/250, so the cache key space stays bounded; the client narrows
      the superset to its exact radius (every card carries ``distance_miles``).
    * Signed-in users' hidden dealerships are excluded in SQL.
    * Cards come from the persisted card store (``grid_cards_repo``), so a response
      is SQL plus string concatenation, not a serializer pass.
    * The gzipped body is cached per (zip, radius, hidden set) in a small LRU
      bounded by entries and bytes; the ETag is a digest of that body, so it moves
      exactly when the served data does (and a 304 is always honest).
    """
    zip_code = _normalize_zip(request.args.get("zip") or request.args.get("zip_code") or "")
    if not zip_code:
        return jsonify({"ok": False, "error": "zip_required"}), 400
    radius = snap_listings_radius(request.args.get("radius"))
    from backend.db.geo import zip_to_coords

    origin = zip_to_coords(zip_code)
    if origin is None:
        return jsonify({"ok": False, "error": "zip_not_found"}), 400

    main = main_module()
    hidden: tuple = ()
    try:
        hidden = tuple(sorted(main.hidden_dealer_ids_for_user(session.get("user_id")) or ()))
    except Exception:
        hidden = ()

    from backend.db.repositories.grid_cards_repo import grid_scope_token

    token = grid_scope_token()
    key = (zip_code, radius, hidden)
    entry = _cars_scope_get(key)
    if not _cars_scope_entry_valid(entry, token):
        with _cars_scope_lock:
            build_lock = _cars_scope_build_locks.setdefault(key, threading.Lock())
            if len(_cars_scope_build_locks) > 4 * _CARS_SCOPE_MAX_ENTRIES:
                _cars_scope_build_locks.clear()
                _cars_scope_build_locks[key] = build_lock
        with build_lock:
            entry = _cars_scope_get(key)
            if not _cars_scope_entry_valid(entry, token):
                entry = _build_cars_scope_entry(
                    zip_code, (float(origin[0]), float(origin[1])), radius, hidden, token
                )
                _cars_scope_put(key, entry)
    return _cars_json_response(entry, private=bool(hidden))


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
        elif k == "inventory_condition":
            keys.append("inventory_condition")
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
    q = (request.args.get("query") or request.args.get("q") or "").strip()[:_SMART_QUERY_MAX_LEN]
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
    q = (data.get("query") or data.get("q") or "").strip()[:_SMART_QUERY_MAX_LEN]
    filters = parse_natural_query(q)
    from backend.utils.hybrid_search import hybrid_smart_search

    # Owner decision 2026-09-28: every search is scoped to the shopper's ZIP +
    # radius, like the grid it replaces. The request's ZIP wins, then the remembered
    # (session) area; with neither there is no area to search.
    zc = _normalize_zip(str(data.get("zip_code") or data.get("zip") or ""))
    rad_raw = data.get("radius")
    if not zc:
        from backend.listings.geo_session import listings_geo_kwargs_from_session

        remembered = listings_geo_kwargs_from_session(session)
        zc = _normalize_zip(str(remembered.get("zip_code") or ""))
        if rad_raw is None or str(rad_raw).strip() == "":
            rad_raw = remembered.get("radius_miles")
    if not zc:
        return jsonify({"ok": False, "error": "zip_required"}), 400
    geo_kw = {"zip_code": zc, "radius_miles": clamp_listings_radius(rad_raw)}

    from backend.utils.hybrid_search import NO_PARSE_MATCH_MESSAGE, public_search_meta

    hidden_dealers: set[str] = set()
    try:
        hidden_dealers = main.hidden_dealer_ids_for_user(session.get("user_id"))
    except Exception:
        hidden_dealers = set()
    results, search_meta = hybrid_smart_search(
        q,
        filters,
        vector_top_k=50,
        listing_geo_kwargs=geo_kw if geo_kw else None,
        exclude_dealer_ids=hidden_dealers or None,
    )
    safe_results = main.serialize_cars_for_listings_grid(results)
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
    _record_smart_search_history(main, q, geo_kw, len(safe_results))
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


def _record_smart_search_history(main, q: str, geo_kw: dict, result_count: int) -> None:
    """Profile -> Recent searches row for a signed-in smart search.

    Stored as the listings-URL shape (``q`` + zip/radius) so "Run again" is a plain
    ``/listings?q=...`` link that re-runs the same hybrid search. Never raises."""
    uid_raw = session.get("user_id")
    if not uid_raw or not q:
        return
    try:
        raw = {"q": q}
        if geo_kw:
            raw["zip_code"] = str(geo_kw.get("zip_code") or "")
            rm = geo_kw.get("radius_miles")
            if rm:
                raw["radius"] = str(int(rm)) if float(rm).is_integer() else str(rm)
        filters = _clean_saved_search_filters(raw)
        if not filters:
            return
        main.record_search_history(
            int(uid_raw), filters, query_text=q, result_count=result_count
        )
    except Exception:
        _logger.warning("search history write failed for user %s", uid_raw, exc_info=True)


def api_saved_cars():
    """Saved inventory for the signed-in user (native clients)."""
    main = main_module()
    uid = session.get("user_id")
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    saved_ids = main.get_saved_car_ids(int(uid))
    raw_saved = main.get_cars_by_ids(saved_ids)
    cars = main.serialize_cars_for_listings_grid(raw_saved)
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


_SAVED_SEARCH_MAX_FILTERS = 40  # generous bound on number of filter keys stored per search


def _clean_saved_search_filters(raw) -> dict | None:
    """Keep only scalar/list-of-scalar filter values (mirrors listings query params)."""
    if not isinstance(raw, dict) or not raw:
        return None
    out: dict = {}
    for k, v in raw.items():
        if len(out) >= _SAVED_SEARCH_MAX_FILTERS:
            break
        key = str(k).strip()
        if not key or len(key) > 80:
            continue
        if isinstance(v, (str, int, float, bool)):
            out[key] = v
        elif isinstance(v, list) and all(isinstance(x, (str, int, float, bool)) for x in v):
            out[key] = v[:50]
    return out or None


def api_saved_searches_list():
    """This user's saved searches (listings toolbar "Saved searches" panel)."""
    main = main_module()
    ok, err = main._require_feature(FEATURE_SAVED_SEARCHES)
    if not ok:
        return jsonify(main._feature_denied_json(FEATURE_SAVED_SEARCHES, err, searches=[])), 403
    uid = session.get("user_id")
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in", "searches": []}), 401
    searches = main.list_saved_searches(int(uid))
    return jsonify({"ok": True, "searches": searches})


def api_saved_searches_create():
    """Persist the listings page's current filter state ("Save this search")."""
    main = main_module()
    ok, err = main._require_feature(FEATURE_SAVED_SEARCHES)
    if not ok:
        return jsonify(main._feature_denied_json(FEATURE_SAVED_SEARCHES, err)), 403
    uid = session.get("user_id")
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    body = request.get_json(silent=True)
    filters = _clean_saved_search_filters((body or {}).get("filters"))
    if filters is None:
        return jsonify({"ok": False, "error": "filters_required"}), 400
    try:
        search_id = main.create_saved_search(int(uid), filters)
    except ValueError:
        return jsonify({"ok": False, "error": "filters_too_large"}), 400
    return jsonify({"ok": True, "id": search_id, "filters": filters})


def api_saved_searches_delete(search_id):
    main = main_module()
    ok, err = main._require_feature(FEATURE_SAVED_SEARCHES)
    if not ok:
        return jsonify(main._feature_denied_json(FEATURE_SAVED_SEARCHES, err)), 403
    uid = session.get("user_id")
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    removed = main.delete_saved_search(int(uid), int(search_id))
    if not removed:
        return jsonify({"ok": False, "error": "not_found"}), 404
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Hidden dealerships (account profile). Free for every signed-in user -- no
# _require_feature gate, only the login check the saved-search routes make.
# ---------------------------------------------------------------------------

_DEALER_ID_MAX_LEN = 128


def _clean_dealer_id_param(raw) -> str | None:
    """Lower-cased ``cars.dealer_id`` key or None when the value cannot be one."""
    import re

    key = str(raw or "").strip().lower()
    if not key or len(key) > _DEALER_ID_MAX_LEN:
        return None
    if not re.match(r"^[a-z0-9][a-z0-9._-]*$", key):
        return None
    return key


def _known_dealer_name(dealer_id: str) -> tuple[bool, str | None]:
    """(exists, display_name) for a ``cars.dealer_id`` key.

    A dealer is known when it has (or had) inventory rows under that key, or when
    a dealerships registry row's website host maps to it (the same resolution the
    dealership research page uses), so a rooftop with no scan yet can still be hidden.
    """
    from backend.db.repositories.base_repo import db_conn

    with db_conn() as conn:
        row = conn.execute(
            "SELECT dealer_name FROM cars WHERE dealer_id = ? "
            "ORDER BY CASE WHEN dealer_name IS NULL OR dealer_name = '' THEN 1 ELSE 0 END LIMIT 1",
            (dealer_id,),
        ).fetchone()
    if row is not None:
        name = row["dealer_name"] if isinstance(row, dict) else row[0]
        return True, (str(name).strip() or None) if name else None
    try:
        from backend.routes.dealership_page import _find_dealership_by_dealer_id

        reg = _find_dealership_by_dealer_id(dealer_id)
    except Exception:
        reg = None
    if reg:
        return True, (str(reg.get("name") or "").strip() or None)
    return False, None


def api_hidden_dealers_list():
    """GET /api/profile/hidden-dealers -- this user's hidden dealerships."""
    main = main_module()
    uid = session.get("user_id")
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in", "dealers": []}), 401
    dealers = main.list_hidden_dealers(int(uid))
    return jsonify({"ok": True, "dealers": dealers})


def api_hidden_dealers_add():
    """POST /api/profile/hidden-dealers {dealer_id} -- hide a dealership for this user."""
    main = main_module()
    uid = session.get("user_id")
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    body = request.get_json(silent=True) or {}
    dealer_id = _clean_dealer_id_param(body.get("dealer_id"))
    if not dealer_id:
        return jsonify({"ok": False, "error": "dealer_id_required"}), 400
    known, name = _known_dealer_name(dealer_id)
    if not known:
        return jsonify({"ok": False, "error": "not_found"}), 404
    try:
        added = main.hide_dealer(int(uid), dealer_id, name)
    except ValueError:
        return jsonify({"ok": False, "error": "list_full"}), 400
    return jsonify(
        {
            "ok": True,
            "hidden": True,
            "added": bool(added),
            "dealer": {"dealer_id": dealer_id, "dealer_name": name},
        }
    )


def api_hidden_dealers_remove(dealer_id):
    """DELETE /api/profile/hidden-dealers/<dealer_id> -- unhide."""
    main = main_module()
    uid = session.get("user_id")
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    key = _clean_dealer_id_param(dealer_id)
    if not key:
        return jsonify({"ok": False, "error": "not_found"}), 404
    removed = main.unhide_dealer(int(uid), key)
    if not removed:
        return jsonify({"ok": False, "error": "not_found"}), 404
    return jsonify({"ok": True, "hidden": False, "dealer_id": key})


# ---------------------------------------------------------------------------
# Recent searches (account profile). Free for every signed-in user, like hidden
# dealerships: login check only, no plan gate. Rows are written by the search
# endpoints above; these routes only read and delete.
# ---------------------------------------------------------------------------

_SEARCH_HISTORY_DEFAULT_LIMIT = 20


def api_search_history_list():
    """GET /api/profile/search-history?limit=20 -- this user's recent searches, newest first."""
    main = main_module()
    uid = session.get("user_id")
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in", "searches": []}), 401
    from backend.utils.search_history_format import decorate_search_rows

    try:
        limit = int(request.args.get("limit", _SEARCH_HISTORY_DEFAULT_LIMIT))
    except (TypeError, ValueError):
        limit = _SEARCH_HISTORY_DEFAULT_LIMIT
    rows = main.list_search_history(int(uid), limit)
    return jsonify({"ok": True, "searches": decorate_search_rows(rows)})


def api_search_history_delete(entry_id):
    """DELETE /api/profile/search-history/<id> -- forget one search."""
    main = main_module()
    uid = session.get("user_id")
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    removed = main.delete_search_history_entry(int(uid), int(entry_id))
    if not removed:
        return jsonify({"ok": False, "error": "not_found"}), 404
    return jsonify({"ok": True, "id": int(entry_id)})


def api_search_history_clear():
    """DELETE /api/profile/search-history -- forget every search."""
    main = main_module()
    uid = session.get("user_id")
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    removed = main.clear_search_history(int(uid))
    return jsonify({"ok": True, "removed": int(removed or 0)})


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
    app.add_url_rule(
        "/api/saved-searches", view_func=api_saved_searches_list, methods=["GET"]
    )
    app.add_url_rule(
        "/api/saved-searches", view_func=api_saved_searches_create, methods=["POST"]
    )
    app.add_url_rule(
        "/api/saved-searches/<int:search_id>",
        view_func=api_saved_searches_delete,
        methods=["DELETE"],
    )
    app.add_url_rule(
        "/api/profile/hidden-dealers", view_func=api_hidden_dealers_list, methods=["GET"]
    )
    app.add_url_rule(
        "/api/profile/hidden-dealers", view_func=api_hidden_dealers_add, methods=["POST"]
    )
    app.add_url_rule(
        "/api/profile/hidden-dealers/<dealer_id>",
        view_func=api_hidden_dealers_remove,
        methods=["DELETE"],
    )
    app.add_url_rule(
        "/api/profile/search-history", view_func=api_search_history_list, methods=["GET"]
    )
    app.add_url_rule(
        "/api/profile/search-history", view_func=api_search_history_clear, methods=["DELETE"]
    )
    app.add_url_rule(
        "/api/profile/search-history/<int:entry_id>",
        view_func=api_search_history_delete,
        methods=["DELETE"],
    )
