"""Listings pages + listings/search/geo/saved-cars JSON APIs.

Inventory accessors that tests monkeypatch on ``backend.main``
(``get_saved_car_ids``, ``get_cars_by_ids``, ``serialize_cars_for_listings_grid``,
``get_car_by_id``, ``listings_geo_kwargs_from_session``) and the env-derived
rate-limit globals are resolved through the module object at request time.
See ``backend.routes._shared``.
"""

from __future__ import annotations

import gzip
import io
import logging
import os
import threading
from typing import Any, NamedTuple

from flask import Response, jsonify, make_response, redirect, render_template, request, session, url_for

from backend.billing.catalog import FEATURE_MARKET_INTEL, FEATURE_SAVED_SEARCHES
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


class _CarsJsonCacheEntry(NamedTuple):
    etag: str | None
    gz_body: bytes | None


# Published/read as a single immutable object so a reader never observes a
# partially-updated (etag, gz_body) pair: CPython name rebinding is
# atomic, whereas mutating two keys of a shared dict in place is not.
#
# Only the gzipped body is retained (~13MB in prod). This used to cache the
# uncompressed bytes alongside it -- well over 100MB, held for the lifetime of
# every worker process -- to serve a case that effectively never occurs, since
# every real client sends ``Accept-Encoding: gzip``. A non-gzip client is now
# served by streaming decompression in 64KB chunks, so it costs one chunk of
# transient memory, not a second permanent copy. See the memory accounting in
# scripts/docker-entrypoint-web.sh.
_cars_json_cache: _CarsJsonCacheEntry = _CarsJsonCacheEntry(etag=None, gz_body=None)
# Guards the rebuild below so concurrent requests racing to rebuild after an
# invalidation don't all redo the (expensive) serialize+gzip work, and so a
# slower thread's stale rebuild can't overwrite a faster thread's newer one.
_cars_json_cache_lock = threading.Lock()


def api_listings_cars():
    """Read-only JSON for the listings grid; supports client refresh while a scan is running.

    The full inventory (100k+ rows) is intentionally sent in one payload — the
    client does its own facet/radius filtering over the whole set for instant,
    no-round-trip interaction (see frontend/static/main.js). What's expensive
    isn't the DB read (already cached in-process by listings_grid_serialized_cars)
    but re-running Flask's jsonify() over that many rows on every single
    request, which is CPU-bound and holds the GIL — under concurrent load,
    requests serialize behind each other's JSON encoding instead of running in
    parallel. Cache the encoded JSON bytes themselves, keyed by the same
    cache-invalidation token already used for the ETag, so repeat requests
    (the common case — nothing changes between scans) skip re-encoding.

    Only the GZIPPED bytes are cached. Keeping the uncompressed copy too cost
    ~109 MB resident per worker forever, to serve clients that do not advertise
    gzip — a case that effectively does not occur. Those now pay one
    ``gzip.decompress`` per request instead.

    Also pre-gzip the cached body ONCE here rather than relying on the
    ``_gzip_large_json`` after_request hook: at ~109 MB raw JSON (113k cars),
    ``gzip.compress()`` alone is real CPU work, and the hook re-ran it on
    every single request — including cache hits — because it only sees the
    final response object, not whether the body it's compressing is identical
    to last time. Setting Content-Encoding here makes the hook's own
    early-exit (`if resp.headers.get("Content-Encoding"): return resp`) skip
    that redundant compression.

    Cache-Control is ``public``: this route has no auth check and its output
    has no per-user content (no session/user_id in the serialization path),
    so it's identical for every visitor. ``s-maxage`` lets Cloudflare's edge
    serve repeat requests straight from cache — off the origin entirely —
    for up to the same 60s window the underlying cache token already uses
    (``_listings_cache_token``), which is what actually bounds staleness.
    """
    global _cars_json_cache
    etag = listings_grid_cache_etag()
    inm = (request.headers.get("If-None-Match") or "").strip()
    if inm and inm == etag:
        resp = make_response("", 304)
        resp.headers["ETag"] = etag
        resp.headers["Cache-Control"] = "public, max-age=0, s-maxage=60, stale-while-revalidate=30"
        resp.headers["Vary"] = "Accept-Encoding"
        return resp
    cache = _cars_json_cache
    if cache.etag != etag:
        with _cars_json_cache_lock:
            # Re-read the validator INSIDE the lock, then re-check. A waiter that
            # compared against its pre-lock `etag` would see a NEWER entry
            # published by the thread ahead of it as "different" and redo the
            # whole serialize+gzip -- serially, lock held, all four gthreads --
            # which is the thundering herd this lock exists to prevent.
            cache = _cars_json_cache
            etag = listings_grid_cache_etag()
            if cache.etag != etag:
                # Validator BEFORE body, never after. listings_grid_serialized_cars()
                # is stale-while-revalidate: it can hand back the OLD list while a
                # daemon thread publishes the new one moments later. Reading the
                # ETag second would then tag that old body with the NEW tag, and
                # every subsequent request (and 304) would confirm stale data as
                # current. Tagging a fresh body with an older tag merely costs one
                # extra rebuild at the next request.
                cars = listings_grid_serialized_cars()
                body = jsonify({"ok": True, "cars": cars}).get_data()
                gz_body = gzip.compress(body, compresslevel=5)
                # Drop the uncompressed bytes here: `body` is a local and goes out
                # of scope, so only the gzip survives in the cache.
                del body
                cache = _CarsJsonCacheEntry(etag=etag, gz_body=gz_body)
                _cars_json_cache = cache
    if cache.gz_body is None:
        # Invariant: after the rebuild block a body exists. If it does not, say so
        # rather than serving `200 OK` with an empty JSON body the client would
        # happily render as "no inventory".
        return jsonify({"ok": False, "error": "cars_cache_unavailable"}), 503
    accepts_gzip = "gzip" in (request.headers.get("Accept-Encoding") or "").lower()
    if accepts_gzip:
        resp = make_response(cache.gz_body)
        resp.headers["Content-Encoding"] = "gzip"
    else:
        # Rare path (client did not advertise gzip): STREAM the decompression in
        # chunks instead of materializing the whole ~100MB+ body per request.
        # `gzip.decompress()` here would allocate the full payload for every
        # non-gzip caller concurrently (bare curl, urllib, uptime probes) and hold
        # it for the entire client write -- a per-request spike larger than the
        # permanent copy this cache used to keep.
        gz_bytes = cache.gz_body

        def _stream():
            with gzip.GzipFile(fileobj=io.BytesIO(gz_bytes), mode="rb") as fh:
                while True:
                    chunk = fh.read(64 * 1024)
                    if not chunk:
                        break
                    yield chunk

        resp = Response(_stream(), direct_passthrough=True)
    resp.headers["Content-Type"] = "application/json"
    # Use the cache entry's own etag (matching the body/gz_body we just
    # served), not a possibly newer value from a concurrent rebuild.
    resp.headers["ETag"] = cache.etag
    resp.headers["Cache-Control"] = "public, max-age=0, s-maxage=60, stale-while-revalidate=30"
    resp.headers["Vary"] = "Accept-Encoding"
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
