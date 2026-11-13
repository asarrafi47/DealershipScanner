"""Landing page, signed-in Home feed, Dashboard, and recommendation helpers.

The recommendation cache (``_reco_cache``/``_RECO_CACHE_TTL_S``) lives here, in
its only reader/writer. Inventory/user-history accessors are imported from
their owning modules; tests patch them on this module.
"""

from __future__ import annotations

import logging
import threading
import time

from flask import make_response, redirect, render_template, request, session, url_for

from backend.db.inventory_db import (
    get_cars_by_ids,
    get_saved_car_ids,
    hidden_dealer_ids_for_user,
    search_cars_by_make_model_pairs,
    serialize_cars_for_listings_grid,
)
from backend.db.user_history_db import (
    count_viewed_cars,
    get_recent_compared_car_ids,
    get_recent_history_ids,
    get_recent_viewed_car_ids,
)
from backend.listings.geo_session import listings_geo_kwargs_from_session
from backend.utils.swr_refresh import BackgroundRefresher

# Recommendation scoring walks recent views/compares against live inventory
# (~1s+); Home and Dashboard both need it on every visit, so cache per
# (user, geo) briefly. A new view/compare signal marks the entry STALE instead of
# deleting it (2026-11-13): the old rows are served at once and ONE background
# rebuild replaces them. Deleting it made every /home visit after a car page (and the
# prefetched car pages each count as a view) pay the whole 1-2 s build on the request.
_RECO_CACHE_TTL_S = 120.0
# A stale entry is still served for this long after it went stale; older than that the
# request rebuilds inline (single flight).
_RECO_STALE_MAX_S = 3600.0
_reco_cache: dict[tuple, tuple[float, list, dict]] = {}
_reco_refreshers: dict[tuple, BackgroundRefresher] = {}
_reco_build_locks: dict[tuple, threading.Lock] = {}
_reco_guard = threading.Lock()

# The cheap part of /home (saved, recently viewed, recently compared) is cached per
# user for a minute and DROPPED (not served stale) on the user's own save, view,
# compare or hide, so the shopper never sees their own action missing.
_LISTS_CACHE_TTL_S = 60.0
_lists_cache: dict[int, tuple[float, dict]] = {}


def _inventory_count_display() -> str:
    from backend.db.inventory_db import public_listings_count

    n = public_listings_count()
    if n >= 1000:
        rounded = (n // 100) * 100
        return f"{rounded:,}+"
    if n > 0:
        return f"{n:,}"
    return "Live"


def _landing_featured_cars(limit: int = 4) -> list[dict]:
    """A few photo+price listings for the landing page's "on the lot" strip."""
    from backend.db.repositories.listings_repo import landing_featured_cars

    try:
        return landing_featured_cars(limit=limit)
    except Exception:
        return []


def home():
    """Public marketing landing; signed-in users go to ``/home``."""
    if session.get("user_id"):
        return redirect(url_for("app_home"))
    from datetime import datetime

    return render_template(
        "landing.html",
        now=datetime.utcnow(),
        inventory_count_display=_inventory_count_display(),
        featured_cars=_landing_featured_cars(),
    )


def app_home():
    """Signed-in feed: recommendations, saved cars, browsing history."""
    if not session.get("user_id"):
        return redirect(url_for("login_page"))
    if request.args.get("part") == "reco":
        return _render_reco_fragment()
    return _render_personal_home()


def _similar_recommendation_rows(
    seen_mm: list[tuple[str, str]],
    viewed_id_set: set[int],
    limit: int,
    geo_kw: dict,
) -> list[dict]:
    """Cars matching recent make/model, excluding viewed ids; optional ZIP radius via ``geo_kw``."""
    if not seen_mm:
        return []
    candidates = search_cars_by_make_model_pairs(
        seen_mm[:5],
        sql_limit=max(limit * 6, 60),
        **geo_kw,
    )
    seen_rec: set[int] = set()
    recs: list[dict] = []
    for c in candidates:
        cid = c.get("id")
        if cid and cid not in viewed_id_set and cid not in seen_rec:
            seen_rec.add(cid)
            recs.append(c)
            if len(recs) >= limit:
                return recs
    return recs


def _make_model_pairs_from_cars(cars: list[dict]) -> list[tuple[str, str]]:
    seen_mm: list[tuple[str, str]] = []
    seen_mm_set: set[tuple[str, str]] = set()
    for c in cars:
        make = (c.get("make") or "").strip()
        model = (c.get("model") or "").strip()
        if make and model:
            key = (make.lower(), model.lower())
            if key not in seen_mm_set:
                seen_mm_set.add(key)
                seen_mm.append((make, model))
    return seen_mm


def _invalidate_reco_cache(user_id: int, *, hard: bool = False) -> None:
    """The user's history changed: refresh recommendations in the background.

    Default: keep serving the old rows (marked stale). ``hard=True`` (a hidden dealership
    changed, so old rows could show a hidden dealer) deletes them instead. The cheap
    lists are always dropped, so the shopper's own action shows at once.
    """
    uid = int(user_id)
    _lists_cache.pop(uid, None)
    for key in [k for k in _reco_cache if k[0] == uid]:
        if hard:
            _reco_cache.pop(key, None)
        else:
            _, rows, heading = _reco_cache[key]
            _reco_cache[key] = (time.monotonic() - 0.001, rows, heading)


def _reco_key(user_id: int, limit: int, geo_kw: dict) -> tuple:
    return (int(user_id), int(limit), tuple(sorted((k, str(v)) for k, v in geo_kw.items())))


def _reco_lookup(key: tuple) -> tuple[str, list, dict]:
    """("fresh" | "stale" | "miss", rows, heading) for one cache key; never builds."""
    hit = _reco_cache.get(key)
    if hit is None:
        return "miss", [], {}
    now = time.monotonic()
    if hit[0] > now:
        return "fresh", hit[1], hit[2]
    if hit[0] + _RECO_STALE_MAX_S > now:
        return "stale", hit[1], hit[2]
    return "miss", [], {}


def _reco_build_and_store(user_id: int, limit: int, geo_kw: dict, key: tuple) -> tuple[list, dict]:
    """Run the real build (hidden dealerships included) and store it under *key*."""
    kw = dict(geo_kw)
    try:
        hidden = hidden_dealer_ids_for_user(user_id)
    except Exception:
        hidden = set()
    if hidden:
        kw["exclude_dealer_ids"] = tuple(sorted(hidden))
    rows, heading = _recommendations_for_user_uncached(user_id, limit, serialize=True, **kw)
    with _reco_guard:
        if len(_reco_cache) >= 500:
            _reco_cache.clear()
        _reco_cache[key] = (time.monotonic() + _RECO_CACHE_TTL_S, rows, heading)
    return rows, heading


def _reco_refresh_async(user_id: int, limit: int, geo_kw: dict, key: tuple) -> bool:
    """Start ONE background rebuild for *key* (no-op while one runs). True if started."""
    with _reco_guard:
        if len(_reco_refreshers) >= 500:
            _reco_refreshers.clear()
        refresher = _reco_refreshers.setdefault(key, BackgroundRefresher(f"home-reco-{key[0]}"))
    return refresher.kick(lambda: _reco_build_and_store(user_id, limit, geo_kw, key))


def _reco_peek_or_refresh(user_id: int, limit: int, geo_kw: dict) -> tuple[bool, list, dict]:
    """Never builds on the caller's thread: (have_value, rows, heading).

    A stale value comes back at once and starts the background rebuild; a miss returns
    ``have_value=False`` (the page then defers the block to a small follow-up request).
    """
    key = _reco_key(user_id, limit, geo_kw)
    state, rows, heading = _reco_lookup(key)
    if state == "stale":
        _reco_refresh_async(user_id, limit, geo_kw, key)
    return state != "miss", rows, heading


def _recommendations_for_user(
    user_id: int,
    limit: int = 20,
    *,
    serialize: bool = True,
    **geo_kw: object,
) -> tuple[list[dict] | int, dict[str, str]]:
    """Cached wrapper: serialized rows (or their count) + heading copy.

    fresh: served; stale: served at once and rebuilt once in the background; miss: built
    inline, one build at a time per key (concurrent callers wait for it, not repeat it).
    """
    key = _reco_key(user_id, limit, geo_kw)
    state, rows, heading = _reco_lookup(key)
    if state == "stale":
        _reco_refresh_async(user_id, limit, geo_kw, key)
    if state == "miss":
        with _reco_guard:
            lock = _reco_build_locks.setdefault(key, threading.Lock())
        with lock:
            state, rows, heading = _reco_lookup(key)
            if state == "miss":
                rows, heading = _reco_build_and_store(user_id, limit, geo_kw, key)
    return (list(rows) if serialize else len(rows)), dict(heading)


def _recommendations_for_user_uncached(
    user_id: int,
    limit: int = 20,
    *,
    serialize: bool = True,
    **geo_kw: object,
) -> tuple[list[dict] | int, dict[str, str]]:
    """Return serialized carousel rows and heading copy for the dashboard."""
    default_heading = {
        "eyebrow": "Based on your history",
        "title": "Recommended for You",
        "hint": "",
    }
    viewed_ids = get_recent_viewed_car_ids(user_id, limit=30)
    compared_ids = get_recent_compared_car_ids(user_id, limit=30)
    if not viewed_ids and not compared_ids:
        return (0 if not serialize else []), default_heading

    viewed_cars = get_cars_by_ids(viewed_ids) if viewed_ids else []
    compared_cars = get_cars_by_ids(compared_ids) if compared_ids else []
    if not viewed_cars and not compared_cars:
        return (0 if not serialize else []), default_heading

    by_id: dict[int, dict] = {}
    for c in viewed_cars + compared_cars:
        cid = c.get("id")
        if cid is not None:
            by_id[int(cid)] = c

    seen_mm = _make_model_pairs_from_cars(viewed_cars)
    seen_keys = {(m.lower(), d.lower()) for m, d in seen_mm}
    for pair in _make_model_pairs_from_cars(compared_cars):
        key = (pair[0].lower(), pair[1].lower())
        if key not in seen_keys:
            seen_keys.add(key)
            seen_mm.append(pair)

    signal_id_set = set(viewed_ids) | set(compared_ids)
    heading = dict(default_heading)
    if compared_ids and viewed_ids:
        heading["eyebrow"] = "Based on your browsing and comparisons"
    elif compared_ids:
        heading["eyebrow"] = "Based on your comparisons"
        heading["title"] = "Similar to what you compared"
    raw_recs: list[dict] = []
    geo_kw_dict = dict(geo_kw)
    geo_active = bool(geo_kw_dict.get("zip_code") and geo_kw_dict.get("radius_miles"))
    # Hidden dealerships apply even when the radius fallback drops the geo kwargs.
    excl_kw = (
        {"exclude_dealer_ids": geo_kw_dict["exclude_dealer_ids"]}
        if geo_kw_dict.get("exclude_dealer_ids")
        else {}
    )

    if seen_mm:
        if geo_active:
            raw_recs = _similar_recommendation_rows(seen_mm, signal_id_set, limit, geo_kw_dict)
            if not raw_recs:
                heading["eyebrow"] = "Outside your search radius"
                heading["title"] = "Recommended & recently viewed"
                raw_recs = _similar_recommendation_rows(seen_mm, signal_id_set, limit, excl_kw)
                if raw_recs:
                    heading["hint"] = (
                        "No similar listings near your saved ZIP and radius. "
                        "Showing similar inventory beyond that area and cars from your history."
                    )
                else:
                    heading["hint"] = (
                        "No close matches in inventory right now. Here are cars from your recent history."
                    )
        else:
            raw_recs = _similar_recommendation_rows(seen_mm, signal_id_set, limit, excl_kw)
    else:
        heading["eyebrow"] = "Your history"
        heading["title"] = "Recently viewed"
        heading["hint"] = ""

    out_cars: list[dict] = []
    seen_out: set[int] = set()
    for c in raw_recs:
        if len(out_cars) >= limit:
            break
        cid = c.get("id")
        if cid is None:
            continue
        cid_i = int(cid)
        if cid_i not in seen_out:
            seen_out.add(cid_i)
            out_cars.append(c)

    for vid in viewed_ids:
        if len(out_cars) >= limit:
            break
        vid_i = int(vid)
        if vid_i in seen_out:
            continue
        row = by_id.get(vid_i)
        if row:
            out_cars.append(row)
            seen_out.add(vid_i)

    for cid in compared_ids:
        if len(out_cars) >= limit:
            break
        cid_i = int(cid)
        if cid_i in seen_out:
            continue
        row = by_id.get(cid_i)
        if row:
            out_cars.append(row)
            seen_out.add(cid_i)

    if not out_cars:
        return (0 if not serialize else []), default_heading

    if not serialize:
        return len(out_cars[:limit]), heading

    return (
        serialize_cars_for_listings_grid(out_cars[:limit]),
        heading,
    )


def _recently_compared_for_user(user_id: int, limit: int = 12) -> list[dict]:
    """Serialized listing rows for cars the user recently compared."""
    compared_ids = get_recent_compared_car_ids(user_id, limit=limit)
    if not compared_ids:
        return []
    raw = get_cars_by_ids(compared_ids)
    by_id = {int(c["id"]): c for c in raw if c.get("id") is not None}
    ordered = [by_id[cid] for cid in compared_ids if cid in by_id]
    return serialize_cars_for_listings_grid(ordered)


def _recently_viewed_for_user(user_id: int, limit: int = 12) -> list[dict]:
    """Serialized listing rows for cars the user recently opened."""
    viewed_ids = get_recent_viewed_car_ids(user_id, limit=limit)
    if not viewed_ids:
        return []
    raw = get_cars_by_ids(viewed_ids)
    by_id = {int(c["id"]): c for c in raw if c.get("id") is not None}
    ordered = [by_id[cid] for cid in viewed_ids if cid in by_id]
    return serialize_cars_for_listings_grid(ordered)


def _browse_trends_for_user(user_id: int, limit: int = 6) -> list[tuple[str, int]]:
    """Top makes from recent view history for dashboard trends."""
    from collections import Counter

    viewed_ids = get_recent_viewed_car_ids(user_id, limit=40)
    if not viewed_ids:
        return []
    counts: Counter[str] = Counter()
    for c in get_cars_by_ids(viewed_ids):
        make = (c.get("make") or "").strip()
        if make:
            counts[make] += 1
    return counts.most_common(limit)


def _home_lists_for_user(uid: int) -> dict:
    """Saved, recently viewed and recently compared cars on few connections.

    One users-store open for both history lists, ONE inventory read for every car,
    ONE attribution read for the lot (it used to be three of each).
    """
    hit = _lists_cache.get(uid)
    now = time.monotonic()
    if hit is not None and hit[0] > now:
        return hit[1]
    viewed_ids, compared_ids = get_recent_history_ids(uid, 12, 12)
    saved_ids = get_saved_car_ids(uid)
    wanted: list[int] = []
    for cid in [*saved_ids, *viewed_ids, *compared_ids]:
        if cid not in wanted:
            wanted.append(cid)
    rows = serialize_cars_for_listings_grid(get_cars_by_ids(wanted))
    by_id = {int(r["id"]): r for r in rows if r.get("id") is not None}

    def pick(ids: list[int]) -> list[dict]:
        return [by_id[i] for i in ids if i in by_id]

    out = {
        "saved": pick(saved_ids),
        "viewed": pick(viewed_ids),
        "compared": pick(compared_ids),
        "has_history": bool(viewed_ids or compared_ids),
    }
    if len(_lists_cache) >= 500:
        _lists_cache.clear()
    _lists_cache[uid] = (now + _LISTS_CACHE_TTL_S, out)
    return out


def _reco_template_vars(recommendations: list, heading: dict) -> dict:
    return {
        "recommendations": recommendations,
        "recommendations_eyebrow": heading.get("eyebrow") or "",
        "recommendations_title": heading.get("title") or "Recommended for You",
        "recommendations_hint": heading.get("hint") or "",
    }


def _render_reco_fragment():
    """``/home?part=reco``: just the recommendation section, for the deferred fetch."""
    uid = int(session["user_id"])
    try:
        rows, heading = _recommendations_for_user(uid, **listings_geo_kwargs_from_session(session))
    except Exception:
        # The section is an extra: on any failure answer "nothing to show" and let the
        # page drop its placeholder, never an error page.
        logging.getLogger(__name__).warning("home recommendations failed", exc_info=True)
        rows, heading = [], {}
    if rows:
        resp = make_response(
            render_template("_home_reco_section.html", **_reco_template_vars(rows, heading))
        )
    else:
        resp = make_response("", 204)
    resp.headers["Cache-Control"] = "private, no-store"
    return resp


def _render_personal_home():
    """Logged-in landing: saved cars and history at once; recommendations from the cache.

    The recommendation block is the slow part (a scan over live inventory). When it is
    cached (even stale) it renders inline; when it is not, the page leaves a placeholder
    and the browser asks ``/home?part=reco`` after first paint, so the shell never waits.
    """
    user_id = session.get("user_id")
    saved_cars_list: list = []
    recently_compared: list = []
    recently_viewed: list = []
    reco_vars = _reco_template_vars([], {})
    reco_deferred = False
    if user_id:
        uid = int(user_id)
        lists = _home_lists_for_user(uid)
        saved_cars_list = lists["saved"]
        recently_viewed = lists["viewed"]
        recently_compared = lists["compared"]
        if lists["has_history"]:
            have, rows, heading = _reco_peek_or_refresh(
                uid, 20, listings_geo_kwargs_from_session(session)
            )
            if have:
                reco_vars = _reco_template_vars(rows, heading)
            else:
                reco_deferred = True
    return render_template(
        "home.html",
        saved_cars=saved_cars_list,
        recently_compared=recently_compared,
        recently_viewed=recently_viewed,
        reco_deferred=reco_deferred,
        **reco_vars,
    )


def dashboard():
    """Stats, preferences, and quick actions (personal feed lives on Home)."""
    if not session.get("user_id"):
        return redirect(url_for("login_page"))
    uid = int(session["user_id"])
    geo = listings_geo_kwargs_from_session(session)
    saved_ids = get_saved_car_ids(uid)
    reco_count, _ = _recommendations_for_user(uid, limit=20, serialize=False, **geo)
    return render_template(
        "dashboard.html",
        viewed_count=count_viewed_cars(uid),
        saved_count=len(saved_ids),
        reco_count=reco_count,
        geo_zip=geo.get("zip_code") or "",
        geo_radius=geo.get("radius_miles"),
        browse_trends=_browse_trends_for_user(uid),
    )


def register(app) -> None:
    """Attach routes to ``app`` keeping the original bare endpoint names."""
    app.add_url_rule("/", view_func=home)
    app.add_url_rule("/home", view_func=app_home)
    app.add_url_rule("/dashboard", view_func=dashboard)
