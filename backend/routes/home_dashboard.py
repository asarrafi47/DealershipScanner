"""Landing page, signed-in Home feed, Dashboard, and recommendation helpers.

The recommendation cache dict (``_reco_cache``/``_RECO_CACHE_TTL_S``) stays on
``backend.main`` so ``importlib.reload(backend.main)`` resets it exactly as
before the extraction; helpers here access it through the module object.
Inventory/user-history accessors are resolved the same way because tests
monkeypatch them on ``backend.main`` (see ``backend.routes._shared``).
"""

from __future__ import annotations

import time

from flask import redirect, render_template, session, url_for

from backend.routes._shared import main_module


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
    candidates = main_module().search_cars_by_make_model_pairs(
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


def _invalidate_reco_cache(user_id: int) -> None:
    cache = main_module()._reco_cache
    for key in [k for k in cache if k[0] == int(user_id)]:
        cache.pop(key, None)


def _recommendations_for_user(
    user_id: int,
    limit: int = 20,
    *,
    serialize: bool = True,
    **geo_kw: object,
) -> tuple[list[dict] | int, dict[str, str]]:
    """Cached wrapper: serialized rows (or their count) + heading copy."""
    main = main_module()
    key = (int(user_id), int(limit), tuple(sorted((k, str(v)) for k, v in geo_kw.items())))
    now = time.monotonic()
    hit = main._reco_cache.get(key)
    if hit is not None and hit[0] > now:
        rows, heading = hit[1], hit[2]
        return (list(rows) if serialize else len(rows)), dict(heading)
    rows, heading = _recommendations_for_user_uncached(user_id, limit, serialize=True, **geo_kw)
    if len(main._reco_cache) >= 500:
        main._reco_cache.clear()
    main._reco_cache[key] = (now + main._RECO_CACHE_TTL_S, rows, heading)
    return (list(rows) if serialize else len(rows)), dict(heading)


def _recommendations_for_user_uncached(
    user_id: int,
    limit: int = 20,
    *,
    serialize: bool = True,
    **geo_kw: object,
) -> tuple[list[dict] | int, dict[str, str]]:
    """Return serialized carousel rows and heading copy for the dashboard."""
    main = main_module()
    default_heading = {
        "eyebrow": "Based on your history",
        "title": "Recommended for You",
        "hint": "",
    }
    viewed_ids = main.get_recent_viewed_car_ids(user_id, limit=30)
    compared_ids = main.get_recent_compared_car_ids(user_id, limit=30)
    if not viewed_ids and not compared_ids:
        return (0 if not serialize else []), default_heading

    viewed_cars = main.get_cars_by_ids(viewed_ids) if viewed_ids else []
    compared_cars = main.get_cars_by_ids(compared_ids) if compared_ids else []
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

    if seen_mm:
        if geo_active:
            raw_recs = _similar_recommendation_rows(seen_mm, signal_id_set, limit, geo_kw_dict)
            if not raw_recs:
                heading["eyebrow"] = "Outside your search radius"
                heading["title"] = "Recommended & recently viewed"
                raw_recs = _similar_recommendation_rows(seen_mm, signal_id_set, limit, {})
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
            raw_recs = _similar_recommendation_rows(seen_mm, signal_id_set, limit, {})
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
        [main.serialize_car_for_listings_grid(c) for c in out_cars[:limit]],
        heading,
    )


def _recently_compared_for_user(user_id: int, limit: int = 12) -> list[dict]:
    """Serialized listing rows for cars the user recently compared."""
    main = main_module()
    compared_ids = main.get_recent_compared_car_ids(user_id, limit=limit)
    if not compared_ids:
        return []
    raw = main.get_cars_by_ids(compared_ids)
    by_id = {int(c["id"]): c for c in raw if c.get("id") is not None}
    ordered = [by_id[cid] for cid in compared_ids if cid in by_id]
    return [main.serialize_car_for_listings_grid(c) for c in ordered]


def _recently_viewed_for_user(user_id: int, limit: int = 12) -> list[dict]:
    """Serialized listing rows for cars the user recently opened."""
    main = main_module()
    viewed_ids = main.get_recent_viewed_car_ids(user_id, limit=limit)
    if not viewed_ids:
        return []
    raw = main.get_cars_by_ids(viewed_ids)
    by_id = {int(c["id"]): c for c in raw if c.get("id") is not None}
    ordered = [by_id[cid] for cid in viewed_ids if cid in by_id]
    return [main.serialize_car_for_listings_grid(c) for c in ordered]


def _browse_trends_for_user(user_id: int, limit: int = 6) -> list[tuple[str, int]]:
    """Top makes from recent view history for dashboard trends."""
    from collections import Counter

    main = main_module()
    viewed_ids = main.get_recent_viewed_car_ids(user_id, limit=40)
    if not viewed_ids:
        return []
    counts: Counter[str] = Counter()
    for c in main.get_cars_by_ids(viewed_ids):
        make = (c.get("make") or "").strip()
        if make:
            counts[make] += 1
    return counts.most_common(limit)


def _render_personal_home():
    """Logged-in landing: recommendations, saved cars, browsing history."""
    main = main_module()
    user_id = session.get("user_id")
    recommendations = []
    saved_cars_list = []
    recently_compared = []
    recently_viewed = []
    recommendations_eyebrow = ""
    recommendations_title = ""
    recommendations_hint = ""
    if user_id:
        uid = int(user_id)
        recommendations, rec_heading = _recommendations_for_user(
            uid, **main.listings_geo_kwargs_from_session(session)
        )
        recommendations_eyebrow = rec_heading.get("eyebrow") or ""
        recommendations_title = rec_heading.get("title") or "Recommended for You"
        recommendations_hint = rec_heading.get("hint") or ""
        saved_ids = main.get_saved_car_ids(uid)
        raw_saved = main.get_cars_by_ids(saved_ids)
        saved_cars_list = [main.serialize_car_for_listings_grid(c) for c in raw_saved]
        recently_compared = _recently_compared_for_user(uid)
        recently_viewed = _recently_viewed_for_user(uid)
    return render_template(
        "home.html",
        saved_cars=saved_cars_list,
        recommendations=recommendations,
        recently_compared=recently_compared,
        recently_viewed=recently_viewed,
        recommendations_eyebrow=recommendations_eyebrow,
        recommendations_title=recommendations_title,
        recommendations_hint=recommendations_hint,
    )


def dashboard():
    """Stats, preferences, and quick actions (personal feed lives on Home)."""
    main = main_module()
    if not session.get("user_id"):
        return redirect(url_for("login_page"))
    uid = int(session["user_id"])
    geo = main.listings_geo_kwargs_from_session(session)
    saved_ids = main.get_saved_car_ids(uid)
    reco_count, _ = _recommendations_for_user(uid, limit=20, serialize=False, **geo)
    return render_template(
        "dashboard.html",
        viewed_count=main.count_viewed_cars(uid),
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
