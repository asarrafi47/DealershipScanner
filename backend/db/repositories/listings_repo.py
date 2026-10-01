"""Listings grid, facet options, and their module-level caches.

All six listings cache slots live in THIS module together with every function that
reads or resets them; the incomplete-snapshot cache lives in ``data_quality_repo``
and is reset via :func:`clear_incomplete_snapshot_cache`.
"""
import hashlib
import logging
import sqlite3
import threading
import time
from typing import Any

from backend.db.inventory_pg import is_inventory_postgres
from backend.db.repositories.base_repo import db_conn
from backend.db.repositories.cars_repo import _parse_car_gallery
from backend.db.repositories.data_quality_repo import (
    _IncompleteIndexSnapshot,
    _car_id_int,
    _car_is_publicly_incomplete,
    _incomplete_car_ids_for_listings,
    _incomplete_index_snapshot_for_listings,
    _listings_cache_token,
    clear_incomplete_snapshot_cache,
    is_car_incomplete,
    listings_include_incomplete_cars,
)
from backend.db.repositories.dealers_repo import ensure_dealership_registry_backfill
from backend.db.repositories.facets.labels import (  # noqa: F401 - re-exported
    _canonical_facet_label,
    _facet_make_valid,
    _facet_transmission_sane,
    _normalize_facet_key,
    _normalize_make_capitalization,
    country_facets,
    make_model_trim_labels,
)
from backend.db.repositories.facets.queries import (
    car_rows_payload,
    distinct_values,
    package_facets,
    paint_family_facets,
    relationship_rows,
    scalar_facets,
)
from backend.db.repositories.schema_repo import LISTINGS_GRID_CAR_COLUMNS
from backend.utils.car_serialize import (
    serialize_car_for_listings_grid as _serialize_car_for_listings_grid,
)
from backend.utils.field_clean import sort_body_style_presets

_log = logging.getLogger(__name__)


def serialize_car_for_listings_grid(
    car: dict,
    *,
    incomplete_ids: set[int] | None = None,
    incomplete_snapshot: _IncompleteIndexSnapshot | None = None,
    attribution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Lightweight grid JSON for listings (see ``car_serialize.serialize_car_for_listings_grid``).
    """
    out = _serialize_car_for_listings_grid(car, attribution=attribution)
    if listings_include_incomplete_cars():
        if incomplete_snapshot is not None:
            if _car_is_publicly_incomplete(car, incomplete_snapshot):
                out["public_incomplete"] = True
        else:
            ids = incomplete_ids if incomplete_ids is not None else _incomplete_car_ids_for_listings()
            cid = _car_id_int(car)
            if cid > 0 and cid in ids:
                out["public_incomplete"] = True
            elif cid <= 0 and is_car_incomplete(car):
                out["public_incomplete"] = True
    return out


def serialize_cars_for_listings_grid(cars: list[dict]) -> list[dict[str, Any]]:
    """Grid JSON for a batch of cars, with the photo-attribution caveat applied.

    ``serialize_car_for_listings_grid`` only adds the ``location_unconfirmed``
    overlay when the caller passes the verdict in, and every route that forgot
    (saved cars, smart search, the home-page rails) stated a dealership we hold
    photographic evidence against as fact. This is the one batch entry point:
    ONE ``car_attribution_states`` read per response, never one per car.
    """
    from backend.db.repositories.cars_repo import car_attribution_states

    rows = list(cars)
    attribution = car_attribution_states(_car_id_int(c) for c in rows)
    return [
        serialize_car_for_listings_grid(c, attribution=attribution.get(_car_id_int(c)))
        for c in rows
    ]


_facet_options_cache_token: Any = None
_facet_options_cache_value: dict[str, Any] | None = None
_geo_coords_cache_token: tuple[float, float] | None = None
_geo_coords_cache_value: dict[str, Any] | None = None
_grid_cars_cache_token: Any = None
_grid_cars_cache_value: list[dict[str, Any]] | None = None
_grid_cars_cache_built_at: float = 0.0
_LISTINGS_GRID_CACHE_REV = 6

# Tables whose contents the serialized grid is derived from. ``cars`` is the row
# source, ``incomplete_listings`` decides which rows are publicly hidden,
# ``market_price_stats`` backs the per-card deal score, and ``car_attribution`` /
# ``dealer_feed_scope`` decide whether a card may state its dealership as fact — an
# attribution batch changes the cards without touching ``cars``, so leaving those two
# out would serve the old, over-confident copy until something else happened to write.
_GRID_SOURCE_TABLES = (
    "cars",
    "incomplete_listings",
    "market_price_stats",
    "car_attribution",
    "dealer_feed_scope",
)

# Never rebuild more often than this even when inventory is being written
# continuously (a running scan writes ``cars`` without pause). 60s matches the
# rate the old time-bucket token allowed, so this is a ceiling on cost, not a
# new one.
_GRID_MIN_REBUILD_INTERVAL_S = 60.0

# Rebuild at least this often even when no table changed. Two grid fields are
# wall-clock dependent rather than row dependent: ``price_drop_days_ago``
# (day granularity) and ``deal_score``, which reads the deal-score band cache
# (``deal_score_cache._CACHE_TTL_SEC`` = 300s). Keeping the ceiling at that TTL
# means no field gets staler than it already could under the old 60s treadmill.
_GRID_MAX_CACHE_AGE_S = 300.0

# The rebuild is tens of seconds of uninterrupted Python on a cold memo. A
# CPU-bound thread starves the request threads sharing its GIL: a car page
# measured 0.35s with the builder idle and 13.9s with it running. Sleeping
# briefly every _GRID_BUILD_YIELD_ROWS rows hands the GIL back often enough for
# requests to get served, at a few tenths of a second added to the build.
_GRID_BUILD_YIELD_ROWS = 100
_GRID_BUILD_YIELD_S = 0.002


def _inventory_listings_cache_token() -> tuple[float, float]:
    """Alias kept for callers outside this module."""
    return _listings_cache_token()


def _pg_grid_write_fingerprint() -> tuple | None:
    """
    Write counters for :data:`_GRID_SOURCE_TABLES`, or ``None`` when unavailable.

    ``pg_stat_user_tables`` totals move on every committed insert/update/delete,
    so this changes exactly when the data behind the grid changes -- unlike the
    60s time bucket, which changed every minute regardless. Returning ``None``
    (stats disabled, table missing, query failed) makes the caller fall back to
    the time bucket, so the worst case is the behaviour we already had.
    """
    if not is_inventory_postgres():
        return None
    names = ", ".join(f"'{t}'" for t in _GRID_SOURCE_TABLES)
    try:
        with db_conn() as conn:
            rows = conn.execute(
                "SELECT relname, "
                "COALESCE(n_tup_ins,0) + COALESCE(n_tup_upd,0) + COALESCE(n_tup_del,0) "
                f"FROM pg_stat_user_tables WHERE relname IN ({names})"
            ).fetchall()
    except Exception:
        return None
    if not rows:
        return None
    try:
        return tuple(sorted((str(r[0]), int(r[1] or 0)) for r in rows))
    except (TypeError, ValueError, IndexError):
        return None


def _grid_cache_token() -> Any:
    """Cache key for the serialized grid: data fingerprint, else the legacy token."""
    fp = _pg_grid_write_fingerprint()
    if fp is not None:
        return fp
    return _listings_cache_token()


def clear_inventory_listings_cache() -> None:
    """Drop facet/grid caches (tests or admin tools after bulk inventory writes)."""
    global _facet_options_cache_token, _facet_options_cache_value
    global _geo_coords_cache_token, _geo_coords_cache_value
    global _grid_cars_cache_token, _grid_cars_cache_value, _grid_cars_cache_built_at
    _facet_options_cache_token = None
    _facet_options_cache_value = None
    _geo_coords_cache_token = None
    _geo_coords_cache_value = None
    _grid_cars_cache_token = None
    _grid_cars_cache_value = None
    _grid_cars_cache_built_at = 0.0
    global _featured_cars_cache, _public_count_cache
    _featured_cars_cache = None
    _public_count_cache = None
    _clear_grid_serialize_memo()
    clear_incomplete_snapshot_cache()
    from backend.db.repositories.grid_cards_repo import reset_grid_cards_state

    reset_grid_cards_state()
    try:
        from backend.routes.listings_api import clear_cars_scope_cache
    except Exception:  # routes not importable (scripts without Flask app)
        return
    clear_cars_scope_cache()


# ``public_listings_count`` cache: (token, built_at, value). The landing page
# calls it on every anonymous request and the COUNT(*) was 153 ms of a 150 ms page
# (efficiency review 2026-09-28, recommendation 3).
_public_count_cache: tuple[Any, float, int] | None = None
_PUBLIC_COUNT_TTL_S = 300.0
# While a scan writes ``cars`` the write fingerprint moves on every request; the
# display rounds to hundreds, so keep the last count at least this long anyway.
_PUBLIC_COUNT_MIN_RECOUNT_S = 60.0


def _count_public_listings_uncached() -> int:
    active = "(COALESCE(listing_active, 1) = 1) AND COALESCE(marked_for_review, 0) = 0"
    with db_conn() as conn:
        row = conn.execute(f"SELECT COUNT(*) AS n FROM cars WHERE {active}").fetchone()
    try:
        return max(0, int(row[0] if row else 0))
    except (TypeError, ValueError, IndexError):
        return 0


def public_listings_count() -> int:
    """Approximate count of active inventory rows for marketing/stats.

    Cached for :data:`_PUBLIC_COUNT_TTL_S`, keyed by the grid's write fingerprint
    (:func:`_pg_grid_write_fingerprint`, a cheap ``pg_stat_user_tables`` read) so a
    finished scan shows up without waiting out the TTL; off Postgres the key is the
    legacy mtime token. :func:`clear_inventory_listings_cache` drops it.
    """
    global _public_count_cache
    now = time.time()
    fp = _pg_grid_write_fingerprint()
    token: Any = fp if fp is not None else _listings_cache_token()
    hit = _public_count_cache
    if hit is not None:
        age = now - hit[1]
        if hit[0] == token and age < _PUBLIC_COUNT_TTL_S:
            return hit[2]
        if fp is not None and age < _PUBLIC_COUNT_MIN_RECOUNT_S:
            return hit[2]
    value = _count_public_listings_uncached()
    _public_count_cache = (token, now, value)
    return value


def listings_grid_serialized_cars() -> list[dict[str, Any]]:
    """
    Per-car JSON for the WHOLE active fleet -- offline tools and tests only.

    No request path may call this (owner decision 2026-09-28): it holds every active
    listing serialized in the process (the web worker peaked at ~9.5 GB on it).
    ``GET /api/listings/cars`` is radius-scoped and the dealership page dealer-scoped,
    both served from the persisted card store in ``grid_cards_repo``; tests
    monkeypatch this to raise on those routes.
    Honors :func:`listings_include_incomplete_cars` and :func:`serialize_car_for_listings_grid`.

    Rebuild policy (the cache token is :func:`_grid_cache_token`, a fingerprint of
    the writes behind the grid -- not a clock):

    * nothing written and the copy is younger than :data:`_GRID_MAX_CACHE_AGE_S`
      -> serve it, do not rebuild. This is the steady state, and it is what
      stops the whole fleet being re-serialized once a minute forever.
    * something written -> rebuild, but never more often than
      :data:`_GRID_MIN_REBUILD_INTERVAL_S` (a running scan writes ``cars``
      continuously and would otherwise rebuild back to back).
    * copy older than :data:`_GRID_MAX_CACHE_AGE_S` -> rebuild regardless, for
      the two wall-clock-dependent fields noted on that constant.

    Stale-while-revalidate is unchanged: a rebuild runs on a daemon thread and
    the existing copy is served meanwhile; only the true cold start builds inline.
    """
    global _grid_cars_cache_token, _grid_cars_cache_value, _grid_cars_cache_built_at
    if _grid_cars_cache_value is None:
        out = _build_grid_cars_uncached()
        _grid_cars_cache_token = _grid_cache_token()
        _grid_cars_cache_value = out
        _grid_cars_cache_built_at = time.monotonic()
        return out

    age = time.monotonic() - _grid_cars_cache_built_at
    if age >= _GRID_MAX_CACHE_AGE_S:
        _spawn_grid_cars_rebuild(_grid_cache_token())
        return _grid_cars_cache_value
    if age < _GRID_MIN_REBUILD_INTERVAL_S:
        # Too soon to rebuild whatever the data says; skip the fingerprint query.
        return _grid_cars_cache_value
    token = _grid_cache_token()
    if token == _grid_cars_cache_token:
        return _grid_cars_cache_value
    _spawn_grid_cars_rebuild(token)
    return _grid_cars_cache_value


# Serialized-row memo: blake2b digest of the raw DB row (plus its public-incomplete
# flag) -> the serialized dict built from it. A rebuild therefore only pays the
# ~6s of Python serialization for rows whose stored values actually changed;
# untouched rows are reused by reference.
#
# Reuse by reference means a caller that MUTATES a dict from the returned list
# would corrupt later rebuilds. That was already true of the cached list itself
# (``listings_grid_serialized_cars`` hands out the live cache object), so the
# contract is unchanged: treat grid dicts as read-only.
_grid_serialize_memo: dict[tuple, dict[str, Any]] = {}
_grid_serialize_memo_day: int | None = None

# How many attribution verdicts the previous grid build saw. ``car_attribution_states``
# fails open to ``{}`` so a broken read cannot 500 the listings grid -- but a rebuild
# that bakes zero caveats where the last one had thousands is the overlay silently
# vanishing fleet-wide, and it deserves more than a debug line.
_grid_attribution_prev_count: int | None = None


def _clear_grid_serialize_memo() -> None:
    global _grid_serialize_memo, _grid_serialize_memo_day
    _grid_serialize_memo = {}
    _grid_serialize_memo_day = None


def _row_memo_digest(row: dict[str, Any]) -> bytes | None:
    """Content digest of a raw ``cars`` row, or ``None`` if it cannot be hashed."""
    try:
        blob = repr(tuple(row.values())).encode("utf-8", "surrogatepass")
    except Exception:
        return None
    return hashlib.blake2b(blob, digest_size=16).digest()


def _build_grid_cars_uncached() -> list[dict[str, Any]]:
    global _grid_serialize_memo, _grid_serialize_memo_day, _grid_attribution_prev_count
    # Guest grid: admin-flagged rows (marked_for_review) never ship to the public
    # listings payload; admin inventory reads its own query path and still sees them.
    active = "(COALESCE(listing_active, 1) = 1) AND COALESCE(marked_for_review, 0) = 0"
    inc = listings_include_incomplete_cars()
    cols = ", ".join(LISTINGS_GRID_CAR_COLUMNS)
    with db_conn(row_factory=sqlite3.Row) as conn2:
        cur = conn2.cursor()
        cur.execute(
            f"SELECT {cols} FROM cars WHERE {active} ORDER BY price ASC"
        )
        all_cars_raw = [dict(r) for r in cur.fetchall()]
    # Digest the rows BEFORE _parse_car_gallery rewrites ``gallery`` in place, so
    # the key describes exactly what came out of the database.
    digests = [_row_memo_digest(c) for c in all_cars_raw]

    # One query for the whole fleet's photo-attribution verdicts (~2,700 rows), not
    # one per card: this loop runs over every active listing, so a per-car lookup
    # would be tens of thousands of round trips on the hottest read in the app.
    from backend.db.repositories.cars_repo import car_attribution_states

    attribution = car_attribution_states()
    if not attribution and _grid_attribution_prev_count:
        # The overlay fails open to {} rather than 500ing the grid, so this rebuild
        # will bake a cache generation with NO location caveats. Fine on a database
        # that never had verdicts; alarming when the previous build had thousands.
        _log.warning(
            "attribution overlay returned no verdicts but the previous grid build had %d; "
            "this generation ships with no location caveats",
            _grid_attribution_prev_count,
        )
    _grid_attribution_prev_count = len(attribution)

    # The incomplete-index snapshot decides which rows are hidden and is baked
    # into the serialized output, so a rebuild must not reuse a snapshot cached
    # under a token this module no longer follows.
    clear_incomplete_snapshot_cache()
    snapshot = _incomplete_index_snapshot_for_listings()
    per_row = bool(getattr(snapshot, "per_row_fallback", False))
    if per_row:
        # Row-by-row completeness inspects the parsed gallery, so it has to be
        # parsed before the check rather than only on a memo miss.
        for c in all_cars_raw:
            _parse_car_gallery(c)

    day = int(time.time() // 86400)
    prev_memo = _grid_serialize_memo if _grid_serialize_memo_day == day else {}
    fresh_memo: dict[tuple, dict[str, Any]] = {}

    out: list[dict[str, Any]] = []
    for row_i, (c, digest) in enumerate(zip(all_cars_raw, digests)):
        if row_i % _GRID_BUILD_YIELD_ROWS == 0:
            time.sleep(_GRID_BUILD_YIELD_S)
        pub_incomplete = _car_is_publicly_incomplete(c, snapshot)
        if not inc and pub_incomplete:
            continue
        car_attr = attribution.get(_car_id_int(c))
        # The verdict is part of the card but lives outside the ``cars`` row the
        # digest describes, so it has to be in the memo key too — otherwise a card
        # whose columns never changed keeps its stale, over-confident dealer line.
        attr_key = (
            (
                car_attr.get("status"),
                car_attr.get("observed_rooftop"),
                car_attr.get("location_unconfirmed"),
                # group_feed picks which caveat sentence renders, so a
                # dealer_feed_scope flip must invalidate the memo too.
                car_attr.get("group_feed"),
            )
            if car_attr
            else None
        )
        key = (digest, pub_incomplete, attr_key) if digest is not None else None
        ser = prev_memo.get(key) if key is not None else None
        if ser is None:
            if not per_row:
                _parse_car_gallery(c)
            ser = serialize_car_for_listings_grid(
                c, incomplete_snapshot=snapshot, attribution=car_attr
            )
        if key is not None:
            fresh_memo[key] = ser
        out.append(ser)

    # Replace (not update) the memo so rows that left the fleet are dropped.
    _grid_serialize_memo = fresh_memo
    _grid_serialize_memo_day = day

    from backend.utils.listings_sort import listing_sort_key_by_price

    out.sort(key=listing_sort_key_by_price)
    return out


_grid_cars_rebuild_thread: threading.Thread | None = None
_grid_cars_rebuild_lock = threading.Lock()


def _spawn_grid_cars_rebuild(token: Any) -> None:
    global _grid_cars_rebuild_thread
    # check-then-act must be atomic: gunicorn gthreads all see the stale cache
    # at a token rollover and would each spawn a ~10s full-fleet rebuild.
    if not _grid_cars_rebuild_lock.acquire(blocking=False):
        return
    try:
        if _grid_cars_rebuild_thread is not None and _grid_cars_rebuild_thread.is_alive():
            return
        _spawn_grid_cars_rebuild_locked(token)
    finally:
        _grid_cars_rebuild_lock.release()


def _spawn_grid_cars_rebuild_locked(token: Any) -> None:
    global _grid_cars_rebuild_thread

    def _run() -> None:
        global _grid_cars_cache_token, _grid_cars_cache_value, _grid_cars_cache_built_at
        try:
            out = _build_grid_cars_uncached()
        except Exception:
            # Bump the build clock anyway: without it a failing rebuild is retried
            # on every single request instead of at the throttled interval.
            _grid_cars_cache_built_at = time.monotonic()
            return
        _grid_cars_cache_token = token
        _grid_cars_cache_value = out
        _grid_cars_cache_built_at = time.monotonic()

    t = threading.Thread(target=_run, name="grid-cars-refresh", daemon=True)
    _grid_cars_rebuild_thread = t
    t.start()


_featured_cars_cache: tuple[float, list[dict[str, Any]]] | None = None
_FEATURED_CARS_TTL_S = 300.0


def landing_featured_cars(limit: int = 4) -> list[dict[str, Any]]:
    """
    A few photo+price cards for the marketing landing. Deliberately a tiny direct
    query — must never trigger the full grid build (the landing page is the first
    thing a guest sees).
    """
    global _featured_cars_cache
    try:
        lim = max(1, min(int(limit), 12))
    except (TypeError, ValueError):
        lim = 4
    if _featured_cars_cache is not None and time.time() - _featured_cars_cache[0] < _FEATURED_CARS_TTL_S:
        return _featured_cars_cache[1][:lim]

    active = "(COALESCE(listing_active, 1) = 1) AND COALESCE(marked_for_review, 0) = 0"
    cols = ", ".join(LISTINGS_GRID_CAR_COLUMNS)
    with db_conn(row_factory=sqlite3.Row) as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT {cols} FROM cars WHERE {active} "
            "AND COALESCE(price, 0) > 0 AND COALESCE(image_url, '') != '' "
            "ORDER BY id DESC LIMIT 12"
        )
        rows = [dict(r) for r in cur.fetchall()]
    for c in rows:
        _parse_car_gallery(c)
    # Same public-incompleteness rule as every other guest surface — a car the
    # listings grid hides must not headline the landing page.
    inc = listings_include_incomplete_cars()
    snapshot = _incomplete_index_snapshot_for_listings()
    # 12 rows, one batched verdict read — the landing page names a dealership on
    # every card, so it has to be as careful about that claim as the grid is.
    from backend.db.repositories.cars_repo import car_attribution_states

    attribution = car_attribution_states(_car_id_int(c) for c in rows)
    out = [
        _serialize_car_for_listings_grid(c, attribution=attribution.get(_car_id_int(c)))
        for c in rows
        if inc or not _car_is_publicly_incomplete(c, snapshot)
    ]
    # The landing template interpolates image_url into a CSS `url('...')`
    # context, where Jinja's HTML-entity escaping of a quote is decoded back to
    # a real quote by the CSS parser — a scraped dealer image_url containing a
    # quote could break out and inject CSS. Real image URLs never contain
    # quotes/parens/whitespace; drop the image to the placeholder if one does.
    for car in out:
        if not _css_url_safe(car.get("image_url")):
            car["image_url"] = None
        gal = car.get("gallery")
        if isinstance(gal, list):
            car["gallery"] = [g for g in gal if _css_url_safe(g)]
    _featured_cars_cache = (time.time(), out)
    return out[:lim]


def _css_url_safe(url: Any) -> bool:
    """A URL that cannot break out of a CSS ``url('...')`` string context."""
    if not url or not isinstance(url, str):
        return False
    return not any(ch in url for ch in ("'", '"', "(", ")", "\\", " ", "\t", "\n", "\r"))


def listings_geo_coords_maps() -> dict[str, Any]:
    """
    ZIP + dealer coordinate maps for client-side radius filtering.
    Loaded lazily via ``GET /api/listings/geo-coords`` (not embedded in HTML).
    """
    global _geo_coords_cache_token, _geo_coords_cache_value
    token = _listings_cache_token()
    if _geo_coords_cache_value is not None and _geo_coords_cache_token == token:
        return _geo_coords_cache_value

    ensure_dealership_registry_backfill()

    from backend.db.dealer_geo import (
        dealer_coords_client_map,
        load_dealer_geo_index,
        load_registry_coords_map,
    )
    from backend.listings.dealer_registry_match import registry_id_by_dealer_host

    # cars.zip_code was dropped (superseded by dealer_geo-based radius search, which is
    # what the client actually uses); zip_coords is kept in the response for API-contract
    # stability but is always empty now.
    zip_coords: dict[str, list[float]] = {}
    registry_id_by_host: dict[str, int] = {}
    registry_coords: dict[str, list[float]] = {}
    with db_conn() as conn:
        dealer_coords = dealer_coords_client_map(load_dealer_geo_index(conn))
        registry_coords = load_registry_coords_map(conn)
        raw_host_map = registry_id_by_dealer_host(conn)
        registry_id_by_host = {h: rid for h, rid in raw_host_map.items()}
    out = {
        "zip_coords": zip_coords,
        "dealer_coords": dealer_coords,
        "registry_coords": registry_coords,
        "registry_id_by_host": registry_id_by_host,
    }
    _geo_coords_cache_token = token
    _geo_coords_cache_value = out
    return out


_facet_options_rebuild_thread: threading.Thread | None = None
_facet_options_rebuild_lock = threading.Lock()


def _spawn_facet_options_rebuild(token: Any) -> None:
    global _facet_options_rebuild_thread
    # Same check-then-act race as the grid cache: at a token rollover every
    # in-flight request sees the stale copy and would spawn its own rebuild.
    if not _facet_options_rebuild_lock.acquire(blocking=False):
        return
    try:
        if _facet_options_rebuild_thread is not None and _facet_options_rebuild_thread.is_alive():
            return

        def _run() -> None:
            global _facet_options_cache_token, _facet_options_cache_value
            try:
                facets = _build_filter_options_uncached()
            except Exception:
                return
            _facet_options_cache_token = token
            _facet_options_cache_value = facets

        t = threading.Thread(target=_run, name="facet-options-refresh", daemon=True)
        _facet_options_rebuild_thread = t
        t.start()
    finally:
        _facet_options_rebuild_lock.release()


def get_filter_options() -> dict[str, Any]:
    """
    Returns all filter option data with full relationship maps so the
    frontend can do bidirectional cascading across every dimension.

    Facets only -- never cars. Every value is computed by SQL (``DISTINCT`` /
    ``GROUP BY`` in :func:`_build_filter_options_uncached`), not by scanning a
    serialized fleet, and cached by the grid's write fingerprint
    (:func:`_grid_cache_token`) so a finished scan shows up and an idle database
    never rebuilds. The listings page loads its cars from the radius-scoped
    ``GET /api/listings/cars?zip=&radius=`` (owner decision 2026-09-28).

    Stale-while-revalidate: every /listings pageview *and* every lazy-facet fetch
    lands here, so only the true cold start builds inline (~0.9s of DISTINCT scans,
    measured 2026-07-29).
    """
    global _facet_options_cache_token, _facet_options_cache_value
    from backend.db.repositories.grid_cards_repo import grid_scope_token

    token = grid_scope_token()
    if _facet_options_cache_value is None:
        facets = _build_filter_options_uncached()
        _facet_options_cache_token = token
        _facet_options_cache_value = facets
    elif _facet_options_cache_token != token:
        _spawn_facet_options_rebuild(token)

    return dict(_facet_options_cache_value or {})


def _build_filter_options_uncached() -> dict[str, Any]:
    """Every /listings facet, read with SQL (steps in ``backend/db/repositories/facets/``)."""
    with db_conn() as conn:
        cursor = conn.cursor()
        scalars = scalar_facets(cursor)
        exterior_colors, interior_colors = paint_family_facets(cursor)
        body_styles_list = sort_body_style_presets(distinct_values(cursor, "body_style"))
        package_rows, all_package_names = package_facets(cursor)
        car_rows = relationship_rows(cursor)

    seen_makes, seen_models, seen_trims = make_model_trim_labels(car_rows)
    countries, country_to_makes = country_facets(seen_makes)

    facets: dict[str, Any] = {
        "makes":           seen_makes,
        "model_rows":      seen_models,
        "trim_rows":       seen_trims,
        "fuel_types":      scalars["fuel_types"],
        "cylinders":       scalars["cylinders"],
        "transmissions":   scalars["transmissions"],
        "drivetrains":     scalars["drivetrains"],
        "forced_inductions": scalars["forced_inductions"],
        "body_styles":     body_styles_list,
        "exterior_colors": exterior_colors,
        "interior_colors": interior_colors,
        "package_rows":    package_rows,
        "all_package_names": all_package_names,
        "countries":       countries,
        "country_to_makes": country_to_makes,
        # Full relationship table for cascade engine
        "car_rows":        car_rows_payload(car_rows),
        # Geo maps are lazy-loaded via GET /api/listings/geo-coords (keeps HTML fast).
        "zip_coords":      {},
        "dealer_coords":   {},
    }
    return facets
