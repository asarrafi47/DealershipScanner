"""Listings grid, facet options, and their module-level caches.

All six listings cache slots live in THIS module together with every function that
reads or resets them; the incomplete-snapshot cache lives in ``data_quality_repo``
and is reset via :func:`clear_incomplete_snapshot_cache`.
"""
import hashlib
import json
import logging
import re
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
from backend.db.repositories.schema_repo import LISTINGS_GRID_CAR_COLUMNS
from backend.db.repositories.search_repo import _lookup_make_country
from backend.utils.car_serialize import (
    serialize_car_for_listings_grid as _serialize_car_for_listings_grid,
)
from backend.utils.field_clean import (
    coerce_body_style_stored,
    coerce_fuel_type_stored,
    is_effectively_empty,
    sort_body_style_presets,
    sort_fuel_type_presets,
)
from backend.utils.fuel_type_normalize import normalize_fuel_type_for_display
from backend.utils.interior_color_buckets import (
    infer_paint_color_buckets,
    parse_stored_buckets,
    sort_paint_family_ids,
)

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


def listings_grid_bootstrap_cars(limit: int = 48) -> list[dict[str, Any]]:
    """First page of cached grid cars for SSR (instant paint while full fleet loads)."""
    try:
        lim = max(1, min(int(limit), 200))
    except (TypeError, ValueError):
        lim = 48
    cars = listings_grid_serialized_cars()
    return cars[:lim] if cars else []


def _normalize_make_capitalization(make: str) -> str:
    """Normalize make name capitalization: title case for most, handle special cases."""
    if not make:
        return make

    # Special cases: handle multi-word makes and known variations
    special_cases = {
        "land rover": "Land Rover",
        "rolls royce": "Rolls Royce",
        "aston martin": "Aston Martin",
        "mclaren": "McLaren",
        "mclaughlin": "McLaughlin",
        "ram": "RAM",
        "gmc": "GMC",
        "bmw": "BMW",
        "tesla": "Tesla",
        "vw": "Volkswagen",
        "mercedes-benz": "Mercedes-Benz",
        "alfa romeo": "Alfa Romeo",
        "mini": "MINI",
        "infiniti": "INFINITI",
        "lexus": "LEXUS",
    }

    m = str(make).strip()
    m_lower = m.lower()

    # Check special cases
    if m_lower in special_cases:
        return special_cases[m_lower]

    # Default: capitalize first letter, lowercase the rest
    return m[0].upper() + m[1:].lower() if m else m


def _normalize_facet_key(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).lower()


def _canonical_facet_label(value: str, *, variants: list[str]) -> str:
    """Pick one display label for case/spacing variants (``LARIAT`` vs ``Lariat``)."""
    pool = []
    for v in variants:
        s = re.sub(r"\s+", " ", (v or "").strip())
        if s and s not in pool:
            pool.append(s)
    if not pool:
        return (value or "").strip()
    if len(pool) == 1:
        return pool[0]

    def _rank(v: str) -> tuple:
        letters = sum(c.isalpha() for c in v)
        all_caps = letters > 0 and v == v.upper()
        has_lower = any(c.islower() for c in v)
        has_upper = any(c.isupper() for c in v)
        mixed = has_lower and has_upper
        return (mixed, not all_caps, v == v.title(), len(v))

    return max(pool, key=_rank)


def _facet_make_valid(make) -> bool:
    """Reject polluted ``make`` values (numeric trims, model names) for facet lists."""
    if is_effectively_empty(make):
        return False
    m = str(make).strip()
    if m.isdigit():
        return False
    if len(m) <= 4 and re.match(r"^\d{3,4}$", m):
        return False
    return _lookup_make_country(m) is not None


def _facet_transmission_sane(val) -> bool:
    """Drop values that are clearly cylinder counts or garbage, not transmissions."""
    if val is None:
        return False
    s = str(val).strip()
    if not s:
        return False
    if s.isdigit() and len(s) <= 2:
        return False
    if s in frozenset({"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}):
        return False
    # Dict/list reprs leaked from a structured feed field, e.g.
    # "{'label': '10-Speed Automatic', 'type': 'Automatic'}"
    if s[0] in "{[" or ("'label'" in s or "'type'" in s or "'value'" in s):
        return False
    # Truncated/malformed fragments missing their leading gear count, e.g. "-Speed"
    if s.startswith("-"):
        return False
    return True


_facet_options_cache_token: tuple[float, float] | None = None
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
    global _featured_cars_cache
    _featured_cars_cache = None
    _clear_grid_serialize_memo()
    clear_incomplete_snapshot_cache()


def public_listings_count() -> int:
    """Approximate count of active inventory rows for marketing/stats (cheap COUNT)."""
    active = "(COALESCE(listing_active, 1) = 1) AND COALESCE(marked_for_review, 0) = 0"
    with db_conn() as conn:
        row = conn.execute(f"SELECT COUNT(*) AS n FROM cars WHERE {active}").fetchone()
    try:
        return max(0, int(row[0] if row else 0))
    except (TypeError, ValueError, IndexError):
        return 0


def listings_grid_serialized_cars() -> list[dict[str, Any]]:
    """
    Per-car JSON for the listings grid (``options.all_cars`` and ``GET /api/listings/cars``).
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


def listings_grid_cache_etag() -> str:
    """
    Cheap cache validator for ``GET /api/listings/cars`` (If-None-Match / 304).

    Must describe the data the endpoint will actually SERVE: under
    stale-while-revalidate that is the cached copy (tagged with the token it
    was built for), not the current time bucket — otherwise a stale body ships
    under the fresh ETag and clients 304 on it after the rebuild lands.
    """
    if _grid_cars_cache_value is not None:
        token = _grid_cars_cache_token
        n = len(_grid_cars_cache_value)
    else:
        token = _grid_cache_token()
        with db_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM cars WHERE (COALESCE(listing_active, 1) = 1)"
                " AND COALESCE(marked_for_review, 0) = 0"
            ).fetchone()
            n = int(row[0] if row else 0)
    # The token is a structured value (write-counter fingerprint or mtime pair);
    # digest it so the header stays a well-formed quoted-string whatever it holds.
    tag = hashlib.blake2b(repr(token).encode("utf-8", "surrogatepass"), digest_size=8).hexdigest()
    return f'W/"{_LISTINGS_GRID_CACHE_REV}-{tag}-{n}"'


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


def _spawn_facet_options_rebuild(token: tuple[float, float]) -> None:
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


def get_filter_options(*, include_all_cars: bool = False) -> dict[str, Any]:
    """
    Returns all filter option data with full relationship maps so the
    frontend can do bidirectional cascading across every dimension.

    ``include_all_cars`` embeds the full grid payload (~12MB); listings HTML loads
    cars via ``GET /api/listings/cars`` instead (``include_all_cars=False``, default).

    Stale-while-revalidate, exactly like :func:`listings_grid_serialized_cars`.
    Every /listings pageview *and* every lazy-facet fetch lands here, and on
    Postgres the cache token is a 60s time bucket, so rebuilding inline (~0.9s of
    DISTINCT scans over 71k active rows, measured 2026-07-29) stalled one request
    a minute. Only the true cold start builds inline now.
    """
    global _facet_options_cache_token, _facet_options_cache_value
    token = _listings_cache_token()
    if _facet_options_cache_value is None:
        facets = _build_filter_options_uncached()
        _facet_options_cache_token = token
        _facet_options_cache_value = facets
    elif _facet_options_cache_token != token:
        _spawn_facet_options_rebuild(token)

    out = dict(_facet_options_cache_value or {})
    out["all_cars"] = listings_grid_serialized_cars() if include_all_cars else []
    return out


def _build_filter_options_uncached() -> dict[str, Any]:
    active = "(COALESCE(listing_active, 1) = 1)"

    with db_conn() as conn:
        cursor = conn.cursor()

        def distinct(col):
            cursor.execute(
                f"SELECT DISTINCT {col} FROM cars WHERE {active} AND {col} IS NOT NULL ORDER BY {col}"
            )
            return [r[0] for r in cursor.fetchall() if not is_effectively_empty(r[0])]

        def distinct_title_cased(col):
            """Get distinct values, deduplicated with title-case normalization."""
            values = distinct(col)
            seen = {}
            result = []
            for v in values:
                if v:
                    # Normalize to title case, but keep as-is for short acronyms
                    normalized = v if len(v) <= 3 and v.isupper() else v.title()
                    key = normalized.lower()
                    if key not in seen:
                        seen[key] = normalized
                        result.append(normalized)
            return result

        fuel_types      = sort_fuel_type_presets(distinct("fuel_type"))
        cylinders       = distinct("cylinders")
        transmissions   = [t for t in distinct("transmission") if _facet_transmission_sane(t)]
        drivetrains     = distinct("drivetrain")
        forced_inductions = distinct("forced_induction")
        cursor.execute(
            f"""
            SELECT exterior_color, interior_color, interior_color_buckets
            FROM cars
            WHERE {active}
            """
        )
        ext_facet_ids: set[str] = set()
        int_facet_ids: set[str] = set()
        for ext_raw, int_raw, int_bucks in cursor.fetchall():
            if not is_effectively_empty(ext_raw):
                ext_facet_ids.update(infer_paint_color_buckets(ext_raw, None))
            ib = parse_stored_buckets(int_bucks)
            if ib:
                int_facet_ids.update(ib)
            elif not is_effectively_empty(int_raw):
                int_facet_ids.update(infer_paint_color_buckets(int_raw, None))
        exterior_colors = sort_paint_family_ids(ext_facet_ids)
        interior_colors = sort_paint_family_ids(int_facet_ids)
        body_styles_list = sort_body_style_presets(distinct("body_style"))

        # Extract packages per make/model for filter cascade
        package_rows: list[dict[str, str]] = []
        all_package_names: list[str] = []
        _seen_pkg_keys: set[tuple] = set()
        _seen_pkg_names: set[str] = set()
        cursor.execute(
            f"SELECT make, model, packages FROM cars "
            f"WHERE {active} AND packages IS NOT NULL "
            f"AND packages NOT IN ('{{}}', '[]', 'null', '')"
        )
        for _make, _model, _pkg_raw in cursor.fetchall():
            if not _make or not _model:
                continue
            try:
                _p = json.loads(_pkg_raw)
            except Exception:
                continue
            _names: list[str] = []
            for _entry in (_p.get("packages_normalized") or []):
                if isinstance(_entry, dict):
                    _n = (_entry.get("canonical_name") or _entry.get("name") or "").strip()
                    if _n:
                        _names.append(_n)
            for _n in (_p.get("possible_packages") or []):
                if isinstance(_n, str) and _n.strip():
                    _names.append(_n.strip())
            for _n in _names:
                _key = (_make.lower(), _model.lower(), _n.lower())
                if _key not in _seen_pkg_keys:
                    _seen_pkg_keys.add(_key)
                    package_rows.append({"make": _make, "model": _model, "name": _n})
                if _n.lower() not in _seen_pkg_names:
                    _seen_pkg_names.add(_n.lower())
                    all_package_names.append(_n)
        all_package_names.sort()

        # Full relationship rows — every unique combo of all filterable dims.
        # The frontend embeds these as data-* on each checkbox so it can filter
        # any dropdown based on any combination of other active filters.
        # ``year`` / ``engine_description`` / ``engine_l`` are selected but NOT
        # emitted: the mild-hybrid fuel correction below is a per-row rule that
        # needs them, and they are the same engine evidence the card serializer
        # sees (a Ram 1500 whose engine_description is feed junk still matches on
        # engine_l — one such row today). They roughly double the DISTINCT row
        # count (12.8k → 26.6k, +0.02s on 71k active rows) and are deduped back
        # out afterwards, so the payload the frontend embeds is unchanged.
        cursor.execute(f"""
            SELECT DISTINCT make, model, trim, fuel_type, cylinders, drivetrain, body_style,
                   forced_induction, year, engine_description, engine_l
            FROM cars
            WHERE {active}
              AND make IS NOT NULL AND TRIM(make) != ''
            ORDER BY make, model, trim
        """)
        raw_car_rows = cursor.fetchall()
        car_rows: list = []
        _seen_car_rows: set[tuple] = set()
        for row in raw_car_rows:
            make, model, trim, fuel_type, cyl, drive, body_st, induction = (
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
            )
            year, engine_description, engine_l = row[8], row[9], row[10]
            if is_effectively_empty(make) or is_effectively_empty(model):
                continue
            if not _facet_make_valid(make):
                continue
            if is_effectively_empty(trim):
                trim = None
            if is_effectively_empty(fuel_type):
                fuel_type = None
            else:
                fuel_type = coerce_fuel_type_stored(fuel_type)
                # Same rule the card serializer applies, so the cascade never
                # advertises a fuel the grid cannot show: a Ram 1500 eTorque is
                # fed to us as "Hybrid" but renders (and must filter) as gas.
                # The client card filter matches the SERIALIZED value, so an
                # unnormalized facet here yields an empty grid when picked.
                corrected = normalize_fuel_type_for_display(
                    {
                        "make": make,
                        "model": model,
                        "trim": trim,
                        "year": year,
                        "engine_description": engine_description,
                        "engine_l": engine_l,
                    },
                    fuel_type=fuel_type,
                    # No epa_master_id in this DISTINCT projection, so skip the
                    # catalog read rather than firing one per facet row.
                    catalog_fuel_type=None,
                )
                if corrected:
                    fuel_type = corrected
            if is_effectively_empty(drive):
                drive = None
            if is_effectively_empty(body_st):
                body_st = None
            else:
                body_st = coerce_body_style_stored(body_st)
            if is_effectively_empty(induction):
                induction = None
            entry = (make, model, trim, fuel_type, cyl, drive, body_st, induction)
            if entry in _seen_car_rows:
                continue
            _seen_car_rows.add(entry)
            car_rows.append(entry)

    # Derive distinct makes/models/trims with normalized keys (one UI option per logical value).
    make_variants: dict[str, list[str]] = {}
    model_variants: dict[tuple[str, str], list[str]] = {}
    trim_variants: dict[tuple[str, str, str], list[str]] = {}
    for row in car_rows:
        make, model, trim = row[0], row[1], row[2]
        make_normalized = _normalize_make_capitalization(make)
        make_key = _normalize_facet_key(make_normalized)
        make_variants.setdefault(make_key, [])
        if make_normalized not in make_variants[make_key]:
            make_variants[make_key].append(make_normalized)

        model_key = _normalize_facet_key(model)
        mk = (make_key, model_key)
        model_variants.setdefault(mk, [])
        if model not in model_variants[mk]:
            model_variants[mk].append(model)

        if trim is not None and str(trim).strip():
            trim_key = _normalize_facet_key(trim)
            tk = (make_key, model_key, trim_key)
            trim_variants.setdefault(tk, [])
            if trim not in trim_variants[tk]:
                trim_variants[tk].append(trim)

    seen_makes: list[str] = []
    for make_key in sorted(make_variants.keys()):
        seen_makes.append(_canonical_facet_label("", variants=make_variants[make_key]))

    seen_models: list[tuple[str, str]] = []
    for (make_key, model_key) in sorted(model_variants.keys()):
        make_label = _canonical_facet_label("", variants=make_variants[make_key])
        model_label = _canonical_facet_label("", variants=model_variants[(make_key, model_key)])
        seen_models.append((make_label, model_label))

    seen_trims: list[tuple[str, str, str | None]] = []
    for (make_key, model_key, trim_key) in sorted(trim_variants.keys()):
        make_label = _canonical_facet_label("", variants=make_variants[make_key])
        model_label = _canonical_facet_label("", variants=model_variants[(make_key, model_key)])
        trim_label = _canonical_facet_label("", variants=trim_variants[(make_key, model_key, trim_key)])
        seen_trims.append((make_label, model_label, trim_label))

    # Countries that have at least one make in our DB
    country_set = set()
    country_to_makes = {}
    for make in seen_makes:
        # Try normalized make first, fall back to original for legacy compatibility
        c = _lookup_make_country(make) or _lookup_make_country(make.lower())
        if c:
            country_set.add(c)
            country_to_makes.setdefault(c, []).append(make)
    for lst in country_to_makes.values():
        lst.sort()
    countries = sorted(country_set)

    facets: dict[str, Any] = {
        "makes":           seen_makes,
        "model_rows":      seen_models,
        "trim_rows":       seen_trims,
        "fuel_types":      fuel_types,
        "cylinders":       cylinders,
        "transmissions":   transmissions,
        "drivetrains":     drivetrains,
        "forced_inductions": forced_inductions,
        "body_styles":     body_styles_list,
        "exterior_colors": exterior_colors,
        "interior_colors": interior_colors,
        "package_rows":    package_rows,
        "all_package_names": all_package_names,
        "countries":       countries,
        "country_to_makes": country_to_makes,
        # Full relationship table for cascade engine
        "car_rows":        [
            {
                "make": r[0],
                "model": r[1],
                "trim": r[2],
                "fuel": r[3],
                "cyl": r[4],
                "drive": r[5],
                "body_style": r[6] if len(r) > 6 else None,
                "induction": r[7] if len(r) > 7 else None,
            }
            for r in car_rows
        ],
        # Geo maps are lazy-loaded via GET /api/listings/geo-coords (keeps HTML fast).
        "zip_coords":      {},
        "dealer_coords":   {},
    }
    return facets
