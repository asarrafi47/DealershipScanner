"""Listings grid, facet options, and their module-level caches.

All six listings cache slots live in THIS module together with every function that
reads or resets them; the incomplete-snapshot cache lives in ``data_quality_repo``
and is reset via :func:`clear_incomplete_snapshot_cache`.
"""
import json
import re
import sqlite3
import threading
import time
from typing import Any

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
from backend.utils.interior_color_buckets import (
    infer_paint_color_buckets,
    parse_stored_buckets,
    sort_paint_family_ids,
)


def serialize_car_for_listings_grid(
    car: dict,
    *,
    incomplete_ids: set[int] | None = None,
    incomplete_snapshot: _IncompleteIndexSnapshot | None = None,
) -> dict[str, Any]:
    """
    Lightweight grid JSON for listings (see ``car_serialize.serialize_car_for_listings_grid``).
    """
    out = _serialize_car_for_listings_grid(car)
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
    return True


_facet_options_cache_token: tuple[float, float] | None = None
_facet_options_cache_value: dict[str, Any] | None = None
_geo_coords_cache_token: tuple[float, float] | None = None
_geo_coords_cache_value: dict[str, Any] | None = None
_grid_cars_cache_token: tuple[float, float] | None = None
_grid_cars_cache_value: list[dict[str, Any]] | None = None
_LISTINGS_GRID_CACHE_REV = 5


def _inventory_listings_cache_token() -> tuple[float, float]:
    """Alias kept for callers outside this module."""
    return _listings_cache_token()


def clear_inventory_listings_cache() -> None:
    """Drop facet/grid caches (tests or admin tools after bulk inventory writes)."""
    global _facet_options_cache_token, _facet_options_cache_value
    global _geo_coords_cache_token, _geo_coords_cache_value
    global _grid_cars_cache_token, _grid_cars_cache_value
    _facet_options_cache_token = None
    _facet_options_cache_value = None
    _geo_coords_cache_token = None
    _geo_coords_cache_value = None
    _grid_cars_cache_token = None
    _grid_cars_cache_value = None
    global _featured_cars_cache
    _featured_cars_cache = None
    clear_incomplete_snapshot_cache()


def public_listings_count() -> int:
    """Approximate count of active inventory rows for marketing/stats (cheap COUNT)."""
    active = "(COALESCE(listing_active, 1) = 1)"
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

    Stale-while-revalidate: on Postgres the cache token is a 60s time bucket, so
    a synchronous rebuild (~10s on a large fleet) would stall one request every
    minute. When a stale copy exists it is served immediately and the rebuild
    runs on a daemon thread; only the true cold start builds inline.
    """
    global _grid_cars_cache_token, _grid_cars_cache_value
    token = _listings_cache_token()
    if _grid_cars_cache_value is not None and _grid_cars_cache_token == token:
        return _grid_cars_cache_value
    if _grid_cars_cache_value is not None:
        _spawn_grid_cars_rebuild(token)
        return _grid_cars_cache_value

    out = _build_grid_cars_uncached()
    _grid_cars_cache_token = token
    _grid_cars_cache_value = out
    return out


def _build_grid_cars_uncached() -> list[dict[str, Any]]:
    active = "(COALESCE(listing_active, 1) = 1)"
    inc = listings_include_incomplete_cars()
    cols = ", ".join(LISTINGS_GRID_CAR_COLUMNS)
    with db_conn(row_factory=sqlite3.Row) as conn2:
        cur = conn2.cursor()
        cur.execute(
            f"SELECT {cols} FROM cars WHERE {active} ORDER BY price ASC"
        )
        all_cars_raw = [dict(r) for r in cur.fetchall()]
    snapshot = _incomplete_index_snapshot_for_listings()
    for c in all_cars_raw:
        _parse_car_gallery(c)
    out: list[dict[str, Any]] = []
    for c in all_cars_raw:
        if not inc and _car_is_publicly_incomplete(c, snapshot):
            continue
        out.append(serialize_car_for_listings_grid(c, incomplete_snapshot=snapshot))
    from backend.utils.listings_sort import listing_sort_key_by_price

    out.sort(key=listing_sort_key_by_price)
    return out


_grid_cars_rebuild_thread: threading.Thread | None = None


def _spawn_grid_cars_rebuild(token: tuple[float, float]) -> None:
    global _grid_cars_rebuild_thread
    if _grid_cars_rebuild_thread is not None and _grid_cars_rebuild_thread.is_alive():
        return

    def _run() -> None:
        global _grid_cars_cache_token, _grid_cars_cache_value
        try:
            out = _build_grid_cars_uncached()
        except Exception:
            return
        _grid_cars_cache_token = token
        _grid_cars_cache_value = out

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

    active = "(COALESCE(listing_active, 1) = 1)"
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
    out = [_serialize_car_for_listings_grid(c) for c in rows]
    _featured_cars_cache = (time.time(), out)
    return out[:lim]


def listings_grid_cache_etag() -> str:
    """Cheap cache validator for ``GET /api/listings/cars`` (If-None-Match / 304)."""
    token = _listings_cache_token()
    if _grid_cars_cache_value is not None and _grid_cars_cache_token == token:
        n = len(_grid_cars_cache_value)
    else:
        with db_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM cars WHERE (COALESCE(listing_active, 1) = 1)"
            ).fetchone()
            n = int(row[0] if row else 0)
    return f'W/"{_LISTINGS_GRID_CACHE_REV}-{token}-{n}"'


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


def get_filter_options(*, include_all_cars: bool = False) -> dict[str, Any]:
    """
    Returns all filter option data with full relationship maps so the
    frontend can do bidirectional cascading across every dimension.

    ``include_all_cars`` embeds the full grid payload (~12MB); listings HTML loads
    cars via ``GET /api/listings/cars`` instead (``include_all_cars=False``, default).
    Facet metadata is cached until inventory.db changes.
    """
    global _facet_options_cache_token, _facet_options_cache_value
    token = _listings_cache_token()
    if _facet_options_cache_value is not None and _facet_options_cache_token == token:
        out = dict(_facet_options_cache_value)
        out["all_cars"] = listings_grid_serialized_cars() if include_all_cars else []
        return out

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
        cursor.execute(f"""
            SELECT DISTINCT make, model, trim, fuel_type, cylinders, drivetrain, body_style, forced_induction
            FROM cars
            WHERE {active}
              AND make IS NOT NULL AND TRIM(make) != ''
            ORDER BY make, model, trim
        """)
        raw_car_rows = cursor.fetchall()
        car_rows: list = []
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
            if is_effectively_empty(drive):
                drive = None
            if is_effectively_empty(body_st):
                body_st = None
            else:
                body_st = coerce_body_style_stored(body_st)
            if is_effectively_empty(induction):
                induction = None
            car_rows.append((make, model, trim, fuel_type, cyl, drive, body_st, induction))

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
    _facet_options_cache_token = token
    _facet_options_cache_value = dict(facets)
    out = dict(facets)
    out["all_cars"] = listings_grid_serialized_cars() if include_all_cars else []
    return out
