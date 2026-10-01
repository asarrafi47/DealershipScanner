"""Inventory search query builder (``search_cars`` and friends).

``search_cars`` is a short orchestrator; its steps (WHERE builders per filter
family, Python post-filters, ranking, hydration, ordering) live in
``backend/db/repositories/search/``. The helpers that moved there are re-exported
below under their old names, because ``inventory_db`` and ``listings_repo`` import
them from this module.
"""
import sqlite3
from typing import Any

from backend.db.repositories.base_repo import db_conn
from backend.db.repositories.cars_repo import (
    _parse_car_gallery,
    _parse_car_history_highlights,
)
from backend.db.repositories.data_quality_repo import (
    _filter_public_listings_cars,
    listings_include_incomplete_cars,
)
from backend.db.repositories.search.countries import (  # noqa: F401 - re-exported
    MAKE_TO_COUNTRY,
    _lookup_make_country,
    _makes_for_countries,
)
from backend.db.repositories.search.hydrate import hydrate_in_rank_order
from backend.db.repositories.search.ordering import (  # noqa: F401 - re-exported
    _sort_cars_by_price,
    order_results,
)
from backend.db.repositories.search.post_filter import (  # noqa: F401 - re-exported
    PostSqlFilters,
    _normalized_interior_bucket_filters,
)
from backend.db.repositories.search.query import SearchQuery
from backend.db.repositories.search.ranking import (  # noqa: F401 - re-exported
    _SEARCH_PRICE_ORDER_SQL,
    rank_ids_by_distance,
    rank_ids_by_price,
    resolve_origin,
)
from backend.db.repositories.search.where import (  # noqa: F401 - re-exported
    _EQUIPMENT_SEARCH_COLUMNS,
    _equipment_needle_params,
    _equipment_needle_sql_clause,
    _exclude_dealer_ids_clause,
    _normalize_equipment_needles,
    add_dealer_registry_clause,
    build_where,
    registry_filter_ids,
)

# ``search_cars`` returns at most this many rows unless the caller passes ``limit``
# (``limit=None`` is the explicit opt-out for admin/ops tooling). Before the cap an
# unfiltered call hydrated the whole fleet (214k rows, ~500 MB of dicts) into Python.
SEARCH_CARS_DEFAULT_LIMIT = 500
# Rows hydrated per round trip while collecting ``limit`` post-filtered results.
_SEARCH_HYDRATE_CHUNK = 500
# Ids the non-geo ranking query may hand back: the Python-side filters (paint
# family, interior bucket, displacement, incomplete index) run after the SQL, so
# the id scan over-fetches to still fill ``limit`` when they are selective.
_SEARCH_SCAN_ID_CAP = 20_000


def search_cars(makes=None, models=None, trims=None, fuel_types=None,
                cylinders=None, transmissions=None, drivetrains=None,
                forced_inductions=None,
                body_styles=None,
                exterior_colors=None, interior_colors=None,
                interior_color_bucket_filters=None,
                engine_displacement_l_min=None,
                engine_displacement_l_max=None,
                countries=None,
                min_year=None, max_year=None,
                max_price=None, max_mileage=None,
                cpo_only=None,
                inventory_condition=None,
                zip_code=None, radius_miles=None,
                dealership_registry_id=None,
                dealer_registry_ids=None,
                candidate_ids=None,
                packages_json_contains=None,
                packages_json_contains_list=None,
                packages_json_contains_all=None,
                trim_contains=None,
                trim_contains_list=None,
                vehicle_or=None,
                vin=None,
                include_incomplete: bool | None = None,
                include_flagged: bool = False,
                exclude_dealer_ids=None,
                limit: int | None = SEARCH_CARS_DEFAULT_LIMIT):
    """
    ``exclude_dealer_ids``: optional iterable of ``cars.dealer_id`` keys (case-insensitive) whose
    rows are dropped -- the signed-in user's hidden dealerships (hidden_dealers_repo). One clause
    for both engines: inventory_compat rewrites the ``?`` placeholders for Postgres.

    ``candidate_ids``: optional list of SQLite ``cars.id`` values (e.g. pgvector semantic recall).
    When set, results are restricted to ``id IN (candidate_ids)`` in addition to other filters.

    ``vin``: optional full 17-character VIN (normalized: spaces stripped, case-insensitive). When
    set, only that VIN row is considered (with other filters AND).

    ``packages_json_contains``: optional **literal** substring (case-insensitive) matched against
    the raw ``cars.packages`` TEXT (uses ``INSTR``, not ``LIKE``, so ``%``/``_`` in the needle are
    not SQL wildcards). Hybrid search: ``backend.utils.hybrid_search`` kwargs builder.

    ``packages_json_contains_list``: optional list of substrings; a row matches if **any** needle
    appears in equipment text fields (OR across needles). Sidebar ``package`` checkboxes map here.

    ``packages_json_contains_all``: optional list of substrings; a row must match **every** needle
    (AND), each needle matched in ``packages``, ``description``, ``title``, ``trim``, or
    ``engine_description``. Smart search uses this when the user names multiple features.

    ``max_price`` / ``max_mileage`` when set to ``0`` are applied; they are not treated as
    "unset." ``dealership_registry_id`` must be a positive int; invalid values are ignored.

    ``interior_color_bucket_filters``: optional list of bucket ids (e.g. ``black``, ``tan``);
    a row matches if its ``interior_color_buckets`` JSON array intersects the selection (OR).
    When combined with ``interior_colors``, both constraints apply (AND).

    ``exterior_colors`` / ``interior_colors``: each value is a **paint-family bucket id**
    (e.g. ``red``, ``black``), not the raw dealer string. A row matches if any inferred family
    for that side intersects the selection (OR within one column). Raw ``exterior_color`` /
    ``interior_color`` on the row is unchanged for detail pages.

    ``engine_displacement_l_min`` / ``engine_displacement_l_max``: optional inclusive range in
    liters, matched using ``engine_l`` (numeric) or a leading ``N.NL`` / ``NL`` token in
    ``engine_description``. Rows with no parseable displacement are excluded when either bound
    is set. Combined with ``cylinders`` as AND when both are provided.

    ``include_incomplete``: when False, rows that fail :func:`is_car_incomplete` are omitted
    (public listings). When None, use :func:`listings_include_incomplete_cars`.

    ``include_flagged``: default False — rows an admin flagged for review
    (``cars.marked_for_review = 1``) are suppressed, since every caller of this
    builder is a public/guest surface. Admin/ops tooling that needs to SEE
    flagged rows (that is the whole point of the flag) must opt in with
    ``include_flagged=True``.

    ``limit``: at most this many rows come back (default
    :data:`SEARCH_CARS_DEFAULT_LIMIT`); ``None`` opts out. The database ranks the
    matching ids (cheapest first, or nearest first inside a ZIP radius) and rows are
    hydrated in chunks until the cap is met, so no call pulls the fleet into Python.
    """
    q = SearchQuery.from_call(locals())  # must stay the first statement
    if include_incomplete is None:
        inc = listings_include_incomplete_cars()
    else:
        inc = bool(include_incomplete)

    where = build_where(q)
    post = PostSqlFilters.from_query(q)
    lim = _normalize_search_limit(limit)
    radius_requested, origin = resolve_origin(zip_code, radius_miles)
    if radius_requested and origin is None:
        return []

    with db_conn(row_factory=sqlite3.Row) as conn:
        add_dealer_registry_clause(
            where, conn, registry_filter_ids(dealership_registry_id, dealer_registry_ids)
        )
        distance_by_id = None
        if origin is not None:
            ordered_ids, distance_by_id = rank_ids_by_distance(
                conn, where, origin, radius_miles, open_conn=db_conn
            )
        else:
            ordered_ids = rank_ids_by_price(conn, where, lim, scan_cap=_SEARCH_SCAN_ID_CAP)
        collected = hydrate_in_rank_order(
            conn,
            ordered_ids,
            lim=lim,
            chunk_size=_SEARCH_HYDRATE_CHUNK,
            keep=lambda rows: post.apply(
                _filter_public_listings_cars(rows, include_incomplete=inc)
            ),
            distance_by_id=distance_by_id,
        )
    return order_results(collected, lim, by_distance=origin is not None)


def _normalize_search_limit(limit) -> int | None:
    """``None`` means unbounded; anything that is not a positive int means the default."""
    if limit is None:
        return None
    try:
        n = int(limit)
    except (TypeError, ValueError):
        return SEARCH_CARS_DEFAULT_LIMIT
    return n if n > 0 else SEARCH_CARS_DEFAULT_LIMIT


def search_cars_by_make_model_pairs(
    pairs: list[tuple[str, str]],
    *,
    zip_code: str | None = None,
    radius_miles: float | None = None,
    include_incomplete: bool | None = None,
    include_flagged: bool = False,
    sql_limit: int | None = 120,
    exclude_dealer_ids=None,
) -> list[dict]:
    """Fetch active cars matching any (make, model) pair in one SQL round-trip.

    ``exclude_dealer_ids``: the user's hidden dealerships (see :func:`search_cars`).

    ``include_flagged``: default False — admin-flagged (``marked_for_review``)
    rows are suppressed; admin callers must opt in (same contract as
    :func:`search_cars`).
    """
    if not pairs:
        return []
    if include_incomplete is None:
        inc = listings_include_incomplete_cars()
    else:
        inc = bool(include_incomplete)

    clauses: list[str] = []
    params: list[Any] = []
    seen: set[tuple[str, str]] = set()
    for make, model in pairs[:8]:
        mk = str(make or "").strip().lower()
        mo = str(model or "").strip().lower()
        if not mk or not mo or (mk, mo) in seen:
            continue
        seen.add((mk, mo))
        clauses.append(
            "(LOWER(TRIM(IFNULL(make, ''))) = ? AND LOWER(TRIM(IFNULL(model, ''))) = ?)"
        )
        params.extend([mk, mo])
    if not clauses:
        return []

    from backend.db.geo import haversine, zip_to_coords

    flagged_sql = "" if include_flagged else " AND COALESCE(marked_for_review, 0) = 0"
    query = (
        "SELECT * FROM cars WHERE (COALESCE(listing_active, 1) = 1)"
        f"{flagged_sql}"
        f" AND ({' OR '.join(clauses)})"
    )
    excl_clause, excl_params = _exclude_dealer_ids_clause(exclude_dealer_ids)
    query += excl_clause
    params.extend(excl_params)
    query += " ORDER BY CASE WHEN price IS NULL OR price = 0 THEN 1 ELSE 0 END, price ASC"
    if sql_limit is not None and int(sql_limit) > 0:
        query += " LIMIT ?"
        params.append(int(sql_limit))
    with db_conn(row_factory=sqlite3.Row) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = [dict(row) for row in cursor.fetchall()]

    if zip_code and radius_miles:
        origin = zip_to_coords(zip_code)
        if origin is None:
            return []
        from backend.db.dealer_geo import load_dealer_geo_index, lookup_dealer_coords

        with db_conn() as _gc:
            dealer_geo = load_dealer_geo_index(_gc)
        filtered = []
        for car in results:
            dest = lookup_dealer_coords(str(car.get("dealer_url") or ""), dealer_geo)
            if dest:
                dist = haversine(origin[0], origin[1], dest[0], dest[1])
                if dist <= radius_miles:
                    car["distance_miles"] = round(dist, 1)
                    filtered.append(car)
        for c in filtered:
            _parse_car_gallery(c)
            _parse_car_history_highlights(c)
        base = _filter_public_listings_cars(filtered, include_incomplete=inc)
        from backend.utils.listings_sort import listing_sort_depriority

        return sorted(
            base,
            key=lambda c: (*listing_sort_depriority(c), c.get("distance_miles", 0)),
        )

    for c in results:
        _parse_car_gallery(c)
        _parse_car_history_highlights(c)
    base = _filter_public_listings_cars(results, include_incomplete=inc)
    return _sort_cars_by_price(base)
