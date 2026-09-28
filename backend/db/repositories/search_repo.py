"""Inventory search query builder (``search_cars`` and friends)."""
import re
import sqlite3
from typing import Any

from backend.db.repositories.base_repo import _placeholders, db_conn
from backend.db.repositories.cars_repo import (
    _parse_car_gallery,
    _parse_car_history_highlights,
)
from backend.db.repositories.data_quality_repo import (
    _filter_public_listings_cars,
    listings_include_incomplete_cars,
)
from backend.utils.car_serialize import car_matches_engine_displacement_l_range
from backend.utils.interior_color_buckets import (
    infer_paint_color_buckets,
    parse_stored_buckets,
    row_matches_interior_bucket_filter,
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
# Same order ``_sort_cars_by_price`` applies at the end: priced rows cheapest
# first, "call for price" rows last; ``id`` makes the LIMIT deterministic.
_SEARCH_PRICE_ORDER_SQL = (
    "ORDER BY CASE WHEN price IS NULL OR price = 0 THEN 1 ELSE 0 END, price ASC, id ASC"
)


# Major automakers by country of origin (for country filter)
MAKE_TO_COUNTRY = {
    "BMW": "Germany", "Mercedes-Benz": "Germany", "Audi": "Germany",
    "Porsche": "Germany", "Volkswagen": "Germany", "VW": "Germany",
    "Toyota": "Japan", "Honda": "Japan", "Nissan": "Japan", "Lexus": "Japan",
    "Mazda": "Japan", "Subaru": "Japan", "Mitsubishi": "Japan",
    "Acura": "Japan", "Infiniti": "Japan",
    "Ford": "USA", "Chevrolet": "USA", "GM": "USA", "Ram": "USA",
    "Tesla": "USA", "Jeep": "USA", "Dodge": "USA", "Cadillac": "USA",
    "Buick": "USA", "GMC": "USA", "Chrysler": "USA", "Lincoln": "USA",
    "Hyundai": "South Korea", "Kia": "South Korea", "Genesis": "South Korea",
    "Jaguar": "UK", "Land Rover": "UK", "Bentley": "UK", "Mini": "UK",
    "Ferrari": "Italy", "Lamborghini": "Italy", "Fiat": "Italy", "Maserati": "Italy",
    "Renault": "France", "Peugeot": "France", "Citroën": "France",
    "Volvo": "Sweden", "Alfa Romeo": "Italy",
}


def _makes_for_countries(countries):
    """Return set of makes whose country of origin is in the given list."""
    if not countries:
        return None
    countries_set = set(c.strip() for c in countries if c and c.strip())
    return {make for make, country in MAKE_TO_COUNTRY.items() if country in countries_set}


def _lookup_make_country(make: str):
    """Resolve country for a DB make string; case-insensitive vs MAKE_TO_COUNTRY keys."""
    if make is None:
        return None
    m = str(make).strip()
    if not m:
        return None
    if m in MAKE_TO_COUNTRY:
        return MAKE_TO_COUNTRY[m]
    ml = m.lower()
    for k, v in MAKE_TO_COUNTRY.items():
        if k.lower() == ml:
            return v
    return None


def _sort_cars_by_price(cars: list) -> list:
    """Priced + multi-photo first; call-for-price and single-photo listings sink."""
    from backend.utils.listings_sort import listing_sort_key_by_price

    return sorted(cars, key=listing_sort_key_by_price)


def _normalized_interior_bucket_filters(raw) -> set[str] | None:
    if not raw:
        return None
    from backend.utils.interior_color_buckets import ALLOWED_BUCKETS

    sel = {str(x).strip().lower() for x in raw if str(x).strip()}
    sel &= ALLOWED_BUCKETS
    return sel or None


# Substrings for equipment smart-search (packages JSON, listing text, trim/title).
_EQUIPMENT_SEARCH_COLUMNS = (
    "packages",
    "description",
    "title",
    "trim",
    "engine_description",
)


def _normalize_equipment_needles(
    single: str | None,
    needles_list: list | None,
    needles_all: list | None,
) -> tuple[str | None, list[str], list[str]]:
    """Dedupe and cap equipment needles; return (single, or_list, and_list)."""
    or_needles: list[str] = []
    and_needles: list[str] = []
    seen: set[str] = set()

    def add(target: list[str], raw: str) -> None:
        needle = (raw or "").strip().lower()
        if not needle or needle in seen:
            return
        if len(needle) > 200:
            needle = needle[:200]
        seen.add(needle)
        target.append(needle)

    if needles_all:
        for raw in needles_all:
            add(and_needles, str(raw or ""))
    if single and str(single).strip():
        add(or_needles, str(single))
    if needles_list:
        for raw in needles_list:
            add(or_needles, str(raw or ""))
    if len(or_needles) == 1 and not and_needles:
        return or_needles[0], [], []
    if len(or_needles) > 1 and not and_needles:
        return None, or_needles, []
    if and_needles:
        return None, [], and_needles
    return None, or_needles, []


def _equipment_needle_sql_clause() -> str:
    parts = [
        f"INSTR(LOWER(IFNULL({col}, '')), ?) > 0" for col in _EQUIPMENT_SEARCH_COLUMNS
    ]
    return "(" + " OR ".join(parts) + ")"


def _equipment_needle_params(needle: str) -> list[str]:
    """One binding per column in :func:`_equipment_needle_sql_clause`."""
    return [needle] * len(_EQUIPMENT_SEARCH_COLUMNS)


def _exclude_dealer_ids_clause(exclude_dealer_ids) -> tuple[str, list[str]]:
    """``AND dealer_id NOT IN (?, ...)`` for a user's hidden dealerships ('' when none).

    ``cars.dealer_id`` is the lower-case host slug (0 mixed-case ids across 466
    dealers, 2026-09-28) and the keys are lower-cased here, so the comparison is
    exact and ``idx_cars_dealer_listing`` stays usable; ``LOWER(dealer_id)``
    forced a sequential scan on every search for a user with hidden dealers.
    """
    if not exclude_dealer_ids:
        return "", []
    keys = sorted({str(d or "").strip().lower() for d in exclude_dealer_ids} - {""})
    if not keys:
        return "", []
    return (
        f" AND (dealer_id IS NULL OR dealer_id NOT IN ({_placeholders(keys)}))",
        keys,
    )


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
    if include_incomplete is None:
        inc = listings_include_incomplete_cars()
    else:
        inc = bool(include_incomplete)

    from backend.db.geo import zip_to_coords, haversine

    where = "(COALESCE(listing_active, 1) = 1)"
    if not include_flagged:
        # Admin "marked for review" flag hides the row from public search results.
        where += " AND COALESCE(marked_for_review, 0) = 0"
    params = []

    if candidate_ids:
        ids = []
        for x in candidate_ids:
            try:
                i = int(x)
                if i > 0:
                    ids.append(i)
            except (TypeError, ValueError):
                continue
        if ids:
            where += f" AND id IN ({_placeholders(ids)})"
            params.extend(ids)

    dealer_registry_filter_ids: list[int] = []
    if dealership_registry_id is not None:
        try:
            dr = int(dealership_registry_id)
        except (TypeError, ValueError):
            dr = 0
        if dr > 0:
            dealer_registry_filter_ids = [dr]

    if dealer_registry_ids:
        for x in dealer_registry_ids:
            try:
                i = int(x)
                if i > 0 and i not in dealer_registry_filter_ids:
                    dealer_registry_filter_ids.append(i)
            except (TypeError, ValueError):
                continue

    if vin and str(vin).strip():
        vnorm = re.sub(r"\s+", "", str(vin).strip().upper())[:20]
        if len(vnorm) == 17:
            where += " AND REPLACE(UPPER(TRIM(IFNULL(vin, ''))), ' ', '') = ?"
            params.append(vnorm)

    def add_multi(col, values):
        nonlocal where
        if values:
            where += f" AND {col} IN ({_placeholders(values)})"
            params.extend(values)

    def add_multi_ci(col, values):
        """Case-insensitive match for scraped text fields (e.g. DODGE vs Dodge)."""
        nonlocal where
        if values:
            lowered = [str(v).lower().strip() for v in values]
            where += (
                f" AND LOWER(TRIM(IFNULL({col}, ''))) IN ({_placeholders(lowered)})"
            )
            params.extend(lowered)

    # Country of origin filter: resolve countries to makes, combine with explicit makes
    makes_for_countries = _makes_for_countries(countries)
    if makes_for_countries is not None:
        if makes:
            allow_lower = {k.lower(): k for k in makes_for_countries}
            normalized = []
            for m in makes:
                hit = allow_lower.get(str(m).lower().strip())
                if hit is not None:
                    normalized.append(hit)
            makes = list(dict.fromkeys(normalized))
        else:
            makes = list(makes_for_countries)

    vehicle_or_clauses: list[str] = []
    if vehicle_or and isinstance(vehicle_or, list) and len(vehicle_or) >= 2:
        for branch in vehicle_or[:8]:
            if not isinstance(branch, dict):
                continue
            sub_parts: list[str] = []
            mk = branch.get("make")
            if mk and str(mk).strip():
                sub_parts.append("LOWER(TRIM(IFNULL(make, ''))) = ?")
                params.append(str(mk).lower().strip())
            branch_models = branch.get("models") or branch.get("model")
            if branch_models:
                if isinstance(branch_models, str):
                    branch_models = [branch_models]
                lowered_models = [str(v).lower().strip() for v in branch_models if str(v).strip()]
                if lowered_models:
                    sub_parts.append(
                        f"LOWER(TRIM(IFNULL(model, ''))) IN ({_placeholders(lowered_models)})"
                    )
                    params.extend(lowered_models)
            branch_trim = branch.get("trim_contains")
            if branch_trim and str(branch_trim).strip():
                sub_parts.append("INSTR(LOWER(IFNULL(trim, '')), ?) > 0")
                params.append(str(branch_trim).strip().lower()[:100])
            if sub_parts:
                vehicle_or_clauses.append("(" + " AND ".join(sub_parts) + ")")
        if vehicle_or_clauses:
            where += " AND (" + " OR ".join(vehicle_or_clauses) + ")"
    else:
        add_multi_ci("make", makes)
        add_multi_ci("model", models)
    add_multi_ci("trim", trims)
    add_multi("fuel_type", fuel_types)
    add_multi("cylinders", [int(c) for c in cylinders] if cylinders else None)
    add_multi("transmission", transmissions)
    add_multi("forced_induction", forced_inductions)
    add_multi("drivetrain", drivetrains)
    add_multi_ci("body_style", body_styles)

    pkg_single, pkg_or, pkg_and = _normalize_equipment_needles(
        packages_json_contains,
        packages_json_contains_list,
        packages_json_contains_all,
    )
    clause = _equipment_needle_sql_clause()
    if pkg_single:
        where += f" AND {clause}"
        params.extend(_equipment_needle_params(pkg_single))
    if pkg_or:
        where += " AND (" + " OR ".join([clause] * len(pkg_or)) + ")"
        for needle in pkg_or:
            params.extend(_equipment_needle_params(needle))
    if pkg_and:
        for needle in pkg_and:
            where += f" AND {clause}"
            params.extend(_equipment_needle_params(needle))

    if trim_contains_list:
        needles = [str(t).strip().lower()[:100] for t in trim_contains_list if str(t).strip()]
        if needles:
            where += " AND (" + " OR ".join(
                ["INSTR(LOWER(IFNULL(trim, '')), ?) > 0"] * len(needles)
            ) + ")"
            params.extend(needles)
    elif trim_contains and str(trim_contains).strip():
        needle = str(trim_contains).strip().lower()
        if len(needle) > 100:
            needle = needle[:100]
        where += " AND INSTR(LOWER(IFNULL(trim, '')), ?) > 0"
        params.append(needle)

    if min_year is not None:
        where += " AND year >= ?"
        params.append(int(min_year))
    if max_year is not None:
        where += " AND year <= ?"
        params.append(int(max_year))

    if max_price is not None:
        try:
            mp = float(max_price)
        except (TypeError, ValueError):
            pass
        else:
            where += " AND (price IS NULL OR price <= ? OR price = 0)"
            params.append(mp)
    if max_mileage is not None:
        try:
            mm = int(float(max_mileage))
        except (TypeError, ValueError):
            pass
        else:
            where += " AND (mileage IS NULL OR mileage <= ? OR mileage = 0)"
            params.append(mm)
    if cpo_only:
        where += " AND is_cpo = 1"
    excl_clause, excl_params = _exclude_dealer_ids_clause(exclude_dealer_ids)
    where += excl_clause
    params.extend(excl_params)

    bucket_sel = _normalized_interior_bucket_filters(interior_color_bucket_filters)
    ext_family_sel = _normalized_interior_bucket_filters(exterior_colors)
    int_family_sel = _normalized_interior_bucket_filters(interior_colors)

    def _row_exterior_families(car: dict) -> set[str]:
        return set(infer_paint_color_buckets(car.get("exterior_color"), car.get("make")))

    def _row_interior_families(car: dict) -> set[str]:
        stored = set(parse_stored_buckets(car.get("interior_color_buckets")))
        if stored:
            return stored
        return set(infer_paint_color_buckets(car.get("interior_color"), car.get("make")))

    eng_lo = eng_hi = None
    if engine_displacement_l_min is not None:
        try:
            eng_lo = float(engine_displacement_l_min)
        except (TypeError, ValueError):
            eng_lo = None
    if engine_displacement_l_max is not None:
        try:
            eng_hi = float(engine_displacement_l_max)
        except (TypeError, ValueError):
            eng_hi = None

    def _post_sql_filters(cars: list[dict]) -> list[dict]:
        out = cars
        if ext_family_sel:
            out = [c for c in out if _row_exterior_families(c) & ext_family_sel]
        if int_family_sel:
            out = [c for c in out if _row_interior_families(c) & int_family_sel]
        if bucket_sel:
            out = [c for c in out if row_matches_interior_bucket_filter(c, bucket_sel)]
        if eng_lo is not None or eng_hi is not None:
            out = [c for c in out if car_matches_engine_displacement_l_range(c, eng_lo, eng_hi)]
        return out

    lim = _normalize_search_limit(limit)
    origin = None
    if zip_code and radius_miles:
        origin = zip_to_coords(zip_code)
        if origin is None:
            return []

    with db_conn(row_factory=sqlite3.Row) as conn:
        if dealer_registry_filter_ids:
            from backend.listings.dealer_registry_match import (
                dealer_registry_sql_filter,
                registry_id_by_dealer_host,
            )

            host_map = registry_id_by_dealer_host(conn)
            clause, extra = dealer_registry_sql_filter(
                dealer_registry_filter_ids,
                host_map,
                placeholders_fn=_placeholders,
            )
            where += clause
            params.extend(extra)

        distance_by_id: dict[int, float] = {}
        if origin is not None:
            # Phase 1 (radius): the distance test needs every candidate's dealer,
            # not its row -- scan three narrow columns, rank nearest first.
            from backend.db.dealer_geo import load_dealer_geo_index, lookup_dealer_coords

            # Plain connection: the index reader unpacks rows positionally, which
            # the name-keyed rows of this ``row_factory`` connection do not allow.
            with db_conn() as _gc:
                dealer_geo = load_dealer_geo_index(_gc)
            narrow = conn.execute(
                f"SELECT id, price, dealer_url FROM cars WHERE {where}", params
            ).fetchall()
            dist_by_url: dict[str, float] = {}
            ranked: list[tuple[float, int, float, int]] = []
            for r in narrow:
                r = dict(r)  # Postgres compat rows are name-keyed, sqlite3.Row takes both
                du = str(r["dealer_url"] or "")
                dist = dist_by_url.get(du)
                if dist is None:
                    dest = lookup_dealer_coords(du, dealer_geo)
                    dist = (
                        haversine(origin[0], origin[1], dest[0], dest[1]) if dest else -1.0
                    )
                    dist_by_url[du] = dist
                if dist < 0 or dist > float(radius_miles):
                    continue
                try:
                    price = float(r["price"] or 0)
                except (TypeError, ValueError):
                    price = 0.0
                cid = int(r["id"])
                ranked.append((round(dist, 1), 1 if price <= 0 else 0, price, cid))
            ranked.sort()
            ordered_ids = [t[3] for t in ranked]
            distance_by_id = {t[3]: t[0] for t in ranked}
        else:
            # Phase 1: let the database rank the matching ids; only ints cross the wire.
            scan_sql = f"SELECT id FROM cars WHERE {where} {_SEARCH_PRICE_ORDER_SQL}"
            scan_params = list(params)
            if lim is not None:
                scan_sql += " LIMIT ?"
                scan_params.append(max(lim, _SEARCH_SCAN_ID_CAP))
            ordered_ids = [
                int(dict(r)["id"]) for r in conn.execute(scan_sql, scan_params).fetchall()
            ]

        # Phase 2: hydrate in chunks, in rank order, until ``limit`` rows survive
        # the Python-side filters.
        collected: list[dict] = []
        cursor = conn.cursor()
        for start in range(0, len(ordered_ids), _SEARCH_HYDRATE_CHUNK):
            chunk = ordered_ids[start : start + _SEARCH_HYDRATE_CHUNK]
            cursor.execute(
                f"SELECT * FROM cars WHERE id IN ({_placeholders(chunk)})", chunk
            )
            by_id: dict[int, dict] = {}
            for row in cursor.fetchall():
                d = dict(row)
                by_id[int(d["id"])] = d
            rows = [by_id[i] for i in chunk if i in by_id]
            for c in rows:
                _parse_car_gallery(c)
                _parse_car_history_highlights(c)
            rows = _post_sql_filters(_filter_public_listings_cars(rows, include_incomplete=inc))
            if origin is not None:
                for c in rows:
                    c["distance_miles"] = distance_by_id.get(int(c["id"]), 0.0)
            collected.extend(rows)
            if lim is not None and len(collected) >= lim:
                break

    if lim is not None:
        collected = collected[:lim]
    if origin is not None:
        from backend.utils.listings_sort import listing_sort_depriority

        return sorted(
            collected,
            key=lambda c: (*listing_sort_depriority(c), c["distance_miles"]),
        )
    return _sort_cars_by_price(collected)


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
