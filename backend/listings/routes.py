import logging

from flask import render_template, request, session

from backend.db.inventory_db import (
    get_filter_options,
    get_saved_car_ids,
    hidden_dealer_ids_for_user,
    record_search_history,
    serialize_cars_for_listings_grid,
)
from backend.listings.geo_session import (
    listings_geo_kwargs_from_session,
    persist_listings_geo_from_request,
)
from backend.utils.hybrid_search import (
    flask_request_to_search_cars_kwargs,
    hybrid_search_with_kwargs,
    query_is_actionable,
)
from backend.utils.query_parser import parse_natural_query

_logger = logging.getLogger(__name__)

# Column order for the packed cascade table. Every column except ``cyl`` is
# dictionary-encoded: the values repeat across ~12,700 rows, so shipping integer
# codes plus one vocabulary per column is far smaller than a list of dicts.
# Most cars a free-text search (``?q=``) embeds in the page. Before the cap a search
# with pgvector unconfigured embedded the whole fleet (214k cars, 237 MB, 37 s --
# efficiency review 2026-09-28, D1).
LISTINGS_SEARCH_GRID_MAX = 200

_CAR_ROW_COLUMNS = ("make", "model", "trim", "fuel", "cyl", "drive", "body_style", "induction")
_CAR_ROW_RAW_COLUMNS = frozenset({"cyl"})


def pack_car_rows(car_rows: list[dict]) -> dict:
    """Dictionary-encode the cascade table for the listings HTML payload.

    The uncompressed document is what gates first paint on /listings (it is too
    large for the response compressor), and the repeated JSON keys plus repeated
    make/model/trim strings dominated it. ``main.js`` unpacks this back into the
    same row objects, so the cascade sees exactly what it did before.
    """
    vocabs: list[dict | None] = []
    orders: list[list | None] = []
    for col in _CAR_ROW_COLUMNS:
        if col in _CAR_ROW_RAW_COLUMNS:
            vocabs.append(None)
            orders.append(None)
        else:
            vocabs.append({})
            orders.append([])

    packed_rows: list[list] = []
    for row in car_rows or []:
        out: list = []
        for idx, col in enumerate(_CAR_ROW_COLUMNS):
            val = row.get(col)
            vocab = vocabs[idx]
            if vocab is None:
                out.append(val)
                continue
            if val is None:
                out.append(-1)
                continue
            code = vocab.get(val)
            if code is None:
                order = orders[idx]
                assert order is not None
                code = len(order)
                order.append(val)
                vocab[val] = code
            out.append(code)
        packed_rows.append(out)

    return {"c": list(_CAR_ROW_COLUMNS), "v": orders, "r": packed_rows}


def history_filters_from_active(active: dict) -> dict:
    """The ``active`` filter dict as the cleaned listings query-param object saved
    searches store: empties dropped, ``dealer_registry_ids`` renamed to the
    ``dealer_registry_id`` param the page reads, values scalar or list-of-scalar."""
    from backend.routes.listings_api import _clean_saved_search_filters

    raw: dict = {}
    for key, value in (active or {}).items():
        name = "dealer_registry_id" if key == "dealer_registry_ids" else str(key)
        if isinstance(value, (list, tuple)):
            vals = [str(x).strip() for x in value if str(x or "").strip()]
            if vals:
                raw[name] = vals
        elif value is not None and str(value).strip() != "":
            raw[name] = str(value).strip()
    return _clean_saved_search_filters(raw) or {}


def record_listings_search_history(active: dict, q_text: str, result_count) -> None:
    """Remember this search for the signed-in user (profile -> Recent searches).

    Anonymous visitors are never recorded, an empty filter set is not a search, and
    a failing write is logged and swallowed: it must never fail the page."""
    uid_raw = session.get("user_id")
    if not uid_raw:
        return
    try:
        filters = history_filters_from_active(active)
        if not filters:
            return
        record_search_history(
            int(uid_raw),
            filters,
            query_text=q_text or None,
            result_count=result_count,
        )
    except Exception:
        _logger.warning("search history write failed for user %s", uid_raw, exc_info=True)


# Query parameters that make a /listings URL a search already begun (a deep link,
# a saved search, "Run again"): the page fetches the shopper's area immediately
# instead of waiting for a first interaction. Paging/sorting alone is not a search.
_SEARCH_STARTING_PARAMS = frozenset({
    "make", "model", "trim", "fuel_type", "cylinders", "transmission", "drivetrain",
    "forced_induction", "body_style", "exterior_color", "interior_color", "country",
    "package", "max_price", "max_mileage", "cpo_only", "inventory_condition",
    "engine_l_min", "engine_l_max", "engine_displacement_l_min",
    "engine_displacement_l_max", "zip_code", "radius", "dealership_registry_id",
    "dealer_registry_id", "q", "search",
})


def listings_search_started(args) -> bool:
    """True when the URL already carries a search (any filter, ZIP/radius or query)."""
    for key in _SEARCH_STARTING_PARAMS:
        if any(str(v).strip() for v in args.getlist(key)):
            return True
    return False


def _listings_geo_default(zip_code: str, radius: str, geo_kwargs: dict) -> dict:
    """ZIP + radius to prefill: the URL's, else the remembered (session) area.

    The radius is clamped to the scoped grid's 5..250 mi and defaults to 50."""
    from backend.routes.listings_api import clamp_listings_radius

    z = (zip_code or "").strip() or str(geo_kwargs.get("zip_code") or "").strip()
    raw_r = radius or geo_kwargs.get("radius_miles")
    r = clamp_listings_radius(raw_r) if raw_r not in (None, "") else 50.0
    return {"zip": z, "radius": int(r) if float(r).is_integer() else r}


def listings_page(*, listings_poll_ms: int = 0):
    persist_listings_geo_from_request(request, session)
    g = request.args.getlist

    def scalar(key: str) -> str:
        vals = [v.strip() for v in request.args.getlist(key) if v.strip()]
        return vals[-1] if vals else ""

    zip_code = scalar("zip_code")
    radius = scalar("radius")
    q_text = scalar("q") or scalar("search")

    active = {
        "make": g("make"),
        "model": g("model"),
        "trim": g("trim"),
        "fuel_type": g("fuel_type"),
        "cylinders": g("cylinders"),
        "transmission": g("transmission"),
        "drivetrain": g("drivetrain"),
        "forced_induction": g("forced_induction"),
        "body_style": g("body_style"),
        "exterior_color": g("exterior_color"),
        "interior_color": g("interior_color"),
        "country": g("country"),
        "package": g("package"),
        "max_price": scalar("max_price"),
        "max_mileage": scalar("max_mileage"),
        "cpo_only": scalar("cpo_only"),
        "inventory_condition": scalar("inventory_condition"),
        "engine_l_min": scalar("engine_l_min") or scalar("engine_displacement_l_min"),
        "engine_l_max": scalar("engine_l_max") or scalar("engine_displacement_l_max"),
        "zip_code": zip_code,
        "radius": radius,
        "dealership_registry_id": scalar("dealership_registry_id"),
        "dealer_registry_ids": g("dealer_registry_id"),
        "q": q_text,
    }

    sql_kwargs = flask_request_to_search_cars_kwargs(request)
    # Owner decision 2026-09-28: every search is scoped to the shopper's ZIP + radius.
    # A deep link without one falls back to the remembered area (session).
    geo_kwargs = listings_geo_kwargs_from_session(session)
    if not sql_kwargs.get("zip_code") and geo_kwargs.get("zip_code"):
        sql_kwargs["zip_code"] = geo_kwargs["zip_code"]
        sql_kwargs["radius_miles"] = geo_kwargs.get("radius_miles")
    if sql_kwargs.get("zip_code"):
        from backend.routes.listings_api import clamp_listings_radius

        sql_kwargs["radius_miles"] = clamp_listings_radius(sql_kwargs.get("radius_miles"))
    # Signed-in users' hidden dealerships (profile -> Hidden dealerships). Anonymous
    # visitors get an empty list and every path below stays unchanged for them.
    hidden_dealer_ids: list[str] = []
    try:
        hidden_dealer_ids = sorted(hidden_dealer_ids_for_user(session.get("user_id")))
    except Exception:
        hidden_dealer_ids = []
    if hidden_dealer_ids:
        sql_kwargs["exclude_dealer_ids"] = list(hidden_dealer_ids)
    has_package_filter = bool(
        sql_kwargs.get("packages_json_contains")
        or sql_kwargs.get("packages_json_contains_list")
    )
    initial_grid_cars = []
    search_ran = False
    # No ZIP known (URL or session): there is no area to search, so nothing runs and
    # the page asks for a ZIP (main.js shows the prompt for a started search).
    if (q_text or has_package_filter) and sql_kwargs.get("zip_code"):
        parsed_q = parse_natural_query(q_text) if q_text else {}
        if q_text and not query_is_actionable(q_text, parsed_q, sql_kwargs):
            initial_grid_cars = []
            search_ran = True
        else:
            results, _ = hybrid_search_with_kwargs(
                q_text or None,
                {**sql_kwargs, "limit": LISTINGS_SEARCH_GRID_MAX},
                vector_top_k=100,
                parsed_filters=parsed_q if q_text else None,
            )
            initial_grid_cars = serialize_cars_for_listings_grid(
                results[:LISTINGS_SEARCH_GRID_MAX]
            )
            search_ran = True

    if search_ran:
        try:
            from backend.db.search_analytics_db import analytics_session_key, record_search_event

            uid_raw = session.get("user_id")
            user_id = int(uid_raw) if uid_raw is not None else None
            record_search_event(
                source="listings",
                query_text=q_text or None,
                filters={**active, **{k: v for k, v in sql_kwargs.items() if v}},
                result_count=len(initial_grid_cars),
                user_id=user_id,
                session_key=analytics_session_key(session),
                geo_zip=zip_code or None,
            )
        except Exception:
            pass

    # Per-user history: server-side searches carry their count; filter-only visits
    # (the grid filters client-side) are stored without one.
    record_listings_search_history(
        active, q_text, len(initial_grid_cars) if search_ran else None
    )

    options = get_filter_options()

    saved_car_ids: list[int] = []
    uid = session.get("user_id")
    if uid is not None:
        try:
            saved_car_ids = get_saved_car_ids(int(uid))
        except (TypeError, ValueError):
            saved_car_ids = []

    listings_zip_coords_boot: dict[str, list[float]] = {}
    geo_zip = (zip_code or "").strip()
    if not geo_zip:
        from backend.listings.geo_session import LISTINGS_GEO_ZIP_SESSION_KEY

        geo_zip = str(session.get(LISTINGS_GEO_ZIP_SESSION_KEY) or "").strip()
    if geo_zip:
        from backend.db.geo import zip_to_coords

        coords = zip_to_coords(geo_zip)
        if coords:
            listings_zip_coords_boot[geo_zip] = [float(coords[0]), float(coords[1])]

    return render_template(
        "listings.html",
        options=options,
        car_rows_packed=pack_car_rows(options.get("car_rows") or []),
        listings_zip_coords_boot=listings_zip_coords_boot,
        active=active,
        initial_grid_cars=initial_grid_cars,
        search_started=listings_search_started(request.args),
        listings_geo_default=_listings_geo_default(zip_code, radius, geo_kwargs),
        listings_poll_ms=int(listings_poll_ms),
        saved_car_ids=saved_car_ids,
        hidden_dealer_ids=hidden_dealer_ids,
    )
