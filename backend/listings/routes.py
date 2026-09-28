from flask import render_template, request, session

from backend.db.inventory_db import (
    get_filter_options,
    get_saved_car_ids,
    hidden_dealer_ids_for_user,
    listings_grid_bootstrap_cars,
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

# Column order for the packed cascade table. Every column except ``cyl`` is
# dictionary-encoded: the values repeat across ~12,700 rows, so shipping integer
# codes plus one vocabulary per column is far smaller than a list of dicts.
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


def drop_hidden_dealer_cars(cars: list[dict], hidden_dealer_ids) -> list[dict]:
    """Serialized grid rows minus the ones from ``hidden_dealer_ids`` (case-insensitive)."""
    hidden = {str(d or "").strip().lower() for d in (hidden_dealer_ids or [])} - {""}
    if not hidden:
        return list(cars)
    return [c for c in cars if str(c.get("dealer_id") or "").strip().lower() not in hidden]


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
    if q_text or has_package_filter:
        parsed_q = parse_natural_query(q_text) if q_text else {}
        if q_text and not query_is_actionable(q_text, parsed_q, sql_kwargs):
            initial_grid_cars = []
            search_ran = True
        else:
            results, _ = hybrid_search_with_kwargs(q_text or None, sql_kwargs, vector_top_k=100)
            initial_grid_cars = serialize_cars_for_listings_grid(results)
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

    options = get_filter_options()

    saved_car_ids: list[int] = []
    uid = session.get("user_id")
    if uid is not None:
        try:
            saved_car_ids = get_saved_car_ids(int(uid))
        except (TypeError, ValueError):
            saved_car_ids = []

    geo_kwargs = listings_geo_kwargs_from_session(session)
    bootstrap_grid_cars: list[dict] = []
    if not q_text and not has_package_filter:
        try:
            bootstrap_grid_cars = listings_grid_bootstrap_cars(
                48,
                zip_code=geo_kwargs.get("zip_code"),
                radius_mi=geo_kwargs.get("radius_miles"),
            )
        except Exception:
            bootstrap_grid_cars = []
        if hidden_dealer_ids and bootstrap_grid_cars:
            bootstrap_grid_cars = drop_hidden_dealer_cars(bootstrap_grid_cars, hidden_dealer_ids)

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
        bootstrap_grid_cars=bootstrap_grid_cars,
        listings_poll_ms=int(listings_poll_ms),
        saved_car_ids=saved_car_ids,
        hidden_dealer_ids=hidden_dealer_ids,
    )
