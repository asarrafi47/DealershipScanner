from flask import render_template, request, session

from backend.db.inventory_db import (
    get_filter_options,
    get_saved_car_ids,
    listings_grid_bootstrap_cars,
    serialize_car_for_listings_grid,
)
from backend.listings.geo_session import persist_listings_geo_from_request
from backend.utils.hybrid_search import (
    flask_request_to_search_cars_kwargs,
    hybrid_search_with_kwargs,
    query_is_actionable,
)
from backend.utils.query_parser import parse_natural_query


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
        "body_style": g("body_style"),
        "exterior_color": g("exterior_color"),
        "interior_color": g("interior_color"),
        "country": g("country"),
        "package": g("package"),
        "max_price": scalar("max_price"),
        "max_mileage": scalar("max_mileage"),
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
    has_package_filter = bool(
        sql_kwargs.get("packages_json_contains")
        or sql_kwargs.get("packages_json_contains_list")
    )
    initial_grid_cars = []
    if q_text or has_package_filter:
        parsed_q = parse_natural_query(q_text) if q_text else {}
        if q_text and not query_is_actionable(q_text, parsed_q, sql_kwargs):
            initial_grid_cars = []
        else:
            results, _ = hybrid_search_with_kwargs(q_text or None, sql_kwargs, vector_top_k=100)
            initial_grid_cars = [serialize_car_for_listings_grid(c) for c in results]

    options = get_filter_options()

    saved_car_ids: list[int] = []
    uid = session.get("user_id")
    if uid is not None:
        try:
            saved_car_ids = get_saved_car_ids(int(uid))
        except (TypeError, ValueError):
            saved_car_ids = []

    bootstrap_grid_cars: list[dict] = []
    if not q_text and not has_package_filter:
        try:
            bootstrap_grid_cars = listings_grid_bootstrap_cars(48)
        except Exception:
            bootstrap_grid_cars = []

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
        listings_zip_coords_boot=listings_zip_coords_boot,
        active=active,
        initial_grid_cars=initial_grid_cars,
        bootstrap_grid_cars=bootstrap_grid_cars,
        listings_poll_ms=int(listings_poll_ms),
        saved_car_ids=saved_car_ids,
    )
