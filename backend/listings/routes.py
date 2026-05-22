from flask import render_template, request, session

from backend.db.inventory_db import get_filter_options, serialize_car_for_listings_grid
from backend.listings.geo_session import persist_listings_geo_from_request
from backend.utils.hybrid_search import (
    flask_request_to_search_cars_kwargs,
    hybrid_search_with_kwargs,
)


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
        "max_price": scalar("max_price"),
        "max_mileage": scalar("max_mileage"),
        "engine_l_min": scalar("engine_l_min") or scalar("engine_displacement_l_min"),
        "engine_l_max": scalar("engine_l_max") or scalar("engine_displacement_l_max"),
        "zip_code": zip_code,
        "radius": radius,
        "dealership_registry_id": scalar("dealership_registry_id"),
        "dealer_registry_ids": g("dealer_registry_id"),
        "q": q_text,
    }

    initial_grid_cars = []
    if q_text:
        sql_kwargs = flask_request_to_search_cars_kwargs(request)
        results, _ = hybrid_search_with_kwargs(q_text, sql_kwargs, vector_top_k=100)
        initial_grid_cars = [serialize_car_for_listings_grid(c) for c in results]

    options = get_filter_options()

    return render_template(
        "listings.html",
        options=options,
        active=active,
        initial_grid_cars=initial_grid_cars,
        listings_poll_ms=int(listings_poll_ms),
    )
