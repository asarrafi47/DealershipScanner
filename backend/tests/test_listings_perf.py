"""Listings performance helpers (ETag, batched make/model search).

Client listings page debounces ZIP input (~280ms) and defers geo-coords,
nearby-dealers, and market-stats work until idle so partial ZIP keystrokes
do not replay full-grid radius filters.
"""

import json

from backend.db.inventory_db import listings_grid_cache_etag, search_cars_by_make_model_pairs
from backend.main import app


def test_listings_grid_cache_etag_stable_when_cached():
    etag1 = listings_grid_cache_etag()
    etag2 = listings_grid_cache_etag()
    assert etag1 == etag2
    assert etag1.startswith('W/"')


def test_api_listings_geo_coords():
    with app.test_client() as client:
        r = client.get("/api/listings/geo-coords")
        assert r.status_code == 200
        data = r.get_json()
        assert data["ok"] is True
        assert isinstance(data.get("zip_coords"), dict)
        assert isinstance(data.get("dealer_coords"), dict)
        assert isinstance(data.get("registry_id_by_host"), dict)


def test_filter_options_omit_inline_geo_maps():
    from backend.db.inventory_db import get_filter_options

    opts = get_filter_options()
    assert opts.get("zip_coords") == {}
    assert opts.get("dealer_coords") == {}


def test_api_listings_cars_304_when_etag_matches():
    with app.test_client() as client:
        r1 = client.get("/api/listings/cars")
        assert r1.status_code == 200
        etag = r1.headers.get("ETag")
        assert etag
        assert r1.get_json()["ok"] is True
        assert isinstance(r1.get_json()["cars"], list)
        r2 = client.get("/api/listings/cars", headers={"If-None-Match": etag})
        assert r2.status_code == 304
        assert r2.get_data(as_text=True) == ""


def test_search_cars_by_make_model_pairs_empty():
    assert search_cars_by_make_model_pairs([]) == []


def test_listings_grid_cold_build_under_one_second():
    import time

    from backend.db.inventory_db import clear_inventory_listings_cache, listings_grid_serialized_cars

    clear_inventory_listings_cache()
    t0 = time.perf_counter()
    cars = listings_grid_serialized_cars()
    elapsed = time.perf_counter() - t0
    assert isinstance(cars, list)
    assert elapsed < 1.0, f"cold grid build too slow: {elapsed:.2f}s"


def test_listings_grid_photo_count_matches_full_gallery() -> None:
    from backend.utils.car_serialize import serialize_car_for_listings_grid

    urls = [f"https://cdn.example.com/inventory/{i}.jpg" for i in range(27)]
    car = {
        "id": 99,
        "title": "2022 Toyota Tacoma",
        "make": "Toyota",
        "model": "Tacoma",
        "trim": "SR5",
        "price": 28397,
        "mileage": 77007,
        "fuel_type": "Gasoline",
        "drivetrain": "4WD",
        "body_style": "Truck Double Cab",
        "gallery": json.dumps(urls),
        "image_url": urls[0],
    }
    out = serialize_car_for_listings_grid(car)
    assert len(out["gallery"]) <= 4
    assert out["photo_count"] == 27


def test_listings_grid_serializer_has_filter_fields():
    from backend.utils.car_serialize import serialize_car_for_listings_grid

    car = {
        "id": 1,
        "title": "2024 Test Car",
        "make": "Toyota",
        "model": "Camry",
        "trim": "LE",
        "price": 25000,
        "mileage": 1000,
        "fuel_type": "Gas",
        "cylinders": 4,
        "transmission": "Automatic",
        "drivetrain": "FWD",
        "body_style": "Sedan",
        "exterior_color": "Red",
        "interior_color": "Black",
        "packages": '{"packages_normalized":[{"canonical_name":"Premium Pkg"}]}',
        "gallery": '["https://example.com/a.jpg","https://example.com/b.jpg"]',
    }
    out = serialize_car_for_listings_grid(car)
    assert out["package_names"] == ["Premium Pkg"]
    assert "packages" not in out
    assert len(out["gallery"]) <= 4
    assert out["photo_count"] == 2
    assert out["exterior_color_families"]
    assert out["interior_color_families"]


def test_listings_grid_serializer_includes_dealership_registry_id():
    from backend.utils.car_serialize import serialize_car_for_listings_grid

    out = serialize_car_for_listings_grid(
        {"id": 1, "title": "Test", "price": 1, "dealership_registry_id": 42}
    )
    assert out["dealership_registry_id"] == 42
