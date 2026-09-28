"""
IH-01 / ES-2 (visual review 2026-09-28): advertised monthly payments ($85,
$122) scraped into the price field rendered as sale prices on the default
Browse page, and Best match sorted them first because nothing fed the
``payment_listed`` branch of the card.

The listings grid serializer and the API serializer now set ``payment_listed``
from ``is_payment_shaped_price(price, year)``; the sorter sinks those rows with
call-for-price; the card and hero print the figure as a payment.
"""

import re
from pathlib import Path

from backend.utils.car_serialize import serialize as ser
from backend.utils.listings_sort import (
    listing_is_call_for_price,
    listing_is_payment_listed,
    listing_price_value,
    listing_sort_key_by_price,
)
from backend.utils.market_price import is_payment_shaped_price

_ROOT = Path(__file__).resolve().parents[2]


def test_predicate_floors():
    # Under $1,000 on a 2015-or-newer car.
    assert is_payment_shaped_price(122, year=2027)
    assert is_payment_shaped_price(999, year=2015)
    assert not is_payment_shaped_price(1000, year=2015)
    # Under $500 on any car, even with no year.
    assert is_payment_shaped_price(85, year=None)
    assert is_payment_shaped_price(499, year=1998)
    # A cheap old car above the floors is a real price.
    assert not is_payment_shaped_price(999, year=2014)
    assert not is_payment_shaped_price(2500, year=2008)
    # Floors win over a cohort average that would otherwise clear the figure.
    assert is_payment_shaped_price(400, reference_avg=3000, year=2005)
    # Zero / missing is call-for-price, not a payment.
    assert not is_payment_shaped_price(0, year=2027)
    assert not is_payment_shaped_price(None, year=2027)


def _grid(price, year, cid=1):
    return ser.serialize_car_for_listings_grid(
        {
            "id": cid,
            "title": f"{year} Volkswagen Taos S",
            "year": year,
            "make": "Volkswagen",
            "model": "Taos",
            "trim": "S",
            "price": price,
            "mileage": 5,
            "image_url": f"https://cdn.example.com/{cid}.jpg",
            "gallery": "[]",
            "condition": "New",
        }
    )


def test_grid_serializer_flags_payment_and_keeps_figure():
    out = _grid(122, 2027)
    assert out["payment_listed"] is True
    assert out["price"] == 122  # the card prints "$122/mo advertised"
    assert _grid(28443, 2027)["payment_listed"] is False
    assert _grid(None, 2027)["payment_listed"] is False


def test_api_serializer_flags_payment_and_drops_msrp():
    out = ser.serialize_car_for_api(
        {
            "id": 7,
            "title": "2027 Volkswagen Taos S",
            "year": 2027,
            "make": "Volkswagen",
            "model": "Taos",
            "price": 122,
            "msrp": 28443,
            "condition": "New",
            "is_cpo": 0,
        }
    )
    assert out["payment_listed"] is True
    assert out["msrp"] is None
    assert out["below_msrp"] is None


def test_sort_sinks_payment_rows_with_call_for_price():
    def car(cid, price, year):
        return {
            "id": cid,
            "price": price,
            "year": year,
            "gallery": [f"https://cdn.example.com/{cid}-{i}.jpg" for i in range(3)],
            "image_url": f"https://cdn.example.com/{cid}-0.jpg",
            "photo_count": 3,
        }

    cars = [
        car(1, 122, 2027),  # payment-shaped, raw row (no flag)
        car(2, 25000, 2024),
        car(3, None, 2024),  # call for price
        car(4, 19000, 2021),
        dict(car(5, 85, 2019), payment_listed=True),  # flag honoured
    ]
    ordered = [c["id"] for c in sorted(cars, key=listing_sort_key_by_price)]
    assert ordered[:2] == [4, 2]
    assert set(ordered[2:]) == {1, 3, 5}
    assert listing_is_payment_listed(cars[0])
    assert listing_is_call_for_price(cars[0])
    assert listing_price_value(cars[0]) == float("inf")
    # An explicit False flag is trusted over the raw figure.
    assert not listing_is_payment_listed({"price": 122, "year": 2027, "payment_listed": False})


def test_card_and_hero_branch_on_payment_listed():
    js = (_ROOT / "frontend" / "static" / "main.js").read_text(encoding="utf-8")
    assert "c.payment_listed === true" in js
    assert "/mo advertised" in js
    html = (_ROOT / "frontend" / "templates" / "car.html").read_text(encoding="utf-8")
    assert re.search(r"\{% if car\.payment_listed and car\.price", html)
    assert "/mo advertised" in html
    # The finance estimate must not run on an advertised payment.
    assert "car.price > 0 and not car.payment_listed" in html
