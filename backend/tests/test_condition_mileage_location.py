"""
IH-02, DC-6 / IH-03, IH-08 (visual review 2026-09-28).

* Cards state condition (New / Pre-owned / CPO) at the head of the meta line.
* 0 or NULL mileage on a used or older car is the feed's "not listed"
  sentinel and prints "Mileage not listed", never "0 mi", on cards, hero,
  tiles and compare; the JS no longer reads mileage 0 as "new".
* The car hero eyebrow carries the dealer's place and distance; a Condition
  tile replaces the Year tile.
"""

from datetime import date
from pathlib import Path

from backend.listings.dealer_map import hero_location_for_car
from backend.utils.car_serialize import serialize_car_for_api, serialize_car_for_listings_grid
from backend.utils.compare_specs import _fmt_mileage
from backend.utils.mileage_display import mileage_not_listed

_ROOT = Path(__file__).resolve().parents[2]
_TODAY = date(2026, 9, 28)


def test_mileage_not_listed_rule():
    assert mileage_not_listed(0, condition="Used", year=2011, today=_TODAY) is True
    assert mileage_not_listed(None, condition="New", year=2027, today=_TODAY) is True
    assert mileage_not_listed("", condition="New", year=2027, today=_TODAY) is True
    assert mileage_not_listed(0, condition="New", year=2027, today=_TODAY) is False
    assert mileage_not_listed(0, condition="New", year=2025, today=_TODAY) is False
    assert mileage_not_listed(0, condition="New", year=2023, today=_TODAY) is True
    assert mileage_not_listed(0, condition="New", is_cpo=1, year=2026, today=_TODAY) is True
    assert mileage_not_listed(0, condition="", year=2026, today=_TODAY) is True
    assert mileage_not_listed(172_567, condition="Used", year=2011, today=_TODAY) is False


def test_serializers_flag_the_used_rav4():
    rav4 = {"id": 1434577, "title": "2011 Toyota RAV4", "make": "Toyota", "model": "RAV4",
            "year": 2011, "mileage": 0, "condition": "Used", "price": 9995}
    assert serialize_car_for_listings_grid(dict(rav4))["mileage_not_listed"] is True
    assert serialize_car_for_api(dict(rav4), verified_specs={}, include_extended_display=False)["mileage_not_listed"] is True
    new = dict(rav4, year=date.today().year + 1, condition="New", title="New Toyota RAV4")
    assert serialize_car_for_listings_grid(new)["mileage_not_listed"] is False


def test_compare_mileage_cell():
    assert _fmt_mileage({"mileage": 0, "condition": "Used", "year": 2011}) == "Mileage not listed"
    assert _fmt_mileage({"mileage": 0, "mileage_not_listed": False}) == "0 mi"
    assert _fmt_mileage({"mileage": 12000, "condition": "Used", "year": 2020}) == "12,000 mi"
    assert _fmt_mileage({"mileage": None}) == "Mileage not listed"


def test_hero_location_line(monkeypatch):
    monkeypatch.setattr("backend.listings.dealer_map._coords_from_zip", lambda z: (35.2271, -80.8431))
    dm = {"city": "Charlotte", "state": "NC", "zip_code": "28273", "lat": 35.1300, "lon": -80.9500}
    got = hero_location_for_car(dm, "28202")
    assert got["place"] == "Charlotte, NC 28273"
    assert isinstance(got["distance_mi"], int) and 5 <= got["distance_mi"] <= 12
    assert got["from_zip"] == "28202"
    assert hero_location_for_car(dm, None) == {"place": "Charlotte, NC 28273", "distance_mi": None, "from_zip": None}
    assert hero_location_for_car(dict(dm, location_confirmed=False), "28202") is None
    assert hero_location_for_car({"city": "", "state": "NC"}, "28202") is None


def test_card_js_condition_token_and_mileage():
    # The card renderer and sort comparators moved from main.js to static/listings/*.js.
    static = _ROOT / "frontend/static"
    js = "\n".join(
        p.read_text() for p in [static / "main.js", *sorted((static / "listings").glob("*.js"))]
    )
    assert "function cardConditionToken(car)" in js
    assert "cardMileageNotListed(c) ? \"Mileage not listed\"" in js
    assert 'mi === 0) return "new"' not in js
    assert "if (cardMileageNotListed(c)) return Infinity;" in js


def test_car_template_condition_tile_and_eyebrow():
    html = (_ROOT / "frontend/templates/car.html").read_text()
    assert "<dt>Condition</dt>" in html
    assert "<dt>Year</dt>" not in html
    assert "hero_location.place" in html
    assert "Mileage not listed" in html
