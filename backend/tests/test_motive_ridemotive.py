"""Motive (ridemotive) Algolia parser: per-car store stamp, shape routing, body (N8.4)."""
from backend.parsers import parse
from backend.parsers import motive_ridemotive as motive

# Two hits of the Lexus Escondido pool (trimmed from the 2026-10-16 live payload): both
# list both stores in dealer_ids; dealer_id is the one that holds the car.
_VINS = ("JTJMD7CX4T4010984", "JTJMD7CX4T4010985", "JTJMD7CX4T4010986")


def _hit(vin, store_id, store, **over):
    doc = {
        "vin": vin, "car_condition": "New", "make_year": 2026, "make": "Lexus", "model": "LX 700h",
        "car_trim": "F SPORT Handling", "category": "SUV", "stock_number": "T4010984",
        "price": 123539, "odometer": 3517, "drivetrain": "AWD", "engine": "3.4L Twin Turbo V6",
        "images": ["95h17130puz5uree9ac5340qnu5g", "1hry7llzy4z5p4x30zizngl6imzc"],
        "dealer_id": store_id, "dealer_ids": [3086, 3087], "dealership": store,
    }
    doc.update(over)
    return doc


def _page():
    return {"hits": [
        _hit(_VINS[0], 3086, "Lexus Escondido"),
        _hit(_VINS[1], 3087, "Lexus Carlsbad", stock_number="T4010985"),
        _hit(_VINS[2], 3086, "Lexus Escondido", category="Other", car_trim=""),
    ], "nbHits": 3, "nbPages": 1}


def _kw(dealer_id, name):
    return dict(base_url="https://www.lexusescondido.com", dealer_id=dealer_id, dealer_name=name,
                dealer_url="https://www.lexusescondido.com")


def test_every_row_names_the_store_that_holds_it_not_the_shared_list():
    rows = motive.parse(_page(), **_kw("lexusescondido-com", "Lexus Escondido"))
    got = {r["vin"]: r["_rooftop"] for r in rows}
    assert got[_VINS[0]] == {"key": "motive-3086", "name": "Lexus Escondido", "store_id": "3086"}
    assert got[_VINS[1]]["key"] == "motive-3087" and got[_VINS[1]]["name"] == "Lexus Carlsbad"


def test_gate_keeps_own_store_and_refuses_the_sister_store():
    refused: list = []
    rows = parse("motive_ridemotive", _page(), rejected_out=refused, **_kw("lexusescondido-com", "Lexus Escondido"))
    assert {r["vin"] for r in rows} == {_VINS[0], _VINS[2]}
    assert [r["vin"] for r in refused] == [_VINS[1]]
    refused = []
    rows = parse("motive_ridemotive", _page(), rejected_out=refused, **_kw("lexuscarlsbad-com", "Lexus Carlsbad"))
    assert {r["vin"] for r in rows} == {_VINS[1]}


def test_hit_without_store_fields_is_not_stamped():
    doc = _hit(_VINS[0], "", "")
    del doc["dealer_id"], doc["dealership"]
    rows = motive.parse({"hits": [doc]}, **_kw("lexusescondido-com", "Lexus Escondido"))
    assert len(rows) == 1 and "_rooftop" not in rows[0]


def test_mapped_fields_trim_body_stock_gallery():
    rows = {r["vin"]: r for r in motive.parse(_page(), **_kw("lexusescondido-com", "Lexus Escondido"))}
    r = rows[_VINS[0]]
    assert (r["year"], r["trim"], r["body_style"], r["stock_number"]) == (2026, "F SPORT Handling", "SUV", "T4010984")
    assert r["gallery"][0] == "https://images.app.ridemotive.com/95h17130puz5uree9ac5340qnu5g" and len(r["gallery"]) == 2
    assert not rows[_VINS[2]]["body_style"]  # category "Other" is no body class


def test_detect_requires_the_motive_hit_shape():
    assert motive.detect(_page()) is True
    assert motive.detect({"hits": [{"vin": _VINS[0], "make": "Lexus"}]}) is False
    assert motive.detect({"hits": []}) is False
    assert motive.detect([{"vin": _VINS[0]}]) is False


def test_mistagged_recipe_is_routed_by_shape():
    # browser-captured Motive recipes are tagged dealer_dot_com; trim/stock/gallery were 0% there
    for provider in ("dealer_dot_com", "unknown", ""):
        rows = parse(provider, _page(), rejected_out=[], **_kw("lexusescondido-com", "Lexus Escondido"))
        assert {r["vin"] for r in rows} == {_VINS[0], _VINS[2]}
        assert rows[0]["trim"] == "F SPORT Handling" and rows[0]["stock_number"] and rows[0]["gallery"]
