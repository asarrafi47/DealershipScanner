"""
TC-5 (visual review 2026-09-28): the VDP price history was double-encoded.

``_price_history_json_for_vdp`` returned a JSON *string* and ``car.html``
applied ``| tojson`` on top of it, so ``JSON.parse`` in ``car_page.js`` produced
a string, ``Array.isArray`` failed, and every car showed "No pricing
adjustments recorded yet" (39,824 active listings carry >= 2 points).

The serializer now hands the template a Python list (``price_history``) and the
template encodes it exactly once.  Same-day flips between two scan sources are
collapsed to one point per day, then consecutive equal prices are dropped.
"""

import json
import re
from pathlib import Path

from flask import Flask

from backend.utils.car_serialize import serialize as ser

_CAR_HTML = Path(__file__).resolve().parents[2] / "frontend" / "templates" / "car.html"


def _prov(*pairs):
    return json.dumps([{"date": d, "price": p} for d, p in pairs])


def test_history_is_a_list_not_a_json_string():
    car = {"price_provenance_json": _prov(("2026-06-01T00:00:00Z", 30000), ("2026-07-10T00:00:00Z", 27000))}
    hist = ser._price_history_for_vdp(car)
    assert isinstance(hist, list)
    assert hist == [
        {"date": "2026-06-01T00:00:00Z", "price": 30000.0},
        {"date": "2026-07-10T00:00:00Z", "price": 27000.0},
    ]
    # The compat wrapper is still a string, and decodes to the same list.
    assert json.loads(ser._price_history_json_for_vdp(car)) == hist


def test_same_day_source_flips_are_collapsed():
    # Real trail from car 1013935: two scan sources alternated 32,100 / 30,000
    # with two points on 09-24 and two on 09-26.
    car = {
        "price_provenance_json": _prov(
            ("2026-09-24T14:45:29Z", 32100.0),
            ("2026-09-24T16:15:39Z", 30000.0),
            ("2026-09-25T21:08:21Z", 32100.0),
            ("2026-09-26T01:00:38Z", 30000.0),
            ("2026-09-26T13:13:34Z", 32100.0),
            ("2026-09-28T14:23:47Z", 30000.0),
        )
    }
    hist = ser._price_history_for_vdp(car)
    assert [h["date"][:10] for h in hist] == ["2026-09-24", "2026-09-25", "2026-09-28"]
    assert [h["price"] for h in hist] == [30000.0, 32100.0, 30000.0]
    # Last observation of the day wins.
    assert hist[0]["date"] == "2026-09-24T16:15:39Z"


def test_empty_and_malformed_provenance():
    assert ser._price_history_for_vdp({}) == []
    assert ser._price_history_for_vdp({"price_provenance_json": "not json"}) == []
    assert ser._price_history_for_vdp({"price_provenance_json": json.dumps([{"date": "x"}, 5])}) == []


def test_template_encodes_once():
    src = _CAR_HTML.read_text(encoding="utf-8")
    m = re.search(r"data-price-history='(\{\{.*?\}\})'", src)
    assert m, "data-price-history attribute missing from car.html"
    expr = m.group(1)
    assert "price_history_json" not in expr
    app = Flask(__name__)
    with app.app_context():
        rendered = app.jinja_env.from_string(expr).render(
            car={"price_history": [{"date": "2026-06-01T00:00:00Z", "price": 30000.0}]}
        )
        empty = app.jinja_env.from_string(expr).render(car={"price_history": []})
        missing = app.jinja_env.from_string(expr).render(car={})
    # What JSON.parse sees in the browser must be an array, not a string.
    assert isinstance(json.loads(rendered), list)
    assert json.loads(rendered)[0]["price"] == 30000.0
    assert json.loads(empty) == []
    assert json.loads(missing) == []
    # Single-quoted attribute: tojson must not emit a raw apostrophe.
    assert "'" not in rendered
