"""
IH-09 / DC-3 / ES-3 (visual review 2026-09-28): "Days on Market: 0", a
"Fresh inventory: firm pricing" verdict and a constant "Predictive Local
Turnaround" became one sourced line, "First seen by Sarrafi Cars on <date>
(N days)", with the leverage badge only after a week of observation.
"""

from datetime import datetime, timezone
from pathlib import Path

from backend.utils.car_serialize import serialize_car_for_api
from backend.utils.first_seen import first_seen_fields

_ROOT = Path(__file__).resolve().parents[2]
_NOW = datetime(2026, 9, 28, 20, 0, tzinfo=timezone.utc)


def test_same_day_is_zero_days_and_no_badge():
    got = first_seen_fields({"first_seen_at": "2026-09-28T18:01:12.886125Z"}, now=_NOW)
    assert got["first_seen_date"] == "Sep 28, 2026"
    assert got["first_seen_days"] == 0
    assert got["first_seen_badge"] is None


def test_badge_waits_for_seven_days():
    assert first_seen_fields({"first_seen_at": "2026-09-22T00:00:00Z"}, now=_NOW)["first_seen_badge"] is None
    b = first_seen_fields({"first_seen_at": "2026-09-21T00:00:00Z"}, now=_NOW)
    assert b["first_seen_days"] == 7
    assert b["first_seen_badge"]["cls"] == "recent-listing"
    assert "firm" not in b["first_seen_badge"]["label"].lower()
    assert first_seen_fields({"first_seen_at": "2026-08-20T00:00:00Z"}, now=_NOW)["first_seen_badge"]["cls"] == "mid-leverage"
    assert first_seen_fields({"first_seen_at": "2026-07-01T00:00:00Z"}, now=_NOW)["first_seen_badge"]["cls"] == "high-leverage"


def test_falls_back_to_created_at_never_scraped_at():
    got = first_seen_fields({"created_at": datetime(2026, 9, 1, tzinfo=timezone.utc), "scraped_at": "2026-09-28T00:00:00Z"}, now=_NOW)
    assert got["first_seen_days"] == 27
    none = first_seen_fields({"scraped_at": "2026-09-28T00:00:00Z"}, now=_NOW)
    assert none == {"first_seen_iso": None, "first_seen_date": None, "first_seen_days": None, "first_seen_badge": None}


def test_serializer_emits_first_seen_fields():
    out = serialize_car_for_api({"id": 1, "make": "Honda", "model": "CR-V", "year": 2027,
                                 "first_seen_at": "2026-09-28T18:01:12Z"}, verified_specs={})
    assert out["first_seen_date"] == "Sep 28, 2026"
    assert isinstance(out["first_seen_days"], int)


def test_turnaround_card_and_gradient_are_gone():
    html = (_ROOT / "frontend/templates/car.html").read_text()
    js = (_ROOT / "frontend/static/car_page.js").read_text()
    helpers = (_ROOT / "frontend/static/car_page_helpers.js").read_text()
    css = (_ROOT / "frontend/static/css/03-car-page.css").read_text()
    assert "Days on Market" not in html
    assert "Predictive Local Turnaround" not in html
    assert "market-velocity-card" not in html
    assert "First seen by Sarrafi Cars on" in html
    assert "Fresh Inventory: Firm Pricing" not in js
    assert "initMarketVelocityHeatmap" not in js
    assert "SEGMENT_BASELINE_DAYS" not in helpers
    assert "velocity-heatmap-track" not in css
