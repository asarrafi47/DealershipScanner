"""
DC-7 / SA-03 (visual review 2026-09-28).

* Compare printed "Packages & options —" for two CR-Vs that each list twenty
  features: ``_package_summary`` only read ``packages_normalized`` /
  ``possible_packages``, which 22 of 104,252 active blobs carry.
* No warranty row although the same blob files one.
* "Automatic vs CVT" flagged as a difference: feed code DDU bucketed an e-CVT
  hybrid as Automatic; vPIC TransmissionStyle now wins in the serializer and
  e-CVT/CVT compare as one family.
* Differs rows were marked by a 1.13:1 tint alone; they now carry a text tag.
"""

import json
from pathlib import Path

from backend.utils.car_serialize import serialize_car_for_api
from backend.utils.compare_specs import _compare_rows, _package_summary, _warranty_summary
from backend.utils.vpic_specs import transmission_family, vpic_transmission_label

_BLOB = json.dumps({
    "features": ["Adaptive Cruise Control", "Android Auto", "Apple CarPlay", "Backup Camera",
                 "Blind Spot Monitor", "Bluetooth", "Heated Seats", "Leather Seats", "Moonroof"],
    "warranty": [
        {"name": "Basic Years", "value": "3"}, {"name": "Basic Miles/km", "value": "36,000"},
        {"name": "Drivetrain Years", "value": "5"}, {"name": "Drivetrain Miles/km", "value": "60,000"},
        {"name": "Corrosion Years", "value": "5"}, {"name": "Corrosion Miles/km", "value": "Unlimited"},
    ],
})


def test_package_summary_falls_back_to_features():
    got = _package_summary({"packages": _BLOB})
    assert got.startswith("Adaptive Cruise Control, Android Auto")
    assert got.endswith("…")  # nine features, first eight shown
    assert got.count(",") == 7


def test_package_summary_prefers_packages_when_present():
    blob = json.dumps({"possible_packages": ["Tech Package"], "features": ["Bluetooth"]})
    assert _package_summary({"packages": blob}) == "Tech Package"
    assert _package_summary({"packages": "{}"}) == "—"
    assert _package_summary({"packages": None}) == "—"


def test_warranty_summary_pairs_years_and_miles():
    got = _warranty_summary({"packages": _BLOB})
    assert got == "Basic 3 yr / 36,000 mi; Drivetrain 5 yr / 60,000 mi; Corrosion 5 yr / unlimited mi"
    assert _warranty_summary({"packages": json.dumps({"features": []})}) == "—"


def test_vpic_transmission_label_and_family():
    assert vpic_transmission_label({"transmission_style": "Electronic Continuously Variable (e-CVT)"}) == "e-CVT"
    assert vpic_transmission_label({"transmission_style": "Continuously Variable Transmission (CVT)"}) == "CVT"
    assert vpic_transmission_label({"transmission_style": "Automatic", "transmission_speeds": 10}) == "10-Speed Automatic"
    assert vpic_transmission_label({"transmission_style": "Dual-Clutch Transmission (DCT)", "transmission_speeds": 8}) == "8-Speed Dual-Clutch Automatic"
    assert vpic_transmission_label({}) is None
    assert transmission_family("e-CVT") == transmission_family("CVT") == "cvt"
    assert transmission_family("Automatic") == "automatic"
    assert transmission_family("DDU") is None


_CRV_HYBRID = {"id": 1470314, "vin": "5J6RS5H81VL000548", "make": "Honda", "model": "CR-V Hybrid",
               "year": 2027, "trim": "Sport-L", "transmission": "DDU", "transmission_type": "Automatic"}


def test_vpic_transmission_wins_over_feed_bucket(monkeypatch):
    monkeypatch.setattr(
        "backend.utils.vpic_specs.vpic_specs_for_vin",
        lambda vin: {"transmission_style": "Electronic Continuously Variable (e-CVT)"},
    )
    out = serialize_car_for_api(dict(_CRV_HYBRID), verified_specs={})
    assert out["transmission_display"] == "e-CVT"
    assert out["transmission_source"] == "NHTSA vPIC"
    assert out["transmission_feed"] == "DDU"


def test_feed_gear_count_kept_when_same_family(monkeypatch):
    monkeypatch.setattr("backend.utils.vpic_specs.vpic_specs_for_vin", lambda vin: {"transmission_style": "Automatic"})
    out = serialize_car_for_api(
        {"id": 2, "vin": "1FTFW1E50NFA00000", "make": "Ford", "model": "F-150", "year": 2022,
         "transmission": "10-Speed Automatic", "transmission_type": "Automatic"},
        verified_specs={},
    )
    assert out["transmission_display"] == "10-Speed Automatic"
    assert out["transmission_source"] is None
    assert out["transmission_feed"] is None


def test_no_decode_leaves_feed_alone(monkeypatch):
    monkeypatch.setattr("backend.utils.vpic_specs.vpic_specs_for_vin", lambda vin: {})
    out = serialize_car_for_api(dict(_CRV_HYBRID), verified_specs={})
    assert out["transmission_display"] == "Automatic"
    assert out["transmission_source"] is None


def _row(rows, key):
    return next(r for r in rows if r["key"] == key)


def test_ecvt_vs_cvt_does_not_differ_but_rows_carry_features_and_warranty():
    cars = [
        {"transmission_display": "e-CVT", "compare_packages_summary": "A, B",
         "compare_warranty_summary": "Basic 3 yr / 36,000 mi", "price": 41297},
        {"transmission_display": "CVT", "compare_packages_summary": "A, C",
         "compare_warranty_summary": "Basic 3 yr / 36,000 mi", "price": 40098},
    ]
    rows = _compare_rows(cars)
    assert _row(rows, "transmission_display")["differs"] is False
    assert _row(rows, "compare_packages_summary")["label"] == "Features & options"
    assert _row(rows, "compare_warranty_summary")["differs"] is False
    assert _row(rows, "price")["differs"] is True
    cars[1]["transmission_display"] = "8-Speed Automatic"
    assert _row(_compare_rows(cars), "transmission_display")["differs"] is True


def test_compare_template_has_text_differs_tag():
    src = (Path(__file__).resolve().parents[2] / "frontend" / "templates" / "compare.html").read_text()
    assert '<span class="compare-diff-tag">differs</span>' in src
