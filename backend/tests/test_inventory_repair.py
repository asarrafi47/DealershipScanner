"""inventory_repair helpers."""

from __future__ import annotations

from backend.utils.field_clean import clean_car_row_dict
from backend.utils.inventory_repair import collect_cleaned_field_updates, collect_row_storage_repairs


def test_collect_cleaned_field_updates_nulls_double_dash() -> None:
    raw = {
        "id": 1,
        "vin": "1HGBH41JXMN109185",
        "drivetrain": "--",
        "transmission": "—",
        "year": 2020,
        "make": "Honda",
        "model": "Accord",
    }
    u = collect_cleaned_field_updates(raw)
    assert u.get("drivetrain") is None
    assert u.get("transmission") is None


def test_clean_car_row_dict_strips_booleanish_condition() -> None:
    raw = {"vin": "X", "condition": "0"}
    c = clean_car_row_dict(raw)
    assert c.get("condition") is None


def test_collect_row_adds_condition_when_missing() -> None:
    raw = {
        "id": 2,
        "vin": "2HGCM82633A004352",
        "year": 2021,
        "make": "Honda",
        "model": "Civic",
        "title": "2021 Honda Civic Sport",
        "condition": None,
        "mileage": None,
        "is_cpo": 0,
        "source_url": "https://dealer.example.com/used-inventory/",
        "drivetrain": None,
        "transmission": None,
    }
    u = collect_row_storage_repairs(raw)
    assert u.get("condition") == "Used"


# --- P8B.6a: proposals with honest tier-1 labels ---------------------------


def _raw(**kw):
    base = {"id": 1, "vin": "1HGBH41JXMN109185", "title": "2020 Honda Accord LX", "year": 2020,
            "make": "Honda", "model": "Accord", "trim": "LX", "fuel_type": "Gasoline",
            "transmission": "Automatic", "drivetrain": None, "cylinders": None,
            "engine_description": "2.0L I4", "condition": "Used"}
    base.update(kw)
    return base


def test_row_proposals_label_dealer_text_heuristic_cylinders(monkeypatch) -> None:
    from backend.utils import inventory_repair as ir
    from backend.utils.inventory_repair import collect_row_storage_proposals

    monkeypatch.setattr(ir, "merge_verified_specs", lambda _r: {})
    out = collect_row_storage_proposals(_raw())
    assert ("cylinders", 4, "dealer_text_heuristic") in out.proposals


def test_row_proposals_respelling_and_clears_stay_plain() -> None:
    from backend.utils.inventory_repair import collect_row_storage_proposals

    out = collect_row_storage_proposals(_raw(drivetrain="All-Wheel Drive", cylinders=4, exterior_color="--"))
    assert not [p for p in out.proposals if p[0] == "drivetrain"]
    assert out.plain.get("drivetrain") == "AWD"
    assert out.plain.get("exterior_color") is None and "exterior_color" in out.plain


def test_merge_proposals_are_catalog_labelled_and_match_updates(monkeypatch) -> None:
    from backend.utils import inventory_repair as ir

    monkeypatch.setattr(ir, "merge_verified_specs", lambda _r: {
        "drivetrain_display": "RWD", "body_style_display": "Sedan", "fuel_type_hint": "Gasoline"})
    raw = _raw(fuel_type=None, body_style=None)
    upd = ir.collect_merge_spec_storage_updates(raw)
    out = ir.collect_merge_spec_storage_proposals(raw)
    assert ("drivetrain", "RWD", "epa_catalog") in out.proposals
    assert ("fuel_type", "Gasoline", "epa_catalog") in out.proposals
    assert out.plain == {"body_style": "Sedan"} and out.sources["body_style"] == "epa_catalog"
    assert {p[0] for p in out.proposals} | set(out.plain) == set(upd)


def test_tier1_cylinders_never_contradict_engine_text(monkeypatch) -> None:
    from backend.utils import inventory_repair as ir

    monkeypatch.setattr(ir, "merge_verified_specs", lambda _r: {"cylinders": 6})
    out = ir.collect_merge_spec_storage_proposals(_raw(cylinders=None, engine_description="2.0L I4 Turbo"))
    assert not [p for p in out.proposals if p[0] == "cylinders"]


def test_row_proposals_condition_and_buckets_plain() -> None:
    from backend.utils.inventory_repair import collect_row_storage_proposals, storage_derived_columns

    out = collect_row_storage_proposals(_raw(condition="Pre-Owned"))
    assert out.plain["condition"] == "Used"
    assert storage_derived_columns({}, {"x": 1}) == {}
    assert "interior_color_buckets" in storage_derived_columns({"make": "Honda"}, {"interior_color": "Black"})
