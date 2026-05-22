"""Sister-store / off-lot location matching for scanner inventory."""

from __future__ import annotations

from backend.scanner.dealer_location import (
    DealerSiteProfile,
    build_dealer_site_profile,
    classify_vehicle_location,
    extract_location_from_inventory_object,
    filter_sister_store_vehicles,
)


def _profile() -> DealerSiteProfile:
    return DealerSiteProfile(
        dealer_id="db-127",
        name="Hendrick Chevrolet Monroe",
        url="https://www.hendrickchevroletmonroe.com",
        city="Monroe",
        state="NC",
    )


def test_classify_monroe_match() -> None:
    assert (
        classify_vehicle_location("Located at Hendrick Chevrolet Monroe, NC", _profile())
        == "match"
    )


def test_classify_charlotte_bmw_mismatch() -> None:
    assert (
        classify_vehicle_location("Located at Hendrick BMW of Charlotte, NC", _profile())
        == "mismatch"
    )


def test_classify_unknown_when_no_location() -> None:
    assert classify_vehicle_location("", _profile()) == "unknown"


def test_extract_from_inventory_dealer_name() -> None:
    loc = extract_location_from_inventory_object(
        {"vin": "1" * 17, "dealerName": "Hendrick Honda Hickory"}
    )
    assert "Hickory" in loc or "Honda" in loc


def test_filter_drops_mismatch_keeps_unknown() -> None:
    prof = _profile()
    vehicles = [
        {"vin": "1" * 17, "_lot_location": "Located at Hendrick BMW Charlotte, NC"},
        {"vin": "2" * 17},
        {"vin": "3" * 17, "_lot_location": "Hendrick Chevrolet Monroe, NC"},
    ]
    kept, stats = filter_sister_store_vehicles(vehicles, prof, source="test")
    assert stats["excluded"] == 1
    assert len(kept) == 2
    assert stats["unknown_location"] == 1


def test_build_profile_uses_registry_city(monkeypatch) -> None:
    def fake_get(_id: int):
        return {"city": "Monroe", "state": "NC"}

    monkeypatch.setattr(
        "backend.db.dealerships_db.get_dealership_by_id",
        fake_get,
    )
    p = build_dealer_site_profile(
        {
            "dealer_id": "db-127",
            "name": "Hendrick Chevrolet Monroe",
            "url": "https://www.hendrickchevroletmonroe.com",
            "dealership_registry_id": 127,
        }
    )
    assert p.city == "Monroe"
    assert p.state == "NC"
