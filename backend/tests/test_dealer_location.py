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


def test_classify_city_state_match_for_hendrick_charlotte() -> None:
    prof = DealerSiteProfile(
        dealer_id="db-1",
        name="Rick Hendrick City Chevrolet",
        url="https://www.citychevrolet.com",
        city="Charlotte",
        state="NC",
    )
    assert classify_vehicle_location("Charlotte, NC", prof) == "match"


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


def test_group_feed_domain_is_not_a_location_label() -> None:
    """
    Dealer.com group feeds put the storefront's own domain on every row, so
    sweeping it into the location text made the host check match a sister
    store's car. Real case: nissanofcostamesa.com serves seven rooftops.
    """
    from backend.scanner.dealer.location import extract_location_from_inventory_object

    row = {
        "dealerName": "Nissan of Van Nuys",
        "dealerCity": "Van Nuys",
        "dealerState": "CA",
        "dealerZip": "91401",
        "dealerDomain": "https://www.nissanofcostamesa.com",
    }
    loc = extract_location_from_inventory_object(row)
    assert "nissanofcostamesa.com" not in loc
    assert "Van Nuys" in loc

    prof = DealerSiteProfile(
        dealer_id="nissanofcostamesa-com",
        name="Nissan of Costa Mesa",
        url="https://www.nissanofcostamesa.com",
        city="",
        state="",
    )
    assert classify_vehicle_location(loc, prof) != "match"

    own = extract_location_from_inventory_object(
        {"dealerName": "Nissan of Costa Mesa", "dealerCity": "Costa Mesa",
         "dealerState": "CA", "dealerZip": "92626"}
    )
    assert classify_vehicle_location(own, prof) == "match"
