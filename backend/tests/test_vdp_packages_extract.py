"""Structured VDP packages/options merge."""

from __future__ import annotations

import json

from backend.scanner.vdp_packages_extract import (
    dealer_notes_from_bundle,
    merge_vdp_packages_into_vehicle,
    structured_items_from_bundle,
)


def test_structured_items_from_dom_packages():
    bundle = {
        "domPackagesStructured": [
            {
                "section": "included_packages",
                "name": "Premium Package",
                "price_label": "$1,600",
                "price": 1600,
                "features": ["Remote Engine Start", "Panoramic Moonroof"],
            },
            {
                "section": "standard_features",
                "name": "Sport Seats",
                "features": ["Power Front Seats"],
            },
            {"section": "detailed_specifications", "name": "Navigation", "features": []},
            {"section": "included_options", "name": "10 Speakers", "features": []},
        ],
        "domFeatures": ["Heated front seats", "Navigation system"],
    }
    items = structured_items_from_bundle(bundle)
    assert len(items) == 2
    premium = next(i for i in items if i["name"] == "Premium Package")
    assert premium["price"] == 1600
    assert "Remote Engine Start" in premium["features"]
    assert not any(i["name"] == "Navigation" for i in items)
    assert not any(i["name"] == "10 Speakers" for i in items)


def test_merge_vdp_packages_into_vehicle():
    vehicle: dict = {"vin": "1" * 17}
    bundle = {
        "domPackagesStructured": [
            {"section": "included_options", "name": "Extended Shadowline Trim", "price_label": "$300"},
        ]
    }
    assert merge_vdp_packages_into_vehicle(vehicle, bundle) is True
    pkg = json.loads(vehicle["packages"])
    assert pkg["vdp_packages_source"] == "vdp_dom"
    assert pkg["packages_normalized"][0]["name"] == "Extended Shadowline Trim"
    assert pkg["packages_normalized"][0]["price"] == 300


def test_dealer_notes_from_bundle_prefers_dom_dealer_notes():
    bundle = {
        "domDealerNotes": "Irvine BMW proudly presents this exclusive service loaner. " * 3,
        "domDescription": "Short generic description.",
    }
    notes = dealer_notes_from_bundle(bundle)
    assert "Irvine BMW" in notes
    assert len(notes) >= 40
