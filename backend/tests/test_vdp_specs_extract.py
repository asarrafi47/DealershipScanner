"""VDP spec sheet extraction."""

from __future__ import annotations

from backend.scanner.vdp_specs_extract import merge_spec_sheet_into_vehicle, spec_sheet_from_bundle


def test_spec_sheet_from_dom_specs():
    bundle = {
        "domSpecs": {"Engine": "3.0L I6", "Transmission": "8-Speed Automatic"},
        "domPackagesSections": ["Included Packages & Options"],
    }
    sheet = spec_sheet_from_bundle(bundle)
    assert sheet["source"] == "vdp_dom"
    assert len(sheet["rows"]) == 2
    assert sheet["rows"][0]["label"] == "Engine"


def test_merge_spec_sheet_into_vehicle():
    vehicle: dict = {"vin": "1" * 17}
    bundle = {"domSpecs": {"Drivetrain": "AWD"}}
    assert merge_spec_sheet_into_vehicle(vehicle, bundle) is True
    import json

    spec = json.loads(vehicle["spec_source_json"])
    assert spec["spec_sheet_normalized"]["rows"][0]["value"] == "AWD"
