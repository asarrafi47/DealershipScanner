"""P8B.4b (IL-20): heavy-duty / commercial drivetrain and transmission table in
vpic_facts, gated on GVWR over 8,500 lb (ProMaster on van body class)."""
from __future__ import annotations

import pytest

from backend.enrichment import vpic_facts as vf
from backend.vehicle_facts import precedence as prec

SILVERADO_HD = {"Make": "CHEVROLET", "Model": "Silverado 2500 HD", "GVWR": "Class 2H: 8,501 - 10,000 lb (3,856 - 4,536 kg)",
                "BodyClass": "Pickup", "DriveType": "4x2"}
EXPRESS = {"Make": "CHEVROLET", "Model": "Express Cutaway 4500", "GVWR": "Class 4: 14,001 - 16,000 lb", "BodyClass": "Cutaway",
           "DriveType": ""}
PROMASTER = {"Make": "RAM", "Model": "ProMaster 2500", "GVWR": "", "BodyClass": "Van", "DriveType": ""}
LIGHT_SILVERADO = {"Make": "CHEVROLET", "Model": "Silverado 1500", "GVWR": "Class 2E: 6,001 - 7,000 lb", "BodyClass": "Pickup",
                   "DriveType": "4x2"}
MAZDA3 = {"Make": "MAZDA", "Model": "Mazda3", "GVWR": "", "BodyClass": "Sedan", "DriveType": "4x2"}


@pytest.mark.parametrize("flat,want", [
    (SILVERADO_HD, ("RWD", "Automatic")),
    (EXPRESS, ("RWD", "Automatic")),
    (PROMASTER, ("FWD", "Automatic")),
    (LIGHT_SILVERADO, None),
    (MAZDA3, None),
    ({}, None),
])
def test_family_table_and_gate(flat, want):
    assert vf.hd_family_facts(flat) == want


def test_ford_and_ram_hd_rwd():
    ford = {"Make": "FORD", "Model": "F-350 Super Duty", "GVWR": "Class 3: 10,001 - 14,000 lb"}
    ram = {"Make": "RAM", "Model": "3500", "GVWR": "Class 3: 10,001 - 14,000 lb"}
    assert vf.hd_family_facts(ford) == ("RWD", "Automatic")
    assert vf.hd_family_facts(ram) == ("RWD", "Automatic")
    assert vf.hd_family_facts({**ram, "GVWR": ""}) is None


def test_4x2_blank_drive_fills_rwd_with_provenance_source():
    out = vf.hd_fills({"drivetrain": None, "transmission": None}, {"drive_type_raw": "4x2"}, SILVERADO_HD)
    assert out["drivetrain"][1:] == ("RWD", prec.TRIM_HEURISTIC)
    assert out["transmission"][1:] == ("Automatic", prec.TRIM_HEURISTIC)


def test_blank_drive_type_fills():
    out = vf.hd_fills({"drivetrain": "", "transmission": ""}, {}, EXPRESS)
    assert out["drivetrain"][1] == "RWD"


def test_stated_end_is_never_replaced():
    for stored in ("RWD", "FWD", "4WD", "AWD"):
        out = vf.hd_fills({"drivetrain": stored, "transmission": "Manual"}, {"drive_type_raw": "4x2"}, SILVERADO_HD)
        assert out == {}


def test_vpic_end_decides_so_table_is_silent():
    out = vf.hd_fills({"drivetrain": None, "transmission": "10-Speed Automatic"}, {"drivetrain": "4WD"}, SILVERADO_HD)
    assert out == {}


def test_light_vehicle_4x2_stays_unmapped():
    assert vf.hd_fills({"drivetrain": None, "transmission": None}, {"drive_type_raw": "4x2"}, MAZDA3) == {}


def test_vpic_transmission_preferred_over_table():
    out = vf.hd_fills({"drivetrain": "RWD", "transmission": None}, {"transmission": "6-Speed Automatic"}, SILVERADO_HD)
    assert out["transmission"][1:] == ("6-Speed Automatic", prec.NHTSA_VPIC)
    assert "drivetrain" not in out


def test_heal_chunk_prefers_hd_end_over_bare_2wd(monkeypatch):
    calls = []

    class Cur:
        def execute(self, sql, params=()):
            calls.append((sql, params))

    row = {"id": 1, "vin": "1GB2ALE79TF340458", "drivetrain": None, "fuel_type": "Gasoline", "cylinders": None,
           "spec_source_json": None, "catalog_drive": None, "engine_description": "6.6L", "body_style": "Truck",
           "transmission": None}
    stats = {"decoded": 0, "drivetrain": 0, "fuel_type": 0, "cylinders": 0, "engine_description": 0,
             "body_style": 0, "transmission": 0, "examples": []}
    vf._heal_chunk(Cur(), [row], {row["vin"]: SILVERADO_HD}, stats, False,
                   lambda vin: {"drive_type_raw": "4x2", "fuel_type": "Gasoline"},
                   lambda cur, prov: __import__("json").dumps(prov), None)
    assert stats["drivetrain"] == 1 and stats["transmission"] == 1
    sql, params = calls[0]
    assert "drivetrain = ?" in sql and "transmission = ?" in sql
    assert "RWD" in params and "Automatic" in params
