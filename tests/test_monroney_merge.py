from __future__ import annotations

from backend.vision import monroney_merge as mm
from scanner_vdp import (
    _is_generic_vhr_vin_only_url,
    _merge_vdp_vehicle_history_url,
    _pick_best_vehicle_history_url,
)


def test_pick_best_vehicle_history_url_prefers_partner() -> None:
    generic = "https://vhr.carfax.com/main?vin=1HGBH41JXMN109186"
    rich = "https://www.carfax.com/cfm/ccc_displayhistoryrpt.cfm?partner=ABC_123&vin=1HGBH41JXMN109186"
    assert _pick_best_vehicle_history_url([generic, rich]) == rich


def test_is_generic_vhr_vin_only() -> None:
    assert _is_generic_vhr_vin_only_url("https://vhr.carfax.com/main?vin=1HGBH41JXMN109186")
    assert not _is_generic_vhr_vin_only_url(
        "https://vhr.carfax.com/main?vin=1HGBH41JXMN109186&partnerToken=xyz"
    )


def test_merge_vdp_vehicle_history_url_replaces_generic() -> None:
    v: dict = {"carfax_url": "https://vhr.carfax.com/main?vin=1HGBH41JXMN109186"}
    dom = ["https://www.carfax.com/vehicle-history?vin=1HGBH41JXMN109186&dealer=1"]
    assert _merge_vdp_vehicle_history_url(v, dom) is True
    assert "carfax.com/vehicle-history" in (v.get("carfax_url") or "")


def test_merge_monroney_fills_empty_engine() -> None:
    v: dict = {"engine_description": None, "packages": None}
    parsed = {
        "engine_description": "2.0L Turbo I4",
        "optional_packages": ["Premium Package"],
        "confidence": 0.85,
        "vision_model": "llava:13b",
    }
    filled = mm.merge_monroney_parsed_into_vehicle(v, parsed)
    assert "engine_description" in filled
    assert v.get("engine_description") == "2.0L Turbo I4"
    assert isinstance(v.get("packages"), dict)
    assert v["packages"].get("monroney_options")
