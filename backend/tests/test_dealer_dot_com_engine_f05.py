"""F05 (docs/DATA_COMPLETENESS_FINDINGS_2026_09_28.md): dealer.com getInventory carries
the engine in attributes / trackingAttributes (engine, engineSize) and the VDP in
DDC.WS.state['ws-quick-specs'] and JSON-LD vehicleEngine (a plain string); none
was read. Fixtures follow the saved cavendertoyota feed item and the
autonationtoyotacerritos VDP (h2/)."""
from __future__ import annotations

from backend.parsers import dealer_dot_com
from backend.parsers.dealer_dot_com import _extract_engine_dealer_com
from backend.scanner.utils.vdp_extras_parse import parse_vdp_extras_from_html

PAD = "<!-- " + "x" * 500 + " -->"


def _item(tracking: list, attributes: list | None = None, **over) -> dict:
    d = {"vin": "5TFLA5DB2TX254221", "stockNumber": "T5046928", "year": 2026, "make": "Toyota", "model": "Tacoma",
         "trim": "TRD Off-Road", "condition": "New", "type": "new",
         "trackingPricing": {"internetPrice": "66503", "msrp": "$64,783"},
         "attributes": attributes if attributes is not None else [{"name": "stockNumber", "value": "T5046928"}],
         "trackingAttributes": tracking}
    d.update(over)
    return d


def test_engine_with_size_prefix_when_text_lacks_displacement():
    eng, litres = _extract_engine_dealer_com(_item([
        {"name": "normalFuelType", "value": "Hybrid"}, {"name": "engineSize", "value": "2.4L"},
        {"name": "engine", "value": "i-FORCE MAX 4-Cyl. Turbo Hybrid Powertrain"},
        {"name": "driveLine", "value": "Part-time 4-Wheel Drive"},
    ]))
    assert eng == "2.4L i-FORCE MAX 4-Cyl. Turbo Hybrid Powertrain" and litres == 2.4


def test_engine_text_already_naming_displacement_is_not_doubled():
    eng, litres = _extract_engine_dealer_com(_item([
        {"name": "engine", "value": "2.5L 4-Cyl. Hybrid Engine"}, {"name": "engineSize", "value": "2.5 L"},
    ]))
    assert eng == "2.5L 4-Cyl. Hybrid Engine" and litres == 2.5


def test_engine_size_alone_and_placeholders():
    eng, litres = _extract_engine_dealer_com(_item([{"name": "engineSize", "value": "3.5 L"}]))
    assert eng == "3.5L" and litres == 3.5
    eng, litres = _extract_engine_dealer_com(_item([{"name": "engine", "value": "null"}, {"name": "engineSize", "value": ""}]))
    assert eng is None and litres is None
    eng, _ = _extract_engine_dealer_com(_item([], attributes=[{"name": "engine", "value": "Electric Motor"}]))
    assert eng == "Electric Motor"


def test_map_vehicle_emits_engine_description_and_engine_l():
    row = dealer_dot_com._map_vehicle(_item([
        {"name": "engine", "value": "i-FORCE MAX 4-Cyl. Turbo Hybrid Powertrain"}, {"name": "engineSize", "value": "2.4L"},
    ]), "https://www.cavendertoyota.com", "cavendertoyota-com", "Cavender Toyota", "https://www.cavendertoyota.com")
    assert row["engine_description"] == "2.4L i-FORCE MAX 4-Cyl. Turbo Hybrid Powertrain"
    assert row["engine_l"] == 2.4
    bare = dealer_dot_com._map_vehicle(_item([]), "https://www.cavendertoyota.com", "cavendertoyota-com",
                                       "Cavender Toyota", "https://www.cavendertoyota.com")
    assert "engine_description" not in bare  # absent stays absent so COALESCE keeps a healed value


QUICK_SPECS_VDP = (
    "<html><head><script>"
    "DDC.WS.state['ws-quick-specs'] = DDC.WS.state['ws-quick-specs'] || {};\n"
    "DDC.WS.state['ws-quick-specs']['quick-specs1'] = {\"quickSpecs\":{\"exteriorColor\":\"Storm Cloud\","
    "\"interiorColor\":\"Black SofTex® trim\",\"transmission\":\"Electronically controlled Continuously Variable "
    "Transmission (ECVT)\",\"driveLine\":\"Front-Wheel Drive\",\"engine\":\"2.5L 4-Cyl. Hybrid Engine\","
    "\"vin\":\"2T36DRBV8TW029760\",\"stockNumber\":\"TW029760\",\"horsePower\":\"183hp @ 6,000RPM\"},"
    "\"attributesFromIDC\":{},\"isElectric\":true,\"vehicle\":{\"uuid\":\"a785df5b\",\"vin\":\"2T36DRBV8TW029760\"}};\n"
    "</script></head><body></body></html>" + PAD
)

JSONLD_STRING_ENGINE_VDP = (
    '<html><head><script type="application/ld+json">{"@context":"https://schema.org","@type":"Car",'
    '"name":"2026 Toyota RAV4 XLE Premium","vehicleIdentificationNumber":"2T36DRBV8TW029760","brand":{"name":"Toyota"},'
    '"model":"RAV4","vehicleEngine": "4-Cyl. Hybrid Engine","offers":{"@type":"Offer","price":"44064"}}</script>'
    "</head><body></body></html>" + PAD
)


def test_vdp_quick_specs_engine_and_driveline():
    x = parse_vdp_extras_from_html(QUICK_SPECS_VDP, "2T36DRBV8TW029760")
    assert x["engine_description"] == "2.5L 4-Cyl. Hybrid Engine"
    assert x["drivetrain"] == "Front-Wheel Drive" and x["exterior_color"] == "Storm Cloud"
    assert "engine_description" not in parse_vdp_extras_from_html(QUICK_SPECS_VDP, "4T1DAACK7TU358044")


def test_jsonld_vehicle_engine_string():
    x = parse_vdp_extras_from_html(JSONLD_STRING_ENGINE_VDP, "2T36DRBV8TW029760")
    assert x["engine_description"] == "4-Cyl. Hybrid Engine"
    assert x["price"] == 44064.0
