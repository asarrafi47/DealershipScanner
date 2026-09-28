"""F02 (docs/DATA_COMPLETENESS_FINDINGS_2026_09_28.md): real dealer.com stores carry
MSRP in trackingPricing.msrp, pricing.dprice[].typeClass == "msrp" and
pricing.retailPrice (never pricing.msrp), and the 2026-09 VDP template keeps the
vehicle record in DDC.WS.state['ws-vehicle-ctas'] / DDC.dataLayer.vehicles[0]
instead of a data-vehicle attribute. Fixtures follow the saved cavendertoyota
feed item and the autonationtoyotacerritos VDP (h2/)."""
from __future__ import annotations

from backend.parsers import dealer_dot_com
from backend.parsers.dealer_dot_com import _extract_msrp_dealer_com
from backend.scanner.utils.vdp_extras_parse import parse_vdp_extras_from_html

PAD = "<!-- " + "x" * 500 + " -->"


def _feed_item(**over) -> dict:
    item = {
        "vin": "5TFLA5DB2TX254221", "stockNumber": "T5046928", "year": 2026, "make": "Toyota", "model": "Tacoma",
        "trim": "TRD Off-Road", "condition": "New", "type": "new", "certified": False,
        "trackingPricing": {"internetPrice": "66503", "salePrice": "66503", "retailValue": "66503",
                            "msrp": "$64,783", "askingPrice": "$66,503"},
        "pricing": {
            "retailPrice": "$64,783",
            "dprice": [
                {"label": "Total SRP", "type": "MIDDLE", "typeClass": "msrp", "value": "$64,783"},
                {"label": "Documentary Fee", "type": "MIDDLE", "typeClass": "documentFee", "value": "$225"},
                {"label": "Advertised Price", "type": "TOTAL", "typeClass": "askingPrice", "value": "$66,503",
                 "isFinalPrice": True},
            ],
            "vehicle": {"category": "AUTO"}, "ePriceStatus": "NON_APPLICABLE",
        },
        "attributes": [{"name": "stockNumber", "value": "T5046928"}],
        "trackingAttributes": [{"name": "engine", "value": "i-FORCE MAX 4-Cyl. Turbo Hybrid Powertrain"}],
    }
    item.update(over)
    return item


def test_tracking_pricing_msrp_is_read():
    assert _extract_msrp_dealer_com(_feed_item()) == 64783


def test_dprice_typed_entry_when_tracking_pricing_lacks_msrp():
    item = _feed_item(trackingPricing={"internetPrice": "66503"})
    assert _extract_msrp_dealer_com(item) == 64783
    # askingPrice / documentFee entries never count as MSRP
    item["pricing"]["dprice"] = [e for e in item["pricing"]["dprice"] if e["typeClass"] != "msrp"]
    item["pricing"].pop("retailPrice")
    assert _extract_msrp_dealer_com(item) == 0


def test_retail_price_only_on_new_or_certified():
    item = _feed_item(trackingPricing={}, pricing={"retailPrice": "$64,783"})
    assert _extract_msrp_dealer_com(item) == 64783
    used = _feed_item(trackingPricing={}, pricing={"retailPrice": "$31,500"}, condition="Used", type="used")
    assert _extract_msrp_dealer_com(used) == 0
    cpo = _feed_item(trackingPricing={}, pricing={"retailPrice": "$31,500"}, condition="Used", type="used",
                     certified=True)
    assert _extract_msrp_dealer_com(cpo) == 31500


def test_msrp_sanity_bounds():
    assert _extract_msrp_dealer_com(_feed_item(trackingPricing={"msrp": "490"}, pricing={})) == 0  # masked placeholder
    assert _extract_msrp_dealer_com(_feed_item(trackingPricing={"msrp": "5046928"}, pricing={})) == 0  # id fragment
    legacy = _feed_item(trackingPricing={}, pricing={"msrp": 41063})
    assert _extract_msrp_dealer_com(legacy) == 41063  # old key still wins first


def test_map_vehicle_carries_msrp_and_price_separately():
    row = dealer_dot_com._map_vehicle(_feed_item(), "https://www.cavendertoyota.com", "cavendertoyota-com",
                                      "Cavender Toyota", "https://www.cavendertoyota.com")
    assert row["price"] == 66503 and row["msrp"] == 64783


CTAS_VDP = (
    "<html><head><script>"
    "DDC.WS.state['ws-vehicle-ctas'] = DDC.WS.state['ws-vehicle-ctas'] || {};\n"
    "DDC.WS.state['ws-vehicle-ctas']['vehicle-ctas1'] = {\"vehicle\":{\"uuid\":\"a785df5bac1812be80b66b4f05837570\","
    "\"vin\":\"2T36DRBV8TW029760\",\"stockNumber\":\"TW029760\",\"year\":2026,\"make\":\"Toyota\",\"type\":\"NEW\","
    "\"transmission\":\"Electronically controlled Continuously Variable Transmission (ECVT)\",\"model\":\"RAV4\","
    "\"normalFuelType\":\"Hybrid\",\"trim\":\"XLE Premium\",\"bodyStyle\":\"HYBRID FWD\",\"normalDriveLine\":\"FWD\","
    "\"salePrice\":43979,\"askingPrice\":43979,\"msrp\":41063,\"internetPrice\":44064,\"odometer\":0,"
    "\"engine\":\"2.5L 4-Cyl. Hybrid Engine\",\"account\":{\"name\":\"AutoNation Toyota Cerritos\"}},"
    "\"items\":[{\"btnLabel\":\"GET_MORE_INFO\",\"btnHref\":\"/eprice-form.htm?itemId=a785df5bac18\"}]};\n"
    "</script><script>"
    "DDC.dataLayer.vehicles = DDC.dataLayer.vehicles || [];\n"
    "DDC.dataLayer.vehicles[0] = DDC.dataLayer.vehicles[0] || {};\n"
    "DDC.dataLayer.vehicles[0].make = DDC.dataLayer.vehicles[0].make || \"Toyota\";\n"
    "DDC.dataLayer.vehicles[0].trim = DDC.dataLayer.vehicles[0].trim || \"XLE Premium\";\n"
    "DDC.dataLayer.vehicles[0].vin = DDC.dataLayer.vehicles[0].vin || \"2T36DRBV8TW029760\";\n"
    "</script></head><body class='vdp_main'></body></html>" + PAD
)

DATALAYER_ONLY_VDP = (
    "<html><head><script>"
    "DDC.dataLayer.vehicles[0] = DDC.dataLayer.vehicles[0] || {};\n"
    "DDC.dataLayer.vehicles[0].msrp = DDC.dataLayer.vehicles[0].msrp || \"41063\";\n"
    "DDC.dataLayer.vehicles[0].stockNumber = DDC.dataLayer.vehicles[0].stockNumber || \"TW029760\";\n"
    "DDC.dataLayer.vehicles[0].trim = DDC.dataLayer.vehicles[0].trim || \"XLE Premium\";\n"
    "DDC.dataLayer.vehicles[0].vin = DDC.dataLayer.vehicles[0].vin || \"2T36DRBV8TW029760\";\n"
    "</script></head><body></body></html>" + PAD
)


def test_vdp_ws_state_vehicle_ctas_without_data_vehicle():
    x = parse_vdp_extras_from_html(CTAS_VDP, "2T36DRBV8TW029760")
    assert x["msrp"] == 41063.0
    assert x["stock_number"] == "TW029760" and x["trim"] == "XLE Premium"
    assert x["engine_description"] == "2.5L 4-Cyl. Hybrid Engine"
    assert x["drivetrain"] == "FWD" and x["fuel_type"] == "Hybrid"


def test_vdp_ws_state_is_vin_gated():
    x = parse_vdp_extras_from_html(CTAS_VDP, "4T1DAACK7TU358044")
    assert "msrp" not in x and "stock_number" not in x


def test_vdp_datalayer_fallback():
    x = parse_vdp_extras_from_html(DATALAYER_ONLY_VDP, "2T36DRBV8TW029760")
    assert x["msrp"] == 41063.0 and x["stock_number"] == "TW029760" and x["trim"] == "XLE Premium"
    assert "msrp" not in parse_vdp_extras_from_html(DATALAYER_ONLY_VDP, "4T1DAACK7TU358044")


def test_capture_coverage_treats_numeric_zero_as_missing():
    from backend.scanner.phases.dealer_run import _capture_coverage

    cov = _capture_coverage([
        {"vin": "5TFLA5DB2TX254221", "price": 66503, "msrp": 0, "mileage": 0},
        {"vin": "2T36DRBV8TW029760", "price": 0, "msrp": 41063, "mileage": 12},
    ])
    assert cov["price"] == 0.5 and cov["msrp"] == 0.5 and cov["mileage"] == 0.5
