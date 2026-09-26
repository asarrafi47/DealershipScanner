"""Inline vehicle JSON on detail pages: MSRP, stock, history link, colours (2026-09-23)."""
from __future__ import annotations

from backend.scanner.utils.vdp_extras_parse import parse_vdp_extras_from_html

PAD = "<!-- " + "x" * 500 + " -->"

DI = (
    '<script>var customData={"env":"production","vehicleInfo":{"vin":"5TFLA5BC9TX004025","stock":"TX004025",'
    '"type":"New","year":"2026","make":"Toyota","model":"Tundra","trim":" SR5","msrp":"52349","price":"50434"}};'
    'dataLayer.push({"make":"Toyota","model":"Tundra","trim":" SR5","year":"2026","vin":"5TFLA5BC9TX004025",'
    '"engine_description":"i-FORCE V6 Engine","ext_color":"Ice Cap","int_color":"Black","msrp":"52349",'
    '"our_price":49434,"fueltype":"Gas","drivetrain":"Rear-Wheel Drive","stock":"TX004025"});</script>' + PAD
)

DDC = (
    "<html><head></head><body class='vdp_main' data-vehicle='{\"status\":\"Used\",\"year\":2025,\"make\":\"Chevrolet\","
    "\"model\":\"Equinox EV\",\"trim\":\"LT\",\"engine\":\"Electric Motor\",\"transmission\":\"1-Speed Automatic\","
    "\"interiorColor\":\"Black\",\"exteriorColor\":\"Black\",\"vin\":\"3GN7DMRP0SS240422\",\"msrp\":25988,"
    "\"displayedPrice\":24110,\"fuelType\":\"Electric\",\"drivetrain\":\"FWD\",\"stockNumber\":\"U58691\"}'>"
    "<a class='carfax-btn' href='https://www.carfax.com/vehiclehistory/ar20/S5RcmJlxmr1c6nOj0DajYeGdP'>Carfax</a>"
    "</body></html>" + PAD
)


def test_dealer_inspire_inline_json():
    x = parse_vdp_extras_from_html(DI, "5TFLA5BC9TX004025")
    assert x["msrp"] == 52349.0 and x["stock_number"] == "TX004025"
    assert x["exterior_color"] == "Ice Cap" and x["interior_color"] == "Black"
    assert x["drivetrain"] == "Rear-Wheel Drive" and x["trim"] == "SR5"
    assert "carfax_url" not in x


def test_dealer_com_data_vehicle_and_carfax():
    x = parse_vdp_extras_from_html(DDC, "3GN7DMRP0SS240422")
    assert x["msrp"] == 25988.0 and x["stock_number"] == "U58691"
    assert x["interior_color"] == "Black" and x["drivetrain"] == "FWD"
    assert x["carfax_url"].startswith("https://www.carfax.com/vehiclehistory/ar20/")


def test_other_cars_json_is_ignored():
    x = parse_vdp_extras_from_html(DI, "1HGCY2F63TA066331")  # page VIN differs from the blob's
    assert "msrp" not in x and "stock_number" not in x


def test_too_short_or_empty_html():
    assert parse_vdp_extras_from_html("", "5TFLA5BC9TX004025") == {}
    assert parse_vdp_extras_from_html("<html></html>", None) == {}


def test_carscommerce_listing_emits_msrp():
    from backend.parsers.carscommerce import parse as parse_carscommerce

    raw = {"data": {"listings": [{
        "vin": "5TFLA5BC9TX004025", "stock": "TX004025", "year": 2026, "make": "Toyota", "model": "Tundra",
        "trim": "SR5", "type": "New", "vdp_url": "https://d.example/inventory/x/",
        "pricing": {"msrp": 52349, "our_price": 49434, "price": 50434},
        "styles": {"exterior_color": "Ice Cap", "interior_color": "Black"},
        "mechanical": {"engine": "3.4L V6", "drivetrain": "RWD", "fuel_type": "Gasoline Fuel", "transmission": "10-Speed Automatic"},
        "media": {"images": []}, "history_report": {"carfax_url": None},
    }]}}
    rows = list(parse_carscommerce(raw, base_url="https://d.example", dealer_id="d", dealer_name="D", dealer_url="https://d.example"))
    assert rows and rows[0]["msrp"] == 52349 and rows[0]["price"] == 49434


def test_dealer_eprocess_inline_filter_data_fills_body_drive_and_certified():
    """Dealer eProcess SRPs carry a filter_vehicle_data JSON map with body /
    drivetrain / flag_certified that the JSON-LD blocks lack (Honda of El Cajon)."""
    import json as _json

    from backend.parsers.dealer_eprocess import parse

    vin = "1HGCY2F63TA066331"
    ld = {"@context": "https://schema.org", "@type": "Vehicle", "vehicleIdentificationNumber": vin, "name": "2026 Honda Accord",
          "brand": {"@type": "Brand", "name": "Honda"}, "model": "Accord", "vehicleModelDate": "2026",
          "vehicleConfiguration": "EX-L", "color": "White", "mileage": {"@type": "QuantitativeValue", "value": 5},
          "offers": {"@type": "Offer", "price": "31500", "sku": "H1234", "url": "https://d.example/vehicle/x/"}}
    inline = {"H1234": {"vin": vin, "body": "Sedan", "drivetrain": "FWD", "flag_certified": "1", "highest_price": "33990"}}
    html = (f'<html><body><script type="application/ld+json">{_json.dumps(ld)}</script>'
            f'<script>var filter_vehicle_data = {_json.dumps(inline)};</script></body></html>')
    rows = parse(html, base_url="https://d.example", dealer_id="d", dealer_name="D", dealer_url="https://d.example")
    assert rows and rows[0]["vin"] == vin
    assert rows[0]["body_style"] == "Sedan" and rows[0]["drivetrain"] == "FWD"
    assert rows[0]["condition"] == "Certified" and rows[0]["msrp"] == 33990.0
