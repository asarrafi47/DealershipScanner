"""Fixes from the 2026-09-24 synthesis-failure analysis (workspace/dealer_logs/*):
store place evidence for group feeds, the WordPress vehicles index platform, and
JSON-LD identity from detail pages."""
from __future__ import annotations

from backend.parsers.wp_vehicles_index import detect as wp_detect
from backend.parsers.wp_vehicles_index import parse as wp_parse
from backend.scanner.dealer_place import place_from_html, place_kwargs
from backend.scanner.utils.vdp_extras_parse import parse_vdp_extras_from_html


def test_place_from_title():
    html = "<html><head><title>Hendrick Buick GMC Cary | Buick, GMC Dealer in Cary, NC</title></head></html>" + "x" * 500
    p = place_from_html(html)
    assert p["dealer_city"] == "Cary" and p["dealer_state"] == "NC" and p["place_source"] == "title"
    assert place_kwargs(p) == {"dealer_city": "Cary", "dealer_state": "NC"}
    assert place_kwargs({"dealer_city": "Jasper", "dealer_state": "GA", "dealer_address": "1050 Highway 515 S", "place_source": "jsonld"}) == {
        "dealer_address": "1050 Highway 515 S", "dealer_city": "Jasper", "dealer_state": "GA"}


def test_place_from_jsonld_address():
    html = ('<script type="application/ld+json">{"@type":"AutoDealer","address":{"@type":"PostalAddress",'
            '"addressLocality":"Buford","addressRegion":"GA","postalCode":"30519"}}</script>' + "x" * 500)
    p = place_from_html(html)
    assert (p["dealer_city"], p["dealer_state"], p["dealer_zip"]) == ("Buford", "GA", "30519")


def test_place_absent():
    assert place_from_html("<title>Dealer Website</title>" + "x" * 500) == {}


def test_wp_vehicles_index_parse():
    raw = {"vehicles": [
        {"title": "Used 2021 Ram 1500 Big Horn 1C6SRFFTXMN566184 MN566184T", "link": "https://burnshonda.com/used-listings/x/", "search": ""},
        {"title": "New 2026 Honda Civic Sport 2HGFE2F51TH635109 TH635109", "link": "https://burnshonda.com/new-listings/y/", "search": ""},
        {"title": "Certified Pre-Owned 2023 Honda CR-V EX-L 7FARS6H50SE045355", "link": "https://burnshonda.com/z/", "search": ""},
        {"title": "not a car", "link": "", "search": ""},
    ]}
    assert wp_detect(raw)
    rows = wp_parse(raw, base_url="https://burnshonda.com", dealer_id="burnshonda-com", dealer_name="Burns Honda", dealer_url="https://burnshonda.com")
    assert [r["vin"] for r in rows] == ["1C6SRFFTXMN566184", "2HGFE2F51TH635109", "7FARS6H50SE045355"]
    assert rows[0]["condition"] == "Used" and rows[0]["year"] == 2021 and rows[0]["make"] == "Ram" and rows[0]["stock_number"] == "MN566184T"
    assert rows[1]["condition"] == "New" and rows[1]["make"] == "Honda" and rows[1]["_detail_url"].startswith("https://")
    assert rows[2]["condition"] == "Certified Pre-Owned" and not rows[2].get("stock_number")
    assert not wp_detect({"data": {"listings": []}})


def test_jsonld_vehicle_identity_and_price():
    html = ('<html><body><script type="application/ld+json">{"@context":"https://schema.org","@type":"Vehicle",'
            '"vehicleIdentificationNumber":"1C6SRFFTXMN566184","brand":{"@type":"Brand","name":"Ram"},"model":"1500",'
            '"vehicleConfiguration":"Big Horn","color":"Granite","vehicleInteriorColor":"Black",'
            '"mileageFromOdometer":{"@type":"QuantitativeValue","value":"48,147"},'
            '"offers":{"@type":"Offer","price":"30584","sku":"MN566184T"}}</script></body></html>' + "x" * 500)
    x = parse_vdp_extras_from_html(html, "1C6SRFFTXMN566184")
    assert x["make"] == "Ram" and x["model"] == "1500" and x["trim"] == "Big Horn"
    assert x["price"] == 30584.0 and x["mileage"] == 48147 and x["stock_number"] == "MN566184T"
    assert x["exterior_color"] == "Granite" and x["interior_color"] == "Black"
    # another car's block is ignored
    assert "make" not in parse_vdp_extras_from_html(html, "2HGFE2F51TH635109")
