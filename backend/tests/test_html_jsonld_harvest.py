"""Tests for the generic schema.org Vehicle JSON-LD harvesters.

Network-free: HTML is supplied as fixtures. Covers the multi-vehicle SRP
harvester (``harvest_vehicles_from_html``) that backs the universal browser-free
fallback for untemplated-but-reachable dealers.
"""
from __future__ import annotations

from backend.scanner.html_jsonld_harvest import (
    harvest_fields_from_html,
    harvest_vehicles_from_html,
)

# A server-rendered SRP with two schema.org Vehicle nodes (one @graph-wrapped,
# one bare) plus a non-vehicle node that must be ignored.
_SRP_HTML = """
<html><head>
<script type="application/ld+json">
{"@context":"https://schema.org","@type":"WebSite","name":"Luxury Motors"}
</script>
<script type="application/ld+json">
{"@context":"https://schema.org","@graph":[
  {"@type":"Car","vehicleIdentificationNumber":"1HGCM82633A004352",
   "modelDate":"2022","brand":{"@type":"Brand","name":"Honda"},
   "model":"Accord","vehicleConfiguration":"EX-L",
   "mileageFromOdometer":{"@type":"QuantitativeValue","value":"18543"},
   "color":"Blue","vehicleInteriorColor":"Black",
   "image":["https://cdn.example/a1.jpg","https://cdn.example/a2.jpg"],
   "offers":{"@type":"Offer","price":"28995","priceCurrency":"USD"}}
]}
</script>
</head><body>
<script type="application/ld+json">
{"@context":"https://schema.org","@type":"Vehicle",
 "vin":"5xyktca69fg566472","name":"2015 Kia Sorento",
 "brand":"Kia","model":{"@type":"Model","name":"Sorento"},
 "offers":{"@type":"AggregateOffer","lowPrice":14500}}
</script>
</body></html>
"""


def test_harvest_vehicles_returns_one_per_vin():
    vehicles = harvest_vehicles_from_html(_SRP_HTML)
    vins = {v["vin"] for v in vehicles}
    assert vins == {"1HGCM82633A004352", "5XYKTCA69FG566472"}  # VIN upper-cased


def test_harvest_vehicles_extracts_fields():
    by_vin = {v["vin"]: v for v in harvest_vehicles_from_html(_SRP_HTML)}
    honda = by_vin["1HGCM82633A004352"]
    assert honda["year"] == 2022
    assert honda["make"] == "Honda"
    assert honda["model"] == "Accord"
    assert honda["trim"] == "EX-L"
    assert honda["mileage"] == 18543
    assert honda["price"] == 28995.0
    assert honda["exterior_color"] == "Blue"
    assert honda["interior_color"] == "Black"
    assert honda["images"] == ["https://cdn.example/a1.jpg", "https://cdn.example/a2.jpg"]


def test_harvest_vehicles_price_from_aggregate_offer():
    by_vin = {v["vin"]: v for v in harvest_vehicles_from_html(_SRP_HTML)}
    assert by_vin["5XYKTCA69FG566472"]["price"] == 14500.0


def test_harvest_vehicles_skips_vinless_nodes():
    html = """
    <script type="application/ld+json">
    {"@type":"Vehicle","name":"No VIN here","model":"Ghost"}
    </script>
    """
    assert harvest_vehicles_from_html(html) == []


def test_harvest_vehicles_dedupes_repeated_vin():
    html = _SRP_HTML + _SRP_HTML  # same page twice
    vins = [v["vin"] for v in harvest_vehicles_from_html(html)]
    assert len(vins) == len(set(vins)) == 2


def test_harvest_vehicles_empty_html():
    assert harvest_vehicles_from_html("") == []
    assert harvest_vehicles_from_html("<html>no jsonld</html>") == []


def test_single_vdp_harvester_still_works():
    # The existing VDP-level harvester must not regress with the new helpers.
    fields = harvest_fields_from_html(_SRP_HTML)
    assert fields["vin"] == "1HGCM82633A004352"
    assert fields["images"]  # gallery collected
