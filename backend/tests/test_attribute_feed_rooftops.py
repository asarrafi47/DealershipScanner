"""Group-feed cars belong to the rooftop that holds them, not the storefront."""

import json

from backend.scripts import attribute_feed_rooftops as afr


def test_reads_rooftop_from_carscommerce_nested_dealer():
    listing = {
        "vin": "4JGFB4KB8RA123456",
        "dealer": {
            "name": "Mercedes-Benz of Ontario",
            "address": "3787 E Guasti Rd, Ontario, CA 91761",
            "city": "Ontario",
            "state": "CA",
            "zipcode": "91761",
            "location": '<a href="https://www.mbontario.com/"> MB of Ontario</a>',
        },
    }
    rt = afr.rooftop_of(listing)
    assert rt["name"] == "Mercedes-Benz of Ontario"
    assert rt["zip"] == "91761"
    assert rt["site"] == "mbontario.com"


def test_reads_rooftop_from_flat_dealer_dot_com_row():
    rt = afr.rooftop_of({
        "vin": "1N4BL4BV5RN123456",
        "dealerName": "Nissan of Van Nuys",
        "dealerCity": "Van Nuys",
        "dealerState": "CA",
        "dealerZip": "91401-5628",
    })
    assert rt["name"] == "Nissan of Van Nuys"
    assert rt["zip"] == "91401", "ZIP+4 must reduce to the 5-digit lot ZIP"
    assert rt["site"] == ""


def test_rooftop_identity_prefers_the_rooftops_own_site():
    """Two lots of one brand in one city are distinct; their domains say so."""
    a = afr.rooftop_of({"vin": "X", "dealer": {
        "name": "Nissan", "city": "Carson", "state": "CA", "zipcode": "90745",
        "location": '<a href="https://carsonnissan.com/">x</a>'}})
    b = afr.rooftop_of({"vin": "Y", "dealer": {
        "name": "Nissan", "city": "Carson", "state": "CA", "zipcode": "90745",
        "location": '<a href="https://gardenanissan.com/">y</a>'}})
    assert afr.rooftop_key(a) != afr.rooftop_key(b)

    same = afr.rooftop_of({"vin": "Z", "dealer": {
        "name": "Nissan", "city": "Carson", "state": "CA", "zipcode": "90745",
        "location": '<a href="https://carsonnissan.com/inventory">z</a>'}})
    assert afr.rooftop_key(a) == afr.rooftop_key(same)


def test_listing_nodes_finds_vins_at_any_depth():
    body = json.dumps({"data": {"listings": [
        {"vin": "A" * 17, "dealer": {"zipcode": "90504"}},
        {"vin": "B" * 17, "dealer": {"zipcode": "92008"}},
    ]}})
    found = afr._listing_nodes([("https://feed", body)])
    assert len(found) == 2
    assert {f["vin"] for f in found} == {"A" * 17, "B" * 17}


def test_rooftop_is_unresolved_without_an_identity_match(monkeypatch):
    """Right ZIP, wrong site, wrong name -> refuse rather than place it."""
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "test-key")
    rt = {"name": "", "city": "Las Vegas", "state": "NV", "zip": "89117",
          "site": "fjimports.com", "address": "7300 W Sahara Ave"}
    monkeypatch.setattr(afr, "_places_search", lambda q, k: [{
        "location": {"latitude": 36.1, "longitude": -115.2},
        "websiteUri": "https://www.some-other-dealer.com",
        "addressComponents": [{"types": ["postal_code"], "shortText": "89999"}],
    }])
    assert afr.resolve_rooftop(rt) is None


def test_rooftop_resolves_on_site_match_and_takes_its_real_name(monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "test-key")
    rt = {"name": "", "city": "Las Vegas", "state": "NV", "zip": "89117",
          "site": "fjimports.com", "address": "7300 W Sahara Ave"}
    monkeypatch.setattr(afr, "_places_search", lambda q, k: [{
        "location": {"latitude": 36.1448, "longitude": -115.2530},
        "websiteUri": "https://www.fjimports.com/",
        "displayName": {"text": "Fletcher Jones Imports"},
        "formattedAddress": "7300 W Sahara Ave, Las Vegas, NV 89117, USA",
        "addressComponents": [
            {"types": ["postal_code"], "shortText": "89117"},
            {"types": ["locality"], "shortText": "Las Vegas"},
            {"types": ["administrative_area_level_1"], "shortText": "NV"},
        ],
    }])
    geo = afr.resolve_rooftop(rt)
    assert geo is not None
    assert geo["verified_by"] == "site"
    # Without this the rooftop registers as "7300 W Sahara Ave".
    assert geo["display_name"] == "Fletcher Jones Imports"


def test_unnamed_rooftop_is_named_after_the_group_not_the_zip():
    """
    Norm Reeves' feed gives a lot only a city and ZIP. "Cerritos 90703" is a
    placeholder that will not dedupe against the real store found later.
    """
    rt = {"name": "", "city": "Cerritos", "state": "CA", "zip": "90703",
          "site": "", "address": ""}
    geo = {"city": "Cerritos", "state": "CA", "display_name": ""}
    assert afr.rooftop_name(rt, geo, "Norm Reeves Buick/GMC") == \
        "Norm Reeves Buick/GMC (Cerritos, CA)"


def test_rooftop_name_prefers_the_feeds_own_name():
    rt = {"name": "Carson Nissan", "city": "Carson", "state": "CA", "zip": "90745",
          "site": "", "address": ""}
    geo = {"city": "Carson", "state": "CA", "display_name": "Nissan Dealer"}
    assert afr.rooftop_name(rt, geo, "Nissan of Costa Mesa") == "Carson Nissan"
