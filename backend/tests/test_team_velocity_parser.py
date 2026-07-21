"""Team Velocity handler: feed parsing + VDP image/carfax completion (offline)."""

from __future__ import annotations

import backend.parsers as parsers
from backend.parsers import team_velocity as tv

# A trimmed real Team Velocity ``/inventory-used.json`` page shape.
_FEED = {
    "totalVehicles": 2,
    "totalPages": 1,
    "pageSize": 50,
    "vehicles": [
        {
            "vin": "4T1BE46KX9U887747",
            "year": 2009,
            "make": "Toyota",
            "model": "Camry",
            "trim": "LE",
            "bodyStyle": "4D Sedan",
            "driveTrain": "FWD",
            "transmission": "Automatic",
            "fuelType": "Gasoline Fuel",
            "exteriorColor": "Sky Blue Pearl",
            "interiorColor": "Bisque",
            "engine": "Gas I4 2.4L/144",
            "engineCylinders": "4",
            "miles": 72328,
            "sellingPrice": 10269.0,
            "msrp": 15999.0,
            "cityMpg": 21,
            "highwayMpg": 31,
            "certified": False,
            "inventoryType": "Used",
            "stockNumber": "44382A",
            "dealerZip": "85260-1001",
            "imageUrls": None,
            "vdpUrl": "https://www.righttoyota.com/viewdetails/used/4t1be46kx9u887747/2009-toyota-camry",
        },
        {
            "vin": "1HGCV1F34LA000111",
            "year": 2020,
            "make": "Honda",
            "model": "Accord",
            "trim": "EX-L",
            "certified": True,
            "inventoryType": "Used",
            "sellingPrice": 24990.0,
            "miles": 30100,
            "exteriorColor": "Modern Steel",
            "engine": "I4 1.5L Turbo",
            "imageUrls": None,
            "vdpUrl": "https://www.righthonda.com/viewdetails/used/1hgcv1f34la000111/2020-honda-accord",
        },
    ],
}


def test_detect_only_team_velocity_shape():
    assert tv.detect(_FEED) is True
    assert tv.detect({"vehicles": [{"vin": "X", "title": ["a"], "trackingPricing": {}}]}) is False
    assert tv.detect({}) is False
    assert tv.detect([]) is False


def test_parse_maps_full_feed_specs():
    rows = tv.parse(
        _FEED, base_url="https://www.righttoyota.com",
        dealer_id="righttoyota-com", dealer_name="Right Toyota",
        dealer_url="https://www.righttoyota.com",
    )
    assert len(rows) == 2
    r = rows[0]
    assert r["vin"] == "4T1BE46KX9U887747"
    assert r["year"] == 2009 and r["make"] == "Toyota" and r["model"] == "Camry"
    assert r["trim"] == "LE"
    assert r["price"] == 10269.0
    assert r["msrp"] == 15999.0
    assert r["mileage"] == 72328
    assert r["exterior_color"] == "Sky Blue Pearl"
    assert r["interior_color"] == "Bisque"
    assert r["engine_description"] == "Gas I4 2.4L/144"
    assert r["cylinders"] == 4
    assert r["mpg_city"] == 21 and r["mpg_highway"] == 31
    assert r["stock_number"] == "44382A"
    assert r["condition"] == "Used" and r["is_cpo"] == 0
    assert r["source_url"].endswith("/2009-toyota-camry")
    # imageUrls:null -> placeholder seeded (non-http normalized away on image_url)
    assert not (r.get("image_url") or "").startswith("http")


def test_parse_certified_flags():
    rows = tv.parse(_FEED, base_url="https://x", dealer_id="d", dealer_name="D", dealer_url="https://x")
    accord = rows[1]
    assert accord["condition"] == "Certified"
    assert accord["is_cpo"] == 1


def test_package_parse_routes_by_shape_over_wrong_provider():
    # TV feeds are hinted "dealer_dot_com"; shape routing must still pick the TV parser.
    rows = parsers.parse(
        "dealer_dot_com", _FEED, base_url="https://www.righttoyota.com",
        dealer_id="righttoyota-com",
    )
    assert len(rows) == 2
    assert rows[0]["engine_description"] == "Gas I4 2.4L/144"


def test_extract_gallery_from_vdp_html():
    html = (
        "<div><oem-gallery-component :vin=\"'ABC'\" :imageid=\"'1'\" "
        ":photoUrls=\"'https://cdn/a.jpg,https://cdn/b.jpg,https://cdn/a.jpg,not-a-url'\">"
        "</oem-gallery-component></div>"
    )
    assert tv.extract_gallery(html) == ["https://cdn/a.jpg", "https://cdn/b.jpg"]
    assert tv.extract_gallery("<html>no gallery</html>") == []


def test_extract_gallery_renamed_element_2026_07():
    # TV renamed <oem-gallery-component> but kept the :photoUrls attribute; the
    # extractor anchors on the attribute, not the tag name (Toyota of Riverside
    # et al. — 4 dealers, ~3,867 placeholder-image cars).
    html = (
        "<vdp-gallery-widget :vin=\"'X'\" "
        ":photoUrls=\"'https://content.homenetiol.com/1/2/0x0/a.jpg,"
        "https://content.homenetiol.com/1/2/0x0/b.jpg'\"></vdp-gallery-widget>"
    )
    assert tv.extract_gallery(html) == [
        "https://content.homenetiol.com/1/2/0x0/a.jpg",
        "https://content.homenetiol.com/1/2/0x0/b.jpg",
    ]


def test_extract_carfax_report_url_ignores_badges():
    html = (
        "<img src='https://partnerstatic.carfax.com/img/valuebadge/1own.svg'>"
        "<a href='https://www.carfax.com/vehiclehistory/ar20/AbC_1-2xYz'>report</a>"
        "<script src='https://snapshot.carfax.com/latest/snapshot.js'></script>"
    )
    assert tv.extract_carfax_url(html) == "https://www.carfax.com/vehiclehistory/ar20/AbC_1-2xYz"
    assert tv.extract_carfax_url("<p>no carfax</p>") is None


def test_completion_fillable_guards():
    assert tv._image_url_fillable("/static/placeholder.svg") is True
    assert tv._image_url_fillable("https://cdn/a.jpg") is False
    assert tv._gallery_fillable('["/static/placeholder.svg"]') is True
    assert tv._gallery_fillable('["https://cdn/a.jpg"]') is False
    assert tv._carfax_fillable("") is True
    assert tv._carfax_fillable("https://www.carfax.com/vehiclehistory/x") is False


def test_complete_from_vdp_uses_fetched_html(monkeypatch):
    html = (
        "<oem-gallery-component :photoUrls=\"'https://cdn/a.jpg,https://cdn/b.jpg'\"></oem-gallery-component>"
        "<a href='https://www.carfax.com/vehiclehistory/ar20/tok123'>c</a>"
    )
    monkeypatch.setattr(tv, "fetch_vdp_html", lambda url, **kw: html)
    data = tv.complete_from_vdp("https://dealer/vdp")
    assert data["gallery"] == ["https://cdn/a.jpg", "https://cdn/b.jpg"]
    assert data["carfax_url"] == "https://www.carfax.com/vehiclehistory/ar20/tok123"


def test_request_pacer_enforces_min_interval():
    """The pacer spaces consecutive request starts by >= min_interval even when
    called from a tight loop (Team Velocity throttles un-paced bursts)."""
    import time
    pacer = tv._RequestPacer(0.05)
    starts = []
    for _ in range(4):
        pacer.wait()
        starts.append(time.monotonic())
    gaps = [b - a for a, b in zip(starts, starts[1:])]
    assert all(g >= 0.05 for g in gaps), gaps


def test_request_pacer_zero_interval_is_noop():
    pacer = tv._RequestPacer(0.0)
    import time
    t0 = time.monotonic()
    for _ in range(5):
        pacer.wait()
    assert time.monotonic() - t0 < 0.05


def test_known_tv_dealer_registry():
    assert tv.is_team_velocity_dealer("markkia-com") is True
    assert tv.is_team_velocity_dealer("some-other-com") is False


def test_detect_accepts_raw_json_feed_body():
    """The delta replay passes the UNPARSED feed body to detect(); it must parse
    raw JSON text/bytes, else every TV dealer fails detection and its photos are
    never recovered (only 3 of 18 were in the hardcoded registry)."""
    body = '{"vehicles":[{"vin":"X","imageUrls":null,"sellingPrice":100,"vdpUrl":"u","engineCylinders":4}]}'
    assert tv.detect(body) is True
    assert tv.detect(body.encode()) is True
    assert tv.detect(tv.json.loads(body)) is True
    # Non-TV / garbage must still be rejected.
    assert tv.detect("not json at all") is False
    assert tv.detect('{"vehicles":[{"foo":"bar"}]}') is False
