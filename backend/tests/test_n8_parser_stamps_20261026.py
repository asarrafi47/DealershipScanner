"""N8.3 / N8.8 (CF-5a, CF-11, W16): per-car store stamps from the Jazel, Chapman
and CarsCommerce parsers; OneAudi stays unstamped (cannot_assess)."""
from __future__ import annotations

import json

from backend.parsers import chapman, jazel, oneaudi
from backend.parsers.carscommerce import parse as cc_parse, rooftop_of

NRH_VDP = "https://www.5starford.com/vehicle/{vin}/Used--2018--Jeep--Wrangler_JK--Sport--North_Richland_Hills--TX/"


def _jazel_html(docs: list[dict]) -> str:
    return "<html>" + "".join(f"<script>window.jzlSetVehicleInfoContext('{d['vin']}', {json.dumps(d)});</script>" for d in docs) + "</html>"


def _jdoc(vin: str, **over) -> dict:
    d = {"vin": vin, "year": 2018, "make": "Jeep", "model": "Wrangler JK", "trim": "Sport", "displayPrice": 17000, "mileage": 100,
         "newOrUsed": "used", "extColor": "Black", "intColor": "Bright White", "vdpLink": NRH_VDP.format(vin=vin)}
    d.update(over)
    return d


def test_jazel_account_name_beats_manifest_name_and_stamps_rooftop():
    html = _jazel_html([
        _jdoc("1C4AJWAG8JL890702", accountId="7", accountName="Five Star Ford North Richland Hills", accountAddress="6618 NE Loop 820 North North Richland Hills TX 76180"),
        _jdoc("1C4AJWAG8JL890703", accountId="6", accountName="Five Star Ford Carrollton", accountAddress="1635 Interstate 35 East Carrollton TX 75006",
              vdpLink=NRH_VDP.format(vin="1C4AJWAG8JL890703").replace("North_Richland_Hills", "Carrollton")),
        _jdoc("1C4AJWAG8JL890704"),
    ])
    rows = jazel.parse(html, base_url="https://www.5starford.com", dealer_id="5starford-com", dealer_name="Five Star Ford Parts")
    by = {r["vin"]: r for r in rows}
    a, b, c = by["1C4AJWAG8JL890702"], by["1C4AJWAG8JL890703"], by["1C4AJWAG8JL890704"]
    assert a["dealer_name"] == "Five Star Ford North Richland Hills"
    assert a["_rooftop"]["key"] == "7" and a["_rooftop"]["state"] == "TX" and a["_rooftop"]["zip"] == "76180"
    assert a["_rooftop"]["city"] == "North Richland Hills"
    assert b["dealer_name"] == "Five Star Ford Carrollton" and b["_rooftop"]["city"] == "Carrollton"
    # no account on the card: manifest name stays, rooftop falls back to the VDP city
    assert c["dealer_name"] == "Five Star Ford Parts"
    assert c["_rooftop"] == {"city": "North Richland Hills", "state": "TX"}


def test_jazel_colors_are_read_back_swapped():
    rows = jazel.parse(_jazel_html([_jdoc("1C4AJWAG8JL890702", extColor="Black Leather", intColor="Bright White")]),
                       dealer_id="5starford-com", dealer_name="x")
    assert rows[0]["exterior_color"] == "Bright White" and rows[0]["interior_color"] == "Black Leather"


def _cdoc(vin: str, lot: str, **over) -> dict:
    d = {"vin": vin, "year": 2025, "make": "Ford", "model": "F-150", "type": "New", "colorExt": "Red", "colorInt": "Black",
         "imageUrls": [f"https://photos.chapmanchoice.com/vehicles/{lot}/{vin}/1.jpg"], "msrp": 50000, "arkona": "X"}
    d.update(over)
    return d


def test_chapman_lot_code_stamps_rooftop():
    docs = [_cdoc("1FTFW1E50NFA00001", "CAU"), _cdoc("1FTFW1E50NFA00002", "CAY"), _cdoc("1FTFW1E50NFA00003", "CAV"),
            _cdoc("1FTFW1E50NFA00004", "CAE"), _cdoc("1FTFW1E50NFA00005", "ZZZ"), _cdoc("1FTFW1E50NFA00006", "CAU", imageUrls=[])]
    rows = {r["vin"]: r for r in chapman.parse(docs, base_url="https://www.chapmanford.com", dealer_id="chapmanfordaz-com", dealer_name="Chapman Ford")}
    own = rows["1FTFW1E50NFA00001"]["_rooftop"]
    assert own["name"] == "Chapman Ford" and own["own_site"] == "1" and own["lot_id"] == "CAU"
    assert rows["1FTFW1E50NFA00002"]["_rooftop"]["name"] == "Chapman Hyundai Scottsdale"
    assert rows["1FTFW1E50NFA00003"]["_rooftop"]["name"] == "Chapman Volkswagen Scottsdale"
    assert rows["1FTFW1E50NFA00004"]["_rooftop"]["name"] == "Chapman Dodge Chrysler Jeep Ram"  # Dodge car in the Ford feed
    assert "_rooftop" not in rows["1FTFW1E50NFA00005"]  # unknown lot: kept, unstamped
    assert "_rooftop" not in rows["1FTFW1E50NFA00006"]  # no photo, no lot


def test_chapman_dealer_without_known_own_lot_is_unstamped():
    rows = chapman.parse([_cdoc("1FTFW1E50NFA00001", "CAU")], dealer_id="somechapman-com", dealer_name="X")
    assert "_rooftop" not in rows[0]


def test_oneaudi_is_not_stamped_with_the_publishing_store():
    car = {"stockCar": {"vin": "WA1EVBF13SD035902", "titleText": "2025 Audi Q8", "cartypeText": "U",
                        "dealer": {"id": "USA07B04", "name": "Audi Huntsville", "city": "Huntsville"}}}
    payload = {"data": {"stockCarSearch": {"resultNumber": 1, "results": {"cars": [car]}}}}
    rows = oneaudi.parse(payload, dealer_id="audihuntsville-com", dealer_name="Audi Huntsville")
    assert len(rows) == 1 and "_rooftop" not in rows[0]


def _listing(**over) -> dict:
    d = {"vin": "WDDGF8AB1EA000001", "source_id": "FJAudiBeverlyHills", "make": "Audi", "model": "A4", "year": 2025, "type": "New",
         "dealer": {"id": "1", "ccid": 2309549, "city": None, "name": None, "state": None, "address": None, "zipcode": None, "location": None},
         "extra_fields": {"location_name": "Audi Beverly Hills", "feed_ccid": "209129"}}
    d.update(over)
    return d


def test_carscommerce_rooftop_falls_back_to_extra_fields_label_chain():
    rt = rooftop_of(_listing())
    assert rt and rt["name"] == "Audi Beverly Hills" and rt["source"] == "FJAudiBeverlyHills"
    rt = rooftop_of(_listing(extra_fields={"used_location_oct2026": "Basswood CDJR"}))
    assert rt and rt["name"] == "Basswood CDJR"
    assert rooftop_of(_listing(extra_fields={})) is None


def test_carscommerce_dealer_location_still_wins_and_chain_is_alt_name():
    listing = _listing()
    listing["dealer"]["location"] = "Cary, NC"
    rt = rooftop_of(listing)
    assert rt["name"] == "Cary, NC" and rt["alt_name"] == "Audi Beverly Hills"


def test_carscommerce_parse_stamps_feed_ccid():
    payload = {"data": {"ccid": 2309549, "listings": [_listing()]}}
    rows = cc_parse(payload, "https://www.mbbeverlyhills.com", "mbbeverlyhills-com")
    assert rows and rows[0]["_rooftop"]["name"] == "Audi Beverly Hills" and rows[0]["_feed_ccid"] == "209129"
