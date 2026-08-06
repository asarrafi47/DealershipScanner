"""Rooftop attribution: a vehicle belongs to the store that sells it, or to none.

Dealer-group platforms answer every rooftop's site with the WHOLE group's
inventory, so the host that was queried is not evidence of who sells the car.
These cases are cut from real payloads captured on 2026-08-03:

* CarsCommerce account 5379783 (audifletcherjones.com) — nine rooftops behind
  one host; ``dealer.location`` names the store, ``dealer.website`` is null.
* Typesense collection ``vehicles-ACU251637``
  (shottenkirkacurahuntsville.com) — three rooftops; ``dealer.url`` is rewritten
  to whichever host asked, so it is identical on the siblings' rows too.
* Team Velocity ``mazdaofknoxville.com/inventory-used.json`` — 48 of the first
  50 vehicles were Airport Honda's.
"""
from __future__ import annotations

import pytest

from backend.parsers import parse, resolve_rooftop_attribution
from backend.parsers.carscommerce import parse as parse_carscommerce
from backend.parsers.dealer_dot_com import parse as parse_dealer_dot_com
from backend.parsers.team_velocity import parse as parse_team_velocity
from backend.parsers.typesense import parse as parse_typesense
from backend.scanner.database import drop_unattributable_vehicles


def _vin(n: int) -> str:
    return f"WA1ANAFY4L20{n:05d}"


# ── payload builders ────────────────────────────────────────────────────────

def _cc_listing(vin: str, location: str | None) -> dict:
    return {
        "vin": vin,
        "year": 2023,
        "make": "Audi",
        "model": "Q5",
        "trim": "Premium",
        "stock": vin[-6:],
        "mileage": 12000,
        "pricing": {"low_price": 41995},
        "styles": {"exterior_color": "Black", "interior_color": "Black"},
        "mechanical": {"engine": "2.0L I4", "drivetrain": "AWD"},
        "media": {"images": ["https://img.example/1.jpg"]},
        "dealer": {"id": "13395", "ccid": 5379783, "name": None, "website": None,
                   "location": location},
    }


def _cc_payload(rows: list[tuple[str, str | None]]) -> dict:
    return {"data": {"ccid": 5379783,
                     "listings": [_cc_listing(v, loc) for v, loc in rows]}}


def _ts_payload(rows: list[tuple[str, str, str]]) -> dict:
    """(vin, dealerName, dealer.address) — dealer.url is the queried host for all."""
    return {"results": [{"found": len(rows), "hits": [
        {"document": {
            "vin": vin, "yr": 2022, "make": "Honda", "model": "CR-V",
            "sellingPrice": 28995, "mileage": 20000,
            "imageUrls": ["https://img.example/1.jpg"],
            "dealerName": name,
            "dealer": {"name": name, "address": addr, "city": "Huntsville",
                       "url": "www.shottenkirkacurahuntsville.com"},
        }} for vin, name, addr in rows]}]}


def _tv_payload(rows: list[tuple[str, str, str]]) -> dict:
    """(vin, dealerName, dealerZip) — dealerDomain is the queried host for all."""
    return {"totalVehicles": len(rows), "totalPages": 1, "nextPage": None,
            "vehicles": [{
                "vin": vin, "year": 2021, "make": "Mazda", "model": "CX-5",
                "sellingPrice": 24995, "miles": 30000, "imageUrls": None,
                "dealerName": name, "dealerDomain": "https://www.mazdaofknoxville.com",
                "dealerCity": "Knoxville", "dealerState": "TN", "dealerZip": zipc,
            } for vin, name, zipc in rows]}


# ── group feed WITH a per-vehicle rooftop field ─────────────────────────────

def test_carscommerce_group_feed_keeps_only_the_scanned_rooftop():
    payload = _cc_payload([
        (_vin(1), "Audi Fletcher Jones"),
        (_vin(2), "Audi Fletcher Jones"),
        (_vin(3), "Mercedes-Benz of Beverly Hills"),
        (_vin(4), "Fletcher Jones Motorcars Newport"),
    ])
    refused: list[dict] = []
    rows = parse("carscommerce", payload, "https://www.audifletcherjones.com",
                 "audifletcherjones-com", "Audi Fletcher Jones",
                 "https://www.audifletcherjones.com", rejected_out=refused)
    assert sorted(r["vin"] for r in rows) == [_vin(1), _vin(2)]
    assert sorted(r["vin"] for r in refused) == [_vin(3), _vin(4)]
    assert {r["_rooftop_reject"] for r in refused} == {"sibling_rooftop"}


def test_typesense_group_feed_ignores_the_rewritten_dealer_url():
    # Every document repeats the queried host in dealer.url — the field that
    # made the previous guard a no-op. Only dealerName tells the stores apart.
    payload = _ts_payload([
        (_vin(11), "Shottenkirk Acura Huntsville", "2402 Leeman Ferry Rd SW"),
        (_vin(12), "Shottenkirk Honda Huntsville", "6591 Hwy 72 W"),
        (_vin(13), "Shottenkirk Honda Decatur", "735 Beltline Rd SW"),
    ])
    refused: list[dict] = []
    rows = parse("typesense", payload, "https://www.shottenkirkacurahuntsville.com",
                 "shottenkirkacurahuntsville-com", "Shottenkirk Acura Huntsville",
                 "https://www.shottenkirkacurahuntsville.com", rejected_out=refused)
    assert [r["vin"] for r in rows] == [_vin(11)]
    assert sorted(r["vin"] for r in refused) == [_vin(12), _vin(13)]


def test_team_velocity_group_feed_keeps_only_the_scanned_rooftop():
    payload = _tv_payload([
        (_vin(21), "Airport Honda", "37701"),
        (_vin(22), "Airport Honda", "37701"),
        (_vin(23), "Mazda of Knoxville", "37922"),
    ])
    refused: list[dict] = []
    rows = parse("team_velocity", payload, "https://www.mazdaofknoxville.com",
                 "mazdaofknoxville-com", "Mazda of Knoxville",
                 "https://www.mazdaofknoxville.com", rejected_out=refused)
    assert [r["vin"] for r in rows] == [_vin(23)]
    assert sorted(r["vin"] for r in refused) == [_vin(21), _vin(22)]


def test_group_feed_with_unidentifiable_store_refuses_everything():
    # BMW of Murrieta's account pools Rick Hendrick used inventory from NC/TX
    # under address blocks; none of them is this store, so none may be kept.
    payload = _cc_payload([
        (_vin(31), "10720 Northlake Auto Plaza Blvd.<br/>Charlotte, NC 28269<br/>(704) 555-0100"),
        (_vin(32), "2601 N Central EXPY<br/>McKinney, TX 75071<br/>(972) 555-0100"),
        (_vin(33), "41430 Auto MALL PKWY<br/>Murrieta, CA 92562<br/>(951) 555-0100"),
    ])
    refused: list[dict] = []
    rows = parse("carscommerce", payload, "https://www.bmwofmurrieta.com",
                 "bmwofmurrieta-com", "BMW of Murrieta",
                 "https://www.bmwofmurrieta.com", rejected_out=refused)
    assert rows == []
    assert len(refused) == 3
    assert {r["_rooftop_reject"] for r in refused} == {"target_rooftop_unidentified"}


# ── group feed WITHOUT a per-vehicle rooftop field ──────────────────────────

def test_single_store_feed_is_untouched():
    payload = _cc_payload([(_vin(41), "Toyota of Orange"), (_vin(42), "Toyota of Orange")])
    refused: list[dict] = []
    rows = parse("carscommerce", payload, "https://www.toyotaoforange.com",
                 "toyotaoforange-com", "Toyota of Orange",
                 "https://www.toyotaoforange.com", rejected_out=refused)
    assert len(rows) == 2 and refused == []


def test_feed_without_any_rooftop_field_is_kept_whole():
    # dealer.location is null on most CarsCommerce accounts. No evidence means
    # no decision to make: the gate must not invent one and drop the dealer.
    payload = _cc_payload([(_vin(51), None), (_vin(52), None), (_vin(53), None)])
    refused: list[dict] = []
    rows = parse("carscommerce", payload, "https://www.billluke.com",
                 "billluke-com", "Bill Luke Chrysler Jeep Dodge RAM",
                 "https://www.billluke.com", rejected_out=refused)
    assert len(rows) == 3 and refused == []


@pytest.mark.parametrize("tag", ["none", "loaner", "ford,pal", "28900.00", "2keys|gold2"])
def test_inventory_tags_in_the_rooftop_field_are_not_rooftops(tag):
    # Some accounts park option codes and prices in dealer.location. Treating
    # those as storefronts would drop a healthy single-store dealer to zero.
    payload = _cc_payload([(_vin(61), tag), (_vin(62), "none"), (_vin(63), None)])
    refused: list[dict] = []
    rows = parse("carscommerce", payload, "https://www.woodyandersonford.com",
                 "woodyandersonford-com", "Woody Anderson Ford",
                 "https://www.woodyandersonford.com", rejected_out=refused)
    assert len(rows) == 3 and refused == []


# ── matching rules ──────────────────────────────────────────────────────────

def _rows(*labels: str) -> list[dict]:
    return [{"vin": _vin(70 + i), "_rooftop": {"key": lbl, "name": lbl, "site": ""}}
            for i, lbl in enumerate(labels)]


def test_target_matched_when_the_feed_spells_the_store_out_more_fully():
    kept, refused = resolve_rooftop_attribution(
        _rows("Welborn Chevrolet GMC of Rome", "Welborn Nissan of Rome",
              "Welborn GMC Cadillac of Cartersville"),
        dealer_id="welbornchevroletofrome-com", dealer_name="Welborn Chevrolet of Rome",
        dealer_url="https://www.welbornchevroletofrome.com",
    )
    assert [r["vin"] for r in kept] == [_vin(70)]
    assert len(refused) == 2


def test_target_matched_by_host_when_roster_name_is_a_trading_name():
    kept, refused = resolve_rooftop_attribution(
        _rows("East Tennessee Dodge", "Dave Kirk Chevy GMC", "Cookeville Honda"),
        dealer_id="davekirk-com", dealer_name="Dave Kirk Chevrolet GMC",
        dealer_url="https://www.davekirk.com",
    )
    assert [r["vin"] for r in kept] == [_vin(71)]
    assert len(refused) == 2


def test_two_plausible_rooftops_are_a_refusal_not_a_coin_flip():
    kept, refused = resolve_rooftop_attribution(
        _rows("Norm Reeves Honda Cerritos", "Norm Reeves Honda Irvine"),
        dealer_id="normreeveshonda-com", dealer_name="Norm Reeves Honda",
        dealer_url="https://www.normreeveshonda.com",
    )
    assert kept == []
    assert {r["_rooftop_reject"] for r in refused} == {"target_rooftop_unidentified"}


def test_unstamped_rows_inside_a_group_feed_are_refused():
    rows = _rows("Audi Fletcher Jones", "Mercedes-Benz of Beverly Hills")
    rows.append({"vin": _vin(99)})  # no rooftop evidence at all
    kept, refused = resolve_rooftop_attribution(
        rows, dealer_id="audifletcherjones-com", dealer_name="Audi Fletcher Jones",
        dealer_url="https://www.audifletcherjones.com",
    )
    assert [r["vin"] for r in kept] == [_vin(70)]
    assert {r["_rooftop_reject"] for r in refused} == {
        "sibling_rooftop", "unstamped_row_in_group_feed"}


def test_a_page_holding_only_a_sibling_is_refused():
    # A paginated group feed hands out whole pages of one sibling's cars; that
    # page looks like a single-store payload unless the name is checked.
    kept, refused = resolve_rooftop_attribution(
        _rows("Airport Honda", "Airport Honda"),
        dealer_id="mazdaofknoxville-com", dealer_name="Mazda of Knoxville",
        dealer_url="https://www.mazdaofknoxville.com",
    )
    assert kept == []
    assert {r["_rooftop_reject"] for r in refused} == {"single_rooftop_is_not_this_store"}


def test_a_page_holding_only_this_store_is_kept():
    kept, refused = resolve_rooftop_attribution(
        _rows("Mazda of Knoxville", "Mazda of Knoxville"),
        dealer_id="mazdaofknoxville-com", dealer_name="Mazda of Knoxville",
        dealer_url="https://www.mazdaofknoxville.com",
    )
    assert len(kept) == 2 and refused == []


def test_a_single_unnamed_rooftop_is_not_evidence_against_the_store():
    # Address-only labels say nothing about whose store it is; refusing here
    # would zero out J&J Ford, whose feed labels every row with its own address.
    kept, refused = resolve_rooftop_attribution(
        _rows("1493 Highway 64 West<br/>Hayesville, NC 28904<br/>828-389-6300"),
        dealer_id="jjfordhayesville-com", dealer_name="J&J Ford",
        dealer_url="https://www.jjfordhayesville.com",
    )
    assert len(kept) == 1 and refused == []


def test_one_store_filing_under_two_addresses_stays_one_store():
    # Norm Reeves Honda Cerritos publishes the same street address with two
    # phone numbers; splitting on the phone line would zero the dealer out.
    kept, refused = resolve_rooftop_attribution(
        _rows("18500 Studebaker Rd<br/>Cerritos, CA 90703<br/>(562) 809-3000",
              "18500 Studebaker Rd<br/>Cerritos, CA 90703<br/>(888) 867-1000"),
        dealer_id="normreeveshondacerritos-com", dealer_name="Norm Reeves Honda Superstore",
        dealer_url="https://www.normreeveshondacerritos.com",
    )
    assert len(kept) == 2 and refused == []


# ── write boundary ──────────────────────────────────────────────────────────

def test_caller_without_rejected_out_gets_marked_rows_that_storage_refuses():
    # The recipe replay counts VINs from parse()'s return value to decide
    # whether a page was the last one, so the siblings must still be returned —
    # marked. Nothing marked may survive the write boundary.
    payload = _cc_payload([
        (_vin(70), "Audi Fletcher Jones"),
        (_vin(71), "Mercedes-Benz of Beverly Hills"),
    ])
    rows = parse("carscommerce", payload, "https://www.audifletcherjones.com",
                 "audifletcherjones-com", "Audi Fletcher Jones",
                 "https://www.audifletcherjones.com")
    assert sorted(r["vin"] for r in rows) == [_vin(70), _vin(71)]
    kept, dropped = drop_unattributable_vehicles(rows)
    assert [r["vin"] for r in kept] == [_vin(70)]
    assert dropped == 1


def test_upsert_boundary_drops_rows_marked_by_the_gate():
    kept, dropped = drop_unattributable_vehicles([
        {"vin": _vin(80)},
        {"vin": _vin(81), "_rooftop_reject": "sibling_rooftop"},
        {"vin": _vin(82), "_rooftop_reject": "target_rooftop_unidentified"},
    ])
    assert [r["vin"] for r in kept] == [_vin(80)]
    assert dropped == 2


# ── parsers still emit the evidence the gate needs ──────────────────────────

def test_parsers_stamp_the_rooftop_they_read():
    cc = parse_carscommerce(_cc_payload([(_vin(90), "Audi Fletcher Jones")]),
                            "https://x.com", "d", "Audi Fletcher Jones", "https://x.com")
    assert cc[0]["_rooftop"]["name"] == "Audi Fletcher Jones"

    ts = parse_typesense(_ts_payload([(_vin(91), "Shottenkirk Honda Decatur", "735 Beltline")]),
                         base_url="https://x.com", dealer_id="d",
                         dealer_name="Shottenkirk Acura Huntsville", dealer_url="https://x.com")
    assert ts[0]["_rooftop"]["name"] == "Shottenkirk Honda Decatur"

    tv = parse_team_velocity(_tv_payload([(_vin(92), "Airport Honda", "37701")]),
                             base_url="https://x.com", dealer_id="d",
                             dealer_name="Mazda of Knoxville", dealer_url="https://x.com")
    assert tv[0]["_rooftop"]["name"] == "Airport Honda"
    assert tv[0]["_rooftop"]["zip"] == "37701"


# ── address-block rooftops: the store is named only by where it stands ───────
#
# CarsCommerce account 5379783 as bmwofmurrieta.com serves it: 52 rooftops on a
# single page, EVERY one an unnamed postal address. The name and host tiers have
# nothing to read, so before the address tiers existed this payload resolved to
# "unidentified" and refused all 1,941 of the dealer's live cars.

_MURRIETA = "41430 Auto Mall Pkwy<br/>Murrieta, CA 92562<br/>(951) 553-2000"
_CHARLOTTE = "1301 N Hendrick Dr<br/>Charlotte, NC 28206<br/>(704) 555-1212"
_DATE_ST = "41300 Date Street<br/>Murrieta, CA 92562"


def _addr_rows(*labels: str) -> list[dict]:
    return [{"vin": _vin(200 + i), "_rooftop": {"key": lab}} for i, lab in enumerate(labels)]


def test_street_address_identifies_the_store_among_unnamed_rooftops():
    kept, rejected = resolve_rooftop_attribution(
        _addr_rows(_MURRIETA, _CHARLOTTE, _DATE_ST),
        dealer_id="bmwofmurrieta-com", dealer_name="BMW of Murrieta",
        dealer_url="https://www.bmwofmurrieta.com",
        dealer_address="41430 Auto Mall Pkwy", dealer_city="Murrieta",
        dealer_state="CA", dealer_zip="92562",
    )
    assert [r["vin"] for r in kept] == [_vin(200)]
    assert {r["_rooftop_reject"] for r in rejected} == {"sibling_rooftop"}


def test_city_state_identifies_the_store_when_it_is_the_only_one_in_town():
    kept, _ = resolve_rooftop_attribution(
        _addr_rows(_MURRIETA, _CHARLOTTE),
        dealer_id="bmwofmurrieta-com", dealer_name="BMW of Murrieta",
        dealer_url="https://www.bmwofmurrieta.com",
        dealer_city="Murrieta", dealer_state="CA",
    )
    assert [r["vin"] for r in kept] == [_vin(200)]


def test_two_rooftops_in_our_own_city_is_a_refusal_not_a_guess():
    """No street address in the roster and two of the group's stores in our
    city: the gate must not pick one. It refuses — and the reason it records
    must be the one that is NOT safe to un-list on (see the test below)."""
    kept, rejected = resolve_rooftop_attribution(
        _addr_rows(_MURRIETA, _DATE_ST),
        dealer_id="bmwofmurrieta-com", dealer_name="BMW of Murrieta",
        dealer_url="https://www.bmwofmurrieta.com",
        dealer_city="Murrieta", dealer_state="CA",
    )
    assert kept == []
    assert {r["_rooftop_reject"] for r in rejected} == {"target_rooftop_unidentified"}


# ── refusing to store is not the same as refusing to keep listed ────────────

def test_unidentified_refusals_are_never_evidence_to_unlist_a_car():
    """``delta_scan`` un-lists a VIN only when the feed positively assigns it to
    a DIFFERENT rooftop. "I could not tell which rooftop is this store" is a
    statement about the roster, not about any car — acting on it retires a live
    dealer's whole inventory because a database field is blank.

    This mirrors ``_EVIDENCE_BACKED_REJECTS`` in backend/scanner/delta_scan.py.
    """
    evidence_backed = {"sibling_rooftop", "single_rooftop_is_not_this_store"}

    _, rejected = resolve_rooftop_attribution(
        _addr_rows(_MURRIETA, _DATE_ST),
        dealer_id="bmwofmurrieta-com", dealer_name="BMW of Murrieta",
        dealer_url="https://www.bmwofmurrieta.com",
        dealer_city="Murrieta", dealer_state="CA",
    )
    assert rejected and not [r for r in rejected if r["_rooftop_reject"] in evidence_backed]

    # The converse: a rooftop identified by STREET (identity, not locality) does
    # yield un-listable siblings. City/state alone deliberately does not — see
    # test_city_state_match_refuses_siblings_but_never_unlists_them.
    _, sibling_rejected = resolve_rooftop_attribution(
        _addr_rows(_MURRIETA, _CHARLOTTE),
        dealer_id="bmwofmurrieta-com", dealer_name="BMW of Murrieta",
        dealer_url="https://www.bmwofmurrieta.com",
        dealer_address="41430 Auto Mall Pkwy", dealer_city="Murrieta",
        dealer_state="CA", dealer_zip="92562",
    )
    assert [r for r in sibling_rejected if r["_rooftop_reject"] in evidence_backed]


def test_a_single_unnamed_rooftop_never_costs_a_dealer_its_inventory():
    """One address block and no way to confirm it is ours: an ordinary
    single-store dealer on a group platform. Keep the rows."""
    kept, rejected = resolve_rooftop_attribution(
        _addr_rows(_CHARLOTTE),
        dealer_id="somedealer-com", dealer_name="Some Dealer",
        dealer_url="https://www.somedealer.com",
    )
    assert len(kept) == 1 and rejected == []


# ── Dealer.com (DDC ws-inv-data): accountId + the body's accounts map ─────────
#
# Captured 2026-08-03 by replaying the stored recipes for crownlexus.com,
# bmwofmonrovia.net and lexuscarlsbad.com. Every vehicle carries an
# ``accountId``; the body carries a sibling ``accounts`` object keyed by that id
# holding the rooftop's display name, its own url and a full postal address.
# The map is PAGE-scoped — it lists only the rooftops whose cars are on that page.
#
# Measured on those replays:
#   crownlexus.com     976 VINs / 12 rooftops, only 159 ``soniccrownlexus``
#   bmwofmonrovia.net 1029 VINs / 12 rooftops, only 220 ``sonicbmwmonrovia``
#   lexuscarlsbad.com  543 VINs /  2 rooftops — 303 ``lexuseofescondido`` vs
#                      240 ``lexusofcarlsbad``, i.e. the modal rooftop is the
#                      SIBLING. "Most rows win" gets this dealer exactly wrong.

_DDC_ACCOUNTS = {
    "soniccrownlexus": {
        "name": "Crown Lexus", "phone": "800-692-3790", "url": "www.crownlexus.com",
        "address": {"accountName": "Crown Lexus", "city": "Ontario", "country": "US",
                    "firstLineAddress": "1125 South Kettering Drive",
                    "secondLineAddress": "", "postalCode": "91761", "state": "CA"}},
    "sonicbmwmonrovia": {
        "name": "BMW of Monrovia", "url": "www.bmwofmonrovia.net",
        "address": {"accountName": "BMW of Monrovia", "city": "Monrovia", "country": "US",
                    "firstLineAddress": "1425 South Mountain Avenue",
                    "secondLineAddress": "", "postalCode": "91016", "state": "CA"}},
    "sonicbeverlyhillsbmw": {
        "name": "Beverly Hills BMW", "url": "www.bmwofbeverlyhills.com",
        "address": {"accountName": "Beverly Hills BMW", "city": "Los Angeles", "country": "US",
                    "firstLineAddress": "5070 Wilshire Blvd",
                    "secondLineAddress": "", "postalCode": "90036", "state": "CA"}},
    "lexusofcarlsbad": {
        "name": "Lexus Carlsbad", "url": "www.lexuscarlsbad.com",
        "address": {"accountName": "Lexus Carlsbad", "city": "Carlsbad", "country": "US",
                    "firstLineAddress": "5444 Paseo Del Norte",
                    "secondLineAddress": "", "postalCode": "92008", "state": "CA"}},
    "lexuseofescondido": {
        "name": "Lexus Escondido", "url": "www.lexusescondido.com",
        "address": {"accountName": "Lexus Escondido", "city": "Escondido", "country": "US",
                    "firstLineAddress": "1205 Auto Park Way",
                    "secondLineAddress": "", "postalCode": "92029", "state": "CA"}},
    "mountainvalleymotorscllc": {
        "name": "Mountain Valley Chrysler Dodge Jeep Ram FIAT",
        "url": "www.mountainvalleymotors.net",
        "address": {"accountName": "Mountain Valley Chrysler Dodge Jeep Ram FIAT",
                    "city": "Andrews", "country": "US",
                    "firstLineAddress": "1710 Andrews Rd",
                    "secondLineAddress": "", "postalCode": "28901", "state": "NC"}},
}


def _ddc_vehicle(vin: str, account_id: str) -> dict:
    return {
        "vin": vin, "stockNumber": vin[-8:], "accountId": account_id,
        "year": 2025, "make": "LEXUS", "model": "ES 300h", "trim": "Base",
        "bodyStyle": "Sedan", "condition": "Certified Pre-Owned", "certified": True,
        "type": "used", "status": "live", "offSite": account_id != "soniccrownlexus",
        "title": ["2025 LEXUS", "ES 300h Base"],
        "link": f"/certified/LEXUS/2025-LEXUS-ES-300h-{vin}.htm",
        "trackingPricing": {"internetPrice": "$42,673", "salePrice": "42673",
                            "askingPrice": "$42,588", "msrp": "0"},
        "trackingAttributes": [{"name": "odometer", "value": "27419"},
                               {"name": "exteriorColor", "value": "Caviar",
                                "normalizedValue": "Black"}],
        "images": [{"uri": f"https://pictures.dealer.com/s/{account_id}/0275/x.jpg",
                    "provider": "ACTUAL_PHOTO"}],
    }


def _ddc_payload(rows: list[tuple[str, str]]) -> dict:
    """A ws-inv-data body: ``inventory`` plus the page-scoped ``accounts`` map."""
    used = {aid for _v, aid in rows}
    return {
        "pageInfo": {"totalCount": len(rows)},
        "inventory": [_ddc_vehicle(v, aid) for v, aid in rows],
        "accounts": {aid: _DDC_ACCOUNTS[aid] for aid in used},
        "facets": [], "filters": [],
    }


def _ddc_parse(payload: dict, dealer_id: str, dealer_name: str, dealer_url: str,
               rejected_out: list | None = None):
    return parse("dealer_dot_com", payload, base_url=dealer_url, dealer_id=dealer_id,
                 dealer_name=dealer_name, dealer_url=dealer_url, rejected_out=rejected_out)


def test_dealer_dot_com_stamps_the_rooftop_its_accounts_map_names():
    rows = parse_dealer_dot_com(
        _ddc_payload([(_vin(300), "soniccrownlexus"), (_vin(301), "soniccrownlexus"),
                      (_vin(302), "soniccrownlexus")]),
        base_url="https://www.crownlexus.com", dealer_id="crownlexus-com",
        dealer_name="Crown Lexus", dealer_url="https://www.crownlexus.com")
    assert rows[0]["_rooftop"] == {
        "key": "soniccrownlexus", "name": "Crown Lexus", "site": "www.crownlexus.com",
        "address": "1125 South Kettering Drive", "city": "Ontario", "state": "CA",
        "zip": "91761",
    }
    # _lot_location is still populated: post-scan code reads it.
    assert rows[0].get("_lot_location")


def test_dealer_dot_com_group_feed_keeps_only_the_scanned_rooftop():
    """crownlexus.com's own feed: 12 rooftops, 24 Mercedes under a Lexus store."""
    rejected: list = []
    kept = _ddc_parse(
        _ddc_payload([(_vin(310), "soniccrownlexus"),
                      (_vin(311), "sonicbeverlyhillsbmw"),
                      (_vin(312), "sonicbmwmonrovia"),
                      (_vin(313), "soniccrownlexus")]),
        "crownlexus-com", "Crown Lexus", "https://www.crownlexus.com", rejected)
    assert [r["vin"] for r in kept] == [_vin(310), _vin(313)]
    assert {r["_rooftop_reject"] for r in rejected} == {"sibling_rooftop"}


def test_the_modal_rooftop_is_not_the_answer():
    """lexuscarlsbad.com serves MORE Escondido cars than its own. Row counts are
    not evidence; the rooftop that IS this store is."""
    rejected: list = []
    kept = _ddc_parse(
        _ddc_payload([(_vin(320), "lexuseofescondido"),
                      (_vin(321), "lexuseofescondido"),
                      (_vin(322), "lexuseofescondido"),
                      (_vin(323), "lexusofcarlsbad")]),
        "lexuscarlsbad-com", "Lexus Carlsbad", "https://www.lexuscarlsbad.com", rejected)
    assert [r["vin"] for r in kept] == [_vin(323)]
    assert len(rejected) == 3


def test_a_ddc_page_holding_only_a_sibling_is_refused():
    """The accounts map is page-scoped, so a group feed hands out whole pages of
    one sibling's cars — which must not read as "this is a single-store feed"."""
    rejected: list = []
    kept = _ddc_parse(
        _ddc_payload([(_vin(330), "lexuseofescondido"), (_vin(331), "lexuseofescondido"),
                      (_vin(332), "lexuseofescondido")]),
        "lexuscarlsbad-com", "Lexus Carlsbad", "https://www.lexuscarlsbad.com", rejected)
    assert kept == []
    assert {r["_rooftop_reject"] for r in rejected} == {"single_rooftop_is_not_this_store"}


def test_a_ddc_page_holding_only_this_store_is_kept():
    rejected: list = []
    kept = _ddc_parse(
        _ddc_payload([(_vin(340), "lexusofcarlsbad"), (_vin(341), "lexusofcarlsbad"),
                      (_vin(342), "lexusofcarlsbad")]),
        "lexuscarlsbad-com", "Lexus Carlsbad", "https://www.lexuscarlsbad.com", rejected)
    assert len(kept) == 3 and rejected == []


def test_the_rooftop_slug_identifies_a_store_its_display_name_does_not():
    """mountainvalleymotors.net's single rooftop is named "Mountain Valley
    Chrysler Dodge Jeep Ram FIAT" while the roster says "Mountain Valley Motors,
    Inc." — no name tier matches, and one rooftop means no host evidence either.
    The accountId ``mountainvalleymotorscllc`` carries the host stem, and without
    reading it this live dealer's 100 cars are refused AND un-listed."""
    rejected: list = []
    kept = _ddc_parse(
        _ddc_payload([(_vin(350), "mountainvalleymotorscllc"),
                      (_vin(351), "mountainvalleymotorscllc"),
                      (_vin(352), "mountainvalleymotorscllc")]),
        "mountainvalleymotors-net", "Mountain Valley Motors, Inc.",
        "https://www.mountainvalleymotors.net", rejected)
    assert len(kept) == 3 and rejected == []


def test_the_feeds_own_rooftop_id_beats_a_shared_display_name():
    """Two rooftops, distinct accountIds. Grouping is by the id the feed keys the
    car on, so the sibling stays separable even where names would collide."""
    rows = [{"vin": _vin(360), "_rooftop": {"key": "sonicbmwmonrovia", "name": "BMW"}},
            {"vin": _vin(361), "_rooftop": {"key": "sonicbeverlyhillsbmw", "name": "BMW"}}]
    kept, rejected = resolve_rooftop_attribution(
        rows, dealer_id="bmwofmonrovia-net", dealer_name="BMW of Monrovia",
        dealer_url="https://www.bmwofmonrovia.net")
    assert [r["vin"] for r in kept] == [_vin(360)]
    assert [r["_rooftop_reject"] for r in rejected] == ["sibling_rooftop"]


@pytest.mark.parametrize("tag", ["loaner", "none", "ford,pal", "28900.00", "certified"])
def test_an_inventory_tag_never_becomes_a_rooftop_slug(tag):
    """CarsCommerce copies its free-text ``dealer.location`` into BOTH key and
    name, so a tag parked in that field arrives as a label in both slots and is
    never read as an identifier. No list of known-bad words is involved."""
    from backend.parsers import _rooftop_slug

    assert _rooftop_slug({"key": tag, "name": tag}) == ""


def test_a_rooftop_slug_is_read_only_from_the_identifier_slot():
    from backend.parsers import _rooftop_slug

    assert _rooftop_slug({"key": "soniccrownlexus", "name": "Crown Lexus"}) == "soniccrownlexus"
    assert _rooftop_slug({"key": "soniccrownlexus"}) == "soniccrownlexus"
    # A prose label is not an identifier, whichever slot it sits in.
    assert _rooftop_slug({"key": "Crown Lexus", "name": "Crown Lexus"}) == ""


def test_a_ddc_group_feed_never_reaches_storage_unfiltered():
    """The end of the road: marked rows are dropped at the write boundary."""
    rejected: list = []
    kept = _ddc_parse(
        _ddc_payload([(_vin(370), "soniccrownlexus"), (_vin(371), "sonicbeverlyhillsbmw"),
                      (_vin(372), "sonicbmwmonrovia")]),
        "crownlexus-com", "Crown Lexus", "https://www.crownlexus.com")
    stored, dropped = drop_unattributable_vehicles(kept)
    assert [r["vin"] for r in stored] == [_vin(370)]
    assert dropped == 2
    assert rejected == []


def test_the_old_ddc_filter_defers_to_the_gate():
    """Two filters over one row is worse than one. ``filter_sister_store_vehicles``
    used to be the only DDC filter and ran on the merged ``_lot_location`` blob;
    now that the gate rules on these rows, it must not overrule them."""
    from backend.scanner.dealer.location import (
        DealerSiteProfile,
        filter_sister_store_vehicles,
    )

    rows = _ddc_parse(
        _ddc_payload([(_vin(380), "soniccrownlexus"), (_vin(381), "soniccrownlexus"),
                      (_vin(382), "soniccrownlexus")]),
        "crownlexus-com", "Crown Lexus", "https://www.crownlexus.com")
    profile = DealerSiteProfile(dealer_id="crownlexus-com", name="Crown Lexus",
                               url="https://www.crownlexus.com", city="Ontario", state="CA")
    kept, stats = filter_sister_store_vehicles(rows, profile, source="inventory")
    assert len(kept) == len(rows)
    assert stats["deferred_to_rooftop_gate"] == len(rows)


def test_three_rooftops_sharing_one_display_name_stay_three_rooftops():
    """mossbroscjdrsanbernardino.com's feed carries three rooftops whose display
    name is the identical string "Moss Bros. Chrysler Dodge Jeep Ram" — San
    Bernardino, Riverside and Moreno Valley. Group them by that name and they
    collapse into one storefront that owns all three stores' cars. The accountId
    keeps them apart; the host then says which one is being scanned."""
    feed = {
        "mossbrosdodgecllc": "www.mossbroscjdrsanbernardino.com",
        "mossbrosdodgeriversidecllc": "www.mossbroscjdrriverside.com",
        "mossbroscdjcllc": "www.mossbroscjdrmorenovalley.com",
    }
    rows = [
        {"vin": _vin(390 + i),
         "_rooftop": {"key": key, "name": "Moss Bros. Chrysler Dodge Jeep Ram", "site": site}}
        for i, (key, site) in enumerate(feed.items())
    ]
    kept, rejected = resolve_rooftop_attribution(
        rows, dealer_id="mossbroscjdrsanbernardino-com",
        dealer_name="Moss Bros Chrysler Dodge Jeep RAM San Bernardino",
        dealer_url="https://www.mossbroscjdrsanbernardino.com")
    assert [r["vin"] for r in kept] == [_vin(390)]
    assert [r["_rooftop_reject"] for r in rejected] == ["sibling_rooftop"] * 2


# ── brand-abbreviation equivalence (a general matcher rule) ─────────────────
#
# Live replays on 2026-08-03: tuttoncdjr.com's feed spells the store "Tutton
# Chrysler Jeep Dodge RAM of Jasper" while the roster carries "Tutton CDJR of
# Jasper"; landersmclartydcjal.com's feed says "Landers McLarty DCJR" against a
# roster "Landers McLarty Chrysler Dodge Jeep Ram FIAT". Both stores held 0
# active cars because no tier could match those strings.

def _named_rows(*names: str) -> list[dict]:
    return [{"vin": _vin(500 + i), "_rooftop": {"key": n, "name": n}}
            for i, n in enumerate(names)]


def test_brand_initials_match_the_franchises_spelled_out():
    """CDJR is the same store as Chrysler Dodge Jeep Ram, in any order."""
    kept, rejected = resolve_rooftop_attribution(
        _named_rows("Tutton Chrysler Jeep Dodge RAM of Jasper",
                    "Voyles CDJR of Birmingham"),
        dealer_id="tuttoncdjr-com", dealer_name="Tutton CDJR of Jasper",
        dealer_url="https://www.tuttoncdjr.com")
    assert [r["vin"] for r in kept] == [_vin(500)]
    assert [r["_rooftop_reject"] for r in rejected] == ["sibling_rooftop"]


def test_trailing_fiat_franchise_does_not_break_the_match():
    kept, rejected = resolve_rooftop_attribution(
        _named_rows("Landers McLarty DCJR", "Landers McLarty Subaru",
                    "Subaru of Gallatin"),
        dealer_id="landersmclartydcjal-com",
        dealer_name="Landers McLarty Chrysler Dodge Jeep Ram FIAT",
        dealer_url="https://www.landersmclartydcjal.com",
        dealer_city="Huntsville", dealer_state="AL")
    assert [r["vin"] for r in kept] == [_vin(500)]
    assert [r["_rooftop_reject"] for r in rejected] == ["sibling_rooftop"] * 2


def test_two_rooftops_with_the_same_franchises_are_still_a_refusal():
    """The rule folds the franchise words; it never breaks a tie. Two of a
    group's stores whose names differ ONLY in how the brands are written are
    indistinguishable, and that is a refusal, not a coin flip."""
    kept, rejected = resolve_rooftop_attribution(
        _named_rows("Landers McLarty Chrysler Dodge Jeep Ram",
                    "Landers McLarty DCJR"),
        dealer_id="landersmclartydcjal-com",
        dealer_name="Landers McLarty CDJR",
        dealer_url="https://www.landersmclartydcjal.com")
    assert kept == []
    assert {r["_rooftop_reject"] for r in rejected} == {"target_rooftop_unidentified"}


def test_brand_rule_is_inert_for_a_store_with_no_stellantis_franchise():
    """It must not reach across franchises: a Ford store's name carries no
    cluster, so the tier never runs and cannot invent a match."""
    kept, rejected = resolve_rooftop_attribution(
        _named_rows("Aaron Ford of Poway", "Aaron Chevrolet"),
        dealer_id="encinitasford-com", dealer_name="Encinitas Ford",
        dealer_url="https://www.encinitasford.com")
    assert kept == []
    assert {r["_rooftop_reject"] for r in rejected} == {"target_rooftop_unidentified"}


def test_j_less_initialisms_are_not_treated_as_brands():
    """"cdr"/"drf" are not dealer abbreviations; expanding them would let the
    gate match names that share no words at all."""
    from backend.parsers import _expand_brand_initials

    assert _expand_brand_initials("cdjr") == {"chrysler", "dodge", "jeep", "ram"}
    assert _expand_brand_initials("cdr") == {"cdr"}
    assert _expand_brand_initials("cddj") == {"cddj"}   # repeated letter
    assert _expand_brand_initials("jd") == {"jd"}       # too short


# ── per-dealer roster aliases (data, not a matching rule) ───────────────────

def test_roster_alias_matches_a_feed_that_misspells_its_own_store():
    """fordlincolnofcookeville.com's account writes "Ford Lincoln of Cookville"
    (no "e"). The alias is the feed's exact string — the gate still matches
    exactly, so it is not guessing."""
    kept, rejected = resolve_rooftop_attribution(
        _named_rows("Ford Lincoln of Cookville", "Hyundai of Cookville",
                    "Toyota of Cool Springs"),
        dealer_id="fordlincolnofcookeville-com", dealer_name="Ford of Cookeville",
        dealer_url="https://www.fordlincolnofcookeville.com")
    assert [r["vin"] for r in kept] == [_vin(500)]
    assert [r["_rooftop_reject"] for r in rejected] == ["sibling_rooftop"] * 2


def test_roster_alias_matches_exactly_and_never_approximately():
    """The alias is one exact string, not a licence to match anything close to
    it: a rooftop that merely rearranges the same words is still a refusal."""
    kept, rejected = resolve_rooftop_attribution(
        _named_rows("Cookville Ford Lincoln", "Hyundai of Cookville"),
        dealer_id="fordlincolnofcookeville-com", dealer_name="Ford of Cookeville",
        dealer_url="https://www.fordlincolnofcookeville.com")
    assert kept == []
    assert {r["_rooftop_reject"] for r in rejected} == {"target_rooftop_unidentified"}


def test_a_dealer_without_an_alias_is_untouched_by_the_alias_table():
    kept, rejected = resolve_rooftop_attribution(
        _named_rows("Ford Lincoln of Cookville", "Hyundai of Cookville"),
        dealer_id="someotherdealer-com", dealer_name="Some Other Dealer",
        dealer_url="https://www.someotherdealer.com")
    assert kept == []
    assert {r["_rooftop_reject"] for r in rejected} == {"target_rooftop_unidentified"}


def test_every_alias_records_the_date_it_was_observed():
    """A stale alias is worse than none: it keeps matching a spelling the feed
    may have fixed. The comment above each entry is the only record of when it
    was seen, so the file must never gain an undated block."""
    import re as _re
    from pathlib import Path

    import backend.parsers.rooftop_aliases as mod

    src = Path(mod.__file__).read_text(encoding="utf-8")
    body = src.split("ROSTER_NAME_ALIASES", 1)[1].split("def ", 1)[0]
    for line in body.splitlines():
        if _re.match(r'\s*"[a-z0-9.-]+":', line):
            block = body[: body.index(line)].rsplit("},", 1)[-1]
            assert _re.search(r"\d{4}-\d{2}-\d{2}", block), (
                f"alias {line.strip()} has no observation date in its comment")


# ── one shared un-listing policy for both scan paths ────────────────────────

def test_both_scan_paths_read_the_same_evidence_list():
    """The delta (HTTP) path and the full (browser) path must not drift on which
    refusals may un-list a car. They import ONE constant; there is no second
    literal to fall out of step."""
    import backend.scanner.delta_scan as delta
    import backend.scanner.phases.dealer_run as dealer_run
    from backend.scanner.rooftop_disown import EVIDENCE_BACKED_REJECTS, split_refusals

    assert delta.split_refusals is split_refusals
    assert dealer_run.split_refusals is split_refusals
    assert delta._disown_foreign_rooftop_vins is dealer_run.disown_foreign_rooftop_vins
    assert "target_rooftop_unidentified" not in EVIDENCE_BACKED_REJECTS
    assert "unstamped_row_in_group_feed" not in EVIDENCE_BACKED_REJECTS


def test_only_evidence_backed_refusals_may_unlist_a_car():
    rows = [
        {"vin": _vin(600), "_rooftop_reject": "sibling_rooftop"},
        {"vin": _vin(601), "_rooftop_reject": "single_rooftop_is_not_this_store"},
        {"vin": _vin(602), "_rooftop_reject": "target_rooftop_unidentified"},
        {"vin": _vin(603), "_rooftop_reject": "unstamped_row_in_group_feed"},
    ]
    from backend.scanner.rooftop_disown import split_refusals

    evidenced, unidentified = split_refusals(rows)
    assert [r["vin"] for r in evidenced] == [_vin(600), _vin(601)]
    assert unidentified == 2


def test_browser_path_hands_parse_a_rejected_out_and_the_roster_address():
    """Regression guard for the reconcile leak: the full-scan path used to call
    parse() with neither, so sibling rows came back marked, stayed in
    ``all_vehicles``, and landed in the VIN set reconcile treats as "still seen"
    — which held pre-existing mis-attributions at listing_active = 1."""
    import inspect

    import backend.scanner.phases.dealer_run as dealer_run

    import re as _re

    src = inspect.getsource(dealer_run.run_dealer)
    calls = []
    for m in _re.finditer(r"(?<![\w.])parse\(", src):
        depth, i = 0, m.end() - 1
        while i < len(src):
            if src[i] == "(":
                depth += 1
            elif src[i] == ")":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        call = src[m.end() : i]
        if "dealer_id=dealer_id" in call:  # skip bare "parse()" mentions in comments
            calls.append(call)
    assert len(calls) == 2, f"expected the two inventory parse() call sites, found {len(calls)}"
    for call in calls:
        assert "rejected_out=" in call
        assert "**roster_place" in call


# ── a locality tier must never be strong enough to un-list a car ────────────

def test_city_state_match_refuses_siblings_but_never_unlists_them():
    """The gate runs PER PAGE. A page carrying only one of two same-city
    rooftops hands ``city_state`` a spurious unique winner, and the next page
    hands it the other one — so pages disagree about who the store is. Measured
    on bmwofmurrieta-com: 1,697 of a 1,941-car dealer were queued for
    un-listing. A locality tier proves where a store is, not which store it is,
    so its sibling refusals must be excluded from EVIDENCE_BACKED_REJECTS.
    """
    from backend.scanner.rooftop_disown import EVIDENCE_BACKED_REJECTS

    page = [{"vin": _vin(300), "_rooftop": {"key": _MURRIETA}},
            {"vin": _vin(301), "_rooftop": {"key": _DATE_ST}}]
    kept, rejected = resolve_rooftop_attribution(
        page, dealer_id="bmwofmurrieta-com", dealer_name="BMW of Murrieta",
        dealer_url="https://www.bmwofmurrieta.com",
        dealer_city="Murrieta", dealer_state="CA", dealer_zip="92562",
    )
    # Whether it keeps one or refuses both, nothing it refused may be un-listed.
    assert not [r for r in rejected if r["_rooftop_reject"] in EVIDENCE_BACKED_REJECTS], (
        "a locality-only tier produced an un-listable refusal"
    )


def test_street_address_match_still_unlists_real_siblings():
    """The weak-tier guard must not defang the strong tiers: a street address
    identifies exactly one storefront, so its siblings remain un-listable."""
    from backend.scanner.rooftop_disown import EVIDENCE_BACKED_REJECTS

    kept, rejected = resolve_rooftop_attribution(
        _addr_rows(_MURRIETA, _CHARLOTTE),
        dealer_id="bmwofmurrieta-com", dealer_name="BMW of Murrieta",
        dealer_url="https://www.bmwofmurrieta.com",
        dealer_address="41430 Auto Mall Pkwy", dealer_city="Murrieta",
        dealer_state="CA", dealer_zip="92562",
    )
    assert len(kept) == 1
    assert [r for r in rejected if r["_rooftop_reject"] in EVIDENCE_BACKED_REJECTS]
