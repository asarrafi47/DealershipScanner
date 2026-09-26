"""CarsCommerce group accounts: the store is identified by its own feed id
(``source_id`` == the page's ``oem_code`` or the store's name), the feed's
``extra_fields.custom_location`` names the selling rooftop, and a store filter is
only trusted after a replay shows one rooftop in the store's city.
Findings from workspace/dealer_logs/{hendrickbuickgmccary,rickhendrickchevynaples,
mallofgamazda}-com/discovery.md, 2026-09-24."""
from __future__ import annotations

import json

import pytest

from backend.parsers.carscommerce import rooftop_of
from backend.parsers import rooftop_aliases
from backend.scanner import recipe_synth as rs
from backend.scanner.dealer_place import name_from_html, oem_code_from_html


def _listing(vin: str, location: str, source_id: str, custom_location: str | None = None) -> dict:
    extra = {"Location": "Cary, NC"}
    if custom_location:
        extra["custom_location"] = custom_location
    return {"vin": vin, "source_id": source_id, "make": "GMC", "model": "Sierra", "year": 2025, "type": "New",
            "dealer": {"id": "1", "ccid": 6034613, "city": None, "name": None, "state": None, "address": None, "zipcode": None, "location": location},
            "extra_fields": extra}


def test_rooftop_of_city_state_without_zip_or_trailing_space():
    rt = rooftop_of(_listing("1GT4UXEY3SF102275", "Naples, FL", "292164"))
    assert (rt["city"], rt["state"], rt["zip"]) == ("Naples", "FL", "")
    assert rt["address"] == ""  # a place line is not a street


def test_rooftop_of_prefers_custom_location_store_name():
    rt = rooftop_of(_listing("1GT4UXEY3SF102275", "Cary, NC", "178465", "Hendrick Buick GMC Cadillac Cary"))
    assert rt["name"] == rt["key"] == "Cary, NC"  # identity stays the feed's location stamp
    assert rt["alt_name"] == "Hendrick Buick GMC Cadillac Cary"
    assert (rt["city"], rt["state"]) == ("Cary", "NC")
    # a place-shaped custom_location is not a store label
    rt2 = rooftop_of(_listing("1GT4UXEY3SF102275", "3546 Highway 20<br/>Buford, GA 30519<br/>(678) 714-9000", "MallofGeorgiaMazda", "Buford, GA"))
    assert rt2["name"].startswith("3546 Highway 20") and rt2["city"] == "Buford" and rt2["zip"] == "30519" and rt2["alt_name"] == ""


def test_page_identity_readers():
    html = '{"dealername":"Hendrick Buick GMC Cary","oem_code":"178465","diCity":"Cary"}'
    assert name_from_html(html) == "Hendrick Buick GMC Cary"
    assert oem_code_from_html(html) == "178465"
    assert name_from_html('<meta property="og:site_name" content="Mall of Georgia Mazda">') == "Mall of Georgia Mazda"


def test_location_facets_from_site_map():
    html = ('"image_sort":"custom_text_1","location_sort":"custom_text_4","meta_location":"custom_text_11",'
            '"Location":"custom_text_25","special_field_3":"custom_text_2"')
    assert rs._cc_location_facets(html) == [("Location", "custom_text_25"), ("location_sort", "custom_text_4"), ("meta_location", "custom_text_11")]


class _FakeFeed:
    """A Hendrick-shaped group account: Location='Cary, NC' spans three stores,
    source_id 178465 is the Buick GMC store's own feed."""

    def __init__(self):
        self.calls: list[dict] = []
        self.rows = (
            [_listing(f"VIN{i:014d}", "Cary, NC", "178465", "Hendrick Buick GMC Cadillac Cary") for i in range(40)]
            + [_listing(f"KIA{i:014d}", "90 MacKenan Dr<br/>Cary, NC 27511<br/>(919) 362-0575", "RHendrickUsed", "Hendrick Kia of Cary") for i in range(5)]
            + [_listing(f"CHV{i:014d}", "Cary, NC", "113974", "Hendrick Chevrolet Cary") for i in range(3)]
            + [_listing(f"NAP{i:014d}", "Naples, FL", "292164", "Chevy Naples") for i in range(10)]
        )

    def __call__(self, recipe, body, origin, url=None):
        self.calls.append(body)
        rows = list(self.rows)
        ff = body.get("facetFilters") or {}
        for facet, keys in ff.items():
            if facet == "source_id":
                rows = [r for r in rows if r["source_id"] in keys]
            elif facet == "custom_text_25":
                rows = [r for r in rows if r["extra_fields"]["Location"] in keys]
        facets = []
        for name in body.get("facets") or []:
            if name == "source_id":
                facets.append({"name": name, "values": [{"key": k, "doc_count": sum(1 for r in self.rows if r["source_id"] == k)} for k in ("178465", "RHendrickUsed", "113974", "292164")]})
            elif name == "custom_text_25":
                facets.append({"name": name, "values": [{"key": "Cary, NC", "doc_count": 48}, {"key": "Naples, FL", "doc_count": 10}]})
            else:
                facets.append({"name": name, "values": []})
        return 200, {"data": {"total_vehicle_count": len(self.rows), "listings": rows[: body.get("perPage", 100)], "facets": facets}}


@pytest.fixture
def cary_recipe():
    return rs.EndpointRecipe(
        dealer_id="hendrickbuickgmccary-com", content_type="application/json",
        url="https://websites-search.api.carscommerce.inc/api/v1/listings/6034613/search", method="POST",
        post_template=json.dumps({"page": 1, "perPage": 100, "filters": {"status": ["publish"]}, "facets": ["year"]}),
        pagination="carscommerce_page", provider_hint="dealer_dot_com", auth_headers={"x-api-key": "k"},
    )


def test_store_filter_picks_identity_feed_and_records_alias(monkeypatch, cary_recipe):
    feed = _FakeFeed()
    monkeypatch.setattr(rs, "_replay_request", feed)
    monkeypatch.setattr("backend.scanner.dealer_place.learn_place", lambda *a, **k: {"dealer_city": "Cary", "dealer_state": "NC", "place_source": "registry"})
    recorded: list[tuple] = []
    monkeypatch.setattr(rooftop_aliases, "record_rooftop_alias", lambda d, n, **k: recorded.append((d, n, k)) or True)
    monkeypatch.setattr(rooftop_aliases, "roster_name_aliases", lambda d: ())
    html = '"dealername":"Hendrick Buick GMC Cary","oem_code":"178465","Location":"custom_text_25","meta_location":"custom_text_11"'
    rs._carscommerce_store_filter(cary_recipe, "hendrickbuickgmccary-com", "https://www.hendrickbuickgmccary.com", html)
    body = json.loads(cary_recipe.post_template)
    assert body["facetFilters"] == {"source_id": ["178465"]}
    assert recorded and recorded[0][:2] == ("hendrickbuickgmccary-com", "Hendrick Buick GMC Cadillac Cary")
    assert "178465" in recorded[0][2]["evidence"]


def test_store_filter_rejects_multi_store_location_value(monkeypatch, cary_recipe):
    """No identity feed on the page: the Location facet value 'Cary, NC' covers
    three stores, so it must NOT be chosen; the gate filters per row instead."""
    feed = _FakeFeed()
    monkeypatch.setattr(rs, "_replay_request", feed)
    monkeypatch.setattr("backend.scanner.dealer_place.learn_place", lambda *a, **k: {"dealer_city": "Cary", "dealer_state": "NC"})
    html = '"Location":"custom_text_25"'
    rs._carscommerce_store_filter(cary_recipe, "hendrickbuickgmccary-com", "https://www.hendrickbuickgmccary.com", html)
    assert "facetFilters" not in json.loads(cary_recipe.post_template)


def test_store_filter_merges_every_identity_feed(monkeypatch, cary_recipe):
    """A store files under its dealer code AND a named feed (Mall of Georgia
    Mazda: 23978 + MallofGeorgiaMazda); both are taken."""
    feed = _FakeFeed()
    feed.rows = ([_listing(f"A{i:016d}", "3546 Highway 20<br/>Buford, GA 30519<br/>(678) 714-9000", "MallofGeorgiaMazda") for i in range(20)]
                 + [_listing(f"B{i:016d}", "3546 Highway 20<br/>Buford, GA 30519<br/>(678) 714-9000", "23978") for i in range(10)]
                 + [_listing(f"C{i:016d}", "3751 Buford Drive<br/>Buford, GA 30519<br/>(855) 204-0816", "RHendrickUsed") for i in range(10)])

    def facets_call(recipe, body, origin, url=None):
        st, d = _FakeFeed.__call__(feed, recipe, body, origin, url)
        for f in d["data"]["facets"]:
            if f["name"] == "source_id":
                f["values"] = [{"key": k, "doc_count": sum(1 for r in feed.rows if r["source_id"] == k)} for k in ("MallofGeorgiaMazda", "23978", "RHendrickUsed")]
        return st, d

    monkeypatch.setattr(rs, "_replay_request", facets_call)
    monkeypatch.setattr("backend.scanner.dealer_place.learn_place", lambda *a, **k: {"dealer_city": "Buford", "dealer_state": "GA"})
    monkeypatch.setattr(rooftop_aliases, "record_rooftop_alias", lambda *a, **k: True)
    html = '"dealername":"Mall of Georgia Mazda","oem_code":"23978"'
    rs._carscommerce_store_filter(cary_recipe, "mallofgamazda-com", "https://www.mallofgamazda.com", html)
    assert json.loads(cary_recipe.post_template)["facetFilters"] == {"source_id": ["MallofGeorgiaMazda", "23978"]}


def test_store_filter_merges_same_rooftop_feed_but_not_same_city_sibling(monkeypatch, cary_recipe):
    """Stevenson Hendrick Honda: oem_code feed 208763 (45 cars) and DMS feed
    9048741 (402) both stamp '6720 Market St'; 9077258 is another Wilmington
    store at another street and RHendrickUsed spans the group."""
    feed = _FakeFeed()
    honda = "6720 Market St<br/>Wilmington, NC 28405<br/>(910) 395-1116"
    honda_typo_phone = "6720 Market St<br/>Wilmington, NC 28405<br/>(910) 396-1116"
    other = "219 S. College Road<br/>Wilmington, NC 28403<br/>(910) 799-1815"
    feed.rows = ([_listing(f"A{i:016d}", honda_typo_phone, "208763") for i in range(5)]
                 + [_listing(f"B{i:016d}", honda, "9048741") for i in range(40)]
                 + [_listing(f"C{i:016d}", other, "9077258") for i in range(17)]
                 + [_listing(f"D{i:016d}", honda if i % 2 else other, "RHendrickUsed") for i in range(30)])

    def call(recipe, body, origin, url=None):
        st, d = _FakeFeed.__call__(feed, recipe, body, origin, url)
        for f in d["data"]["facets"]:
            if f["name"] == "source_id":
                f["values"] = [{"key": k, "doc_count": sum(1 for r in feed.rows if r["source_id"] == k)} for k in ("208763", "9048741", "9077258", "RHendrickUsed")]
        return st, d

    monkeypatch.setattr(rs, "_replay_request", call)
    monkeypatch.setattr("backend.scanner.dealer_place.learn_place", lambda *a, **k: {"dealer_city": "Wilmington", "dealer_state": "NC"})
    monkeypatch.setattr(rooftop_aliases, "record_rooftop_alias", lambda *a, **k: True)
    html = '"dealername":"Stevenson Hendrick Honda Wilmington","oem_code":"208763"'
    rs._carscommerce_store_filter(cary_recipe, "stevensonhendrickhonda-com", "https://stevensonhendrickhonda.com", html)
    assert json.loads(cary_recipe.post_template)["facetFilters"] == {"source_id": ["208763", "9048741"]}


def test_store_filter_never_merges_on_a_bare_city_stamp(monkeypatch, cary_recipe):
    feed = _FakeFeed()
    feed.rows = ([_listing(f"A{i:016d}", "Naples, FL", "292164") for i in range(10)]
                 + [_listing(f"B{i:016d}", "Naples, FL", "555555") for i in range(10)])

    def call(recipe, body, origin, url=None):
        st, d = _FakeFeed.__call__(feed, recipe, body, origin, url)
        for f in d["data"]["facets"]:
            if f["name"] == "source_id":
                f["values"] = [{"key": k, "doc_count": 10} for k in ("292164", "555555")]
        return st, d

    monkeypatch.setattr(rs, "_replay_request", call)
    monkeypatch.setattr("backend.scanner.dealer_place.learn_place", lambda *a, **k: {"dealer_city": "Naples", "dealer_state": "FL"})
    html = '"dealername":"Rick Hendrick Chevrolet Naples","oem_code":"292164"'
    rs._carscommerce_store_filter(cary_recipe, "rickhendrickchevynaples-com", "https://www.rickhendrickchevynaples.com", html)
    assert json.loads(cary_recipe.post_template)["facetFilters"] == {"source_id": ["292164"]}


def test_store_filter_merges_street_feed_when_identity_feed_is_bare_city(monkeypatch, cary_recipe):
    """Stevenson Hendrick Mazda: 24009 stamps 'Wilmington, NC' only; 9055689
    stamps '5911 Market St', the street the page's JSON-LD gives; another
    Wilmington feed at 6720 Market St is not this store."""
    feed = _FakeFeed()
    mazda = "5911 Market St<br/>Wilmington, NC 28405<br/>(910) 347-6678"
    honda = "6720 Market St<br/>Wilmington, NC 28405<br/>(910) 395-1116"
    feed.rows = ([_listing(f"A{i:016d}", "Wilmington, NC", "24009") for i in range(8)]
                 + [_listing(f"B{i:016d}", mazda, "9055689") for i in range(30)]
                 + [_listing(f"C{i:016d}", honda, "9048741") for i in range(30)])

    def call(recipe, body, origin, url=None):
        st, d = _FakeFeed.__call__(feed, recipe, body, origin, url)
        for f in d["data"]["facets"]:
            if f["name"] == "source_id":
                f["values"] = [{"key": k, "doc_count": sum(1 for r in feed.rows if r["source_id"] == k)} for k in ("24009", "9055689", "9048741")]
        return st, d

    monkeypatch.setattr(rs, "_replay_request", call)
    monkeypatch.setattr("backend.scanner.dealer_place.learn_place",
                        lambda *a, **k: {"dealer_city": "Wilmington", "dealer_state": "NC", "dealer_address": "5911 Market Street", "place_source": "title"})
    html = '"dealername":"Stevenson Hendrick Mazda Wilmington","oem_code":"24009"'
    rs._carscommerce_store_filter(cary_recipe, "stevensonhendrickmazda-com", "https://www.stevensonhendrickmazda.com", html)
    assert json.loads(cary_recipe.post_template)["facetFilters"] == {"source_id": ["24009", "9055689"]}


def test_place_from_html_carries_jsonld_street():
    from backend.scanner.dealer_place import place_from_html

    html = ('<title>Mazda Dealer in Wilmington, NC | Stevenson Hendrick Mazda</title>'
            '<script type="application/ld+json">{"address":{"streetAddress":"5911 Market Street","addressLocality":"Wilmington","addressRegion":"NC"}}</script>' + "x" * 500)
    p = place_from_html(html)
    assert (p["dealer_city"], p["dealer_state"], p["dealer_address"]) == ("Wilmington", "NC", "5911 Market Street")


def test_roster_name_aliases_reads_learned_hints(monkeypatch):
    rooftop_aliases._hint_cache.clear()
    monkeypatch.setattr("backend.scanner.recipe_store.get_scan_hints",
                        lambda d: {"rooftop_name_aliases": [{"name": "Chevy Naples", "observed": "2026-09-24", "evidence": "x"}]} if d == "rickhendrickchevynaples-com" else {})
    assert rooftop_aliases.roster_name_aliases("rickhendrickchevynaples-com") == ("Chevy Naples",)
    assert rooftop_aliases.roster_name_aliases("fordlincolnofcookeville-com")[0] == "Ford Lincoln of Cookville"
    rooftop_aliases._hint_cache.clear()


def test_gate_matches_store_label_but_never_refuses_on_it():
    """custom_location is free text on some accounts ("TOW/JORGE R/367676",
    "WRECKED CAR" on Group 1 Toyota North Austin; a sibling's name on Tutton
    CDJR). It may FIND this store, never lose it (2026-09-25)."""
    from backend.parsers import parse

    kw = dict(base_url="https://www.group1toyotanorthaustin.com", dealer_id="group1toyotanorthaustin-com",
              dealer_name="Group 1 Toyota North Austin", dealer_url="https://www.group1toyotanorthaustin.com", dealer_city="Austin", dealer_state="TX")
    junk = [_listing(f"JT{i:015d}", "Austin, TX", "99", "TOW/JORGE R/367676") for i in range(3)] + \
           [_listing(f"JU{i:015d}", "Austin, TX", "99", "WRECKED CAR") for i in range(2)]
    rejected: list[dict] = []
    kept = parse("carscommerce", {"data": {"listings": junk}}, rejected_out=rejected, **kw)
    assert len(kept) == 5 and not rejected  # one locality rooftop, junk labels ignored
    # two Cary rooftops share the "Cary, NC" stamp but only one label is ours
    kw2 = dict(base_url="https://www.hendrickbuickgmccary.com", dealer_id="hendrickbuickgmccary-com", dealer_name="Hendrick Buick GMC Cary",
               dealer_url="https://www.hendrickbuickgmccary.com", dealer_city="Cary", dealer_state="NC")
    mixed = [_listing(f"HB{i:015d}", "90 MacKenan Dr<br/>Cary, NC 27511<br/>(919) 362-0575", "178465", "Hendrick Buick GMC Cadillac Cary") for i in range(4)] + \
            [_listing(f"HK{i:015d}", "1000 Kildaire Farm Rd<br/>Cary, NC 27511<br/>(919) 555-0100", "555", "Hendrick Kia of Cary") for i in range(2)]
    rejected2: list[dict] = []
    kept2 = parse("carscommerce", {"data": {"listings": mixed}}, rejected_out=rejected2, **kw2)
    assert len(kept2) == 4 and len(rejected2) == 2


def test_gate_matches_short_brand_cluster_plus_town():
    from backend.parsers import parse

    kw = dict(base_url="https://www.shottenkirkchrysler.com", dealer_id="shottenkirkchrysler-com",
              dealer_name="Shottenkirk Chrysler Dodge Jeep Ram", dealer_url="https://www.shottenkirkchrysler.com", dealer_city="Canton", dealer_state="GA")
    rows = [_listing(f"SC{i:015d}", "Shottenkirk CDJR Canton", "1") for i in range(5)]
    rejected: list[dict] = []
    assert len(parse("carscommerce", {"data": {"listings": rows}}, rejected_out=rejected, **kw)) == 5 and not rejected
    # a sibling with the same cluster but another town is still refused
    rows2 = rows + [_listing(f"SD{i:015d}", "Shottenkirk CDJR Alpharetta", "2") for i in range(3)]
    rejected2: list[dict] = []
    assert len(parse("carscommerce", {"data": {"listings": rows2}}, rejected_out=rejected2, **kw)) == 0  # two candidates, no unique winner


def test_identity_feed_with_placeless_name_stamp_is_accepted(monkeypatch, cary_recipe):
    """Tutton CDJR: feed 27250 (= page oem_code) stamps rows only with the store
    name, no city; Group 1 Toyota North Austin likewise. Accept for identity
    feeds; a Location facet value with no locale is still refused."""
    feed = _FakeFeed()
    feed.rows = [_listing(f"TC{i:015d}", "Tutton Chrysler Jeep Dodge RAM of Jasper", "27250") for i in range(5)] + \
                [_listing(f"VB{i:015d}", "Voyles CDJR of Birmingham", "88") for i in range(5)]

    def call(recipe, body, origin, url=None):
        st, d = _FakeFeed.__call__(feed, recipe, body, origin, url)
        for f in d["data"]["facets"]:
            if f["name"] == "source_id":
                f["values"] = [{"key": k, "doc_count": 5} for k in ("27250", "88")]
        return st, d

    monkeypatch.setattr(rs, "_replay_request", call)
    monkeypatch.setattr("backend.scanner.dealer_place.learn_place", lambda *a, **k: {"dealer_city": "Jasper", "dealer_state": "GA"})
    monkeypatch.setattr(rooftop_aliases, "record_rooftop_alias", lambda *a, **k: True)
    html = '"dealername":"Tutton Chrysler Dodge Jeep RAM of Jasper","oem_code":"27250"'
    rs._carscommerce_store_filter(cary_recipe, "tuttoncdjr-com", "https://www.tuttoncdjr.com", html)
    assert json.loads(cary_recipe.post_template)["facetFilters"] == {"source_id": ["27250"]}


def test_named_feed_id_without_town_and_stampless_rows_are_identity(monkeypatch, cary_recipe):
    feed = _FakeFeed()
    plain = {"vin": "", "source_id": "", "make": "Ram", "model": "1500", "year": 2026, "type": "New", "dealer": {"location": None, "name": None}, "extra_fields": {}}
    feed.rows = [_listing(f"TC{i:015d}", "Tutton Chrysler Jeep Dodge RAM of Jasper", "27250") for i in range(5)] + \
                [_listing(f"TN{i:015d}", "1050 Highway 515 South", "TuttonChryslerDodgeJeepRam") for i in range(30)] + \
                [dict(plain, vin=f"ST{i:015d}", source_id="42409") for i in range(20)] + \
                [_listing(f"VB{i:015d}", "Voyles CDJR of Birmingham", "88") for i in range(5)]

    def call(recipe, body, origin, url=None):
        st, d = _FakeFeed.__call__(feed, recipe, body, origin, url)
        for f in d["data"]["facets"]:
            if f["name"] == "source_id":
                f["values"] = [{"key": k, "doc_count": sum(1 for r in feed.rows if r["source_id"] == k)} for k in ("27250", "TuttonChryslerDodgeJeepRam", "42409", "88")]
        return st, d

    monkeypatch.setattr(rs, "_replay_request", call)
    monkeypatch.setattr("backend.scanner.dealer_place.learn_place", lambda *a, **k: {"dealer_city": "Jasper", "dealer_state": "GA"})
    monkeypatch.setattr(rooftop_aliases, "record_rooftop_alias", lambda *a, **k: True)
    html = '"dealername":"Tutton Chrysler Dodge Jeep RAM of Jasper","oem_code":"27250"'
    rs._carscommerce_store_filter(cary_recipe, "tuttoncdjr-com", "https://www.tuttoncdjr.com", html)
    assert json.loads(cary_recipe.post_template)["facetFilters"] == {"source_id": ["TuttonChryslerDodgeJeepRam", "27250"]}
    # stampless rows on an identity feed are accepted too
    cary2 = rs.EndpointRecipe(dealer_id="g1-com", content_type="application/json", url=cary_recipe.url, method="POST",
                              post_template=json.dumps({"page": 1, "perPage": 100}), pagination="carscommerce_page", provider_hint="dealer_dot_com")
    rs._carscommerce_store_filter(cary2, "group1toyotanorthaustin-com", "https://www.group1toyotanorthaustin.com", '"dealername":"Group 1 Toyota North Austin","oem_code":"42409"')
    assert json.loads(cary2.post_template)["facetFilters"] == {"source_id": ["42409"]}


def test_store_name_location_facet_becomes_an_extra_recipe(monkeypatch, cary_recipe):
    """Group 1 Toyota North Austin: identity feed 42409 = 259 new cars; the used
    cars sit in group pools reachable only through the site's Location facet
    value 'Group 1 Toyota North Austin' (custom_text_13). Rows under that value
    carry lot tags ('ALL', 'SPC') which must not veto."""
    feed = _FakeFeed()
    plain = {"vin": "", "source_id": "", "make": "Toyota", "model": "Camry", "year": 2026, "type": "New", "dealer": {"location": None, "name": None}, "extra_fields": {}}
    new = [dict(plain, vin=f"GN{i:015d}", source_id="42409") for i in range(20)]
    used = [_listing(f"GU{i:015d}", tag, "GROUPGPI43") for i, tag in enumerate(["ALL", "SPC", "TOW/JORGE R/367676", "Group 1 Toyota North Austin"] * 10)]
    for r in used:
        r["extra_fields"]["Location"] = "Group 1 Toyota North Austin"
    feed.rows = new + used

    def call(recipe, body, origin, url=None):
        rows = list(feed.rows)
        ff = body.get("facetFilters") or {}
        if "source_id" in ff:
            rows = [r for r in rows if r["source_id"] in ff["source_id"]]
        if "custom_text_13" in ff:
            rows = [r for r in rows if r["extra_fields"].get("Location") in ff["custom_text_13"]]
        facets = []
        for name in body.get("facets") or []:
            if name == "source_id":
                facets.append({"name": name, "values": [{"key": "42409", "doc_count": 20}, {"key": "GROUPGPI43", "doc_count": 40}]})
            elif name == "custom_text_13":
                facets.append({"name": name, "values": [{"key": "Group 1 Toyota North Austin", "doc_count": 40}, {"key": "Group 1 Kia", "doc_count": 30}]})
            else:
                facets.append({"name": name, "values": []})
        return 200, {"data": {"total_vehicle_count": len(rows) if ff else 200, "listings": rows[: body.get("perPage", 100)], "facets": facets}}

    monkeypatch.setattr(rs, "_replay_request", call)
    monkeypatch.setattr("backend.scanner.dealer_place.learn_place", lambda *a, **k: {"dealer_city": "Austin", "dealer_state": "TX"})
    monkeypatch.setattr(rooftop_aliases, "record_rooftop_alias", lambda *a, **k: True)
    html = '"dealername":"Group 1 Toyota North Austin","oem_code":"42409","Location":"custom_text_13"'
    extras = rs._carscommerce_store_filter(cary_recipe, "group1toyotanorthaustin-com", "https://www.group1toyotanorthaustin.com", html)
    assert json.loads(cary_recipe.post_template)["facetFilters"] == {"source_id": ["42409"]}
    assert len(extras) == 1 and json.loads(extras[0].post_template)["facetFilters"] == {"custom_text_13": ["Group 1 Toyota North Austin"]}

