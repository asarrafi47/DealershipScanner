"""The rooftop scorer (backend/scanner/rooftop_match.py): signal weights, the
never-sufficient rules, the evidence-only signals, and flag parity with the legacy gate.
The regression corpus (test_rooftop_corpus.py) proves the decisions; this file pins the
scorer's own contract."""
from __future__ import annotations

import pytest

import backend.parsers as parsers_mod
from backend.parsers import parse
from backend.scanner import rooftop_match as rm


@pytest.fixture(autouse=True)
def _no_aliases_no_ledger(monkeypatch):
    monkeypatch.setattr(parsers_mod, "roster_name_aliases", lambda d: ())
    monkeypatch.setattr("backend.scanner.rooftop_ledger.note_refusal", lambda *a, **k: None, raising=False)
    monkeypatch.setattr("backend.scanner.rooftop_ledger.flush", lambda *a, **k: None, raising=False)


def _v(vin: str) -> str:
    return vin.ljust(17, "0")  # the parser drops anything that is not a 17-character VIN


def _listing(vin: str, location, source_id: str, custom_location: str | None = None, facets: dict | None = None) -> dict:
    vin = _v(vin)
    extra = dict(facets or {})
    if custom_location:
        extra["custom_location"] = custom_location
    return {"vin": vin, "type": "New", "year": 2025, "make": "Honda", "model": "Civic", "trim": "EX", "stock": vin[-6:],
            "source_id": source_id, "pricing": {"price": 25000},
            "dealer": {"id": "1", "ccid": 1, "name": None, "api_id": source_id, "location": location, "city": None, "state": None},
            "media": {"images": []}, "extra_fields": extra}


def _rows(*listings) -> list[dict]:
    """Parsed CarsCommerce rows (with ``_rooftop`` / ``_feed_source``), gate NOT run."""
    from backend.parsers.carscommerce import parse as parse_cc

    return list(parse_cc({"data": {"listings": list(listings)}}, base_url="https://x.example", dealer_id="x-com"))


HH = "12815 Statesville Rd<br/>Huntersville, NC 28078<br/>(704) 875-3232"
CLT_OURS = "8901 South Blvd<br/>Charlotte, NC 28273<br/>(704) 552-6041"
CLT_LEXUS = "6025 E Independence Blvd<br/>Charlotte, NC 28212<br/>(704) 568-0000"
CHARLESTON = "1518 Savannah Highway<br/>Charleston, SC 29407<br/>(877) 241-0000"
MAZDA_BLOCK = "3546 Highway 20<br/>Buford, GA 30519<br/>(678) 714-9000"
MINI_BLOCK = "3751 Buford Drive<br/>Buford, GA 30519<br/>(855) 204-0816"


# ── weights ───────────────────────────────────────────────────────────────────

def test_ladder_order_is_descending_weight_and_locality_is_weak():
    ladder = ["site_host", "name_exact", "name_tokens", "name_token_subset", "name_brand_alias", "name_brand_alias_subset",
              "name_contains", "roster_name_alias", "store_label_exact", "store_label_tokens", "store_label_token_subset",
              "store_label_alias", "dealer_id_host", "rooftop_slug_host", "rooftop_slug_name_tokens", "street_address",
              "street_address_partial", "street_address_suffix", "street_address_suffix_partial", "zip_code", "city_state"]
    weights = [rm.SIGNAL_WEIGHTS[t][0] for t in ladder]
    assert weights == sorted(weights, reverse=True) and len(set(weights)) == len(weights)
    for tier in ladder:
        strong = rm.SIGNAL_WEIGHTS[tier][1]
        assert strong == (tier not in parsers_mod._LOCALITY_ONLY_TIERS)
        assert (rm.SIGNAL_WEIGHTS[tier][0] >= rm.STRONG_THRESHOLD) == strong
    # evidence-only and corroboration signals are strong and above the threshold
    for name in ("identity_feed", "page_oem_code", "location_facet_value", "own_street_block", "same_source_feed", "unstamped_under_own_source", "feed_scoped"):
        assert rm.SIGNAL_WEIGHTS[name][1] and rm.SIGNAL_WEIGHTS[name][0] >= rm.STRONG_THRESHOLD
    # no-evidence keeps carry no weight at all
    assert rm.SIGNAL_WEIGHTS["single_rooftop"] == (0.0, False) and rm.SIGNAL_WEIGHTS["no_rooftop_evidence"] == (0.0, False)


def test_every_signal_that_matched_is_listed_and_corroboration_raises_the_score():
    roster = {"dealer_id": "hondahuntersville-com", "name": "Honda Of Huntersville", "url": "https://www.hondahuntersville.com",
              "street": "12815 Statesville Rd", "city": "Huntersville", "state": "NC", "zip": "28078"}
    rows = _rows(_listing("V1", "Honda of Huntersville", "MP23253"), _listing("V2", "Hoover Toyota", "MP99999"))
    plain = rm.score_rows(rows, roster)[0]
    assert plain["decision"] == "keep" and plain["tier"] == "name_exact" and plain["strong"]
    names = [s["signal"] for s in plain["signals"]]
    # a name stamp carries no locale, and "hondahuntersville" is not inside "hondaofhuntersville":
    # exactly the name tiers match, every one a unique winner
    assert names == ["name_exact", "name_tokens", "name_contains"]
    assert all(s["unique"] for s in plain["signals"])
    assert plain["score"] == pytest.approx(rm.SIGNAL_WEIGHTS["name_exact"][0] + 2 * rm.CORROBORATION_BONUS)
    # the bonus is capped: scores sort a triage list, they never decide
    assert plain["score"] <= rm.SIGNAL_WEIGHTS["name_exact"][0] + rm.CORROBORATION_CAP
    sibling = rm.score_rows(rows, roster)[1]
    assert sibling == {"decision": "refuse", "tier": "sibling_rooftop", "score": 0.0, "strong": False, "signals": []}


# ── never sufficient ──────────────────────────────────────────────────────────

def test_city_state_alone_keeps_weakly_never_vouches_never_unlists():
    roster = {"dealer_id": "hendrickhonda-com", "name": "Hendrick Honda", "url": "https://www.hendrickhonda.com",
              "street": "", "city": "Charlotte", "state": "NC", "zip": ""}
    rows = _rows(_listing("V1", CLT_OURS, "RHendrickUsed"), _listing("V2", CHARLESTON, "RHendrickUsed"), _listing("V3", None, "RHendrickUsed"))
    ours, sib, unstamped = rm.score_rows(rows, roster)
    assert ours["decision"] == "keep" and ours["tier"] == "city_state"
    assert ours["strong"] is False and ours["score"] < rm.STRONG_THRESHOLD
    assert sib == {"decision": "refuse", "tier": "sibling_rooftop_weak_tier", "score": 0.0, "strong": False, "signals": []}
    assert unstamped["decision"] == "refuse" and unstamped["tier"] == "unstamped_row_in_group_feed"
    assert "city_state" in rm.NEVER_SUFFICIENT_ALONE and "zip_code" in rm.NEVER_SUFFICIENT_ALONE
    # the same page with a street proves identity: siblings are un-listable and the shared source is still shared
    strong_roster = dict(roster, street="8901 South Blvd", street_source="site_jsonld")
    ours2, sib2, unstamped2 = rm.score_rows(rows, strong_roster)
    assert ours2["tier"] == "street_address" and ours2["strong"]
    assert sib2["tier"] == "sibling_rooftop" and unstamped2["tier"] == "unstamped_row_in_group_feed"


def test_a_locality_only_page_can_keep_a_sibling_and_says_so():
    """The per-page artefact: a page whose only Charlotte block is Hendrick Lexus's is kept
    for Hendrick Honda today (documented in the corpus as expected_ideal=refuse). The scorer
    marks it weak, so nothing downstream may treat it as identity."""
    roster = {"dealer_id": "hendrickhonda-com", "name": "Hendrick Honda", "url": "https://www.hendrickhonda.com", "street": "", "city": "Charlotte", "state": "NC", "zip": ""}
    res = rm.score_rows(_rows(_listing("V1", CLT_LEXUS, "RHendrickUsed"), _listing("V2", CHARLESTON, "RHendrickUsed")), roster)
    assert res[0]["decision"] == "keep" and res[0]["tier"] == "city_state" and res[0]["strong"] is False


def test_store_label_alone_never_stamps_never_refuses():
    roster = {"dealer_id": "group1toyotanorthaustin-com", "name": "Group 1 Toyota North Austin", "url": "https://www.group1toyotanorthaustin.com",
              "street": "", "city": "Austin", "state": "TX", "zip": ""}
    # alt_name with no location stamp: not a rooftop at all -> unstamped inside a group payload
    rows = _rows(_listing("V1", None, "GROUPGPI43", "Group 1 Toyota North Austin"), _listing("V2", "Group 1 Kia of Austin", "K1"))
    assert rows[0].get("_rooftop") is None
    res = rm.score_rows(rows, roster)
    # the page's only rooftop is the Kia store, so the whole page (unmarked row included) is refused
    assert [(r["decision"], r["tier"]) for r in res] == [("refuse", "single_rooftop_is_not_this_store")] * 2
    # a sibling's name / a lot note in the label never refuses a page that is otherwise ours
    rows2 = _rows(_listing("V1", "Austin, TX", "42409", "Voyles CDJR of Birmingham"), _listing("V2", "Austin, TX", "42409", "TOW/JORGE R/367676"))
    res2 = rm.score_rows(rows2, roster)
    assert [r["decision"] for r in res2] == ["keep", "keep"] and {r["tier"] for r in res2} == {"city_state"}
    # and the label alone does find the store when the stamp is a place shared by siblings
    rows3 = _rows(_listing("V1", "Cary, NC", "178465", "Hendrick Buick GMC Cadillac Cary"),
                  _listing("V2", "90 MacKenan Dr<br/>Cary, NC 27511<br/>(919) 362-0575", "RHendrickUsed", "Hendrick Kia of Cary"))
    cary = {"dealer_id": "hendrickbuickgmccary-com", "name": "Hendrick Buick GMC Cary", "url": "https://www.hendrickbuickgmccary.com", "street": "", "city": "Cary", "state": "NC", "zip": ""}
    res3 = rm.score_rows(rows3, cary)
    assert res3[0]["tier"] == "store_label_token_subset" and res3[0]["decision"] == "keep" and res3[1]["tier"] == "sibling_rooftop"


def test_weak_street_provenance_keeps_but_is_not_strong():
    roster = {"dealer_id": "hendrickhonda-com", "name": "Hendrick Honda", "url": "https://www.hendrickhonda.com",
              "street": "8901 South Blvd", "street_source": "site_text", "city": "Charlotte", "state": "NC", "zip": ""}
    ours, sib = rm.score_rows(_rows(_listing("V1", CLT_OURS, "A"), _listing("V2", CLT_LEXUS, "B")), roster)
    assert ours["tier"] == "street_address" and ours["decision"] == "keep" and ours["strong"] is False
    assert sib["tier"] == "sibling_rooftop_weak_tier"


# ── evidence-only signals ─────────────────────────────────────────────────────

def test_identity_feed_evidence_keeps_both_stamps_of_one_store():
    """Mall of Georgia Mazda: two identity feeds stamp the store two ways; the ladder can
    pick one rooftop at most, the identity evidence keeps both."""
    roster = {"dealer_id": "mallofgamazda-com", "name": "Mall of Georgia Mazda", "url": "https://www.mallofgamazda.com", "street": "", "city": "Buford", "state": "GA", "zip": ""}
    rows = _rows(_listing("V1", MAZDA_BLOCK, "MallofGeorgiaMazda"), _listing("V2", "Buford, GA", "23978"),
                 _listing("V3", MINI_BLOCK, "RHendrickUsed"), _listing("V4", None, "RHendrickUsed"), _listing("V5", None, "23978"))
    bare = rm.score_rows(rows, roster)
    assert [r["decision"] for r in bare] == ["refuse"] * 5
    ev = {"identity_sources": ["MallofGeorgiaMazda", "23978"], "page_oem_code": "23978"}
    res = rm.score_rows(rows, roster, ev)
    assert [(r["decision"], r["tier"]) for r in res] == [
        ("keep", "identity_feed"), ("keep", "identity_feed"), ("refuse", "target_rooftop_unidentified"),
        ("refuse", "target_rooftop_unidentified"), ("keep", "identity_feed")]
    assert all(r["strong"] for r in res if r["decision"] == "keep")
    assert {s["signal"] for s in res[1]["signals"]} >= {"identity_feed", "page_oem_code"}
    assert {s["signal"] for s in res[4]["signals"]} >= {"identity_feed", "page_oem_code"}


def test_identity_feed_evidence_keeps_a_sibling_looking_stamp_beside_a_named_target():
    """Tutton CDJR: the street-line rooftop under the named identity slug is refused as a
    sibling by the ladder and kept by the identity evidence."""
    roster = {"dealer_id": "tuttoncdjr-com", "name": "Tutton CDJR of Jasper", "url": "https://www.tuttoncdjr.com", "street": "", "city": "Jasper", "state": "GA", "zip": ""}
    rows = _rows(_listing("V1", "Tutton Chrysler Jeep Dodge RAM of Jasper", "27250"),
                 _listing("V2", "1050 Highway 515 South", "TuttonChryslerDodgeJeepRam"),
                 _listing("V3", "Voyles CDJR of Birmingham", "MP22431"))
    res = rm.score_rows(rows, roster, {"identity_sources": ["TuttonChryslerDodgeJeepRam", "27250"]})
    assert [(r["decision"], r["tier"]) for r in res] == [("keep", "name_brand_alias"), ("keep", "identity_feed"), ("refuse", "sibling_rooftop")]


def test_location_facet_value_evidence_keeps_rows_the_site_files_under_the_store():
    roster = {"dealer_id": "grandhyundaicanton-com", "name": "Grand Hyundai of Canton", "url": "https://www.grandhyundaicanton.com", "street": "", "city": "Canton", "state": "GA", "zip": ""}
    rows = _rows(_listing("V1", None, "GH1", None, {"custom_text_9": "Grand Hyundai of Canton"}),
                 _listing("V2", "Grand Motorcars Kennesaw", "GH4", None, {"custom_text_9": "Grand Motorcars Kennesaw"}),
                 _listing("V3", "Autoplex Atlanta", "GH2", None, {"custom_text_9": "Autoplex Atlanta"}))
    assert rows[0]["_custom_facets"] == {"custom_text_9": "Grand Hyundai of Canton"}
    assert [r["decision"] for r in rm.score_rows(rows, roster)] == ["refuse"] * 3
    res = rm.score_rows(rows, roster, {"location_facet": {"key": "custom_text_9", "value": "Grand Hyundai of Canton"}})
    assert [(r["decision"], r["tier"]) for r in res] == [("keep", "location_facet_value"), ("refuse", "target_rooftop_unidentified"), ("refuse", "target_rooftop_unidentified")]


def test_jsonld_street_evidence_fills_a_missing_roster_street():
    roster = {"dealer_id": "hondahuntersville-com", "name": "Honda Of Huntersville", "url": "https://www.hondahuntersville.com", "street": "", "city": "Huntersville", "state": "NC", "zip": ""}
    rows = _rows(_listing("V1", "Honda of Huntersville", "MP23253"), _listing("V2", HH, "209014"), _listing("V3", None, "MP23253"))
    # without the street: the block under feed 209014 is an unknown rooftop (sibling); the
    # unstamped row is under the matched store's own source and is kept
    assert [r["tier"] for r in rm.score_rows(rows, roster)] == ["name_exact", "sibling_rooftop", "unstamped_under_own_source"]
    res = rm.score_rows(rows, roster, {"jsonld_street": "12815 Statesville Rd"})
    assert [r["tier"] for r in res] == ["name_exact", "own_street_block", "unstamped_under_own_source"]
    assert all(r["decision"] == "keep" and r["strong"] for r in res)


def test_trust_feed_scope_evidence_keeps_everything_as_feed_scoped():
    roster = {"dealer_id": "x-com", "name": "X", "url": "https://x.example"}
    res = rm.score_rows(_rows(_listing("V1", "Somebody Else", "1"), _listing("V2", None, "2")), roster, {"trust_feed_scope": True})
    assert [(r["decision"], r["tier"], r["strong"]) for r in res] == [("keep", "feed_scoped", True)] * 2


# ── single-row API and flag parity ────────────────────────────────────────────

def test_score_row_judges_within_its_payload():
    roster = {"dealer_id": "hondahuntersville-com", "name": "Honda Of Huntersville", "url": "https://www.hondahuntersville.com", "street": "", "city": "Huntersville", "state": "NC", "zip": ""}
    rows = _rows(_listing("V1", "Honda of Huntersville", "MP23253"), _listing("V2", "Hoover Toyota", "MP99999"))
    alone = rm.score_row(rows[1], roster)
    assert alone["decision"] == "refuse" and alone["tier"] == "single_rooftop_is_not_this_store"
    in_page = rm.score_row(rows[1], roster, {"payload_rows": rows})
    assert in_page["decision"] == "refuse" and in_page["tier"] == "sibling_rooftop"
    assert rm.score_row(rows[0], roster, {"payload_rows": rows})["tier"] == "name_exact"
    assert set(rm.score_row(rows[0], roster)) == {"decision", "tier", "score", "strong", "signals"}


@pytest.mark.parametrize("flag,expect_tier", [("0", False), ("1", True), ("", False), ("off", False), ("yes", True)])
def test_flag_selects_the_path_and_both_paths_agree(monkeypatch, flag, expect_tier):
    monkeypatch.setenv(rm.FLAG_ENV, flag)
    assert rm.scorer_enabled() is expect_tier
    payload = {"data": {"listings": [_listing("V1", "Honda of Huntersville", "MP23253"), _listing("V2", HH, "209014"),
                                     _listing("V3", None, "MP23253"), _listing("V4", "Hoover Toyota", "MP99999")]}}
    rej: list[dict] = []
    kept = parse("carscommerce", payload, base_url="https://www.hondahuntersville.com", dealer_id="hondahuntersville-com",
                 dealer_name="Honda Of Huntersville", dealer_url="https://www.hondahuntersville.com", rejected_out=rej,
                 dealer_city="Huntersville", dealer_state="NC", dealer_address="12815 Statesville Rd")
    assert sorted(r["vin"] for r in kept) == [_v("V1"), _v("V2"), _v("V3")] and [r["vin"] for r in rej] == [_v("V4")]
    assert rej[0]["_rooftop_reject"] == "sibling_rooftop"
    assert all(("_rooftop_tier" in r) is expect_tier for r in kept + rej)
    if expect_tier:
        assert {r["vin"]: r["_rooftop_tier"] for r in kept} == {_v("V1"): "name_exact", _v("V2"): "own_street_block", _v("V3"): "unstamped_under_own_source"}
