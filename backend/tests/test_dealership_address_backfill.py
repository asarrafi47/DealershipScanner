"""Tests for the keyless dealership address backfill and the street tier it feeds.

The point of the backfill is that the rooftop gate can tell a store from its
siblings. These tests pin both halves: what the backfill is willing to accept,
and that an accepted address actually identifies the store in a group feed.
"""
from __future__ import annotations

import json

import pytest

from backend.parsers import _street_key, resolve_rooftop_attribution
from backend.scripts.backfill_dealership_addresses import (
    Candidate,
    Dealer,
    choose,
    jsonld_addresses,
    looks_like_street,
    text_addresses,
    verify,
)

# BMW of Murrieta as the roster knows it after the backfill runs.
MURRIETA = Dealer(
    registry_id=560, name="BMW of Murrieta", site="https://www.bmwofmurrieta.com",
    city="Murrieta", state="CA", lat=33.5292034, lon=-117.1704725,
    street_address="", zip_code="", active_cars=1941,
)


def _row(vin: str, label: str) -> dict:
    return {"vin": vin, "_rooftop": {"key": label, "name": "", "site": "", "address": "",
                                     "city": "", "state": "", "zip": ""}}


# --------------------------------------------------------------------------- #
# the gate tier the backfill exists to feed
# --------------------------------------------------------------------------- #
def test_a_spelled_out_suffix_matches_the_feeds_abbreviation():
    """The dealer's own site says "Parkway"; its feed's rooftop block says "PKWY".

    Two Murrieta rooftops share the city, the state and the ZIP 92562, so the
    street is the only tier that can separate them. Plain normalisation makes
    "41430automallparkway" and "41430automallpkwy" non-overlapping strings.
    """
    rows = [
        _row("A" * 17, "41430 Auto MALL PKWY<br/>Murrieta, CA 92562<br/>(888) 737-4550"),
        _row("B" * 17, "41300 Date Street<br/>Murrieta, CA 92562<br/>(951) 000-0000"),
    ]
    kept, rejected = resolve_rooftop_attribution(
        rows, dealer_id="bmwofmurrieta-com", dealer_name="BMW of Murrieta",
        dealer_url="https://www.bmwofmurrieta.com",
        dealer_address="41430 Auto Mall Parkway", dealer_city="Murrieta",
        dealer_state="CA", dealer_zip="92562",
    )
    assert [r["vin"] for r in kept] == ["A" * 17]
    assert [r["_rooftop_reject"] for r in rejected] == ["sibling_rooftop"]


def test_without_a_street_address_two_same_zip_rooftops_are_a_refusal():
    """City, state and ZIP are identical for both — no tier can choose, so the
    gate refuses to store rather than picking. It must NOT un-list anything."""
    rows = [
        _row("A" * 17, "41430 Auto MALL PKWY<br/>Murrieta, CA 92562<br/>(888) 737-4550"),
        _row("B" * 17, "41300 Date Street<br/>Murrieta, CA 92562<br/>(951) 000-0000"),
    ]
    kept, rejected = resolve_rooftop_attribution(
        rows, dealer_id="bmwofmurrieta-com", dealer_name="BMW of Murrieta",
        dealer_url="https://www.bmwofmurrieta.com",
        dealer_address="", dealer_city="Murrieta", dealer_state="CA", dealer_zip="92562",
    )
    assert kept == []
    assert {r["_rooftop_reject"] for r in rejected} == {"target_rooftop_unidentified"}


def test_suffix_expansion_never_merges_two_different_streets():
    assert _street_key("41430 Auto Mall Pkwy") == _street_key("41430 Auto Mall Parkway")
    assert _street_key("100 N Main St") == _street_key("100 North Main Street")
    assert _street_key("41430 Auto Mall Pkwy") != _street_key("41300 Auto Mall Pkwy")
    assert _street_key("100 Main St") != _street_key("100 Maine St")


# --------------------------------------------------------------------------- #
# extraction
# --------------------------------------------------------------------------- #
def test_jsonld_address_is_read_from_a_nested_graph():
    html = (
        '<script type="application/ld+json">'
        + json.dumps({"@graph": [{"@type": "AutoDealer", "url": "https://www.bmwofmurrieta.com/",
                                  "address": {"@type": "PostalAddress",
                                              "streetAddress": "41430 Auto Mall Parkway",
                                              "addressLocality": "Murrieta",
                                              "addressRegion": "CA", "postalCode": "92562"}}]})
        + "</script>"
    )
    got = jsonld_addresses(html)
    assert got and got[0]["street"] == "41430 Auto Mall Parkway"
    assert got[0]["zip"] == "92562"


def test_malformed_jsonld_block_is_skipped_not_fatal():
    assert jsonld_addresses('<script type="application/ld+json">{oops</script>') == []


def test_text_address_line_is_recovered_from_plain_html():
    html = "<footer><p>Visit us at 41430 Auto Mall Parkway, Murrieta, CA 92562</p></footer>"
    got = text_addresses(html)
    assert got and got[0]["street"] == "41430 Auto Mall Parkway"
    assert (got[0]["city"], got[0]["state"], got[0]["zip"]) == ("Murrieta", "CA", "92562")


@pytest.mark.parametrize("value,ok", [
    ("41430 Auto Mall Parkway", True),
    ("100 N Main St", True),
    ("", False),
    ("Murrieta", False),
    ("PO Box 1234", False),
    ("P.O. Box 9", False),
])
def test_street_shape(value, ok):
    assert looks_like_street(value) is ok


# --------------------------------------------------------------------------- #
# verification — the part that keeps a wrong address out
# --------------------------------------------------------------------------- #
def test_an_address_in_another_state_is_refused():
    cand = Candidate("1 Main Street", "Naperville", "IL", "60563", "site_jsonld")
    assert "state_mismatch" in verify(cand, MURRIETA, 25.0)


def test_an_address_whose_zip_is_across_the_country_is_refused():
    """The failure this whole gate exists to prevent: a confident, wrong place."""
    cand = Candidate("1 Main Street", "Murrieta", "CA", "94105", "site_jsonld")
    reason = verify(cand, MURRIETA, 25.0)
    assert reason.startswith("zip_94105_is_") and reason.endswith("mi_from_roster_point")


def test_the_dealers_own_address_is_accepted():
    cand = Candidate("41430 Auto Mall Parkway", "Murrieta", "CA", "92562", "site_jsonld")
    assert verify(cand, MURRIETA, 25.0) == ""


def test_a_candidate_with_no_zip_falls_back_to_city_and_state():
    assert verify(Candidate("41430 Auto Mall Parkway", "Murrieta", "CA", "", "osm_website"),
                  MURRIETA, 25.0) == ""
    assert verify(Candidate("1 Main Street", "Temecula", "CA", "", "osm_website"),
                  MURRIETA, 25.0).startswith("location_unconfirmed")


def test_two_verified_addresses_are_a_refusal_not_a_pick():
    """A group homepage that lists several of its rooftops says nothing about
    which one this host belongs to."""
    refusals: list[str] = []
    got = choose([
        Candidate("41430 Auto Mall Parkway", "Murrieta", "CA", "92562", "site_jsonld"),
        Candidate("41300 Date Street", "Murrieta", "CA", "92562", "site_jsonld"),
    ], MURRIETA, 25.0, refusals)
    assert got is None
    assert any("ambiguous_2_addresses" in r for r in refusals)


def test_one_address_written_two_ways_is_still_one_answer():
    refusals: list[str] = []
    got = choose([
        Candidate("41430 Auto Mall Parkway", "Murrieta", "CA", "92562", "site_jsonld"),
        Candidate("41430 Auto Mall Parkway ", "Murrieta", "CA", "92562", "site_jsonld"),
    ], MURRIETA, 25.0, refusals)
    assert got is not None and got.street.strip() == "41430 Auto Mall Parkway"


def test_nothing_is_returned_when_every_candidate_fails_verification():
    refusals: list[str] = []
    assert choose([Candidate("1 Main Street", "Boston", "MA", "02108", "osm_website")],
                  MURRIETA, 25.0, refusals) is None
    assert refusals


# --------------------------------------------------------------------------- #
# cross-dealer guard
# --------------------------------------------------------------------------- #
def test_one_address_resolved_for_two_dealers_is_withdrawn_from_both(monkeypatch):
    """A group platform that serves the same schema.org block on every sibling
    host would otherwise give two stores one storefront."""
    from backend.scripts import backfill_dealership_addresses as mod

    class _FakeConn:
        def execute(self, *_a, **_k):
            return self

        def fetchall(self):
            return []

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    monkeypatch.setattr("backend.db.inventory_db.db_conn", lambda *a, **k: _FakeConn())

    sibling = Dealer(registry_id=561, name="VW of Murrieta",
                     site="https://www.vwofmurrieta.com", city="Murrieta", state="CA",
                     lat=33.53, lon=-117.17, street_address="", zip_code="", active_cars=300)
    outcomes = {
        560: mod.Outcome(dealer=MURRIETA, street="41430 Auto Mall Pkwy", zip_code="92562",
                         source="site_jsonld"),
        561: mod.Outcome(dealer=sibling, street="41430 Auto Mall Parkway", zip_code="92562",
                         source="site_jsonld"),
    }
    assert mod.drop_shared_storefronts(outcomes) == 2
    assert outcomes[560].street == "" and outcomes[561].street == ""
    assert any("shared_storefront_with" in r for r in outcomes[560].refusals)


def test_distinct_addresses_survive_the_cross_dealer_guard(monkeypatch):
    from backend.scripts import backfill_dealership_addresses as mod

    class _FakeConn:
        def execute(self, *_a, **_k):
            return self

        def fetchall(self):
            return []

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    monkeypatch.setattr("backend.db.inventory_db.db_conn", lambda *a, **k: _FakeConn())

    sibling = Dealer(registry_id=561, name="VW of Murrieta",
                     site="https://www.vwofmurrieta.com", city="Murrieta", state="CA",
                     lat=33.53, lon=-117.17, street_address="", zip_code="", active_cars=300)
    outcomes = {
        560: mod.Outcome(dealer=MURRIETA, street="41430 Auto Mall Pkwy", zip_code="92562",
                         source="site_jsonld"),
        561: mod.Outcome(dealer=sibling, street="41300 Auto Mall Pkwy", zip_code="92562",
                         source="site_jsonld"),
    }
    assert mod.drop_shared_storefronts(outcomes) == 0
    assert outcomes[560].street and outcomes[561].street
