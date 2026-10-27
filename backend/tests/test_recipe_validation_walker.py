"""P12B.2: ``check_all`` / ``judge`` split and the walker's raw-VIN termination.

Stub fetches only (``fetch(recipe, body, base_url, url) -> (status, parsed)``); no HTTP, no DB.
Fixtures come from test_recipe_validation (same feeds, same shapes).
"""
from __future__ import annotations

import json

import pytest

from backend.scanner import recipe_validation as rv
from backend.tests.test_recipe_validation import (
    DEALER, HH, HH_ORIGIN, HH_STREET, NAME, ORIGIN, PLACE, _Feed, _TVSite, _cc_recipe, _hh_cc, _hh_listing, _lot,
    _no_registry_or_db, _tv_recipe, _tv_rows,
)

_HH_PLACE = {"dealer_city": "Huntersville", "dealer_state": "NC", "dealer_zip": "28078", "dealer_address": HH_STREET}


def _golden(rep: rv.RecipeValidationReport) -> dict:
    d = rep.to_dict()
    d.pop("stamp")
    return d


def _fixtures():
    """(recipes, fresh fetch factory) for verdict shapes the existing suite covers."""
    return [
        ([_cc_recipe()], lambda: _Feed(_lot(120, 70))),
        ([_cc_recipe()], lambda: _Feed(_lot(100, 57), per_page=48, page2_empty=True)),
        ([_cc_recipe()], lambda: _Feed(_lot(50, 0), census={"new": 50, "used": 40})),
        ([_cc_recipe()], lambda: _Feed(_lot(10, 10), status=403)),
        ([_tv_recipe("used")], lambda: _TVSite(new=_tv_rows("1C4N", "New", 40))),
        ([_tv_recipe("used")], lambda: _TVSite()),
    ]


@pytest.mark.parametrize("idx", range(6))
def test_check_all_then_judge_equals_validate_recipe_set(idx):
    recipes, make = _fixtures()[idx]
    whole = rv.validate_recipe_set(DEALER, recipes, make(), base_url=ORIGIN, dealer_name=NAME, place=PLACE, one_condition_ok=set())
    checks = rv.check_all(DEALER, recipes, make(), base_url=ORIGIN, dealer_name=NAME, place=PLACE)
    split = rv.judge(checks, one_condition_ok=set())
    assert _golden(split) == _golden(whole)


def test_judge_of_cached_checks_does_not_walk_again():
    feed = _Feed(_lot(120, 70))
    checks = rv.check_all(DEALER, [_cc_recipe()], feed, base_url=ORIGIN, dealer_name=NAME, place=PLACE, pages=2)
    walked = len(feed.calls)
    rep = rv.validate_recipe_set(DEALER, [_cc_recipe()], None, checks=checks, one_condition_ok=set())
    assert len(feed.calls) == walked and rep.verdict == "ok" and rep.pages_replayed == 2


def test_gate_recipes_takes_pages_and_checks(tmp_path):
    feed = _Feed(_lot(120, 70), per_page=50)
    kept, rep = rv.gate_recipes(DEALER, [_cc_recipe()], base_url=ORIGIN, dealer_name=NAME, place=PLACE, fetch=feed,
                                log_root=tmp_path, record=False, pages=3)
    assert [c["page"] for c in feed.calls] == [1, 2, 3] and rep.pages_replayed == 3 and len(kept) == 1
    checks = rv.check_all(DEALER, [_cc_recipe()], feed, base_url=ORIGIN, dealer_name=NAME, place=PLACE)
    n = len(feed.calls)
    kept, rep = rv.gate_recipes(DEALER, [_cc_recipe()], base_url=ORIGIN, dealer_name=NAME, place=PLACE, fetch=feed,
                                log_root=tmp_path, record=False, checks=checks)
    assert len(feed.calls) == n and len(kept) == 1


def _sibling_page_feed(own: int, siblings: int):
    """A group feed whose page 1 is this store's cars and page 2 only sibling rooftops."""
    mine = [_hh_listing(f"00HHS{i:012d}", HH_STREET, "209014") for i in range(own)]
    other = [_hh_listing(f"00HOO{i:012d}", "Hoover Toyota", "MP99999") for i in range(siblings)]
    pages = {1: mine, 2: other}

    def fetch(recipe, body, base_url, url=None):
        return 200, {"data": {"listings": pages.get(int(body.get("page") or 1), []), "total_vehicle_count": own + siblings}}

    return fetch


def test_a_page_of_sibling_rows_is_not_a_short_page():
    """Page 2 holds no kept VIN but six refused siblings: not the end of the feed, and
    the walk must not call it a short page (it used to read kept VINs only)."""
    rep = rv.validate_recipe_set(HH, [_hh_cc()], _sibling_page_feed(6, 6), base_url=HH_ORIGIN,
                                 dealer_name="Honda Of Huntersville", place=_HH_PLACE, one_condition_ok=set())
    chk = rep.recipes[0]
    assert chk.vins == 6 and chk.gate_rejected == 6
    assert chk.short_page is False and rep.flags["short_page"] is False
    assert chk.pages[1]["new_vins"] == 0 and chk.pages[1]["raw_new_vins"] == 6
    assert chk.exhausted is True  # raw 12 reached the site total of 12


def test_refusals_on_an_otherwise_empty_page_do_not_end_the_walk():
    own, sib = 3, 4
    mine = [_hh_listing(f"00HHS{i:012d}", HH_STREET, "209014") for i in range(own)]
    other = [_hh_listing(f"00HOO{i:012d}", "Hoover Toyota", "MP99999") for i in range(sib)]
    more = [_hh_listing(f"00HHT{i:012d}", HH_STREET, "209014") for i in range(2)]
    pages = {1: mine, 2: other, 3: more}

    def fetch(recipe, body, base_url, url=None):
        return 200, {"data": {"listings": pages.get(int(body.get("page") or 1), []), "total_vehicle_count": 99}}

    rep = rv.validate_recipe_set(HH, [_hh_cc()], fetch, base_url=HH_ORIGIN, dealer_name="Honda Of Huntersville",
                                 place=_HH_PLACE, pages=4, one_condition_ok=set())
    chk = rep.recipes[0]
    assert [p["status"] for p in chk.pages] == [200, 200, 200, 200]  # walked through the sibling-only page 2
    assert chk.vins == 5 and chk.gate_rejected == 4


def test_a_truly_empty_page_two_is_still_a_short_page():
    feed = _Feed(_lot(100, 57), per_page=48, page2_empty=True)
    rep = rv.validate_recipe_set(DEALER, [_cc_recipe()], feed, base_url=ORIGIN, dealer_name=NAME, place=PLACE, one_condition_ok=set())
    assert rep.recipes[0].short_page is True and rep.verdict == "reject"


def test_one_condition_rejection_is_preserved():
    """Used side 30 VINs, its new sibling empty at save but answering 40 on the probe:
    rejected as one_condition, as before the refactor."""
    site = _TVSite(new=_tv_rows("1C4N", "New", 40))
    checks = rv.check_all(DEALER, [_tv_recipe("used")], site, base_url=ORIGIN, dealer_name=NAME, place=PLACE)
    rep = rv.judge(checks, one_condition_ok=set())
    assert rep.status == "rejected:one_condition" and rep.other_side_probe["other_vins"] == 40
