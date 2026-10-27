"""P12B.3: ``validate_recipe`` is a paced count over the replay walker.

Stub fetches only (the signature of ``recipes._replay_request``); no HTTP, no DB.
"""
from __future__ import annotations

import inspect
import re
import time

import pytest

from backend.scanner import recipe_synth, recipe_validation as rv
from backend.scanner.recipes import (
    PAGINATION_ALGOLIA, PAGINATION_CARSCOMMERCE, PAGINATION_COSMOS_PT, PAGINATION_DEALER_COM, PAGINATION_DEP_SRP,
    PAGINATION_GRAPHQL_OFFSET, PAGINATION_HTML_PAGE, PAGINATION_JAZEL_SRP, PAGINATION_NONE, PAGINATION_PAGE_QUERY,
    PAGINATION_TYPESENSE, EndpointRecipe, _mutate_for_page, _url_for_page,
)
from backend.tests.test_recipe_synth import _dep_srp
from backend.tests.test_recipe_validation import (
    CC_URL, DEALER, NAME, ORIGIN, PLACE, _Feed, _cc_recipe, _lot, _no_registry_or_db,  # noqa: F401 (autouse fixture)
)


class _Recording:
    """Wraps a stub fetch and records the (url, body) of every call."""

    def __init__(self, inner):
        self.inner, self.calls = inner, []

    def __call__(self, recipe, body, base_url, url=None):
        self.calls.append((url or recipe.url, body))
        return self.inner(recipe, body, base_url, url)


def _dep_site(total=56):
    pages = {1: _dep_srp([f"2HGFC2F5XNH1{i:05d}" for i in range(48)], total),
             2: _dep_srp([f"2HGFC2F5XNH9{i:05d}" for i in range(total - 48)], total)}

    def fetch(recipe, body, base_url, url=None):
        m = re.search(r"[?&](?:p|page)=(\d+)", url or "")
        return 200, pages.get(int(m.group(1)) if m else 1, "<html></html>")
    return fetch


def _html_recipe(pagination, url):
    return EndpointRecipe(dealer_id=DEALER, url=url, method="GET", content_type="text/html", post_template=None,
                          auth_headers={}, pagination=pagination, total_count=56, provider_hint="dealer_eprocess")


def _cases():
    return {
        "carscommerce_post": (_cc_recipe(), lambda: _Feed(_lot(120, 70), per_page=100)),
        "dep": (_html_recipe(PAGINATION_DEP_SRP, ORIGIN + "/search/used/?tp=used&ct=48"), lambda: _dep_site()),
        "html_page": (_html_recipe(PAGINATION_HTML_PAGE, ORIGIN + "/used-inventory/?page=1"), lambda: _dep_site()),
        "page_query": (_html_recipe(PAGINATION_PAGE_QUERY, ORIGIN + "/inventory-used.json"), lambda: _dep_site()),
        "cosmos": (EndpointRecipe(dealer_id=DEALER, url=ORIGIN + "/api/cosmos/srp/vehicles", method="GET",
                                  content_type="application/json", post_template=None, auth_headers={},
                                  pagination=PAGINATION_COSMOS_PT, provider_hint="dealer_on_cosmos"),
                   lambda: (lambda r, b, base, url=None: (200, {"DisplayCards": []}))),
    }


@pytest.mark.parametrize("name", list(_cases()))
def test_validate_recipe_and_set_walk_the_same_fetch_with_equal_counts(name, monkeypatch):
    recipe, make = _cases()[name]
    rec_a, rec_b = _Recording(make()), _Recording(make())
    monkeypatch.setattr(rv, "_replay_request", rec_a)
    n = rv.validate_recipe(recipe, ORIGIN, DEALER, NAME, place=PLACE)
    rep = rv.validate_recipe_set(DEALER, [recipe], rec_b, base_url=ORIGIN, dealer_name=NAME, place=PLACE,
                                 pages=rv._VALIDATE_MAX_PAGES, one_condition_ok=set())
    assert n == rep.vins_total
    assert rec_a.calls == rec_b.calls and rec_a.calls  # one walker, one request sequence
    if name != "cosmos":
        assert n > 0


def test_cosmos_validation_pages_with_pt_and_pn(monkeypatch):
    recipe, make = _cases()["cosmos"]
    rec = _Recording(make())
    monkeypatch.setattr(rv, "_replay_request", rec)
    rv.validate_recipe(recipe, ORIGIN, DEALER, NAME, place=PLACE)
    assert "pt=1" in rec.calls[0][0] and "pn=96" in rec.calls[0][0]


def test_walker_delegates_are_validate_recipe(monkeypatch):
    # the names recipe_synth re-exports (test_recipe_synth_surface) are the same objects
    assert recipe_synth._validate_dep is rv._validate_dep and recipe_synth.validate_recipe is rv.validate_recipe
    seen = []
    monkeypatch.setattr(rv, "validate_recipe", lambda *a, **k: seen.append((a, k)) or 7)
    r = _html_recipe(PAGINATION_DEP_SRP, ORIGIN + "/x/")
    for fn in (rv._validate_json_feed, rv._validate_dep, rv._validate_html_walk, rv._validate_cosmos):
        assert fn(r, ORIGIN, DEALER, NAME, 5, PLACE) == 7
    assert len(seen) == 4 and all(k["max_pages"] == 5 and k["place"] == PLACE for _, k in seen)


def test_validation_is_paced_by_the_synth_fetch_clock(monkeypatch):
    import backend.scanner.synth.http as sh

    paced = []
    monkeypatch.setattr(sh, "_pace", lambda: paced.append(time.monotonic()))
    feed = _Feed(_lot(120, 70), per_page=100)
    monkeypatch.setattr(rv, "_replay_request", feed)
    rv.validate_recipe(_cc_recipe(), ORIGIN, DEALER, NAME, place=PLACE)
    assert len(paced) == len(feed.calls) >= 2  # one wait per request, none skipped


# ── D-OD6: junk post_template ──────────────────────────────────────────────────

def test_get_with_junk_post_template_replays_without_a_body_and_notes_it(monkeypatch):
    r = _html_recipe(PAGINATION_DEP_SRP, ORIGIN + "/search/used/?tp=used")
    r.post_template = "not json {"
    rec = _Recording(_dep_site())
    monkeypatch.setattr(rv, "_replay_request", rec)
    assert rv.validate_recipe(r, ORIGIN, DEALER, NAME, place=PLACE) == 56
    assert all(body is None for _, body in rec.calls)
    rep = rv.validate_recipe_set(DEALER, [r], _dep_site(), base_url=ORIGIN, dealer_name=NAME, place=PLACE, one_condition_ok=set())
    assert any("replayed without a body" in n for n in rep.notes)
    assert not rep.recipes[0].error


def test_post_with_junk_post_template_is_an_error_and_counts_zero(monkeypatch):
    r = _cc_recipe()
    r.post_template = "not json {"
    rec = _Recording(_Feed(_lot(10, 10)))
    monkeypatch.setattr(rv, "_replay_request", rec)
    assert rv.validate_recipe(r, ORIGIN, DEALER, NAME, place=PLACE) == 0
    assert rec.calls == []
    rep = rv.validate_recipe_set(DEALER, [r], _Feed(_lot(10, 10)), base_url=ORIGIN, dealer_name=NAME, place=PLACE, one_condition_ok=set())
    assert rep.recipes[0].error == "post_template is not JSON"


# ── every synth registry template pages through the replay walker ──────────────

_SAMPLE_BODY = {
    PAGINATION_CARSCOMMERCE: {"page": 1, "perPage": 100},
    PAGINATION_TYPESENSE: {"searches": [{"page": 1, "per_page": 100}]},
    PAGINATION_DEALER_COM: {"inventoryParameters": {"start": "0", "pageSize": "48"}},
    PAGINATION_ALGOLIA: {"page": 0, "hitsPerPage": 100},
    PAGINATION_GRAPHQL_OFFSET: {"variables": {"sp": {"paging": {"offset": 0, "limit": 100}}}},
}


def test_every_synth_template_emits_a_pagination_the_replay_walker_can_page():
    from backend.scanner.synth.registry import PLATFORM_TEMPLATES
    import copy

    known = {v for k, v in vars(__import__("backend.scanner.recipes", fromlist=["x"])).items() if k.startswith("PAGINATION_")}
    emitted: set[str] = set()
    for tmpl in PLATFORM_TEMPLATES:
        if tmpl.synth is None:
            continue
        src = inspect.getsource(inspect.getmodule(tmpl.synth))
        emitted |= set(re.findall(r"\b(PAGINATION_[A-Z_]+)\b", src))
    from backend.scanner import recipes as R

    values = {R.__dict__[n] for n in emitted if n in R.__dict__}
    assert values and values <= known
    for pag in values - {PAGINATION_NONE}:
        r = EndpointRecipe(dealer_id=DEALER, url=ORIGIN + "/srp/?q=1", method="GET" if pag not in _SAMPLE_BODY else "POST",
                           content_type="application/json", post_template=None, auth_headers={}, pagination=pag)
        tpl = copy.deepcopy(_SAMPLE_BODY.get(pag))
        page1 = (_url_for_page(r, 0), _mutate_for_page(r, copy.deepcopy(tpl), 0) if tpl else None)
        page2 = (_url_for_page(r, 1), _mutate_for_page(r, copy.deepcopy(tpl), 1) if tpl else None)
        assert page1 != page2, f"{pag}: page 2 asks for the same thing as page 1"
