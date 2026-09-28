"""Recipe-set validation against the site's own count (recipe_validation.py).

Every fetch is a stub with the signature of ``recipes._replay_request``
(``fetch(recipe, body, base_url, url) -> (status, parsed)``); no HTTP, no DB.
Shapes follow the 09-24 / 09-26 censuses: one-condition captures, section-scoped
carscommerce bodies, dealer.com pages stopping at 48 rows, dead auth (401/403).
"""
from __future__ import annotations

import copy
import json
from collections import Counter

import pytest

from backend.scanner import recipe_validation as rv
from backend.scanner.recipes import (
    PAGINATION_CARSCOMMERCE,
    PAGINATION_COSMOS_PT,
    PAGINATION_DEALER_COM,
    PAGINATION_DEP_SRP,
    PAGINATION_HTML_PAGE,
    PAGINATION_NONE,
    PAGINATION_TYPESENSE,
    EndpointRecipe,
)

DEALER = "austintoyota-com"
NAME = "Austin Toyota"
ORIGIN = "https://www.austintoyota.com"
PLACE = {"dealer_city": "Austin", "dealer_state": "TX"}
CC_URL = "https://websites-search.api.carscommerce.inc/api/v1/listings/1/search"


@pytest.fixture(autouse=True)
def _no_registry_or_db(monkeypatch):
    """The rooftop gate's registry lookup and the scan_hints store both open the
    inventory DB; validation must be judged on the stubbed feed alone."""
    import backend.parsers as parsers
    from backend.scanner import recipe_store

    monkeypatch.setattr(parsers, "_cached_roster_place_items", lambda url: ())
    monkeypatch.setattr(recipe_store, "set_scan_hints", lambda *a, **k: True)
    monkeypatch.setattr(recipe_store, "db_load_recipes", lambda *a, **k: [])
    monkeypatch.setattr(recipe_store, "db_save_recipes", lambda *a, **k: None)


def _listing(vin: str, cond: str) -> dict:
    return {"vin": vin, "source_id": "99", "make": "Toyota", "model": "Camry", "year": 2025, "type": cond,
            "dealer": {"id": "1", "ccid": 1, "city": None, "name": None, "state": None, "address": None,
                       "zipcode": None, "location": "Austin, TX"},
            "extra_fields": {"Location": "Austin, TX"}}


def _lot(n_new: int, n_used: int) -> list[dict]:
    """A mixed lot in the order a whole-lot SRP serves it (conditions interleaved)."""
    new = [_listing(f"4T1N{i:013d}", "New") for i in range(n_new)]
    used = [_listing(f"4T1U{i:013d}", "Used") for i in range(n_used)]
    out: list[dict] = []
    while new or used:
        if new:
            out.append(new.pop(0))
        if used:
            out.append(used.pop(0))
    return out


def _cc_recipe(facet_filters: dict | None = None, filters: dict | None = None) -> EndpointRecipe:
    body: dict = {"page": 1, "perPage": 100, "filters": filters or {"status": ["publish"]}, "facets": ["year"]}
    if facet_filters:
        body["facetFilters"] = facet_filters
    return EndpointRecipe(dealer_id=DEALER, url=CC_URL, method="POST", content_type="application/json",
                          post_template=json.dumps(body), pagination=PAGINATION_CARSCOMMERCE,
                          provider_hint="carscommerce", auth_headers={"x-api-key": "k"})


class _Feed:
    """A CarsCommerce account: pages of *listings* (``per_page`` rows whatever the
    body asks, like the real server), an optional ``total_vehicle_count`` and a
    ``facets: [type_slug]`` census that may describe a bigger lot than the pages."""

    def __init__(self, listings: list[dict], *, per_page: int = 100, expose_total: bool = True,
                 census: dict[str, int] | None = None, status: int = 200, page2_empty: bool = False):
        self.listings, self.per_page, self.expose_total = listings, per_page, expose_total
        self.census, self.status, self.page2_empty = census, status, page2_empty
        self.calls: list[dict] = []

    def __call__(self, recipe, body, base_url, url=None):
        self.calls.append(copy.deepcopy(body))
        if self.status != 200:
            return self.status, None
        if body.get("facets") == ["type_slug"] and body.get("perPage") == 1:
            counts = self.census or Counter(l["type"].lower() for l in self.listings)
            return 200, {"data": {"total_vehicle_count": sum(counts.values()), "listings": self.listings[:1],
                                  "facets": [{"name": "type_slug",
                                              "values": [{"key": k, "doc_count": n} for k, n in counts.items()]}]}}
        page = int(body.get("page") or 1)
        chunk = [] if (page > 1 and self.page2_empty) else self.listings[(page - 1) * self.per_page: page * self.per_page]
        data: dict = {"listings": chunk}
        if self.expose_total:
            data["total_vehicle_count"] = len(self.listings)
        return 200, {"data": data}


def _run(recipes, fetch, **kw):
    kw.setdefault("one_condition_ok", set())
    return rv.validate_recipe_set(DEALER, recipes, fetch, base_url=ORIGIN, dealer_name=NAME, place=PLACE, **kw)


# ── verdicts ───────────────────────────────────────────────────────────────────

def test_full_lot_is_ok():
    feed = _Feed(_lot(120, 70))
    rep = _run([_cc_recipe()], feed)
    assert rep.verdict == "ok" and rep.reasons == []
    assert rep.vins_total == 190 and rep.site_total == 190 and rep.coverage == 1.0
    assert rep.per_condition == {"new": 120, "used": 70}
    assert not any(rep.flags.values())
    assert [c["page"] for c in feed.calls] == [1, 2]  # two pages, no census needed
    assert rep.status == "ok"


def test_one_condition_is_rejected_when_the_site_sells_both():
    """The captured recipe is the new SRP's query: 50 new rows, and the account's
    own type_slug census says 40 used exist (none of the nine 09-26 verdicts
    were used-only lots)."""
    feed = _Feed(_lot(50, 0), census={"new": 50, "used": 40})
    rep = _run([_cc_recipe()], feed)
    assert rep.verdict == "reject"
    assert rep.flags["one_condition"] is True
    assert rep.reasons and rep.reasons[0].startswith("one_condition: only new rows while the site sells both")
    assert rep.site_conditions == {"new": 50, "used": 40}
    assert rep.status == "rejected:one_condition"
    assert any(c.get("facets") == ["type_slug"] for c in feed.calls)  # the census request


def test_one_condition_allowed_for_listed_dealers(tmp_path):
    feed = _Feed(_lot(0, 209), census={"new": 5, "used": 209})
    ok_file = tmp_path / "one_condition_ok.txt"
    ok_file.write_text("# used-only lots\nquantumautosales-com   # Quantum\n" f"{DEALER}\n", encoding="utf-8")
    assert rv.one_condition_ok_ids(ok_file) == {"quantumautosales-com", DEALER}
    rep = _run([_cc_recipe()], feed, one_condition_ok=rv.one_condition_ok_ids(ok_file))
    assert rep.verdict == "ok"
    assert rep.flags["one_condition"] is False
    assert any(n.startswith("one condition by design (used only") for n in rep.notes)


def test_section_scoped_recipe_is_rejected():
    """facetFilters carrying type_slug / make / model_slug pin one SRP section
    (16 of the 44 no_rows verdicts on 2026-09-24)."""
    rec = _cc_recipe(facet_filters={"type_slug": ["Certified Used"], "make": ["Honda"]})
    rep = _run([rec], _Feed(_lot(0, 109)))
    assert rep.verdict == "reject"
    assert rep.flags["section_scoped"] is True
    assert rep.recipes[0].section_scoped is True
    assert any(r.startswith("section_scoped: every recipe pins one SRP section") for r in rep.reasons)


def test_section_scoped_is_only_a_note_when_a_whole_lot_recipe_sits_beside_it():
    scoped = _cc_recipe(facet_filters={"type_slug": ["used"]})
    whole = _cc_recipe()
    rep = _run([scoped, whole], _Feed(_lot(60, 40)))
    assert rep.verdict == "uncertain" and rep.reasons == ["section_scoped_partial"]  # saved, flagged
    assert rep.flags["section_scoped"] is True
    assert any(n.startswith("section_scoped: 1 of 2") for n in rep.notes)
    assert rep.status == "uncertain:section_scoped_partial"


def test_short_page_is_flagged_and_low_coverage_rejects():
    """Page 1 stops at the server's 48 rows of 157 and page 2 comes back empty
    (the dealer.com pageSize bug shape, 14 dealers on 2026-09-24)."""
    feed = _Feed(_lot(100, 57), per_page=48, page2_empty=True)
    rep = _run([_cc_recipe()], feed)
    assert rep.flags["short_page"] is True
    assert rep.recipes[0].short_page is True
    assert rep.vins_total == 48 and rep.site_total == 157 and rep.coverage == round(48 / 157, 3)
    assert rep.verdict == "reject"
    assert rep.reasons[0].startswith("short_page:")
    assert "page 1 48 of 157, page 2 empty" in rep.reasons[0]


def test_short_page_above_the_floor_is_uncertain_not_reject():
    feed = _Feed(_lot(50, 30), per_page=60, page2_empty=True)  # 60 of 80 = 75%
    rep = _run([_cc_recipe()], feed)
    assert rep.flags["short_page"] is True
    assert rep.verdict == "uncertain" and rep.reasons == ["short_page"]


def test_unknown_site_total_is_uncertain_not_reject():
    feed = _Feed(_lot(40, 30), per_page=35, expose_total=False)
    rep = _run([_cc_recipe()], feed)
    assert rep.site_total is None and rep.coverage is None
    assert rep.vins_total == 70 and rep.per_condition == {"new": 40, "used": 30}
    assert rep.verdict == "uncertain"
    assert rep.reasons == ["site_total_unknown: platform exposes no count; coverage cannot be judged"]
    assert rep.status == "uncertain:site_total_unknown"
    assert not any(rep.flags.values())


def test_403_sets_auth_needed_and_rejects():
    feed = _Feed(_lot(10, 10), status=403)
    rep = _run([_cc_recipe()], feed)
    assert rep.flags["auth_needed"] is True and rep.flags["zero_rows"] is True
    assert rep.verdict == "reject"
    assert rep.reasons[0].startswith("auth_needed: HTTP 403")
    assert rep.reasons[1].startswith("zero_rows")
    assert rep.status == "rejected:auth_needed"
    assert rep.recipes[0].pages == [{"page": 1, "status": 403, "new_vins": 0}]
    assert len(feed.calls) == 1  # no page 2, no census against a dead edge


def test_401_mid_walk_keeps_the_vins_and_is_uncertain():
    feed = _Feed(_lot(100, 20), per_page=100)
    first = feed.__call__

    def flaky(recipe, body, base_url, url=None):
        if int(body.get("page") or 1) > 1:
            return 401, None
        return first(recipe, body, base_url, url)

    rep = _run([_cc_recipe()], flaky)
    assert rep.vins_total == 100
    assert rep.flags["auth_needed"] is False  # auth died after VINs came back
    assert rep.flags["short_page"] is False  # a 401 page is not a short page
    assert rep.recipes[0].auth_mid_walk is True
    assert rep.verdict == "uncertain" and rep.reasons == ["auth_mid_walk"]


def test_zero_rows_without_auth_error_rejects():
    rep = _run([_cc_recipe()], _Feed([]))
    assert rep.verdict == "reject" and rep.flags["zero_rows"] is True
    assert rep.reasons == ["zero_rows: no VIN from any recipe"]


def test_empty_recipe_set_rejects():
    rep = _run([], _Feed(_lot(1, 1)))
    assert rep.verdict == "reject" and rep.reasons == ["zero_rows: no recipe to validate"]


def _tv_recipe(stem: str) -> EndpointRecipe:
    return EndpointRecipe(dealer_id=DEALER, url=f"{ORIGIN}/inventory-{stem}.json", method="GET", content_type="application/json",
                          post_template=None, pagination=PAGINATION_NONE, provider_hint="generic_json")


def _tv_rows(prefix: str, cond: str, n: int) -> list[dict]:
    return [{"vin": f"{prefix}{i:013d}", "condition": cond, "year": 2022, "make": "Jeep", "model": "Wrangler"} for i in range(n)]


class _TVSite:
    """Team Velocity: one feed file per condition; ``new`` is what /inventory-new.json answers."""

    def __init__(self, used: int = 30, new: list[dict] | None = None, new_status: int = 200):
        self.used, self.new, self.new_status = _tv_rows("1C4U", "Used", used), new, new_status
        self.urls: list[str] = []

    def __call__(self, recipe, body, base_url, url=None):
        self.urls.append(url or recipe.url)
        if (url or recipe.url).endswith("/inventory-new.json"):
            return self.new_status, ([] if self.new is None else self.new) if self.new_status == 200 else None
        return 200, self.used


def test_pinned_set_asks_the_other_side_and_a_live_other_side_rejects():
    """A used-only Team Velocity set is one-condition only when /inventory-new.json
    itself answers cars: then the synth dropped or never built the new recipe."""
    site = _TVSite(new=_tv_rows("1C4N", "New", 40))
    rep = _run([_tv_recipe("used")], site)
    assert rep.recipes[0].pinned_conditions == ["used"]
    assert rep.per_condition == {"used": 30}
    assert rep.site_total == 30  # single-shot list: its length is the site's count
    assert site.urls[-1] == f"{ORIGIN}/inventory-new.json"
    assert rep.other_side_probe == {"url": f"{ORIGIN}/inventory-new.json", "status": 200, "vins": 40, "error": None, "other_vins": 40}
    assert rep.verdict == "reject" and rep.status == "rejected:one_condition"
    assert rep.reasons[0].startswith("one_condition: only used rows while the site sells both (recipes pinned to ['used']")
    assert "the new side answers 40 VIN(s)" in rep.reasons[0]


@pytest.mark.parametrize("new_status,new_rows,expect_status", [
    (200, [], 200),                       # the new feed exists and is empty: a used-only lot
    (404, None, 404),                     # no new feed at all
    (200, _tv_rows("1C4N", "New", 3), 200),  # 3 new cars: under the floor the synth keeps
])
def test_pinned_set_whose_other_side_answers_nothing_is_uncertain_and_saves(tmp_path, monkeypatch, new_status, new_rows, expect_status):
    """The site's own new feed saying 0 (or 404, or a handful) is evidence of a
    used-only lot, not of a missing side: the set saves as uncertain and the
    post-scan assess decides. Rejecting it looped: no recipe -> re-synth rejected
    -> a browser capture a day -> rejected again."""
    site = _TVSite(new=new_rows, new_status=new_status)
    rep = _run([_tv_recipe("used")], site)
    assert rep.verdict == "uncertain" and rep.status == "uncertain:one_condition_unverified"
    assert rep.flags["one_condition"] is False and rep.reasons == ["one_condition_unverified"]
    assert rep.other_side_probe["status"] == expect_status and rep.other_side_probe["other_vins"] == len(new_rows or [])
    assert any(n.startswith("one_condition_unverified: recipes pinned to ['used']; the new side at") for n in rep.notes)
    # and the gate keeps it, with the status on the hint
    monkeypatch.setattr(rv, "LOG_ROOT", tmp_path)
    monkeypatch.setattr(rv, "one_condition_ok_ids", lambda path=None: set())
    kept, report = rv.gate_recipes(DEALER, [_tv_recipe("used")], base_url=ORIGIN, dealer_name=NAME, place=PLACE, fetch=_TVSite(new=new_rows, new_status=new_status), log_root=tmp_path)
    assert len(kept) == 1 and report.status == "uncertain:one_condition_unverified"
    log = (tmp_path / DEALER / "discovery.md").read_text(encoding="utf-8")
    assert "UNCERTAIN" in log and "other side probed" in log


def test_sibling_recipe_that_answered_zero_rows_is_uncertain_not_reject():
    """Case (b): the set carries the new recipe and it replayed 200 with no rows.
    No probe is needed — the site's own feed already answered."""
    site = _TVSite(new=[])
    rep = _run([_tv_recipe("used"), _tv_recipe("new")], site)
    assert rep.per_condition == {"used": 30}
    assert rep.verdict == "uncertain" and rep.status == "uncertain:one_condition_unverified"
    assert rep.other_side_probe is None
    assert any(n.startswith("one_condition_unverified: the new recipe (") and "answered 200 with no rows" in n for n in rep.notes)
    # each feed asked once (page 1 + page 2 of a single-shot list is one request), nothing extra
    assert site.urls.count(f"{ORIGIN}/inventory-new.json") == 1


def test_body_pinned_set_without_a_derivable_other_side_is_uncertain():
    """Typesense ``filter_by: condition:=Used``: the pin lives in the body, no URL for
    the new side exists to ask, so the set is uncertain rather than rejected."""
    body = {"searches": [{"collection": "vehicles", "q": "*", "per_page": 50, "page": 1, "filter_by": "condition:=Used"}]}
    ts = EndpointRecipe(dealer_id=DEALER, url="https://search.example.com/multi_search", method="POST", content_type="application/json",
                        post_template=json.dumps(body), pagination=PAGINATION_TYPESENSE, provider_hint="typesense")
    calls = []

    def fetch(recipe, body, base_url, url=None):
        calls.append(body)
        page = int(body["searches"][0].get("page") or 1)
        hits = [{"document": {"vin": f"1C4U{i:013d}", "condition": "Used", "yr": 2022, "make": "Jeep", "model": "Wrangler",
                              "dealer": {"name": NAME, "city": "Austin", "state": "TX", "url": ORIGIN}}} for i in range(30)] if page == 1 else []
        return 200, {"results": [{"found": 30, "hits": hits}]}

    rep = _run([ts], fetch)
    assert rep.recipes[0].pinned_conditions == ["used"]
    assert rep.verdict == "uncertain" and rep.status == "uncertain:one_condition_unverified"
    assert rep.other_side_probe is None
    assert any("no URL for the new side can be derived" in n for n in rep.notes)


@pytest.mark.parametrize("url,pagination,only,expect", [
    (f"{ORIGIN}/inventory-used.json", PAGINATION_NONE, "used", f"{ORIGIN}/inventory-new.json"),
    (f"{ORIGIN}/inventory-cpo.json", PAGINATION_NONE, "used", f"{ORIGIN}/inventory-new.json"),
    (f"{ORIGIN}/inventory-new.json", PAGINATION_NONE, "new", f"{ORIGIN}/inventory-used.json"),
    (f"{ORIGIN}/search/used/?ct=48&tp=used", PAGINATION_DEP_SRP, "used", f"{ORIGIN}/search/used/?ct=48&tp=new"),
    (f"{ORIGIN}/search/new-toyota/?mk=63&tp=new", PAGINATION_DEP_SRP, "new", f"{ORIGIN}/search/new-toyota/?mk=63&tp=used"),
    (f"{ORIGIN}/inventory/?condition[]=New", PAGINATION_HTML_PAGE, "new", f"{ORIGIN}/inventory/?condition[]=Used"),
    (f"{ORIGIN}/used-inventory/", PAGINATION_DEP_SRP, "used", f"{ORIGIN}/new-inventory/"),
    (f"{ORIGIN}/inventory/new", PAGINATION_HTML_PAGE, "new", f"{ORIGIN}/inventory/used"),
    (f"{ORIGIN}/pre-owned-vehicles/", PAGINATION_HTML_PAGE, "used", f"{ORIGIN}/new-vehicles/"),
    (f"{ORIGIN}/inventory/", PAGINATION_HTML_PAGE, "used", None),  # nothing pinned in the URL
    (f"{ORIGIN}/used-inventory/", PAGINATION_NONE, "used", None),  # a path segment counts only on the HTML walks
])
def test_other_side_recipe_derivation(url, pagination, only, expect):
    r = EndpointRecipe(dealer_id=DEALER, url=url, method="GET", content_type="text/html", post_template=None,
                       pagination=pagination, provider_hint="dealer_eprocess", vehicle_rows=30, total_count=30)
    other = rv.other_side_recipe(r, only)
    if expect is None:
        assert other is None
    else:
        assert other.url == expect and other.pagination == pagination and other.vehicle_rows == 0 and other.total_count is None
        assert r.url == url  # the original is untouched


# ── site-total extractor table ─────────────────────────────────────────────────

def _rec(url: str, pagination: str, provider: str, post: str | None = None) -> EndpointRecipe:
    return EndpointRecipe(dealer_id=DEALER, url=url, method="POST" if post else "GET", content_type="",
                          post_template=post, pagination=pagination, provider_hint=provider)


@pytest.mark.parametrize("recipe,payload,expect", [
    (_rec(CC_URL, PAGINATION_CARSCOMMERCE, "carscommerce", "{}"), {"data": {"total_vehicle_count": 671, "listings": []}}, 671),
    (_rec("https://www.d.com/api/widget/ws-inv-data/getInventory", PAGINATION_DEALER_COM, "dealer_dot_com", "{}"),
     {"pageInfo": {"totalCount": 157, "pageSize": 48}, "inventory": []}, 157),
    (_rec("https://abc.a1.typesense.net/multi_search", PAGINATION_TYPESENSE, "typesense", "{}"),
     {"results": [{"found": 412, "hits": []}, {"found": 3, "hits": []}]}, 412),
    (_rec("https://www.d.com/api/cosmos/srp/vehicles?pt=1", PAGINATION_COSMOS_PT, "dealer_on_cosmos"),
     {"Paging": {"PaginationDataModel": {"TotalCount": 282}}, "DisplayCards": []}, 282),
    (_rec("https://www.d.com/gs-vehicle/list?filter=All", PAGINATION_HTML_PAGE, "autowall"),
     "<html><head><title>  93 Vehicles for Sale | Genesis</title></head></html>", 93),
    (_rec("https://www.d.com/used-vehicles/?p=1", PAGINATION_DEP_SRP, "dealer_eprocess"),
     '<div class="srp" data-vehicle_count="338"></div>', 338),
    (_rec("https://www.d.com/wp-json/wp/v2/vehicles?per_page=100", PAGINATION_NONE, "wp_vehicles_index"), [{"vin": "x"}] * 42, 42),
    (_rec("https://x.algolia.net/1/indexes/*/queries", "algolia_page", "motive_ridemotive", "{}"), {"nbHits": 555, "hits": []}, 555),
    (_rec("https://www.d.com/inventory/?page=1", PAGINATION_HTML_PAGE, "html_cards"), "<html>cards</html>", None),
    (_rec(CC_URL, PAGINATION_CARSCOMMERCE, "carscommerce", "{}"), {"data": {"listings": []}}, None),
])
def test_site_total_extractors(recipe, payload, expect):
    assert rv.extract_site_total(recipe, payload) == expect


def test_normalize_condition_vocabulary():
    assert [rv.normalize_condition(x) for x in ("New", "Certified Used", "cpo", "Pre-Owned", "used", "", None, "demo")] == \
        ["new", "certified", "certified", "used", "used", "unknown", "unknown", "unknown"]


# ── side effects: discovery.md, errors_index.md, scan_hints, gate ─────────────

def test_gate_rejects_log_and_return_nothing(tmp_path, monkeypatch):
    from backend.scanner import recipe_store

    hints: list[tuple] = []
    monkeypatch.setattr(recipe_store, "set_scan_hints", lambda d, h, **k: hints.append((d, h)) or True)
    feed = _Feed(_lot(50, 0), census={"new": 50, "used": 40})
    kept, rep = rv.gate_recipes(DEALER, [_cc_recipe()], base_url=ORIGIN, dealer_name=NAME, place=PLACE,
                                fetch=feed, context="synth", log_root=tmp_path)
    assert kept == [] and rep.verdict == "reject"
    log = (tmp_path / DEALER / "discovery.md").read_text(encoding="utf-8")
    assert "recipe validation (synth) — REJECT" in log
    assert "one_condition: only new rows" in log and "site census" in log
    idx = (tmp_path / "_learning" / "errors_index.md").read_text(encoding="utf-8")
    assert f"recipe_rejected:one_condition -> {DEALER}" in idx
    assert hints and hints[0][0] == DEALER
    assert hints[0][1]["recipe_status"] == "rejected:one_condition"
    assert hints[0][1]["recipe_validation"]["flags"] == {"one_condition": True}


def test_gate_keeps_uncertain_sets_with_a_status(tmp_path, monkeypatch):
    from backend.scanner import recipe_store

    hints: list[tuple] = []
    monkeypatch.setattr(recipe_store, "set_scan_hints", lambda d, h, **k: hints.append((d, h)) or True)
    rec = _cc_recipe()
    kept, rep = rv.gate_recipes(DEALER, [rec], base_url=ORIGIN, dealer_name=NAME, place=PLACE,
                                fetch=_Feed(_lot(40, 30), per_page=35, expose_total=False), log_root=tmp_path)
    assert kept == [rec] and rep.verdict == "uncertain"
    assert hints[0][1]["recipe_status"] == "uncertain:site_total_unknown"
    assert "UNCERTAIN" in (tmp_path / DEALER / "discovery.md").read_text(encoding="utf-8")
    assert not (tmp_path / "_learning" / "errors_index.md").exists()


def test_promote_from_ledger_refuses_a_rejected_capture(tmp_path, monkeypatch):
    import backend.scanner.recipes as rec
    from backend.scanner.network_observer import CapturedEndpoint

    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "recipes")
    ep = CapturedEndpoint(url=CC_URL, method="POST", content_type="application/json",
                          post_data_sample=json.dumps({"page": 1, "perPage": 20, "facetFilters": {"type_slug": ["used"]}}),
                          reason="legacy", sniffed=False, vehicle_rows=20, total_count=109, auth_headers={"x-api-key": "k"})
    out: dict = {}
    n = rec.promote_from_ledger(DEALER, "carscommerce", [ep], validate=True, base_url=ORIGIN, dealer_name=NAME,
                                place=PLACE, fetch=_Feed(_lot(0, 109)), log_root=tmp_path, validation_out=out)
    assert n == 0
    assert rec.load_recipes(DEALER) == []
    assert out["verdict"] == "reject" and out["status"].startswith("rejected:section_scoped")
    assert (tmp_path / DEALER / "discovery.md").exists()


def test_promote_from_ledger_saves_a_validated_capture(tmp_path, monkeypatch):
    import backend.scanner.recipes as rec
    from backend.scanner.network_observer import CapturedEndpoint

    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "recipes")
    ep = CapturedEndpoint(url=CC_URL, method="POST", content_type="application/json",
                          post_data_sample=json.dumps({"page": 1, "perPage": 20}),
                          reason="legacy", sniffed=False, vehicle_rows=20, total_count=190, auth_headers={"x-api-key": "k"})
    out: dict = {}
    n = rec.promote_from_ledger(DEALER, "carscommerce", [ep], validate=True, base_url=ORIGIN, dealer_name=NAME,
                                place=PLACE, fetch=_Feed(_lot(120, 70)), log_root=tmp_path, validation_out=out)
    assert n == 1
    (saved,) = rec.load_recipes(DEALER)
    assert saved.url == CC_URL and saved.vehicle_rows == 190 and saved.last_ok_at > 0
    assert out["verdict"] == "ok" and out["status"] == "ok"
    # the default path is unchanged: no validate, no fetch, nothing logged
    assert rec.promote_from_ledger("other-com", "carscommerce", [ep]) == 1
    assert not (tmp_path / "other-com").exists()


def test_ensure_recipe_refuses_a_rejected_synth(tmp_path, monkeypatch):
    """dealer_pipeline.ensure_recipe: validate_recipe still counts VINs per
    candidate; the set then goes through the gate and a reject is not saved."""
    from backend.scanner import recipe_synth, recipe_validation
    from backend.scripts import dealer_pipeline as dp
    import backend.scanner.recipes as rec

    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "recipes")
    monkeypatch.setattr(recipe_synth, "fetch_dealer_html", lambda url: "<html>cc</html>")
    monkeypatch.setattr(recipe_synth, "fingerprint_platform", lambda html, url: "carscommerce")
    monkeypatch.setattr(recipe_synth, "synthesize_recipes", lambda did, url, html, platform: [_cc_recipe()])
    monkeypatch.setattr(recipe_synth, "validate_recipe", lambda *a, **k: 50)
    monkeypatch.setattr("backend.scanner.dealer_place.learn_place", lambda *a, **k: dict(PLACE, place_source="test"))
    feeds = {"reject": _Feed(_lot(50, 0), census={"new": 50, "used": 40}), "ok": _Feed(_lot(120, 70))}
    which = {"k": "reject"}
    monkeypatch.setattr(recipe_validation, "_replay_request", lambda *a, **k: feeds[which["k"]](*a, **k))
    monkeypatch.setattr(recipe_validation, "LOG_ROOT", tmp_path)
    dealer = {"dealer_id": DEALER, "url": ORIGIN, "name": NAME}

    info = dp.ensure_recipe(dealer)
    assert info["synth"] == "rejected:one_condition"
    assert info["recipe_status"] == "rejected:one_condition"
    assert rec.load_recipes(DEALER) == []
    assert "REJECT" in (tmp_path / DEALER / "discovery.md").read_text(encoding="utf-8")

    which["k"] = "ok"
    info = dp.ensure_recipe(dealer)
    assert info["synth"] == "saved_1" and info["recipe_status"] == "ok"
    assert len(rec.load_recipes(DEALER)) == 1


# ── the discovery capture judges rows with the scan's own place ────────────────

HH = "hondahuntersville-com"
HH_ORIGIN = "https://www.hondahuntersville.com"
HH_STREET = "12815 Statesville Rd"


def _hh_listing(vin: str, location: str, source_id: str) -> dict:
    return {"vin": vin, "type": "New", "year": 2025, "make": "Honda", "model": "Civic", "trim": "LX", "stock": vin[-6:], "mileage": 5,
            "source_id": source_id, "pricing": {"price": 30000},
            "dealer": {"id": "1", "ccid": 6067306, "name": None, "website": None, "api_id": "MP23253",
                       "location": location, "city": None, "state": None, "address": None, "zipcode": None},
            "media": {"images": []}, "extra_fields": {}}


def _hh_feed():
    """A group feed: six of this store's cars stamped with a bare street line
    (the page's JSON-LD street, which the registry never had) and one sibling."""
    rows = [_hh_listing(f"00HHS{i:012d}", HH_STREET, "209014") for i in range(6)] + [_hh_listing("00HOO000000000001", "Hoover Toyota", "MP99999")]

    def fetch(recipe, body, base_url, url=None):
        return 200, {"data": {"listings": rows if int(body.get("page") or 1) == 1 else [], "total_vehicle_count": 7}}

    return fetch


@pytest.fixture()
def _no_ledger(monkeypatch):
    monkeypatch.setattr("backend.scanner.rooftop_ledger.note_refusal", lambda *a, **k: None, raising=False)
    monkeypatch.setattr("backend.scanner.rooftop_ledger.flush", lambda *a, **k: None, raising=False)


def test_capture_place_is_the_scan_replays_place(monkeypatch):
    """capture_endpoints hands the validator ``capture_place``: registry row plus the
    page-learned street from scan hints (roster_place_with_hints, as try_fetch_via_recipes),
    not the id / url / name / provider dict the probe builds (which yields no place)."""
    from backend.scanner import recipe_store, rooftop_disown
    from backend.scanner.discovery_capture import capture_place

    dealer = {"dealer_id": HH, "url": HH_ORIGIN, "name": "Honda Of Huntersville", "provider": "carscommerce"}
    # the roster has the town but no street; the street lives in scan hints (learn_place wrote it)
    monkeypatch.setattr(rooftop_disown, "roster_place", lambda url: {"dealer_city": "Huntersville", "dealer_state": "NC", "dealer_zip": "28078", "dealer_address": ""})
    monkeypatch.setattr(recipe_store, "get_scan_hints", lambda did: {"dealer_address": HH_STREET, "dealer_address_source": "site_jsonld"})
    assert capture_place(dealer, HH_ORIGIN) == {"dealer_city": "Huntersville", "dealer_state": "NC", "dealer_zip": "28078", "dealer_address": HH_STREET}
    # a group-feed dealer missing from the registry: the hinted place alone
    monkeypatch.setattr(rooftop_disown, "roster_place", lambda url: {})
    monkeypatch.setattr(recipe_store, "get_scan_hints", lambda did: {"dealer_address": HH_STREET, "dealer_city": "Huntersville", "dealer_state": "NC"})
    assert capture_place(dealer, HH_ORIGIN) == {"dealer_address": HH_STREET, "dealer_city": "Huntersville", "dealer_state": "NC"}
    # registry and hint store down: what the caller gave, or nothing (never an exception)
    monkeypatch.setattr(rooftop_disown, "roster_place", lambda url: (_ for _ in ()).throw(RuntimeError("db down")))
    monkeypatch.setattr(recipe_store, "get_scan_hints", lambda did: {})
    assert capture_place(dealer, HH_ORIGIN) is None
    assert capture_place({**dealer, "dealer_city": "Huntersville", "dealer_state": "NC"}, HH_ORIGIN) == {"dealer_city": "Huntersville", "dealer_state": "NC"}


def test_hinted_street_is_what_keeps_the_captured_rows(monkeypatch, _no_ledger):
    """Roster town without a street: every bare-street row is refused (target
    rooftop unidentified) and the good capture is judged zero_rows; with the
    hinted street the same feed validates. This is the shape that made
    promote_from_ledger(validate=True) refuse Honda of Huntersville's capture."""
    from backend.scanner import recipe_store, rooftop_disown
    from backend.scanner.discovery_capture import capture_place

    cc = EndpointRecipe(dealer_id=HH, url=CC_URL, method="POST", content_type="application/json",
                        post_template=json.dumps({"page": 1, "perPage": 100, "filters": {"status": ["publish"]}}),
                        pagination=PAGINATION_CARSCOMMERCE, provider_hint="carscommerce", auth_headers={"x-api-key": "k"})
    town_only = {"dealer_city": "Huntersville", "dealer_state": "NC", "dealer_zip": "28078"}
    rep = rv.validate_recipe_set(HH, [cc], _hh_feed(), base_url=HH_ORIGIN, dealer_name="Honda Of Huntersville", place=town_only, one_condition_ok=set())
    assert rep.verdict == "reject" and rep.status == "rejected:zero_rows"
    assert rep.recipes[0].gate_rejected == 7 and rep.vins_total == 0
    # what the probe used to pass: the dealer dict alone -> no place at all -> the same refusal
    rep = rv.validate_recipe_set(HH, [cc], _hh_feed(), base_url=HH_ORIGIN, dealer_name="Honda Of Huntersville", place=None, one_condition_ok=set())
    assert rep.verdict == "reject" and rep.recipes[0].gate_rejected == 7

    monkeypatch.setattr(rooftop_disown, "roster_place", lambda url: dict(town_only, dealer_address=""))
    monkeypatch.setattr(recipe_store, "get_scan_hints", lambda did: {"dealer_address": HH_STREET, "dealer_address_source": "site_jsonld"})
    place = capture_place({"dealer_id": HH, "url": HH_ORIGIN, "name": "Honda Of Huntersville", "provider": "carscommerce"}, HH_ORIGIN)
    assert place["dealer_address"] == HH_STREET
    rep = rv.validate_recipe_set(HH, [cc], _hh_feed(), base_url=HH_ORIGIN, dealer_name="Honda Of Huntersville", place=place, one_condition_ok=set())
    assert rep.vins_total == 6 and rep.recipes[0].gate_rejected == 1  # the Hoover sibling, and only it
    assert rep.verdict in ("ok", "uncertain") and not rep.flags["zero_rows"]
