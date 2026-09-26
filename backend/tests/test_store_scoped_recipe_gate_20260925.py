"""A recipe filtered to the store's own CarsCommerce feed ids returns only that
store's rows by construction, so the rooftop gate must not refuse them when the
feeds stamp the store two ways (Mall of Georgia Mazda, 2026-09-25: 'Buford, GA'
vs '3546 Highway 20' — the gate kept 115 of 424 verified rows)."""
from __future__ import annotations

import json

from backend.parsers import parse, parse_kept
from backend.scanner.recipes import PAGINATION_CARSCOMMERCE, PAGINATION_NONE, EndpointRecipe, recipe_is_store_scoped


def _listing(vin: str, location: str, source_id: str) -> dict:
    return {"vin": vin, "source_id": source_id, "make": "Mazda", "model": "CX-5", "year": 2026, "type": "New",
            "dealer": {"id": "1", "ccid": 2172862, "city": None, "name": None, "state": None, "address": None, "zipcode": None, "location": location},
            "extra_fields": {}}


def _payload(rows):
    return {"data": {"ccid": 2172862, "total_vehicle_count": len(rows), "listings": rows}, "meta": {"pagination": {"total": len(rows)}}}


def _recipe(body: dict, pagination=PAGINATION_CARSCOMMERCE) -> EndpointRecipe:
    return EndpointRecipe(dealer_id="mallofgamazda-com", url="https://websites-search.api.carscommerce.inc/api/v1/listings/2172862/search",
                          method="POST", content_type="application/json", post_template=json.dumps(body), pagination=pagination)


def test_recipe_is_store_scoped_only_with_source_id_filter():
    assert recipe_is_store_scoped(_recipe({"page": 1, "facetFilters": {"source_id": ["MallofGeorgiaMazda", "23978"]}}))
    assert not recipe_is_store_scoped(_recipe({"page": 1, "facetFilters": {"custom_text_11": ["Buford, GA"]}}))
    assert not recipe_is_store_scoped(_recipe({"page": 1}))
    assert not recipe_is_store_scoped(_recipe({"page": 1, "facetFilters": {"source_id": ["x"]}}, pagination=PAGINATION_NONE))


def test_trust_feed_scope_keeps_both_stamps():
    rows = [_listing(f"JM3KFBBM{i:09d}", "3546 Highway 20<br/>Buford, GA 30519<br/>(678) 714-9000", "MallofGeorgiaMazda") for i in range(6)]
    rows += [_listing(f"JM3KFBCM{i:09d}", "Buford, GA", "23978") for i in range(3)]
    kw = dict(base_url="https://www.mallofgamazda.com", dealer_id="mallofgamazda-com", dealer_name="Mall of Georgia Mazda",
              dealer_url="https://www.mallofgamazda.com", dealer_city="Buford", dealer_state="GA")
    gated: list[dict] = []
    kept = parse("carscommerce", _payload(rows), rejected_out=gated, **kw)
    assert len(kept) + len(gated) == 9 and len(kept) < 9  # the gate cannot pick two rooftops
    trusted: list[dict] = []
    assert len(parse("carscommerce", _payload(rows), rejected_out=trusted, trust_feed_scope=True, **kw)) == 9 and not trusted
    assert len(parse_kept("carscommerce", _payload(rows), base_url=kw["base_url"], dealer_id=kw["dealer_id"], dealer_name=kw["dealer_name"],
                          dealer_url=kw["dealer_url"], dealer_city="Buford", dealer_state="GA", trust_feed_scope=True)) == 9


def test_trusted_rows_are_marked_for_the_union_pass():
    rows = [_listing(f"JM3KFBBM{i:09d}", "Buford, GA", "23978") for i in range(3)]
    kw = dict(base_url="https://www.mallofgamazda.com", dealer_id="mallofgamazda-com", dealer_name="Mall of Georgia Mazda",
              dealer_url="https://www.mallofgamazda.com")
    kept = parse("carscommerce", _payload(rows), rejected_out=[], trust_feed_scope=True, **kw)
    assert all(r.get("_feed_scoped") for r in kept)
    assert not any(r.get("_feed_scoped") for r in parse("carscommerce", _payload(rows), rejected_out=[], **kw))


def test_payload_marker_skips_the_gate_for_any_caller():
    rows = [_listing(f"JM3KFBBM{i:09d}", "3546 Highway 20<br/>Buford, GA 30519<br/>(678) 714-9000", "MallofGeorgiaMazda") for i in range(4)]
    rows += [_listing(f"JM3KFBCM{i:09d}", "Buford, GA", "23978") for i in range(2)]
    kw = dict(base_url="https://www.mallofgamazda.com", dealer_id="mallofgamazda-com", dealer_name="Mall of Georgia Mazda",
              dealer_url="https://www.mallofgamazda.com", dealer_city="Buford", dealer_state="GA")
    payload = _payload(rows)
    payload["_feed_scoped"] = True
    rejected: list[dict] = []
    assert len(parse("carscommerce", payload, rejected_out=rejected, **kw)) == 6 and not rejected


def test_store_name_facet_scopes_the_recipe():
    r = _recipe({"page": 1, "facetFilters": {"custom_text_13": ["Group 1 Toyota North Austin"]}})
    assert recipe_is_store_scoped(r, "Group 1 Toyota North Austin")
    assert not recipe_is_store_scoped(r, "Group 1 Kia of Austin")
    assert not recipe_is_store_scoped(_recipe({"page": 1, "facetFilters": {"custom_text_13": ["Austin, TX"]}}), "Group 1 Toyota North Austin")

