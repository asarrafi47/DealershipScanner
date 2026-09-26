"""Two replay defects behind 30 of the 44 no_rows verdicts of the 2026-09-24
fleet run: captured carscommerce recipes pinned to one SRP section, and
dealer.com walks that trusted a pageSize the server ignores."""
from __future__ import annotations

import json

from backend.scanner.recipes import (
    PAGINATION_CARSCOMMERCE,
    PAGINATION_DEALER_COM,
    EndpointRecipe,
    _learn_dealer_com_page_size,
    _mutate_for_page,
    recipe_is_section_scoped,
    recipe_is_store_scoped,
)


def _cc(body: dict) -> EndpointRecipe:
    return EndpointRecipe(dealer_id="lindsayhonda-com", url="https://websites-search.api.carscommerce.inc/api/v1/listings/13848/search",
                          method="POST", content_type="application/json", post_template=json.dumps(body), pagination=PAGINATION_CARSCOMMERCE)


def test_section_facets_are_dropped_at_replay_but_store_facets_kept():
    body = {"page": 1, "perPage": 20, "filters": {"status": ["publish"], "type_slug": ["used"]},
            "facetFilters": {"type_slug": ["Certified Used"], "make": ["Toyota"], "model_slug": ["Camry"],
                             "custom_text_11": ["Buford, GA"], "source_id": ["178465"], "custom_text_1": ["true"]}}
    out = _mutate_for_page(_cc(body), body, 2)
    assert out["facetFilters"] == {"custom_text_11": ["Buford, GA"], "source_id": ["178465"]}
    assert out["page"] == 3 and out["perPage"] == 100
    assert out["filters"] == {"status": ["publish"]}
    assert body["facetFilters"]["type_slug"] == ["Certified Used"]  # template untouched


def test_section_scoped_detection():
    assert recipe_is_section_scoped(_cc({"facetFilters": {"type_slug": ["New"]}}))
    assert recipe_is_section_scoped(_cc({"facetFilters": {"make": ["Kia"], "body_type": ["Cars"]}}))
    assert not recipe_is_section_scoped(_cc({"facetFilters": {"source_id": ["178465"]}}))
    assert not recipe_is_section_scoped(_cc({"filters": {"status": ["publish"]}}))
    assert recipe_is_store_scoped(_cc({"facetFilters": {"source_id": ["178465"]}}))


def test_dealer_com_page_size_learned_from_server():
    template = {"preferences": {"pageSize": "500"}, "inventoryParameters": {"start": ["0"]}}
    assert _learn_dealer_com_page_size(template, {"pageInfo": {"totalCount": 157, "pageSize": 48}}, "Village VW") == 48
    assert template["preferences"]["pageSize"] == "48"
    r = EndpointRecipe(dealer_id="villagevw-com", url="https://www.villagevw.com/api/widget/ws-inv-data/getInventory", method="POST",
                       content_type="application/json", post_template=json.dumps(template), pagination=PAGINATION_DEALER_COM)
    assert _mutate_for_page(r, template, 1)["inventoryParameters"]["start"] == ["48"]
    assert _mutate_for_page(r, template, 3)["inventoryParameters"]["start"] == ["144"]
    assert _learn_dealer_com_page_size(template, {"pageInfo": {}}, "x") == 0
    assert _learn_dealer_com_page_size(template, None, "x") == 0


def test_dealer_com_replay_drops_the_section_config(monkeypatch):
    """Camelback Toyota: listing.config.id=auto-certified-used pinned 99 of 906 cars (2026-09-26)."""
    import json

    from backend.scanner import recipes as rc

    r = rc.EndpointRecipe(dealer_id="d", url="https://www.d.com/api/widget/ws-inv-data/getInventory", method="POST", content_type="application/json",
                          post_template=json.dumps({"pageAlias": "INVENTORY_LISTING_DEFAULT_AUTO_CERTIFIED_USED", "preferences": {"pageSize": "48", "listing.config.id": "auto-certified-used,auto-used-mpp"}, "inventoryParameters": {"start": ["0"]}}),
                          auth_headers={}, pagination=rc.PAGINATION_DEALER_COM, vehicle_rows=48, total_count=None, provider_hint="dealer_dot_com", saved_at=0.0)
    body = rc._mutate_for_page(r, json.loads(r.post_template), 1)
    assert "listing.config.id" not in body["preferences"] and body["preferences"]["pageSize"] == "48"
    assert body["inventoryParameters"]["start"] == ["48"]
    assert "listing.config.id" in json.loads(r.post_template)["preferences"]  # template untouched
