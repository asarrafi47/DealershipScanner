"""Unit tests for Dealer.com bulk inventory POST pagination helpers."""
from __future__ import annotations

from backend.scanner.scrapers.dealer_com_bulk_fetch import (
    is_dealer_com_inventory_post_url,
    next_start_offset,
    prepare_bulk_post_body,
)


def test_is_dealer_com_inventory_post_url():
    assert is_dealer_com_inventory_post_url(
        "https://www.irvinebmw.com/api/widget/ws-inv-data/getInventory"
    )
    assert not is_dealer_com_inventory_post_url(
        "https://www.irvinebmw.com/api/widget/ws-inv-data/getInventoryAndFacets"
    )


def test_prepare_bulk_post_body_sets_page_size_and_start():
    template = {
        "preferences": {"pageSize": "18", "listing.config.id": "auto-new"},
        "inventoryParameters": {},
    }
    body = prepare_bulk_post_body(template, start=100, page_size=500)
    assert body["preferences"]["pageSize"] == "500"
    assert body["inventoryParameters"]["start"] == ["100"]
    assert template["preferences"]["pageSize"] == "18"


def test_next_start_offset_stops_at_total():
    assert next_start_offset(
        start=0,
        page_size=100,
        batch_count=100,
        total_count=420,
        unique_so_far=100,
    ) == 100
    assert next_start_offset(
        start=400,
        page_size=100,
        batch_count=20,
        total_count=420,
        unique_so_far=420,
    ) is None
    assert next_start_offset(
        start=100,
        page_size=100,
        batch_count=9,
        total_count=109,
        unique_so_far=109,
    ) is None
