"""P17B.5: the scrapers' Playwright page halves are gone; the HTTP parsing halves stay."""
from __future__ import annotations

import importlib
import importlib.util

import pytest


@pytest.mark.parametrize("module,name", [
    ("dealer_venom", "scrape_dealer_venom_from_page"),
    ("dealer_on", "scrape_dealer_on_from_page"),
    ("dealer_on", "_scrape_srp_all_pages"),
    ("pixel_motion", "scrape_pixel_motion_from_page"),
    ("pixel_motion", "_scrape_path_with_pagination"),
    ("dealer_eprocess", "_browser_fetch_json"),
    ("dealer_eprocess", "_fetch_facts_with_fallback"),
    ("dealer_eprocess", "_fetch_resrc_inventory"),
    ("dealer_eprocess", "_fetch_jsonld_from_srp"),
])
def test_page_functions_are_gone(module, name):
    mod = importlib.import_module(f"backend.scanner.scrapers.{module}")
    assert not hasattr(mod, name)


def test_dealer_inspire_module_is_gone():
    assert importlib.util.find_spec("backend.scanner.scrapers.dealer_inspire") is None


@pytest.mark.parametrize("module,name", [
    ("dealer_on", "_extract_vehicles_from_srp_body"),
    ("dealer_on", "_map_vehicle_card"),
    ("pixel_motion", "parse_pixel_motion_inventory_html"),
    ("pixel_motion", "_is_pixel_motion_html"),
    ("pixel_motion", "_dismiss_cookie_banner"),
    ("pixel_motion", "_PIXEL_PATHS"),
    ("dealer_venom", "_map_typesense_document"),
    ("dealer_venom", "_extract_typesense_config_from_html"),
    ("dealer_eprocess", "_fetch_results_api"),
    ("dealer_eprocess", "scrape_dealer_eprocess_from_page"),
])
def test_http_halves_stay(module, name):
    assert hasattr(importlib.import_module(f"backend.scanner.scrapers.{module}"), name)
