"""Listing gap-fill helpers (condition from HTML)."""
from __future__ import annotations

from backend.utils.vdp_spec_parse import parse_condition_from_listing_html


def test_parse_condition_embedded_vehicle_condition():
    html = '<script type="text/javascript">x={"vehicleCondition":"CERTIFIED"}</script>'
    assert parse_condition_from_listing_html(html) == "Certified Pre-Owned"


def test_parse_condition_json_ld_new_condition_url():
    snippet = """
    <script type="application/ld+json">
    {"@context":"https://schema.org","@type":"Offer","itemCondition":"https://schema.org/NewCondition"}
    </script>
    """
    assert parse_condition_from_listing_html(snippet) == "New"


def test_parse_condition_dom_dl():
    html = """
    <html><body><dl><dt>Condition</dt><dd>Used</dd></dl></body></html>
    """
    assert parse_condition_from_listing_html(html) == "Used"
