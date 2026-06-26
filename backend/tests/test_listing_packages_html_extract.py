"""Listing HTML → packages extraction (gs-vehicle / option-description tables)."""

from __future__ import annotations

from backend.enrichment.listing_packages_service import (
    extract_listing_option_features,
    merge_listing_option_features_into_packages,
)

_SAMPLE_HTML = """
<html><body>
<table>
<tr><td class="option-description"><h3>MB-Tex Upholstery</h3></td></tr>
<tr><td class="option-description"><h3>Apple CarPlay®/Android Auto®</h3></td></tr>
<tr><td class="option-description"><h3>MB-Tex Upholstery</h3></td></tr>
</table>
</body></html>
"""


def test_extract_listing_option_features_dedupes() -> None:
    feats = extract_listing_option_features(_SAMPLE_HTML)
    assert feats == ["MB-Tex Upholstery", "Apple CarPlay®/Android Auto®"]


def test_merge_listing_option_features_into_packages() -> None:
    merged = merge_listing_option_features_into_packages(None, ["Heated Seats", "Sunroof"])
    assert merged is not None
    import json

    pkg = json.loads(merged)
    assert pkg["listing_options_source"] == "gs_vehicle_option_description"
    names = [x["name"] for x in pkg["packages_normalized"]]
    assert names == ["Heated Seats", "Sunroof"]
