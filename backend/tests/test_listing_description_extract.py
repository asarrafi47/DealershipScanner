"""Synthetic listing-description parsing (no real dealer HTML or marketing blobs)."""

from __future__ import annotations

import json

from backend.utils.listing_description_extract import (
    LISTING_DESCRIPTION_PARSER_VERSION,
    extract_listing_description,
    normalize_listing_description,
    semantic_packages_snippet,
)
from backend.utils.listing_description_persist import (
    listing_description_parse_is_current,
    listing_description_source_fingerprint,
    merge_description_parse_into_packages,
    packages_column_is_sparse,
    process_listing_description_for_row,
)


def test_normalize_strips_tags_and_boilerplate() -> None:
    raw = (
        "<p>Features include AWD.</p>\n"
        "<br/>Call us today for a test drive.\n"
        "Interior: Graphite cloth trim."
    )
    out = normalize_listing_description(raw)
    assert "Call us today" not in out
    assert "Interior:" in out or "interior:" in out.lower()


def test_extract_interior_and_package_sections() -> None:
    text = (
        "Interior: Saddle leather trim.\n"
        "Exterior: Silver paint.\n"
        "Convenience Package\n"
        "- heated front seats\n"
        "- dual-zone climate\n"
        "Technology Package\n"
        "- premium audio\n"
    )
    parsed = extract_listing_description(
        text,
        {"make": "Generic", "model": "Sedan", "year": 2022, "trim": "LX"},
    )
    assert parsed["parser_version"] == LISTING_DESCRIPTION_PARSER_VERSION
    assert parsed["interior_color_hint"]["value"]
    assert "saddle" in parsed["interior_color_hint"]["value"].lower()
    assert parsed["exterior_color_hint"]["value"]
    assert "silver" in parsed["exterior_color_hint"]["value"].lower()
    verbatim = {(p.get("name_verbatim") or p.get("name") or "").lower() for p in parsed["packages"]}
    assert any("convenience" in n and "package" in n for n in verbatim)
    assert any("technology" in n for n in verbatim)
    feat_blob = " ".join(
        f for p in parsed["packages"] for f in (p.get("features") or [])
    ).lower()
    assert "heated" in feat_blob or "climate" in feat_blob


def test_catalog_canonicalizes_from_data_file() -> None:
    text = (
        "Comfort Group Package\n"
        "- premium audio upgrade\n"
    )
    parsed = extract_listing_description(text, {"make": "AnyMake", "year": 2021})
    assert len(parsed["packages"]) >= 1
    pkg0 = parsed["packages"][0]
    assert pkg0.get("catalog_matched") is True
    assert "audio" in (pkg0.get("canonical_name") or "").lower()


def test_merge_into_packages_json_roundtrip() -> None:
    text = "Interior: Tan cloth.\nWinter Package\n- heated seats\n"
    parsed = extract_listing_description(text, {"make": "Test", "year": 2020})
    merged = merge_description_parse_into_packages(
        None,
        parsed,
        source_fingerprint="abc123fingerprint00000001",
    )
    data = json.loads(merged)
    assert data["listing_description_parser_version"] == LISTING_DESCRIPTION_PARSER_VERSION
    assert isinstance(data.get("packages_normalized"), list)
    assert data["dealer_description_parsed"]["source_text_sha256"] == "abc123fingerprint00000001"


def test_packages_column_is_sparse() -> None:
    assert packages_column_is_sparse(None) is True
    assert packages_column_is_sparse("{}") is True
    dense = json.dumps({"observed_features": ["sunroof"]})
    assert packages_column_is_sparse(dense) is False


def test_semantic_snippet_caps_length() -> None:
    long_feats = [f"feature-{i}" for i in range(40)]
    sn = semantic_packages_snippet(
        {
            "packages": [{"name": "Alpha Package", "features": long_feats}],
            "standalone_features": ["standalone-a"],
        },
        max_chars=80,
    )
    assert len(sn) <= 80


def test_listing_description_parse_is_current_matches_fingerprint() -> None:
    text = "Interior: Black.\nCold Weather Package\n- heated seats\n"
    norm = normalize_listing_description(text)
    fp = listing_description_source_fingerprint(norm)
    parsed = extract_listing_description(text, {"make": "Test", "year": 2021})
    merged = merge_description_parse_into_packages(None, parsed, source_fingerprint=fp)
    assert listing_description_parse_is_current(merged, source_fingerprint=fp) is True
    assert listing_description_parse_is_current(merged, source_fingerprint="differenthash0000000001") is False


def test_process_listing_description_skips_unchanged() -> None:
    text = "Interior: Gray cloth.\nSport Package\n- alloy wheels\n"
    norm = normalize_listing_description(text)
    fp = listing_description_source_fingerprint(norm)
    parsed = extract_listing_description(text, {"make": "Test", "year": 2022})
    merged = merge_description_parse_into_packages(None, parsed, source_fingerprint=fp)
    row = {
        "id": 1,
        "vin": "TESTVINPROCESSLISTING1",
        "description": text,
        "packages": merged,
        "interior_color": None,
        "make": "Test",
        "model": "X",
        "year": 2022,
        "trim": None,
        "spec_source_json": None,
    }
    r = process_listing_description_for_row(row, skip_if_unchanged=True, force=False)
    assert r["applied"] is False
    assert r["reason"] == "unchanged"
    r2 = process_listing_description_for_row(row, skip_if_unchanged=False, force=False)
    assert r2["applied"] is True
    assert "packages" in (r2.get("updates") or {})

