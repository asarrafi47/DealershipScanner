"""Tests for brochure LLM analyzer preflight, ladder injection, and parsing."""

from __future__ import annotations

import json
from pathlib import Path

from backend.scripts.analyze_brochure_with_llm import (
    _DEFAULT_MIN_YEAR,
    _empty_reason,
    _parse_response,
    _strip_dimension_sentences,
    _truncate_at_sentence_boundary,
    analyze_one,
    build_llm_user_prompt,
    discovered_trim_hierarchy_for_ck,
    load_brochure_ladder_index,
    prepare_slim_payload,
)


def test_empty_reason_pre_2015_no_longer_hardcoded():
    reason = _empty_reason(
        ck="2011|ford|escape",
        year=2011,
        make="Ford",
        model="Escape",
        text="x" * 500,
        min_year=_DEFAULT_MIN_YEAR,
    )
    assert reason is None


def test_empty_reason_pre_min_year():
    reason = _empty_reason(
        ck="2009|ford|escape",
        year=2009,
        make="Ford",
        model="Escape",
        text="x" * 500,
        min_year=2010,
    )
    assert reason == "empty:pre_2010"


def test_empty_reason_short_text():
    reason = _empty_reason(
        ck="2011|ford|escape",
        year=2011,
        make="Ford",
        model="Escape",
        text="short",
        min_year=2010,
    )
    assert reason == "empty:short_text"


def test_strip_dimension_sentences_removes_specs():
    text = (
        "Adds 12.3-inch touchscreen with wireless Apple CarPlay.\n"
        "Overall length 192.5 in. Wheelbase 111.4 in.\n"
        "Adds leather-trimmed seating surfaces."
    )
    cleaned = _strip_dimension_sentences(text)
    assert "12.3-inch touchscreen" in cleaned
    assert "Adds leather-trimmed" in cleaned
    assert "Wheelbase" not in cleaned
    assert "Overall length" not in cleaned


def test_prepare_slim_payload_strips_footnotes():
    text = "Adds adaptive cruise control[1]. " + ("Feature line. " * 40)
    prepared = prepare_slim_payload(text)
    assert "[1]" not in prepared
    assert "Adds adaptive cruise control." in prepared


def test_truncate_at_sentence_boundary():
    text = "First sentence. Second sentence. Third sentence."
    out = _truncate_at_sentence_boundary(text, 35)
    assert out == "First sentence. Second sentence."
    assert "Third" not in out


def test_discovered_trim_hierarchy_for_known_ck():
    load_brochure_ladder_index(refresh=True)
    hierarchy = discovered_trim_hierarchy_for_ck("2010|acura|mdx")
    assert hierarchy is not None
    assert hierarchy[0]["ladder_position"] == 1
    assert hierarchy[0]["tier"] == "base"
    assert hierarchy[-1]["tier"] == "top"
    assert hierarchy[0]["trim_name"] == "LE"
    assert hierarchy[-1]["trim_name"] == "Advance"


def test_build_llm_user_prompt_injects_hierarchy():
    hierarchy = [
        {"ladder_position": 1, "trim_name": "SE", "tier": "base"},
        {"ladder_position": 2, "trim_name": "Limited", "tier": "top"},
    ]
    prompt = build_llm_user_prompt(
        year=2024,
        make="Hyundai",
        model="Elantra",
        slim_text="Adds to or replaces features offered on SE: • 10.25-inch touchscreen.",
        trim_hierarchy=hierarchy,
    )
    assert "Pre-Defined Trim Hierarchy" in prompt
    assert '"trim_name": "SE"' in prompt
    assert "Adds to or replaces" in prompt
    assert "overlays" in prompt


def test_parse_overlays_response_enforces_verbs_and_order():
    hierarchy = [
        {"ladder_position": 1, "trim_name": "SE", "tier": "base"},
        {"ladder_position": 2, "trim_name": "Limited", "tier": "top"},
    ]
    raw = json.dumps(
        {
            "make": "Hyundai",
            "model": "Elantra",
            "year": 2024,
            "overlays": [
                {
                    "trim_name": "SE",
                    "ladder_position": 1,
                    "upgrades": [
                        "Adds 8.0-inch Display Audio with wireless Apple CarPlay",
                        "Adds cloth seating surfaces with 6-way driver seat",
                    ],
                },
                {
                    "trim_name": "Limited",
                    "ladder_position": 2,
                    "upgrades": [
                        "Upgraded to 10.25-inch touchscreen navigation",
                        "Adds leather-trimmed heated front seats",
                    ],
                },
            ],
        }
    )
    parsed = _parse_response(raw, expected_hierarchy=hierarchy)
    assert parsed is not None
    assert parsed["trims"] == ["Limited", "SE"]
    assert len(parsed["adds"]["Limited"]) == 2
    assert parsed["adds"]["Limited"][0].startswith("Upgraded to")


def test_parse_overlays_rejects_missing_hierarchy_trims():
    hierarchy = [
        {"ladder_position": 1, "trim_name": "SE", "tier": "base"},
        {"ladder_position": 2, "trim_name": "Limited", "tier": "top"},
    ]
    raw = json.dumps(
        {
            "overlays": [
                {
                    "trim_name": "SE",
                    "ladder_position": 1,
                    "upgrades": ["Adds cloth seating surfaces with manual adjustment"],
                }
            ]
        }
    )
    assert _parse_response(raw, expected_hierarchy=hierarchy) is None


def test_analyze_one_rule_only_no_api(tmp_path: Path):
    slim = tmp_path / "2011__honda__accord.json"
    slim.write_text(
        json.dumps(
            {
                "catalog_key": "2011|honda|accord",
                "year": 2011,
                "make": "Honda",
                "model": "Accord",
                "combined_trim_pages_text": "x" * 500,
            }
        ),
        encoding="utf-8",
    )
    status = analyze_one(
        slim,
        client=None,
        rule_only=True,
        min_year=2010,
    )
    assert not status.startswith("error:api")
    assert status in {
        "written:rule:2011__honda__accord.json",
        "empty:rule_failed",
        "skipped:curated",
        "skipped:rule_done",
        "skipped:already_done",
    } or status.startswith("written:rule:")


def test_analyze_one_llm_requires_ladder(tmp_path: Path, monkeypatch):
    slim = tmp_path / "2099__fake__model.json"
    slim.write_text(
        json.dumps(
            {
                "catalog_key": "2099|fake|model",
                "year": 2099,
                "make": "Fake",
                "model": "Model",
                "combined_trim_pages_text": "x" * 500,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "backend.scripts.analyze_brochure_with_llm.discovered_trim_hierarchy_for_ck",
        lambda _ck: None,
    )
    status = analyze_one(
        slim,
        client=None,
        rule_first=False,
        dry_run=True,
    )
    assert status == "empty:no_ladder"
