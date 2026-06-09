"""Trim spec sheet delta engine (offline ladder adds generation)."""

from __future__ import annotations

import json
from pathlib import Path

from backend.enrichment.trim_diff_engine import (
    compute_ladder_adds_by_step_name,
    compute_step_adds,
    feature_map_from_rows,
    load_spec_sheet,
    resolve_trim_spec_rows,
)


def test_base_rung_uses_absolute_spec_format() -> None:
    features = feature_map_from_rows(
        [
            {"label": "Engine Options", "value": "3.6L Pentastar V6"},
            {"label": "Screen Size", "value": "7.0-inch Uconnect 4 touchscreen display"},
        ]
    )
    adds = compute_step_adds(features, cumulative_baseline={}, is_base_rung=True)
    assert adds == [
        "Engine Options: 3.6L Pentastar V6",
        "Screen Size: 7.0-inch Uconnect 4 touchscreen display",
    ]


def test_higher_rung_adds_and_upgrades() -> None:
    baseline = feature_map_from_rows(
        [
            {"label": "Engine Options", "value": "3.6L Pentastar V6"},
            {"label": "Screen Size", "value": "7.0-inch Uconnect 4 touchscreen display"},
        ]
    )
    features = feature_map_from_rows(
        [
            {"label": "Engine Options", "value": "3.6L Pentastar V6"},
            {"label": "Screen Size", "value": "8.4-inch Uconnect 4C NAV touchscreen display"},
            {"label": "Subwoofer", "value": "1 rear cargo side-wall enclosed subwoofer (8-inch)"},
        ]
    )
    adds = compute_step_adds(features, cumulative_baseline=baseline, is_base_rung=False)
    assert "Adds Subwoofer: 1 rear cargo side-wall enclosed subwoofer (8-inch)" in adds
    assert any(
        a.startswith("Upgrades Screen Size:") and "(was 7.0-inch Uconnect 4 touchscreen display)" in a
        for a in adds
    )
    assert not any(a.startswith("Upgrades Engine Options:") for a in adds)


def test_alias_resolves_spec_sheet_trim(tmp_path: Path) -> None:
    sheet = {
        "ladder_id": "test_ladder",
        "trims": {
            "xDrive40i": [
                {"label": "Engine Options", "value": "3.0L TwinPower Turbo I6"},
            ],
        },
    }
    step = {"name": "40i", "aliases": ["xDrive40i"]}
    rows = resolve_trim_spec_rows(sheet, step)
    assert rows[0]["value"] == "3.0L TwinPower Turbo I6"


def test_compute_ladder_follows_baseline_to_top_order() -> None:
    ladder = {
        "id": "demo",
        "steps": [
            {"name": "Limited", "aliases": []},
            {"name": "Laredo", "aliases": []},
        ],
    }
    sheet = {
        "trims": {
            "Laredo": [{"label": "Screen Size", "value": "7.0-inch display"}],
            "Limited": [
                {"label": "Screen Size", "value": "8.4-inch display"},
                {"label": "Subwoofer", "value": "8-inch enclosed"},
            ],
        }
    }
    adds_by_name = compute_ladder_adds_by_step_name(ladder, sheet)
    assert adds_by_name["Laredo"] == ["Screen Size: 7.0-inch display"]
    assert adds_by_name["Limited"][0].startswith("Upgrades Screen Size:")
    assert any(a.startswith("Adds Subwoofer:") for a in adds_by_name["Limited"])


def test_jeep_curated_sheet_limited_x_delta() -> None:
    sheet = load_spec_sheet("jeep_grand_cherokee_wk2")
    if sheet is None:
        return
    ladder = {
        "steps": [
            {"name": "Summit", "aliases": []},
            {"name": "Overland", "aliases": []},
            {"name": "Trailhawk", "aliases": []},
            {"name": "Limited X", "aliases": []},
            {"name": "Limited", "aliases": []},
            {"name": "Laredo", "aliases": []},
        ]
    }
    adds_by_name = compute_ladder_adds_by_step_name(ladder, sheet)
    assert "Laredo" in adds_by_name
    limited_x = adds_by_name.get("Limited X") or []
    assert any(a.startswith("Adds Exterior Styling:") for a in limited_x)


def test_apply_trim_diffs_dry_run(tmp_path: Path) -> None:
    from backend.enrichment.trim_diff_engine import apply_trim_diffs_to_generated_ladders

    sheets_dir = tmp_path / "trim_spec_sheets"
    sheets_dir.mkdir()
    (sheets_dir / "demo_ladder.json").write_text(
        json.dumps(
            {
                "ladder_id": "demo_ladder",
                "trims": {
                    "Base": [{"label": "Engine Options", "value": "2.0L I4"}],
                    "Sport": [{"label": "Engine Options", "value": "2.0L Turbo I4"}],
                },
            }
        ),
        encoding="utf-8",
    )

    gen_path = tmp_path / "trim_ladders_generated.json"
    gen_path.write_text(
        json.dumps(
            {
                "version": 1,
                "ladders": [
                    {
                        "id": "demo_ladder",
                        "steps": [
                            {"name": "Sport", "aliases": [], "adds": ["placeholder"]},
                            {"name": "Base", "aliases": [], "adds": ["placeholder"]},
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    stats = apply_trim_diffs_to_generated_ladders(
        generated_path=gen_path,
        sheets_dir=sheets_dir,
        dry_run=True,
    )
    assert stats["ladders_updated"] == 1
    assert stats["steps_updated"] == 2

    payload = json.loads(gen_path.read_text(encoding="utf-8"))
    assert payload["ladders"][0]["steps"][0]["adds"] == ["placeholder"]

    stats_write = apply_trim_diffs_to_generated_ladders(
        generated_path=gen_path,
        sheets_dir=sheets_dir,
        dry_run=False,
    )
    assert stats_write["steps_updated"] == 2
    payload2 = json.loads(gen_path.read_text(encoding="utf-8"))
    base_step = next(s for s in payload2["ladders"][0]["steps"] if s["name"] == "Base")
    sport_step = next(s for s in payload2["ladders"][0]["steps"] if s["name"] == "Sport")
    assert base_step["adds"][0].startswith("Engine Options:")
    assert sport_step["adds"][0].startswith("Upgrades Engine Options:")
