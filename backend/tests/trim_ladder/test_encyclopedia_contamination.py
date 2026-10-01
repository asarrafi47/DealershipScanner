"""Encyclopedia contamination: strings that were rendered on real cars and must not be.

Split out of the former backend/tests/test_trim_ladder.py. The rung-name gate
fixtures (``_legacy_rung_gate_off`` autouse, ``rung_gate_on``) live in this
package's conftest.py.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Encyclopedia contamination: every string below was rendered on a real car
# page by the fleet sweep of 2026-07-31 (14,466 make/model/year/trim combos,
# 173,304 bullets). They are kept verbatim so a regression is recognisable.
# ---------------------------------------------------------------------------


def test_wikipedia_engine_infobox_row_is_not_an_engine_bullet() -> None:
    """"Engine 351 cu in (5.8 L) 351M V8" reached 90 rungs, incl. 2026 Bronco Sport."""
    from backend.enrichment.trim_ladder_knowledge import is_encyclopedia_spec_artifact

    assert is_encyclopedia_spec_artifact("Engine 351 cu in (5.8 L) 351M V8")
    assert is_encyclopedia_spec_artifact("Engine 4.0 L M178 (Mercedes-AMG) twin-turbocharged V8")
    assert is_encyclopedia_spec_artifact("Displacement: 5328 cc (325 cu. in.)")
    # The "(kW; PS)" dual conversion is an encyclopedia house style — no US OEM
    # publishes PS — so it identifies the row as a lift from an engine-family
    # table. The same LV3 line was correct on a 2020 Sierra and wrong on a 2025
    # Silverado, which is exactly why the SOURCE is disqualified, not the year.
    assert is_encyclopedia_spec_artifact(
        "Engine Options: LV3 EcoTec3 4.3 L V6 285 hp (213 kW; 289 PS)"
    )
    assert is_encyclopedia_spec_artifact("LY6 Vortec 6000 6.0 L V8 360 hp (268 kW; 365 PS)")
    assert is_encyclopedia_spec_artifact("RV3RV4RV5RV6RS1 (electric)")
    # Ordinary equipment wording is untouched.
    assert not is_encyclopedia_spec_artifact("Engine oil cooler")
    assert not is_encyclopedia_spec_artifact("Engine skid plate")
    assert not is_encyclopedia_spec_artifact("6.7L Cummins Turbo Diesel I6")
    assert not is_encyclopedia_spec_artifact("5.7L HEMI V8 engine — added over the GT")
    assert not is_encyclopedia_spec_artifact(
        "Whiplash Protection System (WHIPS) integrated in front seats"
    )


def test_infobox_layout_row_survives_a_column_heading_prefix() -> None:
    """The ^-anchored infobox test used to be defeated by "Engine Options: "."""
    from backend.enrichment.trim_ladder_knowledge import is_generic_trim_add

    for line in (
        "Front-engine, rear-wheel-drive",
        "Engine Options: Front-engine, rear-wheel-drive",
        "Layout Front-engine, front-wheel-drive",
        "Engine Options: Layout Front-engine, all-wheel-drive",
        "Engine Options: L Volkswagen-Audi EA839TT TFSI V6 twin-turbo",
        "Engine Options: Automatic (S8)",
        "Engine Options: Automatic (variable gear ratios)",
    ):
        assert is_generic_trim_add(line), line


def test_engine_label_requires_engine_content() -> None:
    from backend.enrichment.trim_ladder_knowledge import engine_label_without_engine_content

    assert engine_label_without_engine_content("Engine Options: Years Engine Power Torque")
    assert engine_label_without_engine_content(
        "Engine Options: Review standard and optional interior, exterior, mechanical comfort,"
        " entertainment equipme"
    )
    assert engine_label_without_engine_content("Engine Options: EV Electricity")
    assert not engine_label_without_engine_content("Engine Options: 2.0L I4 Turbo")
    assert not engine_label_without_engine_content("Engine Options: Hybrid 2.5L I4 (SIDI & PFI)")


def test_citation_headline_and_plant_list_are_dropped() -> None:
    from backend.enrichment.trim_ladder_knowledge import is_encyclopedia_spec_artifact

    assert is_encyclopedia_spec_artifact(
        'Engine Options: "Toyota Adds to Prius Lineup With Smallest Hybrid".'
    )
    assert is_encyclopedia_spec_artifact('"Here Is 2025 Chevy Trax Pricing With Options And Packages".')
    assert is_encyclopedia_spec_artifact(
        "Malaysia: Kulim, Kedah (Hyundai-Sime Darby Motors, hybrid only)"
    )


def test_foreign_market_variant_is_dropped() -> None:
    from backend.enrichment.trim_ladder_knowledge import is_foreign_market_variant

    assert is_foreign_market_variant("Everus VE-1 (China, electric)")
    assert is_foreign_market_variant("Engine Options: Honda e:NS1/e:NP1 (China, electric, 2022–2025)")
    assert is_foreign_market_variant("Engine Options: L LDF turbo I4 (China only: 25T)")
    assert not is_foreign_market_variant("European-styled front fascia")
    assert not is_foreign_market_variant("Trail Rated badge (Rubicon)")


def test_stated_year_window_is_honoured() -> None:
    """Curated rungs state when their content applied; the renderer ignored it."""
    from backend.enrichment.trim_ladder_knowledge import (
        bullet_year_window_excludes,
        stated_year_window,
    )

    assert stated_year_window("Uconnect 3 with 5-inch display (2020–2021)") == (2020, 2021)
    assert stated_year_window("L5P Duramax 6.6 L V8 (2017-20) 445 hp") == (2017, 2020)
    assert stated_year_window("12-inch Sync 4 screen (2024+)") == (2024, 9999)
    assert stated_year_window("8.4-inch Uconnect touchscreen with NAV") is None

    assert bullet_year_window_excludes("Uconnect 3 with 5-inch display (2020–2021)", 2026)
    assert not bullet_year_window_excludes("Uconnect 3 with 5-inch display (2020–2021)", 2021)
    assert bullet_year_window_excludes("12-inch Sync 4 screen (2024+)", 2023)
    assert not bullet_year_window_excludes("12-inch Sync 4 screen (2024+)", 2025)
    assert not bullet_year_window_excludes("8.4-inch Uconnect touchscreen with NAV", 2026)


def test_a_contaminated_sibling_rejects_the_whole_cell() -> None:
    """Dropping only the bad half published the good half as a 2026 spec."""
    from backend.enrichment.trim_ladder import _bullet_display_parts

    assert (
        _bullet_display_parts(
            "Engine 351 cu in (5.8 L) 351M V8; Based on a design proposal originally used in"
            " the development of the previous-ge",
            trim_name="Sport",
            make="Ford",
            model="Bronco Sport",
            year=2026,
        )
        == []
    )
    # A clean multi-value cell still splits.
    assert _bullet_display_parts(
        "Hybrid 2.5L I4 (SIDI & PFI; Hybrid); 3.4L V6",
        trim_name="XSE",
        make="Toyota",
        model="RAV4",
        year=2024,
    ) == ["Hybrid 2.5L I4 (SIDI & PFI; Hybrid)", "3.4L V6"]


def test_truncated_table_heading_is_a_fragment() -> None:
    from backend.enrichment.trim_ladder_knowledge import (
        is_wellformed_trim_bullet,
        looks_like_spec_table_header,
    )

    assert not is_wellformed_trim_bullet("Model Years of Production Engine &")
    assert looks_like_spec_table_header("Engine Options: Years Engine Power Torque")
    # A "+" is part of the OEM's own naming, not a dangling connective.
    assert is_wellformed_trim_bullet("Pedestrian Detection System as part of Lexus Safety System+")


def test_screen_size_outranks_wheel_diameter() -> None:
    """The user asked for screen size as big-ticket; a wheel inch measure is not."""
    from backend.enrichment.trim_ladder_knowledge import rank_trim_adds, trim_add_category

    assert trim_add_category("20-inch gray machined alloy wheels")[0] == "exterior_wheels"
    assert trim_add_category("12.3-inch digital meter and 14-inch touchscreen audio")[0] == (
        "infotainment"
    )
    assert trim_add_category("Heated and ventilated front seats")[0] == "seat_comfort"
    out = rank_trim_adds(
        [
            "20-inch gray machined alloy wheels",
            "Security alarm system",
            "14-inch touchscreen audio",
            "5.7L HEMI V8 engine",
        ],
        year=2024,
    )
    assert out[0] == "5.7L HEMI V8 engine"
    assert out.index("14-inch touchscreen audio") < out.index("20-inch gray machined alloy wheels")
    assert "Security alarm system" not in out


def test_fallback_position_prose_passes_the_same_gate_as_sourced_bullets() -> None:
    """It sat BELOW the gate, so a line the gate rejects rendered on 1,614 rungs."""
    from backend.enrichment.trim_ladder_knowledge import is_wellformed_trim_bullet

    assert not is_wellformed_trim_bullet(
        "Mid-level trim with added convenience features and nicer interior finishes."
    )


def test_remote_engine_start_is_not_a_powertrain_bullet() -> None:
    """It led the 2026 Pathfinder SV rung, above the AWD system, on one word."""
    from backend.enrichment.trim_ladder_knowledge import trim_add_category

    assert trim_add_category(
        "Power liftgate with Remote Engine Start System and Intelligent Climate Control"
    )[0] == "roof_body"
    assert trim_add_category("Remote engine start")[0] == "climate"
    assert trim_add_category("5.7L HEMI V8 engine — added over the GT")[0] == "powertrain"
    assert trim_add_category("6.4L 392 HEMI® V8 engine")[0] == "powertrain"


def test_hardware_hint_does_not_trust_the_spec_classifier() -> None:
    """It labels a door mirror "Engine Options", which floored it above the roof."""
    from backend.enrichment.trim_ladder import _is_mechanical_trim_bullet
    from backend.enrichment.trim_ladder_knowledge import rank_trim_adds

    assert not _is_mechanical_trim_bullet(
        "Power folding outside mirrors with reverse tilt-down feature"
    )
    assert not _is_mechanical_trim_bullet("Tow hitch receiver with 7-pin wiring harness")
    assert _is_mechanical_trim_bullet("Bilstein® active-damping suspension")
    assert _is_mechanical_trim_bullet("5.7L HEMI V8 engine — added over the GT")

    out = rank_trim_adds(
        [
            "Top Pathfinder equipment",
            "Premium leather seating",
            "Panoramic moonroof",
            "Power folding outside mirrors with reverse tilt-down feature",
            "3.5L V6 (SIDI)",
        ],
        year=2026,
        hardware_hint=_is_mechanical_trim_bullet,
    )
    assert out[0] == "3.5L V6 (SIDI)"
    assert out.index("Panoramic moonroof") < out.index(
        "Power folding outside mirrors with reverse tilt-down feature"
    )


def test_label_strip_is_tested_at_every_step() -> None:
    """One strip exposes the infobox head; a second strip hides it again."""
    from backend.enrichment.trim_ladder_knowledge import (
        is_generic_trim_add,
        spec_label_prefix_strips,
    )

    line = "Engine Options: Engine 5.0 L S85 Uneven firing 90° V10"
    assert spec_label_prefix_strips(line) == [
        line,
        "Engine 5.0 L S85 Uneven firing 90° V10",
        "5.0 L S85 Uneven firing 90° V10",
    ]
    assert is_generic_trim_add(line)
    # The C8 Corvette infobox spells the layout with a position qualifier.
    assert is_generic_trim_add("Layout Rear mid-engine, rear-wheel-drive")
    assert is_generic_trim_add("Mid-engine layout with mid-mounted V-8")


def test_chassis_layout_is_rejected_wherever_it_sits_in_the_line() -> None:
    """"FR (Front-engine, rear-wheel drive)" hid the layout behind an abbreviation."""
    from backend.enrichment.trim_ladder_knowledge import is_generic_trim_add

    assert is_generic_trim_add("FR (Front-engine, rear-wheel drive)")
    assert not is_generic_trim_add("Rear-wheel drive with limited-slip differential")
    assert not is_generic_trim_add("Front and rear skid plates")
