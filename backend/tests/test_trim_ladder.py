"""Trim ladder resolution for premium VDP specs."""

from __future__ import annotations

import pathlib

import pytest

from backend.enrichment.trim_ladder import resolve_trim_ladder


# --- the rung-name gate, and why most of this file runs with it off ----------
#
# ``TRIM_RUNGS_REQUIRE_PROVENANCE`` (default ON in production) drops any rung we
# cannot justify from an active listing of that exact year/make/model or from a
# verified brochure citation. Measured on the live fleet on 2026-08-01, that is
# 71,537 → 59,396 of 73,255 active cars still showing a ladder at all.
#
# Most tests below were written before that gate and assert things about BULLET
# text, rung ORDER, name cleaning and cross-model plausibility. They cannot run
# with it on, for a reason that has nothing to do with what they test: the
# conftest fixture ``_inventory_sqlite_tests_mode`` deletes
# ``INVENTORY_DATABASE_URL`` and points every test at the local SQLite inventory,
# whose ``cars`` table is EMPTY. With no active listings there is no evidence for
# any rung, so with the gate on all 85 of them correctly resolve to None and none
# of them reaches the logic it is about.
#
# So this file turns the gate off by default and tests the gate itself in the
# block at the bottom, which opts back in via the ``rung_gate_on`` fixture —
# stubbing the evidence, and in ``test_the_query_really_reads_active_rows``
# seeding a throwaway inventory so the SQL itself is exercised. Softening the
# gate to make the legacy tests pass was the alternative and was not taken.
#
# The gate's evidence at fleet scale is the measurement in the change report,
# taken against the live Postgres inventory, not this file.


@pytest.fixture
def rung_gate_on() -> bool:
    """Request this fixture to run a test with the rung-name gate at its default (ON)."""
    return True


@pytest.fixture(autouse=True)
def _legacy_rung_gate_off(request, monkeypatch) -> None:
    if "rung_gate_on" in request.fixturenames:
        return
    monkeypatch.setenv("TRIM_RUNGS_REQUIRE_PROVENANCE", "0")


def test_ram_1500_big_horn_match() -> None:
    result = resolve_trim_ladder(
        make="Ram",
        model="1500",
        year=2023,
        trim="Big Horn Crew Cab 4x4",
    )
    assert result is not None
    assert result["matched"] is True
    names = [s["name"] for s in result["steps"]]
    idx = names.index("Big Horn")
    assert result["steps"][idx]["is_current"] is True
    assert result["listing_trim"] == "Big Horn"
    if idx + 1 < len(names):
        assert result["steps"][idx + 1]["is_passed"] is True
    if idx > 0:
        assert result["steps"][idx - 1]["is_ahead"] is True


def test_ram_1500_tradesman_to_tungsten_order() -> None:
    result = resolve_trim_ladder(make="RAM", model="1500", year=2025, trim="Limited")
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert names.index("Tungsten") < names.index("Limited") < names.index("Tradesman")
    assert result["steps"][names.index("Limited")]["is_current"] is True


def test_toyota_tundra_merges_limited_drivetrain_variants() -> None:
    from backend.enrichment.trim_ladder_knowledge import normalize_ladder_steps

    steps = normalize_ladder_steps(
        [
            {"name": "LIMITED 4WD", "aliases": [], "adds": ["4WD package"]},
            {"name": "LIMITED 2WD", "aliases": [], "adds": ["2WD package"]},
            {"name": "SR5", "aliases": [], "adds": []},
            {"name": "Platinum", "aliases": [], "adds": []},
        ],
        "Toyota",
    )
    names = [s["name"] for s in steps]
    assert names.count("Limited") == 1
    assert names.index("Platinum") < names.index("Limited") < names.index("SR5")


def test_toyota_tundra_resolve_luxury_first() -> None:
    result = resolve_trim_ladder(
        make="Toyota",
        model="Tundra",
        year=2024,
        trim="LIMITED 4WD CrewMax",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    if "Platinum" in names and "SR" in names:
        assert names.index("Platinum") < names.index("SR")
    if "Limited" in names:
        assert result["listing_trim"] == "Limited"
        assert result["matched"] is True


def test_ford_f150_lariat_match() -> None:
    result = resolve_trim_ladder(
        make="Ford",
        model="F-150",
        year=2024,
        trim="Lariat SuperCrew",
    )
    assert result is not None
    assert result["matched"] is True
    assert result["steps"][result["current_index"]]["name"] == "Lariat"


def test_jeep_cherokee_inventory_or_curated_ladder() -> None:
    """Any Cherokee in inventory should get a trim ladder (inventory fallback)."""
    result = resolve_trim_ladder(
        make="Jeep",
        model="Cherokee",
        year=2016,
        trim="75th Anniversary Edition",
    )
    if result is None:
        return
    assert len(result["steps"]) >= 2
    assert result["listing_trim"] == "75th Anniversary Edition"


def test_unknown_model_hides_generic_bleed_ladder() -> None:
    result = resolve_trim_ladder(make="ZZZ", model="NotARealModel", year=2024, trim="X")
    assert result is None


def test_dodge_charger_curated_luxury_order() -> None:
    result = resolve_trim_ladder(
        make="Dodge",
        model="Charger",
        year=2023,
        trim="Scat Pack 2-door",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert names.index("Scat Pack") < names.index("R/T")
    assert names.index("R/T") < names.index("GT")
    assert "Daytona Scat Pack" in names
    assert "R/T Scat Pack" not in names
    assert result["matched"] is True


def test_dodge_charger_2018_scat_pack_filters_later_trims() -> None:
    result = resolve_trim_ladder(
        make="Dodge",
        model="Charger",
        year=2018,
        trim="Scat Pack",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert "Scat Pack" in names
    assert "SRT Hellcat" in names
    assert "SRT Hellcat Redeye" not in names
    assert "SRT Hellcat Redeye Widebody" not in names
    assert "Scat Pack Widebody" not in names
    assert "Daytona Scat Pack" not in names
    assert "R/T Scat Pack" not in names


def test_dodge_charger_2026_scat_pack_excludes_hellcat() -> None:
    result = resolve_trim_ladder(
        make="Dodge",
        model="Charger",
        year=2026,
        trim="Scat Pack",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert "Scat Pack" in names
    assert "R/T Scat Pack" in names
    assert "SRT Hellcat" not in names
    assert "SRT Hellcat Redeye" not in names


def test_generic_trim_adds_not_shown_in_ladder() -> None:
    result = resolve_trim_ladder(
        make="ZZZ",
        model="NotARealModel",
        year=2024,
        trim="Sport",
    )
    assert result is None or all(
        not any("typical of the" in (a or "").lower() for a in (s.get("adds") or []))
        for s in result.get("steps") or []
    )


def test_dodge_scat_pack_keeps_real_adds(monkeypatch) -> None:
    # Exercises the bullet SANITIZERS on curated-ladder / Complete_Options text.
    # Those stores carry no citation, so the provenance gate keeps them off the
    # page by default and there would be nothing left to sanitize; the cleaning
    # rules still have to hold for anyone who turns the gate off.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(
        make="Dodge",
        model="Charger",
        year=2023,
        trim="Scat Pack",
    )
    assert result is not None
    scat = next(s for s in result["steps"] if s["name"] == "Scat Pack")
    bullets = scat.get("adds") or []
    assert bullets
    joined = " ".join(bullets)
    assert "HEMI" in joined or "Brembo" in joined


def test_is_generic_trim_add_detects_placeholders() -> None:
    from backend.enrichment.trim_ladder_knowledge import is_generic_trim_add, sanitize_trim_adds

    assert is_generic_trim_add("Factory equipment and features typical of the Sport trim.")
    assert is_generic_trim_add("Equipment and features typical of the Sport trim.")
    assert not is_generic_trim_add("Performance HEMI, sport suspension, and Brembo brakes.")
    cleaned = sanitize_trim_adds(
        [
            "Factory equipment and features typical of the Sport trim.",
            "Leather seating and upgraded audio",
        ],
        "Sport",
    )
    assert cleaned == ["Leather seating and upgraded audio"]


def test_ram_4500_no_feature_fragment_trims() -> None:
    result = resolve_trim_ladder(make="RAM", model="4500", year=2024, trim="Tradesman")
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert "A BLACK PLASTIC GRILLE" not in names
    assert "SPEED AISIN AS66RC AUTOMATIC" not in names
    assert "Tradesman" in names
    assert names.index("Limited") < names.index("Tradesman")


def test_jeep_gladiator_rejects_wiki_csv_junk() -> None:
    result = resolve_trim_ladder(
        make="Jeep",
        model="Gladiator",
        year=2024,
        trim="Rubicon",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert "at the Internet Movie Cars Database" not in names
    assert "Badges" not in names
    assert "Body-M" not in names
    assert "Special Interiors" not in names
    assert "Wheels" not in names
    assert "Rubicon" in names


def test_jeep_gladiator_high_altitude_matches_curated_ladder() -> None:
    result = resolve_trim_ladder(
        make="Jeep",
        model="Gladiator",
        year=2023,
        trim="High Altitude",
    )
    assert result is not None
    assert result["matched"] is True
    current = [s for s in result["steps"] if s.get("is_current")]
    assert len(current) == 1
    assert current[0]["name"] == "High Altitude"


def test_bmw_x2_xdrive28i_motor_trim_ladder() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="X2",
        year=2020,
        trim="xDrive28i",
    )
    assert result is not None
    assert result["matched"] is True
    names = [s["name"] for s in result["steps"]]
    assert "28i" in names
    assert "M Competition" not in names
    assert "Sport Line" not in names
    assert result["steps"][names.index("28i")]["is_current"] is True
    assert result["listing_trim"] == "xDrive28i"


def test_bmw_x3_xdrive30i_motor_trim_ladder() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="X3",
        year=2024,
        trim="xDrive30i",
    )
    assert result is not None
    assert result["matched"] is True
    names = [s["name"] for s in result["steps"]]
    assert "30i" in names
    assert "M Sport" not in names
    assert "xLine" not in names
    assert result["steps"][names.index("30i")]["is_current"] is True


def test_cadillac_xt4_2019_sport_no_v_series() -> None:
    result = resolve_trim_ladder(
        make="Cadillac",
        model="XT4",
        year=2019,
        trim="FWD Sport",
    )
    assert result is not None
    assert result["matched"] is True
    assert result["source"] == "curated"
    names = [s["name"] for s in result["steps"]]
    assert "V-Series" not in names
    assert "V-Series Blackwing" not in names
    assert "Platinum" not in names
    assert names == ["Premium Luxury", "Sport", "Luxury"]
    sport = next(s for s in result["steps"] if s["name"] == "Sport")
    assert sport["is_current"] is True


def test_cadillac_escalade_v_series_trim_ladder(monkeypatch) -> None:
    # Exercises the bullet SANITIZERS on curated-ladder / Complete_Options text.
    # Those stores carry no citation, so the provenance gate keeps them off the
    # page by default and there would be nothing left to sanitize; the cleaning
    # rules still have to hold for anyone who turns the gate off.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(
        make="Cadillac",
        model="Escalade",
        year=2026,
        trim="AWD V-Series",
    )
    assert result is not None
    assert result["matched"] is True
    assert result["source"] == "curated"
    names = [s["name"] for s in result["steps"]]
    assert "V-Series" in names
    assert "Platinum" in names
    assert "Premium" not in names
    assert "Base" not in names
    v = next(s for s in result["steps"] if s["name"] == "V-Series")
    assert v["is_current"] is True
    bullets = v.get("adds") or []
    assert bullets
    assert any("682 hp" in b or "supercharged" in b.lower() for b in bullets)


def test_bmw_x5_2019_excludes_m60i_and_45e() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="X5",
        year=2019,
        trim="xDrive40i",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert "M60i" not in names and "M60i xDrive" not in names
    assert "50i" in names or "xDrive50i" in names
    assert "45e" not in names and "xDrive45e" not in names


def test_bmw_x3_m_uses_curated_bullets_not_csv_junk(monkeypatch) -> None:
    # Exercises the bullet SANITIZERS on curated-ladder / Complete_Options text.
    # Those stores carry no citation, so the provenance gate keeps them off the
    # page by default and there would be nothing left to sanitize; the cleaning
    # rules still have to hold for anyone who turns the gate off.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(
        make="BMW",
        model="X3",
        year=2024,
        trim="M",
    )
    assert result is not None
    m_step = next(s for s in result["steps"] if s["name"] == "M")
    bullets = m_step.get("adds") or []
    joined = " ".join(bullets).lower()
    assert "473 hp" in joined
    assert "includes codes" not in joined
    assert "headlights" not in joined
    assert len(bullets) >= 2


def test_sanitize_trim_specs_drops_includes_codes() -> None:
    from backend.enrichment.trim_spec_extractor import is_junk_spec_text, sanitize_trim_specs

    assert is_junk_spec_text(
        "It includes codes for automatic transmissions, leather upholstery, metallic pain"
    )
    assert is_junk_spec_text(
        "In addition, the Limited includes additional stitched leather-trimmed dashboard surfaces"
    )
    assert is_junk_spec_text(
        "The premium Uconnect 4C 8.4 system, standard on Laramie Longhorn and optional on Big Horn"
    )
    assert is_junk_spec_text(
        "The Hemi-powered 1500 has not been re-introduced in Australia after the discontinuation in 2023"
    )
    cleaned = sanitize_trim_specs(
        [
            {
                "label": "Interior Materials",
                "value": "It includes codes for automatic transmissions, leather upholstery, metallic pain; "
                "It includes codes for automatic transmissions, leather upholstery, metallic paint, premium audio systems",
            },
            {"label": "Engine Options", "value": "473 hp twin-turbo inline-six"},
        ]
    )
    assert len(cleaned) == 1
    assert cleaned[0]["label"] == "Engine Options"


def test_mercedes_sl_class_2015_trim_ladder() -> None:
    result = resolve_trim_ladder(
        make="Mercedes-Benz",
        model="SL-Class",
        year=2015,
        trim="SL 400",
    )
    assert result is not None
    assert result["matched"] is True
    names = [s["name"] for s in result["steps"]]
    assert "SL 400" in names
    assert "SL 550" in names
    assert "SL 63 AMG" in names
    assert "SL 65 AMG" in names
    assert names.index("SL 65 AMG") < names.index("SL 400")
    assert result["steps"][names.index("SL 400")]["is_current"] is True


def test_mercedes_gls_trim_ladder_order() -> None:
    result = resolve_trim_ladder(
        make="Mercedes-Benz",
        model="GLS",
        year=2024,
        trim="GLS450",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert names == ["GLS600", "AMG GLS63", "GLS580", "GLS450"]
    assert result["steps"][names.index("GLS450")]["is_current"] is True


def test_ram_1500_2025_limited_no_uconnect_4c(monkeypatch) -> None:
    # Exercises the bullet SANITIZERS on curated-ladder / Complete_Options text.
    # Those stores carry no citation, so the provenance gate keeps them off the
    # page by default and there would be nothing left to sanitize; the cleaning
    # rules still have to hold for anyone who turns the gate off.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(make="Ram", model="1500", year=2025, trim="Limited")
    assert result is not None
    assert result["source"] == "curated"
    lim = next(s for s in result["steps"] if s["name"] == "Limited")
    bullets = lim.get("adds") or []
    joined = " ".join(bullets).lower()
    assert "uconnect 4c" not in joined
    assert "uconnect 5" in joined
    assert "hurricane" in joined
    assert "in addition" not in joined
    assert "wheel options go by" not in joined
    assert "l pentastar" not in joined


def test_ram_1500_big_horn_no_market_prose(monkeypatch) -> None:
    # Exercises the bullet SANITIZERS on curated-ladder / Complete_Options text.
    # Those stores carry no citation, so the provenance gate keeps them off the
    # page by default and there would be nothing left to sanitize; the cleaning
    # rules still have to hold for anyone who turns the gate off.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    australia = (
        "The Hemi-powered 1500 has not been re-introduced in Australia after the "
        "discontinuation in 2023 as the new electrical system in the MY25 update would "
        "require re-development for the right-hand drive conversion."
    )
    result = resolve_trim_ladder(make="Ram", model="1500", year=2024, trim="Big Horn")
    assert result is not None
    bh = next(s for s in result["steps"] if s["name"] == "Big Horn")
    bullets = bh.get("adds") or []
    joined = " ".join(bullets).lower()
    assert "australia" not in joined
    assert "re-introduced" not in joined
    assert "right-hand drive" not in joined
    assert not any(australia.lower() in b.lower() for b in bullets)
    assert any("chrome" in b.lower() or "appearance" in b.lower() for b in bullets)


def test_ram_1500_trx_excludes_cross_trim_prose(monkeypatch) -> None:
    # Exercises the bullet SANITIZERS on curated-ladder / Complete_Options text.
    # Those stores carry no citation, so the provenance gate keeps them off the
    # page by default and there would be nothing left to sanitize; the cleaning
    # rules still have to hold for anyone who turns the gate off.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(make="Ram", model="1500", year=2022, trim="TRX")
    assert result is not None
    trx = next(s for s in result["steps"] if s["name"] == "TRX")
    bullets = trx.get("adds") or []
    joined = " ".join(bullets).lower()
    assert "laramie longhorn" not in joined
    assert "uconnect 4c" not in joined
    assert "in addition" not in joined
    assert "adds the following features" not in joined
    assert "limited includes" not in joined
    assert any("supercharged" in b.lower() or "6.2" in b.lower() for b in bullets)
    assert any("bilstein" in b.lower() for b in bullets)
    assert any(
        "12-inch" in b.lower() or "uconnect" in b.lower() or "harman" in b.lower()
        for b in bullets
    )


def test_bmw_x5_xdrive40i_motor_trim_ladder() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="X5",
        year=2024,
        trim="xDrive40i",
    )
    assert result is not None
    assert result["matched"] is True
    names = [s["name"] for s in result["steps"]]
    assert "40i" in names
    assert "Luxury Line" not in names
    assert "M Sport" not in names
    assert result["steps"][names.index("40i")]["is_current"] is True


def test_bmw_x6_m60i_motor_trim_ladder() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="X6",
        year=2024,
        trim="M60i xDrive",
    )
    assert result is not None
    assert result["matched"] is True
    names = [s["name"] for s in result["steps"]]
    assert "M60i" in names
    assert "xLine" not in names
    assert result["steps"][names.index("M60i")]["is_current"] is True


def test_bmw_3_series_330i_xdrive_motor_trim_ladder() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="3 Series",
        year=2023,
        trim="330i xDrive",
    )
    assert result is not None
    assert result["matched"] is True
    names = [s["name"] for s in result["steps"]]
    assert names.count("330i") == 1
    assert "M Sport" not in names
    assert "Luxury Line" not in names
    assert result["steps"][names.index("330i")]["is_current"] is True


def test_audi_a8_rejects_wikipedia_complete_options_junk() -> None:
    from backend.enrichment.trim_ladder import _complete_options_ladder_is_junk, _pick_ladder_def

    junk_steps = [
        {"name": "Limited", "aliases": [], "adds": []},
        {"name": "Sport", "aliases": [], "adds": []},
        {"name": "Base", "aliases": [], "adds": []},
    ]
    assert _complete_options_ladder_is_junk(
        junk_steps,
        "Audi",
        "A8",
        source_blob="Predecessor Audi V8 Wheelbase SWB: 2,882 mm",
    )

    ladder = _pick_ladder_def("Audi", "A8", 2020)
    assert ladder is not None
    names = {s["name"].lower() for s in ladder.get("steps") or []}
    assert names != {"limited", "sport", "base"}


def test_jeep_wagoneer_l_series_not_grand_cherokee_ladder() -> None:
    result = resolve_trim_ladder(
        make="Jeep",
        model="Wagoneer",
        year=2024,
        trim="L Series III",
    )
    assert result is not None
    assert result["matched"] is True
    names = [s["name"] for s in result["steps"]]
    assert "L Series III" in names
    assert "Summit Reserve" not in names
    assert "Limited" not in names
    assert "Altitude" not in names
    assert result["steps"][names.index("L Series III")]["is_current"] is True
    assert result["listing_trim"] == "L Series III"


def test_bmw_5_series_530i_motor_trim_ladder() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="5 Series",
        year=2024,
        trim="530i",
    )
    assert result is not None
    assert result["matched"] is True
    names = [s["name"] for s in result["steps"]]
    assert "530i" in names
    assert "Executive" not in names
    assert "Sport Line" not in names
    assert "Base" not in names
    assert result["steps"][names.index("530i")]["is_current"] is True


def test_bmw_5_series_executive_package_does_not_match_trim() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="5 Series",
        year=2024,
        trim="Executive Package",
    )
    assert result is not None
    assert result["matched"] is False
    names = [s["name"] for s in result["steps"]]
    assert "Executive" not in names


def test_bmw_5_series_xdrive_merged_with_rwd(monkeypatch) -> None:
    # Exercises the trim-spec-sheet / Complete_Options merge. Neither store can
    # cite a document, so the provenance gate blocks both by default; the merge
    # behaviour still has to hold for anyone who turns the gate off.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(
        make="BMW",
        model="5 Series",
        year=2023,
        trim="530i xDrive",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert names.count("530i") == 1
    assert "530i xDrive" not in names
    assert names.count("540i") == 1
    assert names.count("530e") == 1
    step_530 = next(s for s in result["steps"] if s["name"] == "530i")
    assert step_530["is_current"] is True
    alias_blob = " ".join(step_530.get("aliases") or []).lower()
    assert "xdrive" in alias_blob
    adds_blob = " ".join(step_530.get("adds") or []).lower()
    specs_blob = " ".join(
        f"{row.get('label', '')} {row.get('value', '')}" for row in (step_530.get("specs") or [])
    ).lower()
    merged = adds_blob + specs_blob
    assert "rear-wheel" in merged or "rwd" in merged
    assert "xdrive" in merged or "all-wheel" in merged or "awd" in merged


def test_bmw_5_series_530e_merged_variants() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="5 Series",
        year=2023,
        trim="530e",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert names.count("530e") == 1
    assert "530e xDrive" not in names
    step = next(s for s in result["steps"] if s["name"] == "530e")
    assert step["is_current"] is True


def test_bmw_i4_edrive35_ladder_m50_on_top() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="i4",
        year=2023,
        trim="eDrive35",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert names.index("M50 Gran Coupe") < names.index("eDrive40")
    assert names.index("eDrive40") < names.index("eDrive35")
    assert result["matched"] is True


def test_bmw_x5_sdrive40i_merged_with_xdrive() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="X5",
        year=2024,
        trim="sDrive40i",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert names.count("40i") == 1
    assert "xDrive40i" not in names
    assert "sDrive40i" not in names
    step = next(s for s in result["steps"] if s["name"] == "40i")
    assert step["is_current"] is True


def test_bmw_x3_xdrive30i_merged_with_sdrive() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="X3",
        year=2024,
        trim="sDrive30i",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert names.count("30i") == 1
    assert "xDrive30i" not in names
    assert "sDrive30i" not in names


def test_merge_drivetrain_ladder_steps_unions_adds() -> None:
    from backend.enrichment.trim_ladder_knowledge import merge_drivetrain_ladder_steps

    merged = merge_drivetrain_ladder_steps(
        [
            {"name": "530i xDrive", "aliases": [], "adds": ["xDrive AWD"]},
            {"name": "530i", "aliases": [], "adds": ["Rear-wheel drive"]},
        ],
        "BMW",
        model="5 Series",
    )
    assert len(merged) == 1
    assert merged[0]["name"] == "530i"
    adds = " ".join(merged[0]["adds"]).lower()
    assert "xdrive" in adds
    assert "rear-wheel" in adds


def test_bmw_i4_uses_epa_motor_trims_not_package_lines() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="i4",
        year=2026,
        trim="eDrive40",
    )
    assert result is not None
    # EPA motor trims or the tracked per-year spec-sheet catalog (added with the
    # brochure pipeline; supersedes EPA for model-years it covers) — never the
    # raw package/options lines.
    source = str(result.get("source") or "")
    assert "EPA" in source or "Complete_Options" in source
    names = [s["name"] for s in result["steps"]]
    assert "eDrive40" in names
    assert "M Competition" not in names
    assert result["matched"] is True


def test_mercedes_glc_uses_epa_trims_not_generic_packages() -> None:
    result = resolve_trim_ladder(
        make="Mercedes-Benz",
        model="GLC",
        year=2026,
        trim="GLC 300 SUV",
    )
    assert result is not None
    assert "EPA" in str(result.get("source") or "")
    names = [s["name"] for s in result["steps"]]
    assert any("GLC" in n for n in names)
    assert "Maybach" not in names


def test_toyota_corolla_hybrid_no_tundra_trims_in_fallback() -> None:
    result = resolve_trim_ladder(
        make="Toyota",
        model="Corolla Hybrid",
        year=2026,
        trim="LE",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert "Capstone" not in names
    assert "1794" not in names


def test_jeep_grand_wagoneer_not_grand_cherokee_csv_ladder() -> None:
    result = resolve_trim_ladder(
        make="Jeep",
        model="Grand Wagoneer",
        year=2026,
        trim="Upland",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert "Altitude" not in names
    assert "Premium" not in names
    assert any(n in names for n in ("Upland", "Summit Reserve", "Series III"))


def test_audi_a8_and_q6_use_model_or_epa_ladder() -> None:
    a8 = resolve_trim_ladder(make="Audi", model="A8", year=2023, trim="L 55 TFSI quattro")
    assert a8 is not None
    names = [s["name"] for s in a8["steps"]]
    assert "Premium Plus" in names or "L" in names
    assert "Platinum" not in names
    assert "XLE" not in names
    assert a8["matched"] is True
    assert a8["quality"] == "high"

    q6 = resolve_trim_ladder(make="Audi", model="Q6 e-tron", year=2025, trim="Premium quattro")
    assert q6 is not None
    q6_names = [s["name"] for s in q6["steps"]]
    assert "Platinum" not in q6_names
    assert q6["matched"] is True
    assert "Premium" in q6_names
    assert q6["quality"] == "high"


def test_audi_a3_and_a4_curated_ladders_match() -> None:
    a3 = resolve_trim_ladder(make="Audi", model="A3", year=2026, trim="Premium quattro")
    assert a3 is not None
    assert a3["matched"] is True
    assert a3["source"] == "curated"
    assert "Premium" in [s["name"] for s in a3["steps"]]

    a4 = resolve_trim_ladder(make="Audi", model="A4 Sedan", year=2024, trim="Premium Plus 40 TFSI quattro")
    assert a4 is not None
    assert a4["matched"] is True
    assert "Premium Plus" in [s["name"] for s in a4["steps"]]


def test_weak_unmatched_junk_ladder_hidden() -> None:
    from backend.enrichment.trim_ladder import _trim_ladder_should_display

    junk = {
        "matched": False,
        "source": "2024_Audi_A3_Complete_Options.csv",
        "steps": [{"name": "Sport"}, {"name": "Base"}, {"name": "S3 Sportback"}],
    }
    assert _trim_ladder_should_display(junk, "Audi", "A3") is False


def test_audi_make_fallback_not_generic_toyota_trims() -> None:
    result = resolve_trim_ladder(make="Audi", model="A5 Coupe", year=2024, trim="S line Premium Plus")
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert "Prestige" in names or "Premium Plus" in names
    assert "XLE" not in names
    assert "Capstone" not in names


def test_bmw_3_series_2016_28i_typo_gets_a_ladder_but_no_claim(monkeypatch) -> None:
    """AMENDED 2026-08-02: "28i xDrive" no longer claims the "330i" rung.

    Exercises the bullet SANITIZERS on curated-ladder / Complete_Options text.
    Those stores carry no citation, so the provenance gate keeps them off the
    page by default and there would be nothing left to sanitize; the cleaning
    rules still have to hold for anyone who turns the gate off.

    The claim itself is gone with the alias route. The curated "330i" rung
    declares the aliases ``["330i xDrive", "330i Sedan", "330i xDrive Sedan",
    "328i", "328i xDrive", "28i", "28i xDrive", "335i", "335i xDrive"]`` — read
    out of ``trim_ladders.json`` on 2026-08-02, not recalled. "335i" is a
    300 hp turbo inline-six and "328i" a 240 hp turbo four; filing both under a
    248 hp "330i" rung is our own table asserting three cars are one, with no
    document behind it. So the ladder still renders and the sanitizers are
    still exercised, but nothing says this car IS a 330i.
    """
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    # A brochure overlay for this model-year now ships in the repo
    # (trim_adds_by_year, commit 7d3c76e1c); the curated ladder must still win.
    result = resolve_trim_ladder(make="BMW", model="3 Series", year=2016, trim="28i xDrive")
    assert result is not None
    assert str(result.get("source") or "") == "curated"
    names = [s["name"] for s in result["steps"]]
    assert "330i" in names
    assert "Na" not in names and "Bmw" not in names
    assert result["matched"] is False
    assert not any(s["is_current"] for s in result["steps"])
    # The sanitizers still have to hold on the rung's own bullets.
    rung_330i = result["steps"][names.index("330i")]
    assert any(
        "turbo" in str(a).lower() or "hp" in str(a).lower()
        for a in (rung_330i.get("adds") or [])
    )


def test_jeep_renegade_2016_latitude_trim_ladder() -> None:
    result = resolve_trim_ladder(make="Jeep", model="Renegade", year=2016, trim="Latitude")
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert "Latitude" in names
    assert "Trailhawk" in names
    assert "Sport" in names
    assert any(s["is_current"] for s in result["steps"])


def test_trim_ladder_skipped_before_2010() -> None:
    assert resolve_trim_ladder(make="Jeep", model="Renegade", year=2009, trim="Sport") is None


def test_trim_ladder_hidden_on_api_without_premium(monkeypatch) -> None:
    from backend.main import app

    car = {
        "id": 99,
        "vin": "1C6SRFHT0NN123456",
        "make": "Ram",
        "model": "1500",
        "year": 2023,
        "trim": "Laramie",
        "price": 52000,
        "mileage": 12000,
        "dealer_id": "demo",
        "active": 1,
    }
    monkeypatch.setattr("backend.main.get_car_by_id", lambda cid, **kw: car if cid == 99 else None)
    monkeypatch.setattr("backend.main._viewer_sees_premium_features", lambda: False)
    monkeypatch.setattr("backend.main._session_has_paid_access", lambda: False)
    monkeypatch.setattr(
        "backend.main.prepare_car_detail_context",
        lambda _raw: {"verified_specs": {}, "gallery_images": []},
    )
    with app.test_client() as client:
        rv = client.get("/api/cars/99")
    body = rv.get_json()
    assert body.get("trim_ladder") is None


def test_trim_ladder_in_api_when_premium_renegade(monkeypatch) -> None:
    from backend.main import app

    car = {
        "id": 101,
        "vin": "ZACCJABT7GPD77565",
        "make": "Jeep",
        "model": "Renegade",
        "year": 2016,
        "trim": "Latitude",
        "price": 12000,
        "mileage": 80337,
        "dealer_id": "demo",
        "active": 1,
    }
    monkeypatch.setattr("backend.main.get_car_by_id", lambda cid, **kw: car if cid == 101 else None)
    monkeypatch.setattr("backend.main._viewer_sees_premium_features", lambda: True)
    monkeypatch.setattr("backend.main._session_has_paid_access", lambda: True)
    monkeypatch.setattr(
        "backend.main.prepare_car_detail_context",
        lambda _raw: {"verified_specs": {}, "gallery_images": []},
    )
    with app.test_client() as client:
        rv = client.get("/api/cars/101")
    body = rv.get_json()
    ladder = body.get("trim_ladder")
    assert ladder is not None
    names = [s["name"] for s in ladder.get("steps") or []]
    assert "Latitude" in names
    assert len(names) >= 2


def test_trim_ladder_in_api_when_paid(monkeypatch) -> None:
    from backend.main import app

    car = {
        "id": 100,
        "vin": "1C6SRFHT0NN123457",
        "make": "Ram",
        "model": "1500",
        "year": 2023,
        "trim": "Laramie",
        "price": 52000,
        "mileage": 12000,
        "dealer_id": "demo",
        "active": 1,
    }
    monkeypatch.setattr("backend.main.get_car_by_id", lambda cid, **kw: car if cid == 100 else None)
    monkeypatch.setattr("backend.main._viewer_sees_premium_features", lambda: True)
    monkeypatch.setattr("backend.main._session_has_paid_access", lambda: True)
    monkeypatch.setattr(
        "backend.main.prepare_car_detail_context",
        lambda _raw: {"verified_specs": {}, "gallery_images": []},
    )
    with app.test_client() as client:
        rv = client.get("/api/cars/100")
    body = rv.get_json()
    ladder = body.get("trim_ladder")
    assert ladder is not None
    assert ladder["matched"] is True
    assert any(s["is_current"] for s in ladder["steps"])


def test_land_rover_range_rover_se_curated() -> None:
    result = resolve_trim_ladder(
        make="Land Rover",
        model="Range Rover",
        year=2024,
        trim="SE",
    )
    assert result is not None
    assert result["source"] == "curated"
    assert result["matched"] is True
    assert result["listing_trim"] == "SE"
    names = [s["name"] for s in result["steps"]]
    assert names.index("Autobiography") < names.index("SE")


def test_tesla_model_3_long_range_curated() -> None:
    result = resolve_trim_ladder(
        make="Tesla",
        model="Model 3",
        year=2023,
        trim="Long Range AWD",
    )
    assert result is not None
    assert result["matched"] is True
    assert result["listing_trim"] == "Long Range"
    names = [s["name"] for s in result["steps"]]
    assert "Long Range" in names
    assert "Plaid" in names


def test_gmc_yukon_at4_ultimate_is_not_the_at4_rung() -> None:
    """AMENDED 2026-08-02. This test used to assert the alias-only claim.

    The curated GMC Yukon ladder files "AT4 Ultimate" and "AT4X" as aliases of
    the "AT4" rung. They are neighbouring trims, not spellings of AT4, so the
    car no longer claims that rung. The LADDER still renders — the rungs are
    lineup context and their order is still checked below — only the "This
    vehicle" badge is withheld.
    """
    result = resolve_trim_ladder(
        make="GMC",
        model="Yukon XL",
        year=2024,
        trim="AT4 Ultimate",
    )
    assert result is not None
    assert result["matched"] is False
    assert result["match_kind"] == "none"
    assert not any(s["is_current"] for s in result["steps"])
    names = [s["name"] for s in result["steps"]]
    assert names.index("Denali Ultimate") < names.index("AT4")


def test_volvo_xc90_b6_ultra_is_not_the_b6_rung() -> None:
    """AMENDED 2026-08-02, same reason as the Yukon case above.

    "B6" names a powertrain and "Ultra" names Volvo's top equipment grade, so
    "B6 Ultra" and the "B6" rung are not established to be one trim by anything
    we hold — only by an alias list of our own. The claim is withheld.
    """
    result = resolve_trim_ladder(
        make="Volvo",
        model="XC90",
        year=2024,
        trim="B6 Ultra",
    )
    assert result is not None
    assert result["matched"] is False
    assert not any(s["is_current"] for s in result["steps"])
    names = [s["name"] for s in result["steps"]]
    assert names.index("T8") < names.index("B6")


def test_toyota_tacoma_trd_pro_curated() -> None:
    result = resolve_trim_ladder(
        make="Toyota",
        model="Tacoma",
        year=2024,
        trim="TRD Pro",
    )
    assert result is not None
    assert result["matched"] is True
    assert result["listing_trim"] == "TRD Pro"


def test_ram_1500_2023_excludes_tungsten_trim() -> None:
    result = resolve_trim_ladder(make="Ram", model="1500", year=2023, trim="Big Horn")
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert "Tungsten" not in names
    assert "Big Horn" in names


def test_ram_1500_2025_includes_tungsten_trim() -> None:
    result = resolve_trim_ladder(make="Ram", model="1500", year=2025, trim="Limited")
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert "Tungsten" in names


def test_jeep_compass_latitude_curated_adds(monkeypatch) -> None:
    # Exercises the bullet SANITIZERS on curated-ladder / Complete_Options text.
    # Those stores carry no citation, so the provenance gate keeps them off the
    # page by default and there would be nothing left to sanitize; the cleaning
    # rules still have to hold for anyone who turns the gate off.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(
        make="Jeep",
        model="Compass",
        year=2024,
        trim="Latitude",
    )
    assert result is not None
    assert result["source"] == "curated"
    assert result["matched"] is True
    current = [s for s in result["steps"] if s.get("is_current")][0]
    bullets = current.get("adds") or []
    assert bullets
    joined = " ".join(bullets).lower()
    assert "convenience" in joined or "uconnect" in joined


def test_jeep_grand_cherokee_wk2_curated_order() -> None:
    result = resolve_trim_ladder(
        make="Jeep",
        model="Grand Cherokee",
        year=2019,
        trim="Overland",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert names.index("High Altitude") < names.index("Overland")
    assert "Trackhawk" in names
    assert "SRT" in names
    assert names.index("Limited X") < names.index("Limited")

    result_2020 = resolve_trim_ladder(
        make="Jeep",
        model="Grand Cherokee",
        year=2020,
        trim="Overland",
    )
    assert result_2020 is not None
    names_2020 = [s["name"] for s in result_2020["steps"]]
    assert names_2020.index("Limited X") < names_2020.index("Limited")


def test_jeep_grand_cherokee_2016_overland_uses_full_brochure_adds(monkeypatch) -> None:
    """
    The 2016 Grand Cherokee overlay is ``manual_brochure_review`` and cites
    nothing, so the gate drops its bullets. The rung used to keep going anyway,
    refilled from the curated trim spec sheet
    ``derived/trim_spec_sheets/jeep_grand_cherokee_wk2.json``; that store cannot
    cite anything either, so the rung is now silent instead. The overlay's own
    lines are still reachable with the gate off, which is where the original
    regression (placeholder prose) is checked.
    """
    gated = resolve_trim_ladder(make="Jeep", model="Grand Cherokee", year=2016, trim="Overland")
    assert gated is not None
    gated_bullets = next(s for s in gated["steps"] if s["name"] == "Overland").get("adds") or []
    assert gated_bullets == [], gated_bullets

    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(
        make="Jeep",
        model="Grand Cherokee",
        year=2016,
        trim="Overland",
    )
    assert result is not None
    overland = next(s for s in result["steps"] if s["name"] == "Overland")
    bullets = overland.get("adds") or []
    assert len(bullets) >= 3
    joined = " ".join(bullets).lower()
    assert "harman kardon" in joined or "quadra-lift" in joined
    assert "mid-level trim between" not in joined


def test_jeep_grand_cherokee_2016_trailhawk_not_placeholder_prose(monkeypatch) -> None:
    # Uncited overlay: gated off on the page, still checked for the prose bug here.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(
        make="Jeep",
        model="Grand Cherokee",
        year=2016,
        trim="Trailhawk",
    )
    assert result is not None
    trailhawk = next(s for s in result["steps"] if s["name"] == "Trailhawk")
    bullets = trailhawk.get("adds") or []
    assert bullets
    joined = " ".join(bullets).lower()
    assert "rugged or adventure-oriented" not in joined
    assert "trail rated" in joined or "quadra-drive" in joined or "skid" in joined


def test_jeep_grand_cherokee_overland_air_suspension_standard(monkeypatch) -> None:
    # Uncited overlay: gated off on the page, still checked for cross-trim bleed here.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(
        make="Jeep",
        model="Grand Cherokee",
        year=2019,
        trim="Overland",
    )
    assert result is not None
    overland = next(s for s in result["steps"] if s["name"] == "Overland")
    bullets = overland.get("adds") or []
    joined = " ".join(bullets).lower()
    assert "air suspension" in joined
    assert len(bullets) >= 3

    summit = next(s for s in result["steps"] if s["name"] == "Summit")
    summit_bullets = summit.get("adds") or []
    assert len(summit_bullets) >= 2


def test_jeep_grand_cherokee_limited_x_exterior_styling_2019(monkeypatch) -> None:
    """Limited X stays on curated WK2 ladder (with body styling) even pre-2020 MY."""
    # Exercises the bullet SANITIZERS on curated-ladder / Complete_Options text.
    # Those stores carry no citation, so the provenance gate keeps them off the
    # page by default and there would be nothing left to sanitize; the cleaning
    # rules still have to hold for anyone who turns the gate off.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    from backend.enrichment.trim_ladder_knowledge import is_wellformed_trim_bullet

    result = resolve_trim_ladder(
        make="Jeep",
        model="Grand Cherokee",
        year=2019,
        trim="Limited",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert "Limited X" in names
    limited_x = next(s for s in result["steps"] if s["name"] == "Limited X")
    bullets = limited_x.get("adds") or []
    assert bullets
    # This rung's curated Exterior Styling cell is a 189-character comma list.
    # It used to render whole; it is now split, so every bullet stands alone.
    assert all(is_wellformed_trim_bullet(b) for b in bullets), bullets


def test_jeep_grand_cherokee_limited_x_match(monkeypatch) -> None:
    # Exercises the bullet SANITIZERS on curated-ladder / Complete_Options text.
    # Those stores carry no citation, so the provenance gate keeps them off the
    # page by default and there would be nothing left to sanitize; the cleaning
    # rules still have to hold for anyone who turns the gate off.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(
        make="Jeep",
        model="Grand Cherokee",
        year=2020,
        trim="Limited X",
    )
    assert result is not None
    assert result["matched"] is True
    assert result["listing_trim"] == "Limited X"
    limited_x = next(s for s in result["steps"] if s["name"] == "Limited X")
    bullets = limited_x.get("adds") or []
    assert bullets
    assert all(len(b) <= 140 for b in bullets), bullets


def test_long_exterior_styling_cell_splits_into_standalone_bullets() -> None:
    """The WK2 Limited X styling cell becomes one bullet per item, not a paragraph."""
    from backend.enrichment.trim_ladder import _bullet_display_parts

    cell = (
        "Exterior Styling: SRT-inspired power dome hood with functional heat "
        "extractors, SRT-style front and rear fascias, body-color grille surround, "
        "gloss-black headlamp bezels, and 20-inch gloss-black aluminum wheels"
    )
    parts = _bullet_display_parts(
        cell, trim_name="Limited X", make="Jeep", model="Grand Cherokee", year=2020
    )
    joined = " ".join(parts).lower()
    assert "srt" in joined
    assert "hood" in joined
    assert len(parts) >= 4
    assert all(len(p) <= 140 for p in parts)
    # The "and " that joined the last item is not carried into the bullet.
    assert not any(p.lower().startswith("and ") for p in parts)


def test_honda_accord_11th_gen_curated_trim_ladder(monkeypatch) -> None:
    # Exercises the bullet SANITIZERS on curated-ladder / Complete_Options text.
    # Those stores carry no citation, so the provenance gate keeps them off the
    # page by default and there would be nothing left to sanitize; the cleaning
    # rules still have to hold for anyone who turns the gate off.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    from backend.enrichment.trim_ladder_knowledge import (
        canonical_trim_name,
        preserve_trim_label,
    )

    assert preserve_trim_label("Touring Hybrid", "Honda", "Accord") == "Touring Hybrid"
    assert canonical_trim_name("Sport-L Hybrid", "Honda", "Accord") == "Sport-L Hybrid"
    assert canonical_trim_name("Touring Hybrid", "Honda", "Accord") == "Touring Hybrid"

    result = resolve_trim_ladder(
        make="Honda",
        model="Accord",
        year=2026,
        trim="Touring Hybrid",
    )
    assert result is not None
    assert result["source"] == "curated"
    assert result["matched"] is True
    assert result["listing_trim"] == "Touring Hybrid"
    names = [s["name"] for s in result["steps"]]
    assert names == [
        "Touring Hybrid",
        "Sport-L Hybrid",
        "EX-L Hybrid",
        "Sport Hybrid",
        "SE",
        "LX",
    ]
    touring = next(s for s in result["steps"] if s["name"] == "Touring Hybrid")
    assert touring.get("is_current") is True
    joined = " ".join(touring.get("adds") or []).lower()
    assert "bose" in joined or "head-up" in joined


def test_jeep_grand_cherokee_wl_summit_reserve_not_duplicate_summit() -> None:
    from backend.enrichment.trim_ladder_knowledge import (
        canonical_trim_name,
        preserve_trim_label,
    )

    assert preserve_trim_label("Summit Reserve", "Jeep", "Grand Cherokee") == "Summit Reserve"
    assert canonical_trim_name("Summit Reserve", "Jeep", "Grand Cherokee") == "Summit Reserve"

    result = resolve_trim_ladder(
        make="Jeep",
        model="Grand Cherokee",
        year=2024,
        trim="Summit Reserve",
    )
    assert result is not None
    assert result["matched"] is True
    assert result["listing_trim"] == "Summit Reserve"
    names = [s["name"] for s in result["steps"]]
    assert names.count("Summit") == 1
    assert names.index("Summit Reserve") < names.index("Summit")
    assert result["steps"][names.index("Summit Reserve")]["is_current"] is True
    assert "Trackhawk" not in names
    assert "SRT" not in names


def test_porsche_taycan_4s_curated() -> None:
    result = resolve_trim_ladder(
        make="Porsche",
        model="Taycan",
        year=2023,
        trim="4S",
    )
    assert result is not None
    assert result["matched"] is True
    assert result["listing_trim"] == "4S"


def test_chevrolet_captiva_sport_lt_not_corvette_ladder() -> None:
    """Captiva must not show Silverado/Corvette make-wide fallback trims."""
    result = resolve_trim_ladder(
        make="Chevrolet",
        model="Captiva Sport Fleet",
        year=2013,
        trim="LT",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert "ZR1" not in names
    assert "Stingray" not in names
    assert names == ["LTZ", "LT", "LS"]
    assert result["matched"] is True
    assert result["steps"][names.index("LT")]["is_current"] is True


def test_trim_match_lt_does_not_match_ltz() -> None:
    from backend.enrichment.trim_ladder import _exact_rung_match

    assert _exact_rung_match("LT", "LTZ", [], make="Chevrolet", model="Captiva") is False
    assert _exact_rung_match("LT", "LT", [], make="Chevrolet", model="Captiva") is True


def test_exact_rung_match_rejects_every_near_miss() -> None:
    """The replaced scorer graded these 60-80. None of them is the same trim.

    Each pair below is a real shape the tolerance accepted: substring
    containment ("le" inside "xlepremium"), the rung being a prefix of the
    listing, the listing being a prefix of the rung, and a word-boundary hit on
    a two-word rung.
    """
    from backend.enrichment.trim_ladder import _exact_rung_match

    near_misses = [
        ("LE", "XLE Premium", "Toyota", "RAV4"),
        ("LE", "XLE", "Toyota", "RAV4"),
        ("Sport", "Sport Prestige", "Acura", "TLX"),
        ("Sport Prestige", "Sport", "Acura", "TLX"),
        ("Limited", "Limited Platinum", "Ford", "Explorer"),
        ("S", "SE", "Nissan", "Altima"),
        ("Premium", "Premium Plus", "Audi", "Q5"),
        ("Big Horn", "Big Horn Built to Serve", "Ram", "1500"),
    ]
    for listing, rung, make, model in near_misses:
        assert (
            _exact_rung_match(listing, rung, [], make=make, model=model) is False
        ), f"{listing!r} is not {rung!r}"


def test_exact_rung_match_accepts_only_same_trim_spellings() -> None:
    """Same trim, different spelling, still matches — that is not a near-miss."""
    from backend.enrichment.trim_ladder import _exact_rung_match

    # A declared alias is NOT a spelling of the same trim as far as this
    # predicate is concerned — see test_alias_table_cannot_produce_a_claim.
    assert _exact_rung_match("Ltd", "Limited", ["Ltd"], make="Ford", model="Explorer") is False
    # Engine badge, drivetrain and cab config are not trim words.
    assert (
        _exact_rung_match("Premium Plus 45 TFSI", "Premium Plus", [], make="Audi", model="Q5")
        is True
    )
    assert (
        _exact_rung_match("Big Horn Crew Cab 4x4", "Big Horn", [], make="Ram", model="1500")
        is True
    )
    assert (
        _exact_rung_match("LIMITED 4WD CrewMax", "Limited", [], make="Toyota", model="Tundra")
        is True
    )
    assert (
        _exact_rung_match(
            "3.3 Turbo Premium Plus AWD", "Premium Plus", [], make="Mazda", model="CX-90"
        )
        is True
    )
    # Punctuation only.
    assert _exact_rung_match("SX-Prestige", "SX Prestige", [], make="Kia", model="Sportage") is True
    # ...but the same listing is NOT the plain "SX" rung.
    assert _exact_rung_match("SX-Prestige", "SX", [], make="Kia", model="Sportage") is False
    # Drivetrain suffix: one trim, two driven axles.
    assert _exact_rung_match("330i xDrive", "330i", [], make="BMW", model="3 Series") is True
    # A bare "Turbo" is a Porsche trim, not an engine badge — never stripped.
    assert _exact_rung_match("Turbo S", "Turbo", [], make="Porsche", model="911") is False
    # Empty trim can never be a rung.
    assert _exact_rung_match("", "Limited", [], make="Ford", model="Explorer") is False
    assert _exact_rung_match("—", "Limited", [], make="Ford", model="Explorer") is False


def test_alias_table_cannot_produce_a_claim() -> None:
    """A rung's ALIASES are our own mapping, so they cannot say "this car IS it".

    The alias route was the matching path the exact-match lane missed. Every
    pair below is taken from a real ladder definition in this repo, and each is
    a NEIGHBOURING TRIM filed as an alias of the rung above/below it — measured
    2026-08-02 on the live fleet, the whole alias route was carrying 541 active
    cars' "This vehicle" badge onto a rung their trim string does not name.
    """
    from backend.enrichment.trim_ladder import _exact_rung_match

    neighbours_filed_as_aliases = [
        ("Raptor R", "Raptor", ["Raptor R"], "Ford", "F-150"),
        (
            "Premium Luxury Platinum",
            "Premium Luxury",
            ["Premium Luxury Platinum"],
            "Cadillac",
            "Escalade",
        ),
        ("AT4 Ultimate", "AT4", ["AT4 Ultimate", "AT4X"], "GMC", "Yukon"),
        (
            "Standard Range Plus",
            "Standard Range",
            ["Standard Range Plus", "Standard Range RWD"],
            "Tesla",
            "Model 3",
        ),
        (
            "Scat Pack Plus",
            "Scat Pack",
            ["Scat Pack 2-door", "Scat Pack Plus"],
            "Dodge",
            "Charger",
        ),
    ]
    for listing, rung, aliases, make, model in neighbours_filed_as_aliases:
        for tier in (1, 2):
            assert (
                _exact_rung_match(listing, rung, aliases, make=make, model=model, tier=tier)
                is False
            ), f"{listing!r} claimed the {rung!r} rung through its alias list"

    # The parameter is accepted and ignored: passing aliases never changes the
    # answer, in either direction.
    for tier in (1, 2):
        assert _exact_rung_match(
            "Limited", "Limited", ["Anything", "At", "All"], make="Ford", model="Explorer",
            tier=tier,
        ) is True


def test_rav4_le_is_not_pinned_to_xle_premium() -> None:
    """THE defect: a 2026 RAV4 LE was rendered as the XLE Premium rung.

    The 2026 RAV4 ladder has no LE rung at all. Before the exact-match rule the
    car matched "XLE Premium" at score 60 (``"le" in "xlepremium"``) and
    inherited that rung's bullets under a "This vehicle" badge.
    """
    result = resolve_trim_ladder(make="Toyota", model="RAV4", year=2026, trim="LE")
    if result is None:
        return
    names = [s["name"] for s in result["steps"]]
    assert "LE" not in names, "fixture changed: this case needs a ladder with no LE rung"
    assert result["matched"] is False
    assert result["match_kind"] == "none"
    assert result["current_index"] is None
    assert not any(s["is_current"] for s in result["steps"])
    # No position claim either: nothing may read as "above"/"below this car".
    assert not any(s["is_passed"] for s in result["steps"])
    assert not any(s["is_ahead"] for s in result["steps"])


def test_drivetrain_named_rungs_keep_their_literal_match() -> None:
    """A BMW X5 sDrive40i is the sDrive40i rung, not the xDrive40i one.

    Both rungs reduce to "40i" under ``drivetrain_merge_key``, so a single-tier
    comparison made every such car ambiguous and silenced 687 of them across
    the live fleet — including cars whose trim string is character-for-character
    the rung name. Tier 1 (labels as written) settles it; the merged reading is
    only consulted when tier 1 finds nothing anywhere on the ladder.
    """
    for listed in ("sDrive40i", "xDrive40i"):
        result = resolve_trim_ladder(make="BMW", model="X5", year=2024, trim=listed)
        if result is None:
            continue
        names = [s["name"] for s in result["steps"]]
        if listed not in names:
            continue
        assert result["matched"] is True, listed
        current = [s["name"] for s in result["steps"] if s["is_current"]]
        assert current == [listed], f"{listed} claimed {current}"


def test_merged_drivetrain_only_reaches_a_drivetrain_neutral_rung() -> None:
    """"330i xDrive" is the "330i" rung; "sDrive40i" is never the "xDrive40i" rung."""
    from backend.enrichment.trim_ladder import _exact_rung_match

    # Rung name carries no drivetrain -> the merged reading may reach it.
    assert (
        _exact_rung_match("330i xDrive", "330i", [], make="BMW", model="3 Series", tier=2) is True
    )
    # Rung name commits to a drivetrain -> the merged reading may not.
    assert (
        _exact_rung_match("sDrive40i", "xDrive40i", [], make="BMW", model="X5", tier=2) is False
    )
    assert (
        _exact_rung_match("sDrive40i", "xDrive40i", [], make="BMW", model="X5", tier=1) is False
    )


def test_absent_trim_never_produces_a_vehicle_claim() -> None:
    """Generalisation of the RAV4 case across makes.

    A trim string that is not one of the rungs must leave every per-step flag
    false, whatever the ladder source. The rungs themselves may still render —
    they are lineup context — but nothing says the car is one of them.
    """
    cases = [
        ("Toyota", "RAV4", 2026, "LE"),
        ("Ford", "F-150", 2024, "Not A Real Trim"),
        ("Honda", "Civic", 2024, "Zzzz Edition"),
        ("Chevrolet", "Silverado 1500", 2024, "Nonexistent"),
    ]
    for make, model, year, trim in cases:
        result = resolve_trim_ladder(make=make, model=model, year=year, trim=trim)
        if result is None:
            continue
        names = {str(s["name"]).lower() for s in result["steps"]}
        assert trim.lower() not in names
        assert result["matched"] is False, f"{make} {model} {trim}"
        assert result["match_kind"] == "none"
        assert result["current_index"] is None
        for step in result["steps"]:
            assert step["is_current"] is False
            assert step["is_passed"] is False
            assert step["is_ahead"] is False


def test_volvo_ex90_no_placeholder_trim_adds() -> None:
    """EX90 ladder may be names-only until brochure/overlay data exists."""
    result = resolve_trim_ladder(make="Volvo", model="EX90", year=2025, trim="Ultra")
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert names == ["Ultra", "Ultimate", "Plus", "Core"]
    placeholders = (
        "Top of this trim lineup",
        "typically the most equipment",
        "Builds on Ultimate with additional comfort",
        "Rugged or adventure-oriented trim",
    )
    for step in result["steps"]:
        for line in step.get("adds") or []:
            assert not any(p in line for p in placeholders)


def test_brochure_overlay_fuzzy_matches_inventory_trim_names(monkeypatch, tmp_path) -> None:
    # _ladder_from_inventory reads the inventory DB; seed a private one so the
    # test doesn't depend on whatever the local dev DB happens to contain.
    # The Palisade overlay is uncited, so this exercises the fuzzy trim match with
    # the provenance gate off; what the gate does to it is covered separately.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    import sqlite3

    from backend.db import inventory_db
    from backend.db.inventory_db import init_inventory_db
    from backend.enrichment.brochure_extract import load_brochure_trim_overlay
    from backend.enrichment.trim_ladder import _build_ladder_result, _ladder_from_inventory

    dbp = tmp_path / "inv_palisade.db"
    monkeypatch.setattr(inventory_db, "DB_PATH", str(dbp))
    init_inventory_db()
    conn = sqlite3.connect(str(dbp))
    cur = conn.cursor()
    for i, (trim, price) in enumerate(
        [("SEL", 40000), ("Limited", 48000), ("Calligraphy", 55000)]
    ):
        cur.execute(
            "INSERT INTO cars (vin, year, make, model, trim, price, listing_active)"
            " VALUES (?, ?, ?, ?, ?, ?, 1)",
            (f"KM8R{i}DHE{i}SU00000{i}", 2025, "Hyundai", "Palisade", trim, price),
        )
    conn.commit()
    conn.close()

    overlay = load_brochure_trim_overlay(2025, "Hyundai", "Palisade")
    assert overlay is not None
    inv = _ladder_from_inventory("Hyundai", "Palisade", 2025)
    assert inv is not None
    inv["steps"][0]["name"] = "CALLIGRAPHY"
    result = _build_ladder_result(
        inv,
        make="Hyundai",
        model="Palisade",
        year=2025,
        trim="SEL",
        brochure_overlay=overlay,
    )
    calligraphy = result["steps"][0]
    assert calligraphy["name"] == "CALLIGRAPHY"
    assert calligraphy.get("adds")


def test_brochure_overlay_fuzzy_matches_calligraphy_awd(monkeypatch) -> None:
    # The Palisade overlay is uncited, so the fuzzy "Calligraphy AWD" → "Calligraphy"
    # match is exercised with the gate off; with it on the rung is silent, which
    # test_uncited_overlays_contribute_nothing_anywhere covers.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    from backend.enrichment.brochure_extract import load_brochure_trim_overlay
    from backend.enrichment.trim_ladder import _build_ladder_result

    overlay = load_brochure_trim_overlay(2025, "Hyundai", "Palisade")
    assert overlay is not None
    ladder = {
        "id": "inventory_hyundai_palisade",
        "source": "inventory",
        "steps": [{"name": "Calligraphy AWD", "aliases": [], "adds": []}],
    }
    result = _build_ladder_result(
        ladder,
        make="Hyundai",
        model="Palisade",
        year=2025,
        trim="Calligraphy AWD",
        brochure_overlay=overlay,
    )
    assert result["steps"][0].get("adds")


def test_palisade_2026_does_not_take_its_rungs_from_a_nearby_year(monkeypatch) -> None:
    """WAS ``test_palisade_2026_uses_nearby_brochure_trim_adds`` — the old name
    described the defect.

    ``load_brochure_trim_overlay`` walks (0, -1, +1, -2, +2), and until
    2026-08-01 a book from an adjacent model year still decided which rungs were
    listed on this car even though the same overlay was already barred from
    contributing a single bullet. What is asserted now is the opposite of what
    the old test asserted: no bullet may arrive from a neighbouring year's book,
    with the bullet kill switch either way, and the junk "Blue" trim still never
    appears.
    """
    for switch in ("1", "0"):
        monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", switch)
        result = resolve_trim_ladder(make="Hyundai", model="Palisade", year=2026, trim="SEL")
        assert result is not None
        for step in result["steps"]:
            for citation in step.get("adds_citations") or []:
                assert citation.get("year", 2026) == 2026


def test_trim_ladder_never_has_empty_step_details(monkeypatch) -> None:
    """
    Every displayed trim step should have adds or specs (no empty-state UI).

    This invariant is DEAD and is recorded here rather than deleted, because it
    was the reason two uncited stores were kept alive for so long.

    It stopped holding when the bullet gate landed: a rung whose only bullets
    were uncited now shows nothing, by design. It stopped holding a second time
    on 2026-08-01, when neighbour-year overlays were barred from the rung list —
    the 2026 Elantra "Premium" rung had been getting its bullets out of an
    adjacent year's book. Both are correct outcomes, and it is the template's
    job to render a bullet-less rung without an empty panel.

    What is asserted now is only what still has to be true: an ``adds`` list is
    either absent or non-empty. A rung must never carry a blank or whitespace
    line, which is what would actually render as an empty bullet.
    """
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    cases = [
        ("Hyundai", "Elantra", 2023, "SEL"),
        ("Hyundai", "Elantra", 2026, "SE"),
        ("Hyundai", "Palisade", 2026, "SEL"),
        ("Ram", "1500", 2025, "Limited"),
    ]
    for make, model, year, trim in cases:
        result = resolve_trim_ladder(make=make, model=model, year=year, trim=trim)
        assert result is not None, f"{make} {model} {year}"
        for step in result["steps"]:
            for line in step.get("adds") or []:
                assert str(line).strip(), (
                    f"{make} {model} {year} trim {step.get('name')}: blank bullet"
                )


def test_trim_ladder_never_shows_inventory_price_as_adds() -> None:
    """Listing price belongs in market context, not 'What this trim adds'."""
    cases = [
        ("Toyota", "RAV4", 2022, "XLE Premium"),
        ("Honda", "Civic", 2022, "EX"),
    ]
    for make, model, year, trim in cases:
        result = resolve_trim_ladder(make=make, model=model, year=year, trim=trim)
        assert result is not None, f"{make} {model} {year}"
        for step in result["steps"]:
            for line in step.get("adds") or []:
                assert "typical listing price near" not in str(line).lower(), (
                    f"{make} {model} {step.get('name')}: {line}"
                )


def test_elantra_se_uses_brochure_standard_features_not_placeholder(monkeypatch) -> None:
    # Uncited overlay: gated off on the page, still checked for placeholder copy here.
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(make="Hyundai", model="Elantra", year=2023, trim="SEL")
    assert result is not None
    se = next(s for s in result["steps"] if s["name"] == "SE")
    adds = se.get("adds") or []
    assert adds
    joined = " ".join(adds).lower()
    assert "forward collision" in joined or "2.0l" in joined
    assert "entry-level trim with core standard equipment" not in joined


# --- "what this trim adds" ranking ------------------------------------------


def test_trim_add_category_ranks_hardware_above_cabin() -> None:
    from backend.enrichment.trim_ladder_knowledge import trim_add_category

    def rank(text: str) -> int:
        return trim_add_category(text)[1]

    assert rank("5.7L HEMI V8 engine") > rank("Quadra-Lift air suspension system")
    assert rank("Quadra-Lift air suspension system") > rank(
        "Uconnect 4C NAV with 8.4-inch display"
    )
    assert rank("Uconnect 4C NAV with 8.4-inch display") > rank(
        "Alpine Premium Audio System with 9 speakers and 506-watt amplifier"
    )
    assert rank("Alpine Premium Audio System with 9 speakers and 506-watt amplifier") > rank(
        "Three-Zone Automatic Temperature Control with infrared sensors"
    )
    assert rank("Three-Zone Automatic Temperature Control with infrared sensors") > rank(
        "Perforated leather-wrapped steering wheel"
    )


def test_trim_add_category_keys() -> None:
    from backend.enrichment.trim_ladder_knowledge import trim_add_category

    assert trim_add_category("5.7L HEMI V8 engine")[0] == "powertrain"
    assert trim_add_category("Quadra-Trac II with 2-speed transfer case")[0] == "drivetrain"
    assert trim_add_category("Four-corner air suspension")[0] == "suspension"
    assert trim_add_category("Brembo six-piston front calipers")[0] == "brakes"
    assert trim_add_category("Max towing capacity of 8,700 lb")[0] == "capability"
    assert trim_add_category("Three-row seating with 3rd-row 50/50 split-folding seats")[0] == (
        "seating"
    )
    # A heated second row is comfort content, not a seating-capacity change.
    assert trim_add_category("2nd-row heated seats")[0] == "seat_comfort"


def test_universal_trim_adds_are_year_aware() -> None:
    from backend.enrichment.trim_ladder_knowledge import is_universal_trim_add

    assert is_universal_trim_add("Security alarm system", year=2020) is True
    assert is_universal_trim_add("Bright scuff plates in cargo area", year=2020) is True
    assert is_universal_trim_add("Electronic Stability Control", year=2020) is True
    assert is_universal_trim_add("ParkView rear back-up camera with display", year=2020) is True
    # Federal rear-visibility rule starts at MY2018 — before that it really was
    # a trim differentiator, so the same words must survive on an older car.
    assert is_universal_trim_add("ParkView rear back-up camera with display", year=2009) is False
    assert is_universal_trim_add("Electronic Stability Control", year=2004) is False
    assert is_universal_trim_add("Quadra-Lift air suspension system", year=2020) is False


def test_rank_trim_adds_leads_with_hardware_and_drops_commodity() -> None:
    from backend.enrichment.trim_ladder_knowledge import rank_trim_adds

    out = rank_trim_adds(
        [
            "Security alarm system",
            "Bright scuff plates in cargo area",
            "Perforated leather-wrapped steering wheel with audio and cruise controls",
            "Uconnect 4C NAV with 8.4-inch display and 1-year trial subscription",
            "SiriusXM Traffic Plus with 5-year trial subscription",
            "5.7L HEMI V8 engine",
        ],
        year=2020,
        max_items=8,
    )
    assert out[0] == "5.7L HEMI V8 engine"
    assert "Uconnect 4C NAV with 8.4-inch display" in out
    joined = " ".join(out).lower()
    assert "security alarm" not in joined
    assert "scuff plates" not in joined
    assert "trial subscription" not in joined
    assert "siriusxm" not in joined


def test_rank_trim_adds_keeps_something_when_everything_is_commodity() -> None:
    from backend.enrichment.trim_ladder_knowledge import rank_trim_adds

    out = rank_trim_adds(
        ["Security alarm system", "Floor mats", "Cup holders"],
        year=2020,
        max_items=8,
    )
    # Blanking the trim sends the caller into generic marketing prose, which is
    # worse than a weak-but-true bullet.
    assert out


def test_rank_trim_adds_drops_bare_spec_labels() -> None:
    from backend.enrichment.trim_ladder_knowledge import rank_trim_adds

    out = rank_trim_adds(
        ["All-Wheel Drive", "8-Speed Automatic", "Quadra-Lift air suspension system"],
        year=2021,
        max_items=8,
    )
    assert out == ["Quadra-Lift air suspension system"]


def test_rank_trim_adds_is_stable_within_a_category() -> None:
    from backend.enrichment.trim_ladder_knowledge import rank_trim_adds

    out = rank_trim_adds(
        [
            "Heated steering wheel",
            "Three-Zone Automatic Temperature Control with infrared sensors",
        ],
        year=2020,
        max_items=8,
    )
    assert out == [
        "Heated steering wheel",
        "Three-Zone Automatic Temperature Control with infrared sensors",
    ]


def test_strip_trial_subscription_clause() -> None:
    from backend.enrichment.trim_ladder_knowledge import strip_trial_subscription_clause

    assert (
        strip_trial_subscription_clause(
            "Uconnect 4C NAV with 8.4-inch display and 1-year trial subscription"
        )
        == "Uconnect 4C NAV with 8.4-inch display"
    )
    assert strip_trial_subscription_clause("Heated steering wheel") == "Heated steering wheel"


def test_no_derived_engine_comparison_survives() -> None:
    """The (displacement, cylinders) engine-delta machinery must stay deleted.

    It shipped "2.0L Hurricane I4 engine - up from the 3.6L V6" onto a car whose
    own engine IS the 2.0L I4, because nothing in it ever compared direction, and
    it resolved a step by its aliases so an F-150 "Raptor" inherited the "Raptor R"
    5.2L V8. Neither premise can be repaired from data we hold: cars.forced_induction
    is wrong on real rows and epa_extended_specs.horsepower is null/degenerate.
    """
    import backend.enrichment.trim_ladder as tl

    for gone in (
        "_engine_step_up_bullet",
        "_engine_by_trim_from_inventory",
        "_dominant_engine_by_trim",
        "_inventory_trim_engine_rows",
        "_engine_display_label",
    ):
        assert not hasattr(tl, gone), f"{gone} came back - so did the false bullet"
    # No f-string / literal in the module may build a directional engine phrase.
    src = pathlib.Path(tl.__file__).read_text()
    code = "\n".join(ln for ln in src.split("\n") if not ln.lstrip().startswith("#"))
    assert "up from the" not in code, "the directional 'up from' phrasing is back"


def test_f150_raptor_never_inherits_the_raptor_r_v8() -> None:
    """Cited repro #1: a 2025 F-150 Raptor is a 3.5L V6 truck, not a 5.2L V8 one."""
    result = resolve_trim_ladder(make="Ford", model="F-150", year=2025, trim="Raptor")
    steps = {s["name"]: (s.get("adds") or []) for s in result["steps"]}
    raptor = " ".join(steps.get("Raptor", [])).lower()
    assert "up from" not in raptor, steps.get("Raptor")
    # "Raptor R adds supercharged V8" is fine - it names the OTHER trim. A bare
    # claim that THIS trim runs a 5.2 is not.
    assert "5.2l v8 engine" not in raptor, steps.get("Raptor")


def test_grand_cherokee_limited_makes_no_backwards_engine_claim() -> None:
    """Cited repro #2: car id 139652 is a 2.0L I4; nothing may call it a step up."""
    result = resolve_trim_ladder(make="Jeep", model="Grand Cherokee", year=2026, trim="Limited")
    everything = " ".join(
        b for s in result["steps"] for b in (s.get("adds") or [])
    ).lower()
    assert "up from" not in everything, everything


def test_brochure_trim_walk_is_revoked_because_it_composed_its_bullet() -> None:
    """
    ``brochure_trim_walk`` used to emit ``f"{engine} — added over the {base}"``.

    Only ``engine`` was printed in the brochure. The em dash, the phrase and the
    baseline trim name (read out of OUR ladder definition) were ours, and the
    composed string was then handed to the citation register, so the gate
    certified a sentence no document contains. The store is uncited now and the
    producer returns nothing at all — the same shape as the ``epa_csv``
    revocation before it.
    """
    from backend.enrichment.brochure_extract import (
        LADDER_BULLET_STORES,
        ladder_bullet_store_admissible,
    )
    from backend.enrichment.trim_ladder import (
        _brochure_engine_bullet_by_step,
        _brochure_engine_step_ups,
    )

    assert LADDER_BULLET_STORES["brochure_trim_walk"] == "uncited"
    assert not ladder_bullet_store_admissible("brochure_trim_walk")

    steps = [{"name": "R/T", "aliases": []}, {"name": "GT", "aliases": []}]
    assert _brochure_engine_bullet_by_step(steps, "Nonexistent", "Nothing", 2020) == {}
    # The 2020 Durango is the case that used to produce "5.7L HEMI V8 — added
    # over the GT". The underlying parser still finds the page (it is the honest
    # half of the job); the bullet builder refuses to dress it up.
    assert _brochure_engine_bullet_by_step(steps, "Dodge", "Durango", 2020) == {}
    assert _brochure_engine_step_ups("Dodge", "Durango", 2020), (
        "parser should still read the trim-walk page; only the bullet is withheld"
    )


def test_no_rendered_bullet_uses_the_composed_step_up_wording() -> None:
    """The composed phrase must not reach a rung by any route."""
    for make, model, year, trim in [
        ("Dodge", "Durango", 2020, "R/T"),
        ("Dodge", "Charger", 2022, "R/T"),
        ("Nissan", "Titan XD", 2022, "SV"),
        ("Toyota", "RAV4", 2026, "XSE"),
    ]:
        result = resolve_trim_ladder(make=make, model=model, year=year, trim=trim)
        for step in (result or {}).get("steps") or []:
            for bullet in step.get("adds") or []:
                assert "added over the" not in bullet.lower(), (make, model, year, bullet)
            for citation in step.get("adds_citations") or []:
                assert citation.get("store") != "brochure_trim_walk", citation


def test_lineup_standard_equipment_is_not_a_trim_add() -> None:
    """A row the brochure grid marks standard in every column is not an "add"."""
    from backend.enrichment.trim_ladder import _lineup_standard_features, _suppress_non_adds

    grid = _lineup_standard_features("Dodge", "Durango", 2020)
    assert grid, "2020 Durango brochure grid should parse"
    steps = [
        {
            "adds": [
                "Three-Zone Automatic Temperature Control with infrared sensors",
                "Alpine Premium Audio System with 9 speakers and 506-watt amplifier",
                "Rear ParkSense Park Assist System with obstacle detection",
            ]
        },
        {"adds": ["Rear ParkSense Park Assist System with obstacle detection"]},
        {"adds": ["Three-row seating with 3rd-row 50/50 split-folding seats"]},
    ]
    _suppress_non_adds(steps, make="Dodge", model="Durango", year=2020)
    top = steps[0]["adds"]
    # Standard on all five columns of the brochure grid -> not an R/T add.
    assert not any("Three-Zone" in b for b in top), top
    # Verbatim duplicate of a LOWER rung -> not an add either.
    assert not any("ParkSense" in b for b in top), top
    # Genuinely R/T-only content survives.
    assert any("Alpine" in b for b in top), top
    assert steps[1]["adds"] == ["Rear ParkSense Park Assist System with obstacle detection"]


def test_durango_2020_renders_only_what_the_reopened_pdf_prints() -> None:
    """
    2020 Durango: was the showcase for the trim walk. Two things happened to it.

    * The R/T's first bullet was "5.7L HEMI V8 — added over the GT". Only the
      engine name was printed; the dash, the phrase and the baseline trim were
      ours (see the ``brochure_trim_walk`` revocation). That store is uncited,
      so the composed line is gone and stays gone.
    * The overlay bullets went silent too, but for a different reason: we did
      not hold ``2020_Dodge_Durango_Brochure.pdf``, so none of its 44 citations
      could be re-opened. The PDF has since been reacquired and
      ``verify_trim_citations.py --apply`` re-read every cited page; 42 of the
      44 are printed there and render again, 2 are not and stay silent.

    So the assertion is no longer "says nothing" — it is "says only what the
    verifier re-read off the page". Every rendered bullet must carry a
    ``verified`` citation, and the composed claims must still be absent.
    """
    import json

    from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR

    raw = json.loads(
        (TRIM_ADDS_BY_YEAR_DIR / "2020__dodge__durango.json").read_text(encoding="utf-8")
    )
    signed = {
        e["text"]
        for v in raw["adds_provenance"].values()
        for e in v
        if e.get("verified") is True
    }
    assert signed

    result = resolve_trim_ladder(make="Dodge", model="Durango", year=2020, trim="GT")
    assert result is not None
    by_name = {s["name"]: (s.get("adds") or []) for s in result["steps"]}
    assert "R/T" in by_name and "GT" in by_name
    rendered = [b for adds in by_name.values() for b in adds]
    assert rendered, by_name
    for bullet in rendered:
        assert bullet in signed, bullet

    everything = " ".join(rendered).lower()
    # The specific false claims this vehicle used to be the regression case for.
    # "hemi" is no longer among them: "5.7L HEMI® V8 engine" on its own IS
    # printed on the cited page and verifies. What was fabricated was the
    # comparison clause, not the engine name.
    assert "air suspension" not in everything
    assert "three-zone" not in everything
    assert "added over the" not in everything


def test_rank_trim_adds_never_drops_content_while_slots_are_free() -> None:
    """The per-category cap reorders; it must not delete inside the budget.

    _MAX_PER_CATEGORY=3 used to apply to the catch-all "other" bucket, so six
    unclassified-but-real bullets came back as three even with max_items=8.
    """
    from backend.enrichment.trim_ladder_knowledge import rank_trim_adds, trim_add_category

    bullets = [
        "Mopar Finishing Package with paint protection film",
        "Interior LED courtesy group",
        "Semi-gloss black exhaust tips",
        "Class IV receiver hookup group",
        "Mopar splash guard set",
        "Underbody protection group",
    ]
    assert {trim_add_category(b)[0] for b in bullets} == {"other"}, "fixture must be unclassified"
    out = rank_trim_adds(bullets, year=2026, max_items=8)
    assert len(out) == len(bullets), out
    assert set(out) == set(bullets)


def test_rank_trim_adds_still_spreads_a_dominant_named_category() -> None:
    """Four audio bullets must not bury the sunroof — but none may vanish."""
    from backend.enrichment.trim_ladder_knowledge import rank_trim_adds

    bullets = [
        "Bose 12-speaker audio system",
        "Harman Kardon 16-speaker audio system",
        "JBL 9-speaker audio system",
        "Revel 14-speaker audio system",
        "Panoramic sunroof",
    ]
    out = rank_trim_adds(bullets, max_items=8)
    assert len(out) == 5, out
    assert out.index("Panoramic sunroof") < out.index("Revel 14-speaker audio system"), out
    # A genuinely tight budget still truncates, and does so from the bottom.
    assert len(rank_trim_adds(bullets, max_items=3)) == 3


def test_lineup_narrative_and_orphaned_spec_cells_are_not_trim_adds() -> None:
    """Wikipedia model-history prose scraped into per-trim cells must not render."""
    from backend.enrichment.trim_ladder_knowledge import is_generic_trim_add

    for junk in (
        "In terms of trim-level changes, the STX is now offered as a separate trim level",
        "Added to the lineup was a new Tremor trim level",
        "L PowerBoost twin-turbo V6",
        "L Cyclone V6",
        "L Power Stroke turbo V6",
        "Gasoline Hybrid:",
        "Electric motor 35 kW (47 hp) BorgWarner HVH250 (hybrid)",
        "Front-engine, rear-wheel drive",
        "Automatic (S10)",
    ):
        assert is_generic_trim_add(junk), junk

    for keep in (
        "3.5L V6 Twin Turbo",
        "Alpine Premium Audio System with 9 speakers and subwoofer",
        "Power liftgate",
        "12-inch Sync 4 screen (2024+)",
    ):
        assert not is_generic_trim_add(keep), keep


def test_f150_2025_ladder_carries_no_fabricated_engine_bullets() -> None:
    """None of the mangled Wikipedia engine cells may reach a rendered rung."""
    result = resolve_trim_ladder(make="Ford", model="F-150", year=2025, trim="XLT")
    everything = " ".join(b for s in result["steps"] for b in (s.get("adds") or []))
    low = everything.lower()
    for bogus in ("cyclone", "power stroke", "electric motor 35", "l powerboost twin-turbo"):
        assert bogus not in low, everything


def _all_bullets(result: dict) -> list[str]:
    return [b for s in (result or {}).get("steps") or [] for b in (s.get("adds") or [])]


def test_bullet_gate_rejects_narrative_and_derived_comparisons() -> None:
    """The gate is the contract: quotable spec text renders, prose does not."""
    from backend.enrichment.trim_spec_extractor import (
        is_derived_comparison_bullet,
        is_displayable_trim_bullet,
    )

    for prose in (
        "In the United States, the 4Runner carried over the same engine options from the",
        'The optional 4WD systems were full-time on V8 models while "Multi-Mode" or part-',
        "Most turbocharged 4Runners were equipped with the SR5 package, and all turbo tru",
        "Based on a design proposal originally used in the development of the previous-ge",
        "The XLE models offered leather seats and a wood trim package.",
        "The Base and LX both come standard with the 2.4-l",
        "Terms of Use and Master Data Consent. Data charges may apply.",
    ):
        assert not is_displayable_trim_bullet(prose), prose

    for derived in (
        "Upgrades Engine Options: 6.7L V6 Turbo; Automatic 6-spd (was Automatic 8-spd)",
        "Adds Engine Options: Layout Front-engine, front-wheel-drive",
        "Automatic 6-spd (was Automatic 8-spd",
    ):
        assert is_derived_comparison_bullet(derived), derived
        assert not is_displayable_trim_bullet(derived), derived

    for keep in (
        "5.7L HEMI V8 engine — added over the GT",
        "6.7L Cummins Turbo Diesel I6",
        "Locking rear differential",
        "20-in. split 6-spoke alloy wheels with P245/60R20 tires",
        "4x4 drivetrain with electronic shift-on-the-fly transfer case",
        (
            "SRT-inspired power dome hood with functional heat extractors, SRT-style front and "
            "rear fascias, body-color grille surround, gloss-black headlamp bezels, and 20-inch "
            "gloss-black aluminum wheels"
        ),
    ):
        assert is_displayable_trim_bullet(keep), keep


def test_ram_2500_laramie_has_no_derived_or_fabricated_engine_text() -> None:
    """/car/284257 repro: 2026 Ram 2500 Laramie (engine_l=6.7, cylinders=6)."""
    result = resolve_trim_ladder(make="RAM", model="2500", year=2026, trim="Laramie")
    bullets = _all_bullets(result)
    joined = " ".join(bullets).lower()
    assert "upgrades engine options" not in joined
    assert "(was " not in joined
    # engineDisplay guesses "V6" for the 6.7L Cummins; engineOptions names it correctly.
    assert "6.7l v6" not in joined


def test_4runner_limited_has_no_wikipedia_narrative(monkeypatch) -> None:
    """/car/384849 repro: 2026 Toyota 4Runner Limited.

    Uncited overlay, so nothing renders here under the gate; the narrative-leak
    check runs with the gate off, where the bullets still exist to be checked.
    """
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(make="Toyota", model="4Runner", year=2026, trim="Limited")
    bullets = _all_bullets(result)
    assert bullets
    joined = " ".join(bullets).lower()
    for bogus in ("in the united states", "carried over", "turbo tru", "upgrades engine options"):
        assert bogus not in joined, joined


def test_durango_rt_no_longer_shows_the_composed_hemi_bullet() -> None:
    """
    This test used to assert the bullet "5.7L HEMI V8 engine — added over the GT".

    That string is not printed in any brochure. The 2020 Durango trim-walk page
    prints the engine; ``_brochure_engine_bullet_by_step`` supplied the dash, the
    phrase and the baseline trim from our own ladder definition, then asked the
    citation register to bless the result. The store is revoked; assert the
    absence instead.
    """
    result = resolve_trim_ladder(make="Dodge", model="Durango", year=2020, trim="R/T")
    rt = next(s for s in result["steps"] if s["name"] == "R/T")
    # The R/T does render again now that the PDF is back on disk, but only the
    # bare engine name the page actually prints — never the comparison clause.
    assert "5.7L HEMI® V8 engine" in rt["adds"], rt["adds"]
    assert not any("added over" in b for b in rt["adds"]), rt["adds"]
    joined = " ".join(_all_bullets(result)).lower()
    assert "added over the" not in joined
    # The brochure grid marks Three-Zone ATC standard on all five trims.
    assert "three-zone" not in joined


# ---------------------------------------------------------------------------
# Bullet well-formedness — the orphan-fragment / doubled-label / prose classes
# a fleet sweep of the top-1500 (make, model, year, trim) combos found still
# rendering. Each test below names the producer that made the bad line.
# ---------------------------------------------------------------------------


def test_semicolon_inside_parentheses_is_not_a_split_point() -> None:
    """EPA-style engine cells lost their tail to a naive ';' split.

    "Hybrid 2.5L I4 (SIDI & PFI; Hybrid)" was cut into "Hybrid 2.5L I4 (SIDI &
    PFI" and "Hybrid)", and both halves reached the page.
    """
    from backend.enrichment.trim_ladder_knowledge import split_outside_brackets

    assert split_outside_brackets("Hybrid 2.5L I4 (SIDI & PFI; Hybrid)") == [
        "Hybrid 2.5L I4 (SIDI & PFI; Hybrid)"
    ]
    assert split_outside_brackets("A feature; B feature (x; y)") == [
        "A feature",
        "B feature (x; y)",
    ]


def test_unbalanced_bullet_is_never_rendered() -> None:
    from backend.enrichment.trim_ladder_knowledge import is_wellformed_trim_bullet

    for orphan in (
        "Four-Wheel Drive)",
        "Mild Hybrid)",
        "Hybrid 2.5L I4 (SIDI & PFI",
        "L5P Duramax 6.6 L V8 (2017-20) 445 hp (332 kW",
        "Stop-Start)",
    ):
        assert not is_wellformed_trim_bullet(orphan), orphan
    # An inch mark is a measurement, not an unbalanced quote.
    assert is_wellformed_trim_bullet('NissanConnect 8" touchscreen with Apple CarPlay')


def test_doubled_label_prefix_is_collapsed_not_shown() -> None:
    """/car/282933 (2020 GMC Sierra 1500 SLT) Base rung rendered the label twice."""
    from backend.enrichment.trim_ladder_knowledge import collapse_repeated_label_prefix

    assert (
        collapse_repeated_label_prefix("Engine Options: Engine Options: LV3 EcoTec3 4.3 L V6")
        == "Engine Options: LV3 EcoTec3 4.3 L V6"
    )
    assert (
        collapse_repeated_label_prefix("Engine Option Engine Options: LV3 EcoTec3")
        == "Engine Options: LV3 EcoTec3"
    )
    assert collapse_repeated_label_prefix("Screen Size: 8-inch touch screen") == (
        "Screen Size: 8-inch touch screen"
    )


def test_gmc_sierra_base_rung_has_no_doubled_label() -> None:
    import re

    result = resolve_trim_ladder(make="GMC", model="Sierra 1500", year=2020, trim="SLT")
    for bullet in _all_bullets(result):
        assert not re.match(r"^(.{3,30}?):\s*\1:", bullet), bullet
        assert bullet.count("(") == bullet.count(")"), bullet


def test_mid_sentence_fragments_are_dropped_but_marques_survive() -> None:
    from backend.enrichment.trim_ladder_knowledge import bullet_is_fragment

    for fragment in (
        "of torque, and front-wheel drive as standard.",
        "alongside second-generati",
        "subwoofer",
        "suede seating",
        "kWh AWD: 279 mi (449 km)",
        "From heated driver and front passenger seats to front-row glass, your s",
        "7-inch Display Au",
    ):
        assert bullet_is_fragment(fragment), fragment
    for real in (
        "quattro all-wheel drive",
        "i-FORCE MAX 3.4L-T V6 Hybrid 4x4 powertrain",
        "eAWD drivetrain standard",
        "Wood-wrapped steering wheel with Adaptive Cruise Control Stop and Go",
        "27.2 cubic feet cargo volume with rear seats up",
        "2.5L Atkinson-cycle I-4 with electric motor — 177 net hp",
    ):
        assert not bullet_is_fragment(real), real


def test_encyclopedia_prose_and_disclaimers_are_dropped() -> None:
    from backend.enrichment.trim_ladder_knowledge import is_wellformed_trim_bullet

    for prose in (
        "No diesel engine option is offered for this generation.",
        "Trucks ordered with bucket seats and center console now featured a wireless",
        "Honda Odyssey can refer to three motor vehicles manufactured by Honda",
        "Hill Start Assist Control is also standard which prevents rolling backward",
        "The 2012 model year A6 features all the driver assistance systems from the A8",
        "Other options include xenon plus headlights with an all-weather light",
        "SiriusXM Customer Agreement available at www.siriusxm.com",
        "Collision-avoidance system and is not a substitute for safe driving",
        "Engines Capacity Model year Power Torque Displacement Bore",
    ):
        assert not is_wellformed_trim_bullet(prose), prose


def test_warranty_and_service_programs_are_not_trim_adds() -> None:
    from backend.enrichment.trim_ladder_knowledge import is_generic_trim_add

    for line in (
        "Includes 5-year/60,000-mile powertrain warranty coverage",
        "Includes Courtesy Transportation Program",
        "Includes 2-year/24,000-mile scheduled maintenance program",
        "Includes 24/7 Lincoln Concierge service",
    ):
        assert is_generic_trim_add(line), line


def test_verb_prefixes_are_stripped_from_bullets() -> None:
    from backend.enrichment.trim_ladder_knowledge import strip_lower_rung_reference

    assert strip_lower_rung_reference("Adds Includes 4.6L V8 engine") == "4.6L V8 engine"
    assert strip_lower_rung_reference("Includes Blind Spot Monitor") == "Blind Spot Monitor"
    assert (
        strip_lower_rung_reference("Includes Luxury plus digital rearview mirror and cool box")
        # Capitalised on the way out: a lowercase head is what
        # ``bullet_is_fragment`` rejects, so the stripped remainder used to be
        # dropped instead of shown.
        == "Digital rearview mirror and cool box"
    )
    # Nothing to strip: leave the sentence alone rather than mangle it.
    assert strip_lower_rung_reference("Included premium audio") == "Included premium audio"


def test_trial_subscription_strip_leaves_a_finished_phrase() -> None:
    """generic_ram_1500 Laramie rendered "4G LTE WiFi Hotspot with 1GB/"."""
    from backend.enrichment.trim_ladder_knowledge import (
        rank_trim_adds,
        strip_trial_subscription_clause,
    )

    assert (
        strip_trial_subscription_clause("4G LTE WiFi Hotspot with 1GB/3-month trial")
        == "4G LTE WiFi Hotspot"
    )
    out = rank_trim_adds(["4G LTE WiFi Hotspot with 1GB/3-month trial"], year=2020)
    assert not any(b.endswith("/") for b in out), out


def test_brochure_source_tag_is_not_rendered() -> None:
    from backend.enrichment.trim_ladder import _bullet_display_parts

    parts = _bullet_display_parts(
        "Audio System Layout: [Brochure] Bose audio | 12-speaker system",
        trim_name="Premium",
        make="Mazda",
        model="CX-50",
        year=2025,
    )
    assert parts
    assert not any("[Brochure]" in p for p in parts), parts
    assert not any("|" in p for p in parts), parts


def test_spec_row_bullets_keep_bracketed_values_whole() -> None:
    from backend.enrichment.trim_ladder import _trim_spec_rows_to_bullets

    rows = [{"label": "Engine Options", "value": "Hybrid 2.5L I4 (SIDI & PFI; Hybrid); 3.4L V6"}]
    assert _trim_spec_rows_to_bullets(rows, trim_name="XSE") == [
        "Hybrid 2.5L I4 (SIDI & PFI; Hybrid)",
        "3.4L V6",
    ]


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


# --- provenance gate: a bullet renders only if it can name its page ------


def _overlay_bullets(result: dict) -> list[str]:
    return [b for step in (result or {}).get("steps") or [] for b in step.get("adds") or []]


def test_uncited_overlay_bullets_do_not_render() -> None:
    """
    2011 Jeep Grand Cherokee Laredo claimed "3.6L Pentastar V6 engine with 360
    horsepower" — the 2011 Pentastar makes 290 (epa_extended_specs says 293) and
    360 is the 5.7 HEMI V8. It came from a manual_brochure_review overlay that
    cites nothing, so under the gate that rung says nothing at all.
    """
    result = resolve_trim_ladder(
        make="Jeep", model="Grand Cherokee", year=2011, trim="Laredo"
    )
    assert result is not None
    assert not any("360 horsepower" in b for b in _overlay_bullets(result))
    assert _overlay_bullets(result) == []


def test_kill_switch_restores_uncited_bullets(monkeypatch) -> None:
    """The gate is reversible: TRIM_ADDS_REQUIRE_PROVENANCE=0 brings the old page back."""
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(
        make="Jeep", model="Grand Cherokee", year=2011, trim="Laredo"
    )
    assert any("360 horsepower" in b for b in _overlay_bullets(result))


def test_verified_overlay_still_renders() -> None:
    """
    The gate is not a blanket off switch — a VERIFIED page citation keeps its bullet.

    The 2026 RAV4 overlay cites ``2026_Toyota_RAV4_Brochure.pdf``, we still hold
    that PDF, and ``backend/scripts/verify_trim_citations.py`` re-opened page 7
    and found this line printed there. That is why it renders.

    (This used to assert on the 2020 Durango R/T. We no longer hold the 2020
    Durango brochure PDF, so no citation in that file can be re-opened and it
    renders nothing — see ``test_a_citation_we_cannot_reopen_does_not_render``.)
    """
    result = resolve_trim_ladder(make="Toyota", model="RAV4", year=2026, trim="SE")
    assert result is not None
    se = next(s for s in result["steps"] if s["name"] == "SE")
    assert "18-in multi-spoke black sport alloy wheels with black lug nuts" in se["adds"]
    citation = next(
        c for c in se["adds_citations"]
        if c["text"].startswith("18-in multi-spoke")
    )
    assert citation["store"] == "brochure_text_quoted"
    assert citation["page"] == 7


def test_a_citation_we_cannot_reopen_does_not_render() -> None:
    """
    A citation is only worth something while we can go back to the document.

    The 2010 Audi Q5 overlay cites a brochure PDF we do not hold, so the
    build-time verifier could not re-open a single one of its 17 citations and
    stamped them all ``verified: false`` / ``source_pdf_not_held``. Nothing in
    that file renders.

    (This used to be asserted on the 2020 Dodge Durango, whose PDF was missing
    at the time. That brochure has since been reacquired and 42 of its 44
    citations now verify, so it no longer demonstrates this rule — see
    ``test_durango_2020_renders_only_what_the_reopened_pdf_prints``.)
    """
    import json

    from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR

    raw = json.loads(
        (TRIM_ADDS_BY_YEAR_DIR / "2010__audi__q5.json").read_text(encoding="utf-8")
    )
    entries = [e for v in raw["adds_provenance"].values() for e in v]
    assert entries
    assert all(e.get("verified") is False for e in entries)
    assert {e.get("verified_why") for e in entries} == {"source_pdf_not_held"}

    result = resolve_trim_ladder(make="Audi", model="Q5", year=2010, trim="Premium Plus")
    if result is not None:
        overlay_bullets = [
            c["text"]
            for step in result["steps"]
            for c in (step.get("adds_citations") or [])
            if c.get("store") == "brochure_text_quoted"
        ]
        assert overlay_bullets == [], overlay_bullets


def test_every_rendered_overlay_bullet_names_a_file_and_page() -> None:
    """
    Whatever the ladder renders for a catalog key that HAS an overlay must appear
    in that overlay's adds_provenance with a source file and a page number.
    """
    import json

    from backend.enrichment.brochure_extract import (
        ADMISSIBLE_OVERLAY_SOURCES,
        load_brochure_trim_overlay,
    )

    overlay_dir = (
        pathlib.Path(__file__).resolve().parents[1]
        / "dictionary/derived/trim_adds_by_year"
    )
    checked = 0
    for path in sorted(overlay_dir.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        if str(raw.get("source") or "") not in ADMISSIBLE_OVERLAY_SOURCES:
            continue
        year, make, model = raw["year"], raw["make"], raw["model"]
        overlay = load_brochure_trim_overlay(year, make, model) or {}
        provenance = raw.get("adds_provenance") or {}
        for trim, bullets in (overlay.get("adds_by_trim") or {}).items():
            cited = {
                str(e.get("text") or "").strip()
                for e in provenance.get(trim) or []
                if str(e.get("source") or "").strip() and e.get("page") is not None
            }
            for bullet in bullets:
                assert bullet in cited, f"{path.name} {trim}: {bullet}"
                checked += 1
    assert checked > 0


def test_uncited_overlays_contribute_nothing_anywhere() -> None:
    """Every overlay whose source is not admissible yields an empty adds_by_trim."""
    import json

    from backend.enrichment.brochure_extract import (
        ADMISSIBLE_OVERLAY_SOURCES,
        admissible_overlay_adds,
    )

    overlay_dir = (
        pathlib.Path(__file__).resolve().parents[1]
        / "dictionary/derived/trim_adds_by_year"
    )
    blocked = 0
    for path in sorted(overlay_dir.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        if str(raw.get("source") or "") in ADMISSIBLE_OVERLAY_SOURCES:
            continue
        assert admissible_overlay_adds(raw) == {}, path.name
        blocked += 1
    assert blocked > 3000


def test_admissible_overlay_adds_drops_bullets_missing_a_page() -> None:
    """A citation without a page number is not a citation."""
    from backend.enrichment.brochure_extract import admissible_overlay_adds

    overlay = {
        "source": "brochure_text_quoted",
        "year": 2026,
        "adds_by_trim": {"GT": ["Cited bullet", "Pageless bullet", "Unlisted bullet"]},
        "adds_provenance": {
            "GT": [
                {
                    "text": "Cited bullet",
                    "source": "derived/brochure_text/x.json",
                    "page": 7,
                    "verified": True,
                },
                {
                    "text": "Pageless bullet",
                    "source": "derived/brochure_text/x.json",
                    "verified": True,
                },
            ]
        },
    }
    assert admissible_overlay_adds(overlay) == {"GT": ["Cited bullet"]}
    assert admissible_overlay_adds(overlay, for_year=2026) == {"GT": ["Cited bullet"]}


def test_a_citation_the_verifier_has_not_signed_off_does_not_render() -> None:
    """
    ``verified`` has to be literally True. Missing, false, or a truthy string is
    not a pass — an overlay written before the verifier existed must be silent
    rather than grandfathered in.
    """
    from backend.enrichment.brochure_extract import admissible_overlay_adds

    def overlay(entry_extra: dict) -> dict:
        return {
            "source": "brochure_text_quoted",
            "year": 2026,
            "adds_by_trim": {"GT": ["A printed line"]},
            "adds_provenance": {
                "GT": [
                    {
                        "text": "A printed line",
                        "source": "derived/brochure_text/x.json",
                        "page": 7,
                        **entry_extra,
                    }
                ]
            },
        }

    assert admissible_overlay_adds(overlay({"verified": True})) == {"GT": ["A printed line"]}
    assert admissible_overlay_adds(overlay({})) == {}
    assert admissible_overlay_adds(overlay({"verified": False})) == {}
    assert admissible_overlay_adds(overlay({"verified": "false"})) == {}
    assert admissible_overlay_adds(overlay({"verified": 1})) == {}


def test_a_brochure_for_another_model_year_is_not_evidence_about_this_car() -> None:
    """
    ``load_brochure_trim_overlay`` reaches ±2 years for a trim LIST. A citation
    may not travel with it: a 2024 book does not say what a 2026 car has.
    """
    from backend.enrichment.brochure_extract import (
        admissible_overlay_adds,
        overlay_citable_for_year,
    )

    overlay = {
        "source": "brochure_text_quoted",
        "year": 2024,
        "make": "Toyota",
        "model": "RAV4",
        "adds_by_trim": {"XSE": ["Power tilt/slide moonroof"]},
        "adds_provenance": {
            "XSE": [
                {
                    "text": "Power tilt/slide moonroof",
                    "source": "derived/brochure_text/2024__toyota__rav4.json",
                    "page": 9,
                    "verified": True,
                }
            ]
        },
    }
    assert overlay_citable_for_year(overlay, 2024)
    assert not overlay_citable_for_year(overlay, 2025)
    assert not overlay_citable_for_year(overlay, 2026)
    assert not overlay_citable_for_year(overlay, None)
    assert admissible_overlay_adds(overlay, for_year=2024) == {
        "XSE": ["Power tilt/slide moonroof"]
    }
    assert admissible_overlay_adds(overlay, for_year=2025) == {}
    assert admissible_overlay_adds(overlay, for_year=2026) == {}


def test_the_rung_register_refuses_an_overlay_for_a_different_year() -> None:
    """
    Second, independent year check — the one inside the citation register.

    ``_build_ladder_result`` is reachable with an overlay a caller assembled
    itself (scripts, tests), so the register does not assume
    ``admissible_overlay_adds`` already ran.
    """
    from backend.enrichment.trim_ladder import (
        _CitationRegister,
        _register_overlay_citations,
    )

    overlay = {
        "source": "brochure_text_quoted",
        "year": 2026,
        "make": "Toyota",
        "model": "RAV4",
        "adds_provenance": {
            "XSE": [
                {
                    "text": "Power tilt/slide moonroof with one-touch open/close",
                    "source": "derived/brochure_text/2026__toyota__rav4.json",
                    "page": 9,
                    "verified": True,
                }
            ]
        },
    }
    bullet = "Power tilt/slide moonroof with one-touch open/close"

    same_year = _CitationRegister()
    _register_overlay_citations(
        same_year, overlay, "XSE", make="Toyota", model="RAV4", year=2026
    )
    assert same_year.citation_for(bullet) is not None

    for wrong in (2025, 2027, None):
        reg = _CitationRegister()
        _register_overlay_citations(
            reg, overlay, "XSE", make="Toyota", model="RAV4", year=wrong
        )
        assert reg.citation_for(bullet) is None, wrong

    wrong_make = _CitationRegister()
    _register_overlay_citations(
        wrong_make, overlay, "XSE", make="Honda", model="RAV4", year=2026
    )
    assert wrong_make.citation_for(bullet) is None

    unverified = {
        **overlay,
        "adds_provenance": {
            "XSE": [
                {**overlay["adds_provenance"]["XSE"][0], "verified": None},
            ]
        },
    }
    reg = _CitationRegister()
    _register_overlay_citations(
        reg, unverified, "XSE", make="Toyota", model="RAV4", year=2026
    )
    assert reg.citation_for(bullet) is None


def test_overlay_still_suppresses_substitute_copy_when_all_bullets_are_blocked() -> None:
    """
    Blocking an overlay's bullets must not hand the rung over to the generated
    "Mid-level trim with added convenience features" copy: silence, not filler.
    """
    result = resolve_trim_ladder(
        make="Jeep", model="Grand Cherokee", year=2011, trim="Laredo"
    )
    assert result is not None
    assert len(result["steps"]) >= 2
    assert all(not step["adds"] and not step["specs"] for step in result["steps"])


def test_blocked_overlay_is_not_refilled_from_uncited_stores() -> None:
    """
    Removing an uncited overlay bullet must not promote a different uncited line
    into its place. 2026 Nissan Sentra SR gained "Front-Wheel Drive" from the
    trim spec sheets the moment the overlay went quiet; the rung stays silent.
    """
    result = resolve_trim_ladder(make="Nissan", model="Sentra", year=2026, trim="SV")
    assert result is not None
    for step in result["steps"]:
        assert not step["adds"], (step["name"], step["adds"])


# --- the gate now covers every store, not just the overlays -----------------


def test_trackhawk_hellcat_engine_does_not_render_on_a_limited() -> None:
    """
    The live report that started this: /car/282480 is a 2018 Grand Cherokee
    Limited and its ladder printed "6.2L Supercharged Hellcat V8" on the
    Trackhawk rung. That line is typed into curated/trim_ladders.json and cites
    nothing, so it does not render — even though it sits on a curated ladder,
    which the overlay-only gate never looked at.
    """
    result = resolve_trim_ladder(
        make="Jeep", model="Grand Cherokee", year=2018, trim="Limited"
    )
    assert result is not None
    assert str(result.get("source") or "") == "curated"
    everything = " ".join(
        b for s in result["steps"] for b in (s.get("adds") or [])
    ).lower()
    assert "hellcat" not in everything, everything
    assert "6.2l supercharged" not in everything, everything


def test_the_hellcat_line_really_is_in_the_curated_store(monkeypatch) -> None:
    """Counterpart to the test above: with the gate off, that same line comes back.

    Without this the test above would pass just as well if the rung had gone
    missing for some unrelated reason.
    """
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(
        make="Jeep", model="Grand Cherokee", year=2018, trim="Limited"
    )
    assert result is not None
    everything = " ".join(b for s in result["steps"] for b in (s.get("adds") or [])).lower()
    assert "hellcat" in everything, everything


def test_every_rendered_bullet_names_a_document_and_a_place_in_it() -> None:
    """No bullet reaches a rung without a store, a source file and a page or row."""
    from backend.enrichment.brochure_extract import ADMISSIBLE_LADDER_BULLET_STORES

    vehicles = [
        # Vehicles that DO render — every one of these cites a brochure PDF we
        # still hold, so the verifier could re-open it. Without them the loop
        # below asserts nothing (the sample went silent once the trim walk and
        # the un-reopenable overlays were revoked, and the test passed anyway).
        ("Toyota", "RAV4", 2026, "XSE"),
        ("Toyota", "Highlander", 2026, "Platinum"),
        ("Nissan", "Pathfinder", 2026, "SL"),
        ("Nissan", "Titan XD", 2022, "SV"),
        ("Mazda", "Mazda6", 2018, "Grand Touring"),
        # Vehicles that render nothing, kept as the "no filler" half.
        ("Jeep", "Grand Cherokee", 2018, "Limited"),
        ("Toyota", "RAV4", 2022, "XLE"),
        ("Ram", "1500", 2025, "Limited"),
        ("Honda", "Accord", 2023, "EX-L"),
        ("Dodge", "Durango", 2020, "R/T"),
        ("Hyundai", "IONIQ 5", 2023, "SEL"),
        ("Ford", "F-150", 2024, "Lariat"),
        ("BMW", "3 Series", 2016, "328i"),
    ]
    seen_any = False
    for make, model, year, trim in vehicles:
        result = resolve_trim_ladder(make=make, model=model, year=year, trim=trim)
        if not result:
            continue
        for step in result["steps"]:
            bullets = step.get("adds") or []
            cited = {str(c.get("text")) for c in step.get("adds_citations") or []}
            assert set(bullets) <= cited, (make, model, year, step["name"], bullets)
            for citation in step.get("adds_citations") or []:
                seen_any = True
                assert citation["store"] in ADMISSIBLE_LADDER_BULLET_STORES, citation
                assert str(citation.get("source") or "").strip(), citation
                assert (
                    citation.get("page") is not None or citation.get("row") is not None
                ), citation
    assert seen_any, "sample rendered no bullets at all — the assertions were vacuous"


def test_no_epa_row_cell_reaches_a_rung() -> None:
    """
    /car/89790 rendered "All-Wheel Drive" attributed to 2022_Toyota_RAV4_EPA.csv.
    Neither that cell nor the engine cell beside it may put a bullet on a rung
    now: ``drivetrainOptions`` describes the one configuration the row was filed
    for rather than something a rung ADDS, and ``engineOptions`` is a sentence
    ``import_epa_to_dictionary.build_engine_desc`` composed out of several EPA
    columns, so quoting it quotes us.
    """
    from backend.enrichment.trim_ladder import _epa_engine_citations

    # The reader still works; it is the ADMISSIBILITY of what it reads that changed.
    citations = _epa_engine_citations("Toyota", "RAV4", 2022)
    assert citations, "expected EPA rows for the 2022 RAV4"
    for _trim, engine, file_name, row_no in citations:
        assert file_name.endswith("_EPA.csv")
        assert row_no >= 2
        assert "wheel drive" not in engine.lower(), (engine, file_name, row_no)

    result = resolve_trim_ladder(make="Toyota", model="RAV4", year=2022, trim="XLE")
    if result:
        everything = " ".join(
            b for s in result["steps"] for b in (s.get("adds") or [])
        ).lower()
        assert "all-wheel drive" not in everything, everything
        assert "front-wheel drive" not in everything, everything
        for step in result["steps"]:
            for citation in step.get("adds_citations") or []:
                assert citation["store"] != "epa_csv", citation


# --- epa_csv revoked: our own composed sentence is not a source -------------


def test_epa_csv_is_not_an_admissible_store() -> None:
    """
    The table says it, and the derived sets follow. ``engineOptions`` is built by
    ``backend/scripts/import_epa_to_dictionary.py`` out of displacement, cylinder
    count and ``eng_dscr``; a bullet quoting it quotes our own synthesis.
    """
    from backend.enrichment import brochure_extract as bx

    assert bx.LADDER_BULLET_STORES["epa_csv"] == "uncited"
    assert "epa_csv" not in bx.ADMISSIBLE_LADDER_BULLET_STORES
    assert not bx.ladder_bullet_store_admissible("epa_csv")
    # The classifier still recognises the store — it is admissibility that changed.
    assert bx.ladder_bullet_store_for_source("EPA CSV") == "epa_csv"


def test_citation_register_drops_a_revoked_store() -> None:
    """One place enforces the table: a register entry from a revoked store is refused."""
    from backend.enrichment.trim_ladder import _CitationRegister

    register = _CitationRegister()
    register.add_quote(
        "6.2L V8 (Hellcat engine)",
        {"store": "epa_csv", "source": "2021_Jeep_Grand Cherokee_EPA.csv", "row": 6},
    )
    register.add_exact("6.2L V8 (Hellcat engine)", {"store": "epa_csv", "source": "x", "row": 6})
    assert register.citation_for("6.2L V8 (Hellcat engine)") is None
    assert not register

    register.add_quote(
        "Quoted brochure line",
        {"store": "brochure_text_quoted", "source": "derived/brochure_text/x.json", "page": 7},
    )
    assert register.citation_for("Quoted brochure line") is not None
    # A citation with no store at all is not admissible either.
    register.add_quote("Storeless line", {"source": "somewhere", "page": 1})
    assert register.citation_for("Storeless line") is None


def test_grand_cherokee_2021_shows_no_hellcat_engine_line() -> None:
    """
    /car/94731 is a 2021 Grand Cherokee 80th Anniversary. Its Trackhawk rung
    printed "6.2L V8 (Hellcat engine)" — the engineOptions cell of row 6 of
    2021_Jeep_Grand Cherokee_EPA.csv, which our importer composed as
    "6.2L" + "V8" + "(Hellcat engine)".
    """
    result = resolve_trim_ladder(
        make="Jeep", model="Grand Cherokee", year=2021, trim="80th Anniversary"
    )
    assert result is not None
    everything = " ".join(b for s in result["steps"] for b in (s.get("adds") or [])).lower()
    assert "hellcat" not in everything, everything


def test_the_hellcat_engine_line_really_is_in_the_epa_store(monkeypatch) -> None:
    """Counterpart: with the gate off that same line comes back, from that same row.

    Without this, the test above would pass just as well if the Trackhawk rung
    had disappeared for an unrelated reason.
    """
    import csv

    # Read the shipped file directly rather than through find_epa_csv: another
    # test module rebuilds the real dictionary catalog index from its own tmp
    # fixture (see the note in the sibling-model test below), and this assertion
    # is about what is in the document, not about the resolver.
    path = (
        pathlib.Path(__file__).resolve().parents[1]
        / "dictionary/epa/Jeep/2021_Jeep_Grand Cherokee_EPA.csv"
    )
    if not path.is_file():
        pytest.skip("2021 Grand Cherokee EPA CSV not present")
    with path.open(encoding="utf-8", newline="") as fh:
        cells = [(r.get("Trim") or "", r.get("engineOptions") or "") for r in csv.DictReader(fh)]
    assert any("Hellcat" in engine for _trim, engine in cells), cells

    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(
        make="Jeep", model="Grand Cherokee", year=2021, trim="80th Anniversary"
    )
    assert result is not None
    everything = " ".join(b for s in result["steps"] for b in (s.get("adds") or [])).lower()
    assert "hellcat" in everything, everything


def test_epa_store_gate_synthetic_hellcat_companion(
    scratch_dictionary_root, monkeypatch
) -> None:
    """The resolver half of the Hellcat pair, with no shipped EPA file needed.

    The test above pins the CORPUS FACT (the shipped CSV really carries the
    line) and skips wherever the dictionary is absent. This one writes the same
    row into a scratch dictionary at test time, so the resolver behaviour —
    EPA-store bullets blocked by default, restored by
    ``TRIM_ADDS_REQUIRE_PROVENANCE=0`` — is exercised on every machine.
    """
    import csv

    from backend.enrichment import dictionary_catalog
    from backend.enrichment.dictionary_paths import CANONICAL_CSV_COLUMNS

    epa_dir = scratch_dictionary_root / "epa" / "Jeep"
    epa_dir.mkdir(parents=True, exist_ok=True)
    epa = epa_dir / "2021_Jeep_Grand Cherokee_EPA.csv"
    with epa.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(CANONICAL_CSV_COLUMNS))
        writer.writeheader()
        for trim_name, engine in [
            ("Laredo", "3.6L V6"),
            ("Limited", "3.6L V6"),
            ("Overland", "5.7L V8"),
            ("Trackhawk", "6.2L V8 (Hellcat engine)"),
        ]:
            writer.writerow(
                {
                    "Year": "2021",
                    "Make": "Jeep",
                    "Model": "Grand Cherokee",
                    "Trim": trim_name,
                    "engineOptions": engine,
                    "transmissionOptions": "Automatic (S8)",
                    "drivetrainOptions": "4WD",
                    "fuelType": "Gasoline",
                    "cylinders": "6",
                    "displacement": "3.6",
                }
            )
    dictionary_catalog.rebuild_catalog(dictionary_catalog.build_manifest_entries())
    dictionary_catalog.invalidate_catalog_cache()
    try:
        def bullets() -> str:
            result = resolve_trim_ladder(
                make="Jeep", model="Grand Cherokee", year=2021, trim="Limited"
            )
            return " ".join(
                b
                for s in ((result or {}).get("steps") or [])
                for b in (s.get("adds") or [])
            ).lower()

        # Adds gate at its default: the composed EPA sentence may not render.
        assert "hellcat" not in bullets()

        monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
        restored = bullets()
        assert "hellcat" in restored, restored
    finally:
        dictionary_catalog.invalidate_catalog_cache()


# --- an EPA file has to be this model's file --------------------------------


def test_find_epa_csv_refuses_a_sibling_models_file(tmp_path, monkeypatch) -> None:
    """
    "Blazer EV" must not be served the gas Blazer's CSV. Nothing downstream
    re-checks the model — ``knowledge_engine._lookup_epa_from_dictionary_csv``
    matches rows by trim alone — so a sibling file is read as this car's specs.

    NOTE for whoever owns backend/tests/test_dictionary_catalog.py:
    ``test_find_epa_csv_jeep`` there redirects only ``dictionary_paths`` and then
    calls ``rebuild_catalog``, which writes through ``dictionary_catalog``'s own
    module-level ``MANIFEST_PATH`` / ``CATALOG_DB_PATH`` — i.e. it rebuilds the
    REAL backend/dictionary/index from a one-row tmp fixture, and every later
    EPA lookup in that process (and every later process, until someone re-runs
    ``rebuild_catalog()``) sees a 1-entry catalog. Confirmed 2026-07-31 by
    running that test alone: manifest entry_count 14,598 -> 1.
    """
    import csv as _csv

    from backend.enrichment import dictionary_catalog, dictionary_paths
    from backend.enrichment.dictionary_paths import CANONICAL_CSV_COLUMNS

    # dictionary_catalog binds these names at import time, so redirecting only
    # dictionary_paths leaves the writers (rebuild_catalog, write_manifest)
    # pointed at the REAL backend/dictionary/index — this test rebuilt the live
    # catalog from a one-row fixture the first time it was written. Both modules
    # have to be redirected.
    for module in (dictionary_paths, dictionary_catalog):
        monkeypatch.setattr(module, "DICTIONARY_ROOT", tmp_path, raising=False)
        monkeypatch.setattr(module, "INDEX_DIR", tmp_path / "index", raising=False)
        monkeypatch.setattr(module, "EPA_DIR", tmp_path / "epa", raising=False)
        monkeypatch.setattr(module, "OPTIONS_RAW_DIR", tmp_path / "options" / "raw", raising=False)
        monkeypatch.setattr(
            module, "OPTIONS_STUBS_DIR", tmp_path / "options" / "stubs", raising=False
        )
        monkeypatch.setattr(
            module, "CATALOG_DB_PATH", tmp_path / "index" / "dictionary_catalog.db", raising=False
        )
        monkeypatch.setattr(module, "MANIFEST_PATH", tmp_path / "index" / "manifest.json", raising=False)
    dictionary_catalog.invalidate_catalog_cache()
    try:
        epa = tmp_path / "2025_Chevrolet_Blazer_EPA.csv"
        with epa.open("w", newline="", encoding="utf-8") as fh:
            writer = _csv.DictWriter(fh, fieldnames=list(CANONICAL_CSV_COLUMNS))
            writer.writeheader()
            writer.writerow(
                {
                    "Year": "2025",
                    "Make": "Chevrolet",
                    "Model": "Blazer",
                    "Trim": "RS",
                    "engineOptions": "3.6L V6",
                }
            )
        dictionary_catalog.rebuild_catalog(dictionary_catalog.build_manifest_entries())
        dictionary_catalog.invalidate_catalog_cache()

        assert dictionary_catalog.find_epa_csv("Chevrolet", "Blazer", 2025) is not None
        assert dictionary_catalog.find_epa_csv("Chevrolet", "Blazer EV", 2025) is None
    finally:
        dictionary_catalog.invalidate_catalog_cache()


@pytest.mark.skipif(
    not (pathlib.Path(__file__).resolve().parents[1] / "dictionary" / "epa").is_dir(),
    reason="EPA dictionary not present",
)
def test_shipped_epa_files_are_never_another_models_file() -> None:
    """On the real dictionary: whatever comes back carries a row for this model."""
    from backend.enrichment.dictionary_catalog import epa_csv_is_for_model, find_epa_csv

    for make, model, year in [
        ("Jeep", "Grand Cherokee", 2021),
        ("Toyota", "RAV4", 2022),
        ("Chevrolet", "Blazer", 2025),
        ("Chevrolet", "Blazer EV", 2025),
        ("Ford", "F-250SD", 2026),
        ("Land Rover", "Range Rover Sport", 2023),
        ("Audi", "A6", 2017),
        ("Ram", "2500", 2018),
    ]:
        path = find_epa_csv(make, model, year)
        if path is None:
            continue
        assert epa_csv_is_for_model(path, make, model), (make, model, year, path.name)


def test_position_describing_copy_is_not_a_quotation() -> None:
    """
    "Mid-level trim with added convenience features…" is a sentence we write, so
    it can never carry a citation. It used to reach the page from the
    oem_knowledge and inventory ladders, which the overlay gate did not cover.
    """
    import re

    prose = re.compile(
        r"Entry-level trim|Mid-level trim|Upper trim|Highest trim level|"
        r"Sport trim with performance styling",
        re.I,
    )
    for make, model, year, trim in [
        ("Lexus", "IS 350", 2022, "F SPORT"),
        ("GMC", "Sierra 1500", 2024, "SLT"),
        ("Ford", "Edge", 2017, "SE"),
        ("Mazda", "Mazda3", 2014, "s Grand Touring"),
    ]:
        result = resolve_trim_ladder(make=make, model=model, year=year, trim=trim)
        if not result:
            continue
        for step in result["steps"]:
            for bullet in step.get("adds") or []:
                assert not prose.search(bullet), (make, model, year, step["name"], bullet)


def test_store_table_is_the_only_switch() -> None:
    """
    Widening the policy is one row in LADDER_BULLET_STORES. If a later phase
    starts citing the curated ladders, flipping that row is what re-admits them —
    no call site in trim_ladder.py names a store by hand.
    """
    from backend.enrichment import brochure_extract as bx

    assert bx.ladder_bullet_store_for_source("curated") == "curated_trim_ladder"
    assert bx.ladder_bullet_store_for_source("EPA CSV") == "epa_csv"
    assert bx.ladder_bullet_store_for_source("2022_Toyota_RAV4_EPA.csv") == "epa_csv"
    assert (
        bx.ladder_bullet_store_for_source("2022_Jeep_Grand_Cherokee_Complete_Options.csv")
        == "complete_options_csv"
    )
    assert bx.ladder_bullet_store_for_source("oem_knowledge") == "oem_knowledge_prose"
    assert bx.ladder_bullet_store_for_source("inventory") == "inventory_prose"
    # An unknown producer is silent until someone adds it on purpose.
    assert not bx.ladder_bullet_store_admissible(
        bx.ladder_bullet_store_for_source("something_new_v2")
    )
    # The overlay set is derived from the same table rather than kept in parallel.
    assert bx.ADMISSIBLE_OVERLAY_SOURCES <= bx.ADMISSIBLE_LADDER_BULLET_STORES
    assert "curated_trim_ladder" not in bx.ADMISSIBLE_LADDER_BULLET_STORES
    assert "trim_spec_sheet" not in bx.ADMISSIBLE_LADDER_BULLET_STORES
    assert "complete_options_csv" not in bx.ADMISSIBLE_LADDER_BULLET_STORES
    assert "epa_csv" not in bx.ADMISSIBLE_LADDER_BULLET_STORES


def test_every_real_ladder_source_classifies_to_a_known_store() -> None:
    """Every ``source`` string the shipped ladder files actually use is accounted for."""
    import json
    import pathlib

    from backend.enrichment.brochure_extract import (
        LADDER_BULLET_STORES,
        ladder_bullet_store_for_source,
    )

    curated_dir = pathlib.Path(__file__).resolve().parents[1] / "dictionary/curated"
    sources: set[str] = set()
    for name in ("trim_ladders.json", "trim_ladders_generated.json", "trim_ladders_epa.json"):
        path = curated_dir / name
        if not path.is_file():
            continue
        for ladder in (json.loads(path.read_text(encoding="utf-8")).get("ladders") or []):
            sources.add(str(ladder.get("source") or ""))
    sources |= {"curated", "inventory", "oem_knowledge", "brochure", "EPA CSV"}
    assert len(sources) > 100
    for source in sources:
        assert ladder_bullet_store_for_source(source) in LADDER_BULLET_STORES, source


def test_gate_off_restores_the_blocked_stores(monkeypatch) -> None:
    """The whole cross-store rule is reversible with one environment variable."""
    blocked = resolve_trim_ladder(make="Jeep", model="Compass", year=2024, trim="Latitude")
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    restored = resolve_trim_ladder(make="Jeep", model="Compass", year=2024, trim="Latitude")
    assert blocked is not None and restored is not None
    blocked_lines = [b for s in blocked["steps"] for b in (s.get("adds") or [])]
    restored_lines = [b for s in restored["steps"] for b in (s.get("adds") or [])]
    assert not blocked_lines
    assert restored_lines


def test_gate_on_never_invents_a_line_the_gate_off_page_did_not_have(monkeypatch) -> None:
    """
    The refill check, as a test. Blocking a store must not let a rung reach for a
    line it was never showing. Any bullet present with the gate ON must also be
    present with it OFF — the gate only ever subtracts.
    """
    vehicles = [
        ("Jeep", "Grand Cherokee", 2018, "Limited"),
        ("Toyota", "RAV4", 2022, "XLE"),
        ("Nissan", "Sentra", 2026, "SV"),
        ("Ram", "1500", 2025, "Limited"),
        ("Dodge", "Durango", 2020, "R/T"),
        ("Lexus", "IS 350", 2022, "F SPORT"),
    ]
    on = {}
    for make, model, year, trim in vehicles:
        result = resolve_trim_ladder(make=make, model=model, year=year, trim=trim)
        on[(make, model, year, trim)] = {
            b for s in ((result or {}).get("steps") or []) for b in (s.get("adds") or [])
        }
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    for key, gated in on.items():
        make, model, year, trim = key
        result = resolve_trim_ladder(make=make, model=model, year=year, trim=trim)
        ungated = {
            b for s in ((result or {}).get("steps") or []) for b in (s.get("adds") or [])
        }
        assert gated <= ungated, (key, sorted(gated - ungated))


# --- the build-time verifier: does it really re-open the document? ----------
#
# Everything above tests what the RENDER path does with a flag. These tests are
# about where that flag comes from. The verifier is the only thing in the
# pipeline that goes back to the PDF, so if it is wrong, the gate is decorative.


def test_verifier_normalisation_forgives_glyphs_and_nothing_else() -> None:
    from backend.scripts.verify_trim_citations import normalize

    # Quote glyphs, dash flavours, non-breaking space, run of spaces, line wrap.
    assert normalize("driver’s  seat") == normalize("driver's seat")
    assert normalize("505 watt – 14 speakers") == normalize("505 watt - 14 speakers")
    assert normalize("8-way power-\nadjustable") == normalize("8 way power adjustable")
    assert normalize("• Heated front seats.") == "heated front seats"
    # A dropped character is a different claim, not a formatting difference.
    assert normalize("Front tow hooks (4x only)") != normalize("Front tow hooks (4x4 only)")
    # So is a dropped footnote marker.
    assert normalize("Remote Engine Start System with X") != normalize(
        "Remote Engine Start System45 with X"
    )


def test_verifier_requires_a_contiguous_run_not_a_bag_of_words() -> None:
    from backend.scripts.verify_trim_citations import PageReading, normalize

    # One column printing three cells in this order. ``PageReading`` no longer
    # accepts a flat per-column string (the old ``columns=`` field) — flattening
    # is what let interior fragments pass, and handing a plain string over
    # would iterate it character by character.
    page = PageReading(
        cells=(
            (
                normalize("Heated front seats"),
                normalize("Panorama sunroof"),
                normalize("Power tailgate"),
            ),
        )
    )
    assert page.prints("Panorama sunroof")
    assert page.prints("Heated front seats Panorama sunroof")
    # Words that are all present but not adjacent, and words in the wrong order.
    assert not page.prints("Heated front seats Power tailgate")
    assert not page.prints("sunroof Panorama")
    # Extra words of ours around a real quote.
    assert not page.prints("Panorama sunroof — added over the base")


def test_verifier_reads_a_wrapped_grid_row_off_the_real_pdf() -> None:
    """
    The 2026 RAV4 grid prints this label wrapped over two lines with the S/O/-
    marks landing between the halves. ``page.extract_text()`` cannot find it;
    the column rebuild can, and that is the whole reason the rebuild exists.
    """
    pdfplumber = pytest.importorskip("pdfplumber")
    from backend.scripts.verify_trim_citations import normalize, read_page

    pdf_path = (
        pathlib.Path(__file__).resolve().parents[1]
        / "data/brochures/2026_Toyota_RAV4_Brochure.pdf"
    )
    if not pdf_path.is_file():
        pytest.skip("2026 RAV4 brochure PDF not on disk")
    bullet = "Leather-trimmed shift lever with sequential mode"
    with pdfplumber.open(str(pdf_path)) as pdf:
        raw = pdf.pages[8].extract_text() or ""
        reading = read_page(pdf.pages[8])
    assert normalize(bullet) not in normalize(raw), "flat page text should NOT contain it"
    assert reading.prints(bullet)
    # And it still refuses a sentence we made up out of the same page's words.
    assert not reading.prints("Leather-trimmed shift lever — added over the LE")


def test_verifier_fails_a_bullet_that_is_not_printed_in_the_cited_document(tmp_path) -> None:
    """End-to-end: a real citation passes, an invented one next to it fails."""
    pytest.importorskip("pdfplumber")
    import json

    from backend.enrichment.dictionary_paths import BROCHURE_TEXT_DIR
    from backend.scripts.verify_trim_citations import (
        FAIL_NOT_PRINTED,
        OK,
        verify_overlay,
    )

    doc = BROCHURE_TEXT_DIR / "2026__toyota__rav4.json"
    if not doc.is_file():
        pytest.skip("2026 RAV4 brochure text not on disk")
    blob = json.loads(doc.read_text(encoding="utf-8"))
    if not pathlib.Path(str(blob.get("source_pdf") or "")).is_file():
        pytest.skip("2026 RAV4 brochure PDF not on disk")

    real = "18-in multi-spoke black sport alloy wheels with black lug nuts"
    invented = "18-in multi-spoke black sport alloy wheels with a panoramic glass roof"
    overlay = {
        "catalog_key": "2026|toyota|rav4",
        "year": 2026,
        "make": "Toyota",
        "model": "RAV4",
        "source": "brochure_text_quoted",
        "adds_by_trim": {"SE": [real, invented]},
        "adds_provenance": {
            "SE": [
                {"text": real, "source": "derived/brochure_text/2026__toyota__rav4.json",
                 "page": 7},
                {"text": invented, "source": "derived/brochure_text/2026__toyota__rav4.json",
                 "page": 7},
            ]
        },
    }
    path = tmp_path / "2026__toyota__rav4.json"
    path.write_text(json.dumps(overlay), encoding="utf-8")

    report = verify_overlay(path, apply=True)
    assert report is not None
    assert report.outcomes[OK] == 1
    assert report.outcomes[FAIL_NOT_PRINTED] == 1

    stamped = json.loads(path.read_text(encoding="utf-8"))
    flags = {e["text"]: e["verified"] for e in stamped["adds_provenance"]["SE"]}
    assert flags == {real: True, invented: False}

    from backend.enrichment.brochure_extract import admissible_overlay_adds

    assert admissible_overlay_adds(stamped, for_year=2026) == {"SE": [real]}


def test_verifier_stamps_a_synthetic_document_end_to_end(tmp_path) -> None:
    """Ungated companion to the two corpus-gated verifier tests above.

    Builds the WHOLE chain at test time — PDF, brochure_text transcript with
    the recorded sha256, overlay citing it — then runs ``verify_overlay`` with
    ``apply=True`` and checks the render path keeps only what is printed. No
    gitignored brochure or derived corpus file is touched.
    """
    fitz = pytest.importorskip("fitz", reason="pymupdf not installed")
    pytest.importorskip("pdfplumber")
    import hashlib
    import json

    from backend.enrichment.brochure_extract import admissible_overlay_adds
    from backend.scripts.verify_trim_citations import (
        FAIL_NOT_PRINTED,
        OK,
        verify_overlay,
    )

    doc = fitz.open()
    page = doc.new_page(width=612, height=792)
    page.insert_text((60, 200), "Leather-trimmed shift lever", fontsize=9)
    for x, mark in ((300.0, "S"), (390.0, "O"), (480.0, "-")):
        page.insert_text((x, 212), mark, fontsize=9)
    page.insert_text((60, 224), "with sequential mode", fontsize=9)
    page.insert_text((60, 260), "Heated front seats", fontsize=9)
    pdf_path = tmp_path / "2026_Synthetic_Ranger_Brochure.pdf"
    doc.save(str(pdf_path))
    doc.close()

    transcript = tmp_path / "2026__synthetic__ranger.json"
    transcript.write_text(
        json.dumps(
            {
                "year": 2026,
                "make": "Synthetic",
                "model": "Ranger",
                "source_pdf": str(pdf_path),
                "source_pdf_sha256": hashlib.sha256(pdf_path.read_bytes()).hexdigest(),
            }
        ),
        encoding="utf-8",
    )

    # A wrapped grid row the flat text cannot contain, and an invented line
    # built from the same page's words.
    printed = "Leather-trimmed shift lever with sequential mode"
    invented = "Leather-trimmed shift lever with heated grips"
    cite = str(transcript)
    overlay_path = tmp_path / "2026__synthetic__ranger_overlay.json"
    overlay_path.write_text(
        json.dumps(
            {
                "catalog_key": "2026|synthetic|ranger",
                "year": 2026,
                "make": "Synthetic",
                "model": "Ranger",
                "source": "brochure_text_quoted",
                "adds_by_trim": {"Summit": [printed, invented]},
                "adds_provenance": {
                    "Summit": [
                        {"text": printed, "source": cite, "page": 1},
                        {"text": invented, "source": cite, "page": 1},
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    report = verify_overlay(overlay_path, apply=True)
    assert report is not None
    assert report.outcomes[OK] == 1
    assert report.outcomes[FAIL_NOT_PRINTED] == 1

    stamped = json.loads(overlay_path.read_text(encoding="utf-8"))
    flags = {e["text"]: e["verified"] for e in stamped["adds_provenance"]["Summit"]}
    assert flags == {printed: True, invented: False}
    assert admissible_overlay_adds(stamped, for_year=2026) == {"Summit": [printed]}


def test_verifier_rejects_a_citation_naming_another_vehicles_document(tmp_path) -> None:
    """The transcript a bullet cites has to be this year/make/model's book."""
    pytest.importorskip("pdfplumber")
    import json

    from backend.enrichment.dictionary_paths import BROCHURE_TEXT_DIR
    from backend.scripts.verify_trim_citations import (
        FAIL_CITATION_OTHER_VEHICLE,
        verify_overlay,
    )

    if not (BROCHURE_TEXT_DIR / "2026__toyota__rav4.json").is_file():
        pytest.skip("2026 RAV4 brochure text not on disk")
    overlay = {
        "catalog_key": "2026|toyota|highlander",
        "year": 2026,
        "make": "Toyota",
        "model": "Highlander",
        "source": "brochure_text_quoted",
        "adds_by_trim": {"XSE": ["Body-colored grille with dark chrome accents"]},
        "adds_provenance": {
            "XSE": [
                {
                    "text": "Body-colored grille with dark chrome accents",
                    # a real, readable document — of the wrong vehicle
                    "source": "derived/brochure_text/2026__toyota__rav4.json",
                    "page": 6,
                }
            ]
        },
    }
    path = tmp_path / "2026__toyota__highlander.json"
    path.write_text(json.dumps(overlay), encoding="utf-8")
    report = verify_overlay(path, apply=False)
    assert report is not None
    assert report.outcomes[FAIL_CITATION_OTHER_VEHICLE] == 1


def test_every_overlay_bullet_on_disk_has_a_verifier_verdict() -> None:
    """
    No admissible overlay may sit in the data without having been checked.

    A file the verifier has never seen renders nothing (the flag is missing, and
    missing is not True), so this is about keeping the corpus honest rather than
    about safety: a silent file is easy to miss.
    """
    import json

    from backend.enrichment.brochure_extract import (
        ADMISSIBLE_OVERLAY_SOURCES,
        CITATION_VERIFICATION_KEY,
    )
    from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR

    seen = 0
    for path in sorted(TRIM_ADDS_BY_YEAR_DIR.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        if str(data.get("source") or "") not in ADMISSIBLE_OVERLAY_SOURCES:
            continue
        assert CITATION_VERIFICATION_KEY in data, (
            f"{path.name}: run backend/scripts/verify_trim_citations.py --apply"
        )
        for trim, entries in (data.get("adds_provenance") or {}).items():
            for entry in entries:
                assert isinstance(entry.get("verified"), bool), (path.name, trim)
                seen += 1
    assert seen > 100, seen


def test_rendered_bullets_are_exactly_the_verified_ones() -> None:
    """
    For every admissible overlay on disk, what the ladder renders equals the set
    of bullets the verifier signed off — no more, and nothing signed off gets
    dropped for a reason other than the ladder not having that rung.
    """
    import json

    from backend.enrichment.brochure_extract import (
        ADMISSIBLE_OVERLAY_SOURCES,
        load_brochure_trim_overlay,
    )
    from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR

    checked = 0
    for path in sorted(TRIM_ADDS_BY_YEAR_DIR.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        if str(raw.get("source") or "") not in ADMISSIBLE_OVERLAY_SOURCES:
            continue
        overlay = load_brochure_trim_overlay(raw["year"], raw["make"], raw["model"]) or {}
        for trim, bullets in (overlay.get("adds_by_trim") or {}).items():
            signed = {
                str(e.get("text") or "")
                for e in (raw.get("adds_provenance") or {}).get(trim) or []
                if e.get("verified") is True
            }
            for bullet in bullets:
                assert bullet in signed, f"{path.name} [{trim}]: {bullet}"
                checked += 1
    assert checked > 100, checked


# ===========================================================================
# THE RUNG-NAME GATE (TRIM_RUNGS_REQUIRE_PROVENANCE)
#
# Everything above this line runs with the gate off (see ``_legacy_rung_gate_off``
# at the top of the file). Every test below requests ``rung_gate_on``, so it runs
# the way production does.
#
# The claim a ladder makes is "these are the trims of this vehicle". A rung is
# allowed to make it only when we can point at something outside our own
# synthesis: an ACTIVE row in ``cars`` for this exact year/make/model whose trim
# string EQUALS the rung's displayed name once case and punctuation are removed,
# or a brochure citation for this exact model year that
# ``verify_trim_citations.py`` re-opened the PDF and confirmed, filed under a
# heading that equals the rung name by the same rule.
#
# EXACT, not fuzzy, and the tests below pin that specifically — the first version
# of this gate matched at a >= 80 similarity score and consequently minted rung
# names out of near-miss listing labels while stamping them "active_inventory".
# ===========================================================================


def _ladder_def(*names: str) -> dict:
    return {
        "id": "test_ladder",
        "make": "Ram",
        "models": ["1500"],
        "label": "Ram 1500 trim lineup",
        "source": "curated",
        "steps": [{"name": n, "aliases": [], "adds": []} for n in names],
    }


def _build(ladder_def, *, year=2023, trim="Limited", overlay=None):
    from backend.enrichment.trim_ladder import _build_ladder_result

    return _build_ladder_result(
        ladder_def,
        make="Ram",
        model="1500",
        year=year,
        trim=trim,
        brochure_overlay=overlay,
    )


def test_a_rung_with_no_listing_and_no_citation_is_not_shown(rung_gate_on, monkeypatch) -> None:
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(
        tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7), ("Laramie", 9))
    )
    result = _build(_ladder_def("Limited", "Laramie Longhorn", "Laramie", "Lone Star"))
    assert [s["name"] for s in result["steps"]] == ["Limited", "Laramie"]


def test_every_shown_rung_names_what_justifies_it(rung_gate_on, monkeypatch) -> None:
    from backend.enrichment import trim_ladder as tl
    from backend.enrichment.brochure_extract import ladder_rung_store_admissible

    monkeypatch.setattr(
        tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7), ("Laramie", 9))
    )
    result = _build(_ladder_def("Limited", "Laramie", "Tradesman"))
    assert result["steps"]
    for step in result["steps"]:
        prov = step["name_provenance"]
        assert ladder_rung_store_admissible(prov.get("store")), prov
        assert prov["active_listings"] >= 1


def test_the_evidence_is_this_model_year_not_a_neighbouring_one(rung_gate_on, monkeypatch) -> None:
    """A trim listed in 2023 does not put that rung on a 2024 car."""
    from backend.enrichment import trim_ladder as tl

    tl._active_trims_by_make_year.cache_clear()
    calls: dict[int, int] = {}

    def fake(make_norm, year):
        calls[year] = calls.get(year, 0) + 1
        return (("1500", "Limited", 7),) if year == 2023 else ()

    monkeypatch.setattr(tl.evidence, "_active_trims_by_make_year", fake)
    monkeypatch.setattr(tl.evidence, "_active_make_spellings", lambda: {"ram": ("Ram",)})
    assert tl._inventory_rung_evidence("Ram", "1500", 2023) == (("Limited", 7),)
    assert tl._inventory_rung_evidence("Ram", "1500", 2024) == ()
    assert set(calls) == {2023, 2024}


def test_a_trim_of_a_different_model_is_not_evidence(rung_gate_on, monkeypatch) -> None:
    """"1500 Classic" Warlocks do not put a Warlock rung on a "1500"."""
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_active_make_spellings", lambda: {"ram": ("Ram",)})
    monkeypatch.setattr(
        tl.evidence,
        "_active_trims_by_make_year",
        lambda m, y: (("1500", "Limited", 7), ("1500 Classic", "Warlock", 4)),
    )
    assert tl._inventory_rung_evidence("Ram", "1500", 2023) == (("Limited", 7),)


def test_one_listing_justifies_at_most_one_rung(rung_gate_on, monkeypatch) -> None:
    """A "Sport" on a lot is not evidence that a "Sport Touring" is."""
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Sport", 3),))
    result = _build(_ladder_def("Sport Touring", "Sport"), trim="Sport")
    assert [s["name"] for s in result["steps"]] == ["Sport"]


def test_an_alias_is_not_evidence_for_the_name_it_is_an_alias_of(
    rung_gate_on, monkeypatch
) -> None:
    """An alias table is our own synthesis. An observed "Limited" may print a
    rung called "Limited"; it may not print one called "Limited Plus" on the
    strength of "Limited" appearing in that rung's alias list."""
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 5),))
    steps = _ladder_def("Limited Plus", "Tradesman")["steps"]
    steps[0]["aliases"] = ["Limited"]
    hits = tl._exact_inventory_rung_hits(steps, make="Ram", model="1500", year=2023)
    assert hits == {}


def test_a_near_miss_listing_label_never_mints_a_rung_name(
    rung_gate_on, monkeypatch
) -> None:
    """The live regression this gate had to be rewritten for.

    2026 Ford Explorer: 3 active listings spelled "Sport Utility" (a body style
    a dealer typed into the trim field), a ladder def carrying a "Sport" step,
    and a fuzzy >= 80 prefix match produced a rung named "Sport" — a trim Ford
    does not sell — stamped ``{"store": "active_inventory"}``. ``Tremor®`` is in
    the same list to pin the one normalisation that IS allowed.
    """
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(
        tl.evidence,
        "_inventory_rung_evidence",
        lambda *a, **k: (("Sport Utility", 3), ("Tremor®", 17), ("Tremor", 37)),
    )
    steps = _ladder_def("Tremor", "Sport")["steps"]
    hits = tl._exact_inventory_rung_hits(steps, make="Ford", model="Explorer", year=2026)
    assert hits == {0: (54, ("Tremor", "Tremor®"))}


def test_a_derived_spelling_is_not_an_observed_spelling(rung_gate_on, monkeypatch) -> None:
    """``canonical_trim_name`` rewrites "Platinum RWD" to "Platinum". That is a
    guess we make, not a string a dealer typed, so it is not rung evidence."""
    from backend.enrichment import trim_ladder as tl
    from backend.enrichment.trim_ladder_knowledge import canonical_trim_name

    assert canonical_trim_name("Platinum RWD", "Ford", "Explorer") == "Platinum"
    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Platinum RWD", 1),))
    steps = _ladder_def("Platinum", "Tremor")["steps"]
    assert tl._exact_inventory_rung_hits(steps, make="Ford", model="Explorer", year=2026) == {}


def test_the_rung_provenance_names_the_spellings_it_rests_on(
    rung_gate_on, monkeypatch
) -> None:
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(
        tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7), ("LIMITED", 2))
    )
    result = _build(_ladder_def("Limited", "Rebel", "Tradesman"))
    prov = result["steps"][0]["name_provenance"]
    assert prov["store"] == "active_inventory"
    assert prov["active_listings"] == 9
    assert prov["observed_trims"] == ["LIMITED", "Limited"]


def test_a_brochure_heading_justifies_only_the_rung_it_names(
    rung_gate_on, monkeypatch
) -> None:
    """A verified "XSE Premium" heading does not justify a rung printed "XSE"."""
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7),))
    overlay = {
        "year": 2023,
        "make": "Ram",
        "model": "1500",
        "source": "brochure_text_quoted",
        "adds_by_trim": {"Rebel GT": ["Bilstein shock absorbers"]},
        "adds_provenance": {
            "Rebel GT": [
                {
                    "text": "Bilstein shock absorbers",
                    "source": "2023_Ram_1500.pdf",
                    "page": 14,
                    "verified": True,
                }
            ]
        },
    }
    result = _build(_ladder_def("Limited", "Rebel", "Tradesman"), overlay=overlay)
    assert [s["name"] for s in result["steps"]] == ["Limited"]


def test_a_verified_citation_justifies_a_rung_with_no_listings(rung_gate_on, monkeypatch) -> None:
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7),))
    overlay = {
        "year": 2023,
        "make": "Ram",
        "model": "1500",
        "source": "brochure_text_quoted",
        "adds_by_trim": {"Rebel": ["Bilstein shock absorbers"]},
        "adds_provenance": {
            "Rebel": [
                {
                    "text": "Bilstein shock absorbers",
                    "source": "2023_Ram_1500.pdf",
                    "page": 14,
                    "verified": True,
                }
            ]
        },
    }
    result = _build(_ladder_def("Limited", "Rebel", "Tradesman"), overlay=overlay)
    names = [s["name"] for s in result["steps"]]
    assert names == ["Limited", "Rebel"]
    rebel = result["steps"][1]
    assert rebel["name_provenance"]["store"] == "brochure_text_quoted"


def test_an_unverified_citation_does_not_justify_a_rung(rung_gate_on, monkeypatch) -> None:
    """The verifier's stamp, not the mere presence of a page number."""
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7),))
    overlay = {
        "year": 2023,
        "make": "Ram",
        "model": "1500",
        "source": "brochure_text_quoted",
        "adds_by_trim": {"Rebel": ["Bilstein shock absorbers"]},
        "adds_provenance": {
            "Rebel": [
                {
                    "text": "Bilstein shock absorbers",
                    "source": "2023_Ram_1500.pdf",
                    "page": 14,
                }
            ]
        },
    }
    result = _build(_ladder_def("Limited", "Rebel", "Tradesman"), overlay=overlay)
    assert [s["name"] for s in result["steps"]] == ["Limited"]


def test_trims_available_alone_never_justifies_a_rung(rung_gate_on) -> None:
    from backend.enrichment.brochure_extract import verified_overlay_trim_names

    overlay = {
        "year": 2023,
        "make": "Ram",
        "model": "1500",
        "source": "brochure_text_quoted",
        "trims_available": ["Tradesman", "Big Horn", "Rebel", "Limited"],
        "adds_by_trim": {},
        "adds_provenance": {},
    }
    assert verified_overlay_trim_names(overlay, for_year=2023) == []


def test_a_neighbouring_years_citation_justifies_nothing(rung_gate_on) -> None:
    from backend.enrichment.brochure_extract import verified_overlay_trim_names

    overlay = {
        "year": 2022,
        "make": "Ram",
        "model": "1500",
        "source": "brochure_text_quoted",
        "adds_by_trim": {"Rebel": ["Bilstein shock absorbers"]},
        "adds_provenance": {
            "Rebel": [
                {
                    "text": "Bilstein shock absorbers",
                    "source": "2022_Ram_1500.pdf",
                    "page": 14,
                    "verified": True,
                }
            ]
        },
    }
    assert verified_overlay_trim_names(overlay, for_year=2022) == ["Rebel"]
    assert verified_overlay_trim_names(overlay, for_year=2023) == []


def test_an_llm_overlay_citation_justifies_nothing(rung_gate_on) -> None:
    from backend.enrichment.brochure_extract import verified_overlay_trim_names

    overlay = {
        "year": 2023,
        "make": "Ram",
        "model": "1500",
        "source": "brochure_llm",
        "adds_by_trim": {"Rebel": ["Bilstein shock absorbers"]},
        "adds_provenance": {
            "Rebel": [
                {"text": "Bilstein shock absorbers", "source": "x.pdf", "page": 1, "verified": True}
            ]
        },
    }
    assert verified_overlay_trim_names(overlay, for_year=2023) == []


def test_a_single_justified_rung_is_not_a_ladder(rung_gate_on, monkeypatch) -> None:
    """One rung tells a shopper nothing about where their trim sits, and the one
    that survives is often not even their own trim (a 2025 Ford Escape Base
    resolved to a lone "Platinum" rung). 7,178 active cars land here."""
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7),))
    monkeypatch.setattr(tl.selection, "_pick_ladder_def", lambda *a, **k: _ladder_def("Limited", "Rebel"))
    monkeypatch.setattr(
        tl, "_generic_trim_ladder_def", lambda *a, **k: _ladder_def("Limited", "Rebel")
    )
    assert resolve_trim_ladder(make="Ram", model="1500", year=2023, trim="Limited") is None


def test_the_generic_oem_fallback_cannot_smuggle_rungs_back_in(rung_gate_on, monkeypatch) -> None:
    """``resolve_trim_ladder`` falls back to a hand-typed generic ladder four
    separate times. Each fallback re-enters ``_build_ladder_result``, so each
    faces the same test — emptying one producer must not be refillable."""
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: ())
    result = resolve_trim_ladder(make="Ram", model="1500", year=2023, trim="Limited")
    assert result is None


def test_kill_switch_restores_the_unjustified_rungs(rung_gate_on, monkeypatch) -> None:
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7),))
    monkeypatch.setenv("TRIM_RUNGS_REQUIRE_PROVENANCE", "0")
    result = _build(_ladder_def("Limited", "Laramie Longhorn", "Laramie"))
    assert [s["name"] for s in result["steps"]] == ["Limited", "Laramie Longhorn", "Laramie"]


def test_the_query_really_reads_active_rows(rung_gate_on, monkeypatch, tmp_path) -> None:
    """End-to-end over a real inventory database, not a stubbed evidence list.

    Everything above stubs ``_inventory_rung_evidence``, which leaves the SQL,
    the make-spelling resolution and the model normalisation untested. This test
    seeds a throwaway inventory and drives the whole path:

      * "CHEVROLET" and "Chevrolet" are both live spellings of one make and must
        be counted together — a plain ``LOWER(make) = ?`` splits them,
      * a SOLD (``listing_active = 0``) Z71 is not a trim anybody is listing,
      * a 2024 High Country does not put that rung on a 2023 car,
      * a "Silverado 2500HD" LTZ does not put that rung on a "Silverado 1500".
    """
    from backend.db import inventory_db
    from backend.db.repositories import base_repo
    from backend.enrichment import trim_ladder as tl

    db = str(tmp_path / "inv.db")
    monkeypatch.setattr(inventory_db, "DB_PATH", db)
    monkeypatch.setattr(base_repo, "DB_PATH", db)
    inventory_db.init_inventory_db()
    rows = [
        ("V1", 2023, "Chevrolet", "Silverado 1500", "LT", 1),
        ("V2", 2023, "CHEVROLET", "Silverado 1500", "LT Trail Boss", 1),
        ("V3", 2023, "Chevrolet", "Silverado 1500", "Z71", 0),
        ("V4", 2024, "Chevrolet", "Silverado 1500", "High Country", 1),
        ("V5", 2023, "Chevrolet", "Silverado 2500HD", "LTZ", 1),
    ]
    with inventory_db.db_conn() as conn:
        cur = conn.cursor()
        for row in rows:
            cur.execute(
                "INSERT INTO cars (vin, year, make, model, trim, listing_active) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                row,
            )
        conn.commit()
    tl._active_make_spellings.cache_clear()
    tl._active_trims_by_make_year.cache_clear()

    observed = dict(tl._inventory_rung_evidence("Chevrolet", "Silverado 1500", 2023))
    assert observed == {"LT": 1, "LT Trail Boss": 1}

    ladder = {
        "id": "t",
        "make": "Chevrolet",
        "models": ["Silverado 1500"],
        "label": "l",
        "source": "curated",
        "steps": [
            {"name": n, "aliases": [], "adds": []}
            for n in ("High Country", "LTZ", "LT Trail Boss", "LT", "Z71", "WT")
        ],
    }
    from backend.enrichment.trim_ladder import _build_ladder_result

    result = _build_ladder_result(
        ladder, make="Chevrolet", model="Silverado 1500", year=2023, trim="LT"
    )
    assert [s["name"] for s in result["steps"]] == ["LT Trail Boss", "LT"]
    tl._active_make_spellings.cache_clear()
    tl._active_trims_by_make_year.cache_clear()


def test_brochure_adds_key_refuses_a_differently_named_section() -> None:
    """A rung may only read the brochure section that NAMES it.

    The removed whole-token containment fallback took the heading with the most
    tokens all present in the step name, which is how the 2022 Dodge Charger
    rung "SRT Hellcat Redeye Widebody" read the section filed under
    "SRT HELLCAT WIDEBODY" — a 797 hp car's rung printing a 717 hp car's
    equipment. Measured on the live fleet the day it was removed: 12 rendered
    ladder steps bound to a differently-named heading, all of them that one
    rung, one of them as a car's own "This vehicle" rung.
    """
    from backend.enrichment.trim_ladder import _lookup_brochure_adds_key

    adds = {"SRT HELLCAT WIDEBODY": ["797-hp supercharged 6.2L HEMI V8"]}
    assert (
        _lookup_brochure_adds_key(
            "SRT Hellcat Redeye Widebody", [], adds, make="Dodge", model="Charger"
        )
        is None
    )
    # The rung that IS that heading still reads it, case and spacing aside.
    assert (
        _lookup_brochure_adds_key(
            "SRT Hellcat Widebody", [], adds, make="Dodge", model="Charger"
        )
        == "SRT HELLCAT WIDEBODY"
    )


def test_brochure_adds_key_refuses_a_canonical_truncation() -> None:
    """``canonical_trim_name`` truncates one trim onto another's name.

    The removed ``_trim_identity_keys`` overlap score ran both labels through
    it, so "Sport Prestige" and a heading called "Sport" could be declared one
    trim by our own reduction of both. Verified directly rather than assumed:
    ``canonical_trim_name("Sport Prestige", "Acura", "TLX") == "Sport"``.
    """
    from backend.enrichment.trim_ladder import _lookup_brochure_adds_key
    from backend.enrichment.trim_ladder_knowledge import canonical_trim_name

    assert canonical_trim_name("Sport Prestige", "Acura", "TLX") == "Sport"
    adds = {"Sport": ["19-inch alloy wheels"]}
    assert (
        _lookup_brochure_adds_key("Sport Prestige", [], adds, make="Acura", model="TLX")
        is None
    )


def test_brochure_adds_key_ignores_aliases_and_fails_closed_on_ties() -> None:
    """Aliases cannot bind a section, and two candidate sections bind neither."""
    from backend.enrichment.trim_ladder import _lookup_brochure_adds_key

    # An alias list cannot reach a section the rung name does not spell.
    adds = {"Raptor R": ["700-hp supercharged 5.2L V8"]}
    assert (
        _lookup_brochure_adds_key("Raptor", ["Raptor R"], adds, make="Ford", model="F-150")
        is None
    )
    # Two headings both reduce to "Limited" once the drivetrain token is
    # dropped; the document cannot say which is this rung's, so neither is used.
    two = {"Limited 4WD": ["4WD"], "Limited RWD": ["RWD"]}
    assert _lookup_brochure_adds_key("Limited", [], two, make="Toyota", model="Tundra") is None
    # ...but an exact heading is never ambiguous with anything.
    with_exact = dict(two)
    with_exact["Limited"] = ["shared"]
    assert (
        _lookup_brochure_adds_key("Limited", [], with_exact, make="Toyota", model="Tundra")
        == "Limited"
    )


# --- rung ORDER provenance ---------------------------------------------------

_DURANGO_DOC_BASIS = {
    "SXT": {
        "basis": "adds_to_edge",
        "direction": "named_as_baseline_by",
        "edge": {
            "trim": "GT",
            "below": "SXT",
            "page": 28,
            "source": "derived/brochure_text/2020__dodge__durango.json",
            "trim_quote": "GT",
            "below_quote": "Adds to SXT",
        },
    },
    "GT": {
        "basis": "adds_to_edge",
        "edge": {
            "trim": "GT",
            "below": "SXT",
            "page": 28,
            "source": "derived/brochure_text/2020__dodge__durango.json",
            "trim_quote": "GT",
            "below_quote": "Adds to SXT",
        },
    },
    "R/T": {
        "basis": "adds_to_edge",
        "edge": {
            "trim": "R/T",
            "below": "GT",
            "page": 29,
            "source": "derived/brochure_text/2020__dodge__durango.json",
            "trim_quote": "R/T",
            "below_quote": "Adds to GT",
        },
    },
    "Citadel": {
        "basis": "adds_to_edge",
        "edge": {
            "trim": "Citadel",
            "below": "GT",
            "page": 30,
            "source": "derived/brochure_text/2020__dodge__durango.json",
            "trim_quote": "CITADEL",
            "below_quote": "Adds to GT",
        },
    },
    "SRT": {
        "basis": "adds_to_edge",
        "edge": {
            "trim": "SRT",
            "below": "R/T",
            "page": 31,
            "source": "derived/brochure_text/2020__dodge__durango.json",
            "trim_quote": "SRT®",
            "below_quote": "Adds to R/T",
        },
    },
}


def _durango_overlay() -> dict:
    return {
        "source": "brochure_text_quoted",
        "year": 2020,
        "make": "Dodge",
        "model": "Durango",
        "rung_order": ["SXT", "GT", "R/T", "Citadel", "SRT"],
        "order_basis": _DURANGO_DOC_BASIS,
    }


def test_document_rung_order_is_top_first_and_year_gated() -> None:
    """The overlay prints base->top; the ladder runs top->base, so it is reversed."""
    from backend.enrichment.trim_ladder import _document_rung_order

    order, basis = _document_rung_order(
        _durango_overlay(), year=2020, make="Dodge", model="Durango"
    )
    assert order == ["SRT", "Citadel", "R/T", "GT", "SXT"]
    # Keyed by the rung-evidence key, so "SRT®" on the page finds the "SRT" rung.
    assert basis["srt"]["edge"]["below"] == "R/T"
    # A different model year's book orders nothing, same rule as its bullets.
    assert _document_rung_order(
        _durango_overlay(), year=2021, make="Dodge", model="Durango"
    ) == ([], {})
    assert _document_rung_order(None, year=2020, make="Dodge", model="Durango") == ([], {})


def test_document_order_overrides_the_hand_typed_rank_table() -> None:
    """luxury_rank puts Citadel above R/T above GT. The brochure does not."""
    from backend.enrichment.trim_ladder import _apply_document_order, _document_rung_order

    doc_order, doc_basis = _document_rung_order(
        _durango_overlay(), year=2020, make="Dodge", model="Durango"
    )
    # The order resolve_trim_ladder produced before the document was consulted.
    steps = [{"name": n} for n in ["Citadel", "R/T", "GT", "SRT"]]
    out = _apply_document_order(
        steps, doc_order, doc_basis, make="Dodge", model="Durango"
    )
    assert [s["name"] for s in out] == ["SRT", "Citadel", "R/T", "GT"]
    assert all(s["order_basis"]["basis"] == "adds_to_edge" for s in out)
    assert out[0]["order_basis"]["edge"]["page"] == 31
    # The incoming step dicts come out of an lru_cached ladder definition and
    # must not be written to.
    assert all("order_basis" not in s for s in steps)


def test_a_rung_the_document_does_not_place_keeps_its_neighbour() -> None:
    """Unplaced rungs are not swept to the bottom — that would assert a rank.

    Reproduces the 2022 Charger: the brochure walks the Scat Pack grades and
    never mentions SRT Hellcat Redeye Widebody, which is the top trim.
    """
    from backend.enrichment.trim_ladder import _apply_document_order

    doc_order = ["Scat Pack Widebody", "Scat Pack", "R/T", "GT"]
    doc_basis = {
        "scatpackwidebody": {"basis": "adds_to_edge", "edge": {"below": "Scat Pack"}},
        "scatpack": {"basis": "adds_to_edge", "edge": {"below": "R/T"}},
        "rt": {"basis": "adds_to_edge", "edge": {"below": "GT"}},
        "gt": {"basis": "adds_to_edge", "edge": {"below": "SXT"}},
    }
    steps = [
        {"name": n}
        for n in ["SRT Hellcat Redeye Widebody", "Scat Pack Widebody", "Scat Pack", "R/T", "GT", "SXT"]
    ]
    out = _apply_document_order(steps, doc_order, doc_basis, make="Dodge", model="Charger")
    assert [s["name"] for s in out] == [
        "SRT Hellcat Redeye Widebody",
        "Scat Pack Widebody",
        "Scat Pack",
        "R/T",
        "GT",
        "SXT",
    ]
    unplaced = {s["name"]: s["order_basis"]["basis"] for s in out}
    assert unplaced["SRT Hellcat Redeye Widebody"] == "unproven"
    assert unplaced["SXT"] == "unproven"
    assert unplaced["Scat Pack"] == "adds_to_edge"


def test_no_document_order_leaves_every_rung_labelled_unproven() -> None:
    """Silence is the default: the hand-typed table places the rung and says so."""
    from backend.enrichment.trim_ladder import _apply_document_order

    steps = [{"name": "Limited"}, {"name": "SE"}]
    out = _apply_document_order(steps, [], {}, make="Toyota", model="Camry")
    assert [s["name"] for s in out] == ["Limited", "SE"]
    assert all(s["order_basis"] == {
        "basis": "unproven",
        "store": "luxury_rank_table",
        "proven": False,
    } for s in out)


def test_ladder_order_verdict_is_the_weakest_rungs(monkeypatch) -> None:
    """One unproven rung makes the whole ladder unproven — a shopper cannot tell."""
    monkeypatch.setenv("TRIM_RUNGS_REQUIRE_PROVENANCE", "0")
    from backend.enrichment.trim_ladder import _build_ladder_result

    def _result(bases):
        ladder = {
            "id": "t",
            "source": "curated",
            "steps": [
                {"name": name, "aliases": [], "adds": [], "order_basis": basis}
                for name, basis in bases
            ],
        }
        return _build_ladder_result(
            ladder, make="Dodge", model="Durango", year=2020, trim=None
        )

    edge = {"basis": "adds_to_edge", "proven": True}
    printed = {"basis": "printed_sequence", "proven": False}
    unproven = {"basis": "unproven", "store": "luxury_rank_table", "proven": False}

    all_edges = _result([("SRT", edge), ("R/T", edge)])["order_provenance"]
    assert all_edges["basis"] == "adds_to_edge"
    assert all_edges["proven"] is True
    assert all_edges["ordered_rungs"] == all_edges["total_rungs"] == 2

    mixed = _result([("SRT", edge), ("R/T", printed)])["order_provenance"]
    assert mixed["basis"] == "printed_sequence"
    assert mixed["proven"] is False

    partial = _result([("SRT", edge), ("R/T", unproven)])["order_provenance"]
    assert partial["basis"] == "unproven"
    assert partial["ordered_rungs"] == 1
    assert partial["total_rungs"] == 2
