"""Rung matching: which rung a listing's trim lands on, rung order, name cleaning,
cross-model plausibility, and the trim-ladder routes.

Split out of the former backend/tests/test_trim_ladder.py. The rung-name gate
fixtures (``_legacy_rung_gate_off`` autouse, ``rung_gate_on``) live in this
package's conftest.py.
"""

from __future__ import annotations

import pytest

from backend.enrichment.trim_ladder import resolve_trim_ladder


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
        pytest.skip("no 2016 Jeep Cherokee ladder resolvable (no curated ladder, no inventory rows)")
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


def test_trim_ladder_free_tier_strips_adds_on_api_without_premium(monkeypatch) -> None:
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
    # Basic ladder position (names + neighbors) is free on every listing —
    # only the per-rung equipment diff is Premium, and must be stripped
    # server-side (not just hidden in the SSR template) so the JSON API
    # never leaks it to a non-premium viewer.
    ladder = body.get("trim_ladder")
    assert ladder is not None
    assert ladder.get("steps")
    for step in ladder["steps"]:
        assert "adds" not in step
        assert "sticker_adds" not in step
        assert "sticker_equipment" not in step


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
        pytest.skip("no 2026 Toyota RAV4 ladder resolvable here")
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
