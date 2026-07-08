"""Trim ladder resolution for premium VDP specs."""

from __future__ import annotations

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


def test_dodge_scat_pack_keeps_real_adds() -> None:
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


def test_cadillac_escalade_v_series_trim_ladder() -> None:
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


def test_bmw_x3_m_uses_curated_bullets_not_csv_junk() -> None:
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


def test_ram_1500_2025_limited_no_uconnect_4c() -> None:
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


def test_ram_1500_big_horn_no_market_prose() -> None:
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


def test_ram_1500_trx_excludes_cross_trim_prose() -> None:
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


def test_bmw_5_series_xdrive_merged_with_rwd() -> None:
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


def test_bmw_3_series_2016_28i_typo_matches_curated_ladder() -> None:
    # A brochure overlay for this model-year now ships in the repo
    # (trim_adds_by_year, commit 7d3c76e1c); the curated ladder must still win.
    result = resolve_trim_ladder(make="BMW", model="3 Series", year=2016, trim="28i xDrive")
    assert result is not None
    assert str(result.get("source") or "") == "curated"
    names = [s["name"] for s in result["steps"]]
    assert "330i" in names
    assert "Na" not in names and "Bmw" not in names
    current = [s for s in result["steps"] if s["is_current"]]
    assert len(current) == 1
    assert current[0]["name"] == "330i"
    assert any(
        "turbo" in str(a).lower() or "hp" in str(a).lower()
        for a in (current[0].get("adds") or [])
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


def test_gmc_yukon_at4_ultimate_curated() -> None:
    result = resolve_trim_ladder(
        make="GMC",
        model="Yukon XL",
        year=2024,
        trim="AT4 Ultimate",
    )
    assert result is not None
    assert result["matched"] is True
    assert result["listing_trim"] == "AT4"
    names = [s["name"] for s in result["steps"]]
    assert names.index("Denali Ultimate") < names.index("AT4")


def test_volvo_xc90_b6_ultra_curated() -> None:
    result = resolve_trim_ladder(
        make="Volvo",
        model="XC90",
        year=2024,
        trim="B6 Ultra",
    )
    assert result is not None
    assert result["matched"] is True
    assert result["listing_trim"] == "B6"
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


def test_jeep_compass_latitude_curated_adds() -> None:
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


def test_jeep_grand_cherokee_2016_overland_uses_full_brochure_adds() -> None:
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


def test_jeep_grand_cherokee_2016_trailhawk_not_placeholder_prose() -> None:
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


def test_jeep_grand_cherokee_overland_air_suspension_standard() -> None:
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


def test_jeep_grand_cherokee_limited_x_exterior_styling_2019() -> None:
    """Limited X stays on curated WK2 ladder (with body styling) even pre-2020 MY."""
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
    joined = " ".join(limited_x.get("adds") or []).lower()
    assert "srt" in joined
    assert "hood" in joined


def test_jeep_grand_cherokee_limited_x_match() -> None:
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
    joined = " ".join(limited_x.get("adds") or []).lower()
    assert "srt" in joined
    assert "hood" in joined


def test_honda_accord_11th_gen_curated_trim_ladder() -> None:
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
    from backend.enrichment.trim_ladder import _trim_match_score

    assert _trim_match_score("LT", "LTZ", [], make="Chevrolet", model="Captiva") == 0
    assert _trim_match_score("LT", "LT", [], make="Chevrolet", model="Captiva") == 100


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


def test_brochure_overlay_fuzzy_matches_calligraphy_awd() -> None:
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


def test_palisade_2026_uses_nearby_brochure_trim_adds() -> None:
    result = resolve_trim_ladder(make="Hyundai", model="Palisade", year=2026, trim="SEL")
    assert result is not None
    calligraphy = next(s for s in result["steps"] if s["name"] == "Calligraphy")
    assert calligraphy.get("adds")
    assert "Blue" not in {s["name"] for s in result["steps"]}


def test_trim_ladder_never_has_empty_step_details() -> None:
    """Every displayed trim step should have adds or specs (no empty-state UI)."""
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
            assert step.get("adds") or step.get("specs"), (
                f"{make} {model} {year} trim {step.get('name')} has no details"
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


def test_elantra_se_uses_brochure_standard_features_not_placeholder() -> None:
    result = resolve_trim_ladder(make="Hyundai", model="Elantra", year=2023, trim="SEL")
    assert result is not None
    se = next(s for s in result["steps"] if s["name"] == "SE")
    adds = se.get("adds") or []
    assert adds
    joined = " ".join(adds).lower()
    assert "forward collision" in joined or "2.0l" in joined
    assert "entry-level trim with core standard equipment" not in joined
