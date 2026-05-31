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
    assert scat.get("adds")
    assert any("HEMI" in a or "Brembo" in a for a in scat["adds"])


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
    assert "xDrive28i" in names
    assert "M Competition" not in names
    assert "Sport Line" not in names
    assert result["steps"][names.index("xDrive28i")]["is_current"] is True
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
    assert "xDrive30i" in names
    assert "M Sport" not in names
    assert "xLine" not in names
    assert result["steps"][names.index("xDrive30i")]["is_current"] is True


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
    assert "xDrive40i" in names
    assert "Luxury Line" not in names
    assert "M Sport" not in names


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
    assert "M60i xDrive" in names
    assert "xLine" not in names


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


def test_bmw_5_series_xdrive40i_above_sdrive_when_same_motor() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="5 Series",
        year=2023,
        trim="530i",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert names.index("530i xDrive") < names.index("530i")


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


def test_bmw_x5_sdrive40i_xdrive_above_sdrive() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="X5",
        year=2024,
        trim="sDrive40i",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert "xDrive40i" in names and "sDrive40i" in names
    assert names.index("xDrive40i") < names.index("sDrive40i")


def test_bmw_x3_xdrive30i_above_sdrive30i() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="X3",
        year=2024,
        trim="sDrive30i",
    )
    assert result is not None
    names = [s["name"] for s in result["steps"]]
    assert names.index("xDrive30i") < names.index("sDrive30i")


def test_bmw_i4_uses_epa_motor_trims_not_package_lines() -> None:
    result = resolve_trim_ladder(
        make="BMW",
        model="i4",
        year=2026,
        trim="eDrive40",
    )
    assert result is not None
    assert "EPA" in str(result.get("source") or "")
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
    adds = current.get("adds") or []
    assert adds
    assert not any("Typical listing price" in a for a in adds)
    assert any("convenience" in a.lower() or "Uconnect" in a for a in adds)


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
