"""Read-time plausibility guard for extended specs (hp / curb / 0-60 / tow).

``epa_extended_specs`` holds MODEL-level scrapes: one variant's numbers were
written onto every trim/year row of the nameplate, so a Ram 1500 Tradesman
renders the TRX's 4.9 s 0-60 and a towing figure as its curb weight. These
tests pin the suppression rules, the curated Ram 5.7 V8 0-60, and — just as
important — that a real TRX/Hellcat keeps its genuinely quick numbers.

Pure functions only: no DB, no network.
"""
import pytest

from backend.enrichment.knowledge_engine_specs import (
    apply_spec_plausibility_guard,
    curated_zero_to_60_sec,
    implausible_extended_spec_fields,
    is_performance_variant,
)

# The real row behind VIN 1C6SRFHT3PN616926 (epa_master_id 62007): curb weight
# equals tow capacity, and the 0-60 belongs to the 702 hp TRX.
RAM_LIMITED = {
    "year": 2023, "make": "Ram", "model": "1500", "trim": "Limited",
    "engine_description": "5.7L V8 Mild Hybrid", "cylinders": 8,
    "fuel_type": "Hybrid", "body_style": "Truck",
}
RAM_SCRAPED_SPECS = {
    "horsepower": 305, "torque_lb_ft": 271,
    "curb_weight_lb": 11580, "tow_capacity_lb": 11580,
    "curb_weight_kg": 5253, "zero_to_60_sec": 4.9,
}


def test_towing_figure_in_the_curb_weight_column_is_suppressed():
    # 11,580 lb is the tow rating. It is caught as a curb weight because it
    # breaks the light-duty ceiling, NOT because it equals tow_capacity_lb — a
    # curb == tow collision on its own means only "model-level scrape", and
    # acting on it erased a believable 4,400 lb from every Audi Q7.
    bad = implausible_extended_spec_fields(RAM_LIMITED, RAM_SCRAPED_SPECS)
    assert "curb_weight_lb" in bad
    assert "curb_weight_kg" in bad  # one fact in two units; both or neither


def test_ram_1500_nameplate_wide_zero_to_60_is_suppressed():
    # 4.9 s is the 702 hp TRX's figure stamped onto all 158 Ram 1500 rows.
    bad = implausible_extended_spec_fields(RAM_LIMITED, RAM_SCRAPED_SPECS)
    assert "zero_to_60_sec" in bad


def test_guard_returns_copy_without_the_bad_keys():
    out = apply_spec_plausibility_guard(RAM_LIMITED, RAM_SCRAPED_SPECS)
    assert "curb_weight_lb" not in out
    assert "zero_to_60_sec" not in out
    assert out["horsepower"] == 305  # untouched fields survive
    assert RAM_SCRAPED_SPECS["curb_weight_lb"] == 11580  # input not mutated


def test_guard_is_a_no_op_on_a_clean_row():
    car = {"year": 2023, "make": "Honda", "model": "Accord", "trim": "EX-L"}
    specs = {"horsepower": 192, "curb_weight_lb": 3300, "zero_to_60_sec": 7.2}
    assert implausible_extended_spec_fields(car, specs) == set()
    assert apply_spec_plausibility_guard(car, specs) is specs


def test_light_duty_curb_weight_over_ceiling_is_suppressed():
    # 2002 Yukon 1500: 8,400 lb is the GVWR, not the ~5,500 lb curb weight.
    car = {"year": 2002, "make": "GMC", "model": "Yukon", "trim": "1500 4WD",
           "body_style": "Sport Utility Vehicle - 4WD"}
    assert "curb_weight_lb" in implausible_extended_spec_fields(car, {"curb_weight_lb": 8400})


def test_heavy_duty_truck_keeps_its_real_curb_weight():
    car = {"year": 2023, "make": "Ram", "model": "3500", "trim": "Laramie 4WD",
           "body_style": "Truck"}
    assert implausible_extended_spec_fields(car, {"curb_weight_lb": 8400}) == set()


def test_sub_four_second_zero_to_60_suppressed_without_the_power():
    car = {"year": 2021, "make": "Toyota", "model": "Camry", "trim": "SE"}
    specs = {"horsepower": 203, "zero_to_60_sec": 3.6}
    assert "zero_to_60_sec" in implausible_extended_spec_fields(car, specs)


@pytest.mark.parametrize(
    "car,specs",
    [
        # A real TRX: 702 hp, 3.7 s to 60 (Ram's own claim). Must survive.
        ({"year": 2023, "make": "Ram", "model": "1500", "trim": "TRX",
          "engine_description": "6.2L Supercharged V8", "cylinders": 8,
          "body_style": "Truck"},
         {"horsepower": 702, "zero_to_60_sec": 3.7}),
        # Hellcat Charger, horsepower unknown on the row — the badge alone saves it.
        ({"year": 2022, "make": "Dodge", "model": "Charger", "trim": "SRT Hellcat"},
         {"zero_to_60_sec": 3.6}),
        # Raptor R, badge in the title rather than the trim field.
        ({"year": 2023, "make": "Ford", "model": "F-150",
          "title": "2023 Ford F-150 Raptor R SuperCrew", "trim": "Raptor R",
          "body_style": "Truck"},
         {"zero_to_60_sec": 3.6}),
        # Genuine power, no recognisable badge: 500+ hp does it regardless.
        ({"year": 2020, "make": "Mercedes-Benz", "model": "E-Class", "trim": "E 63 S"},
         {"horsepower": 603, "zero_to_60_sec": 3.3}),
        # BEV: 400 hp is enough for a 3-second car.
        ({"year": 2023, "make": "Tesla", "model": "Model 3", "trim": "Long Range",
          "fuel_type": "Electric"},
         {"horsepower": 425, "zero_to_60_sec": 3.9}),
        # Sports car with no horsepower on the row: a base 911 really runs 3.x,
        # so an unverifiable-but-possible figure is left alone.
        ({"year": 2021, "make": "Porsche", "model": "911", "trim": "Carrera S"},
         {"zero_to_60_sec": 3.5}),
    ],
)
def test_genuinely_quick_cars_are_not_suppressed(car, specs):
    assert "zero_to_60_sec" not in implausible_extended_spec_fields(car, specs)


def test_pickup_without_power_or_badge_is_suppressed_even_with_hp_unknown():
    car = {"year": 2022, "make": "Ford", "model": "F-150", "trim": "XLT",
           "body_style": "Pickup"}
    assert "zero_to_60_sec" in implausible_extended_spec_fields(car, {"zero_to_60_sec": 3.7})


def test_performance_badge_detection():
    assert is_performance_variant({"trim": "TRX"})
    assert is_performance_variant({"trim": "SRT Hellcat Redeye"})
    assert is_performance_variant({"make": "Hyundai", "model": "Elantra", "trim": "N"})
    assert is_performance_variant({"trim": "Competition", "title": "2021 BMW M3 Competition"})
    assert not is_performance_variant({"make": "Hyundai", "model": "Elantra", "trim": "N Line"})
    assert not is_performance_variant({"trim": "Limited", "model": "1500"})
    assert not is_performance_variant({"trim": "Big Horn", "model": "1500"})


def test_curated_zero_to_60_covers_the_whole_ram_5_7_v8_cohort():
    # eTorque and non-eTorque, both generations, 2WD and 4WD, Classic included.
    for car in (
        RAM_LIMITED,
        {"year": 2019, "make": "RAM", "model": "1500", "trim": "Big Horn",
         "engine_description": "5.7L V8", "cylinders": 8},
        {"year": 2016, "make": "Ram", "model": "1500", "trim": "Tradesman",
         "engine_description": "HEMI 5.7L V8 Multi Displacement VVT", "cylinders": 8},
        {"year": 2022, "make": "Ram", "model": "1500 Classic", "trim": "Warlock",
         "engine_description": "5.7L V8 eTorque", "cylinders": 8},
    ):
        assert curated_zero_to_60_sec(car) == 6.4


def test_curated_zero_to_60_leaves_other_ram_engines_alone():
    trx = {"year": 2023, "make": "Ram", "model": "1500", "trim": "TRX",
           "engine_description": "6.2L Supercharged V8", "cylinders": 8}
    hurricane = {"year": 2025, "make": "Ram", "model": "1500", "trim": "Laramie",
                 "engine_description": "3.0L I6 Twin Turbo", "cylinders": 6}
    hd = {"year": 2021, "make": "Ram", "model": "2500", "trim": "Tradesman",
          "engine_description": "5.7L V8", "cylinders": 8}
    v6 = {"year": 2020, "make": "Ram", "model": "1500", "trim": "Tradesman",
          "engine_description": "3.6L V6", "cylinders": 6}
    for car in (trx, hurricane, hd, v6):
        assert curated_zero_to_60_sec(car) is None


def test_curated_table_needs_a_year_and_a_make():
    assert curated_zero_to_60_sec({"make": "Ram", "model": "1500"}) is None
    assert curated_zero_to_60_sec({}) is None


# ---------------------------------------------------------------------------
# Rows shaped the way dealers actually ship them. The synthetic dicts above are
# tidy — make/model/trim and nothing else — and every leak that reached a live
# page got through on a field those dicts never carry: a title with the box
# length in it, a body_style, a trim word that is another maker's nameplate.
# ---------------------------------------------------------------------------

#: Live row for car 162354. Renders the 11,580 lb tow rating as a curb weight
#: and the TRX's 4.9 s, because "Express" matched the Chevrolet Express van and
#: the title's "5 7 Box" ran into body_style "Truck" to read as a box truck.
RAM_1500_EXPRESS = {
    "year": 2026, "make": "RAM", "model": "1500", "trim": "Express",
    "title": "2026 RAM 1500 Express 4x4 Crew Cab 5 7 Box",
    "body_style": "Truck", "engine_description": "3.6L V6 24V VVT eTorque",
    "cylinders": 6, "fuel_type": "Gasoline",
}


def test_express_trim_on_a_1500_is_not_a_chevrolet_express_van():
    bad = implausible_extended_spec_fields(RAM_1500_EXPRESS, RAM_SCRAPED_SPECS)
    assert "curb_weight_lb" in bad
    assert "zero_to_60_sec" in bad


def test_a_box_length_in_the_title_does_not_make_a_box_truck():
    # Bed length + body_style "Truck" is what every half-ton listing looks like.
    car = {"year": 2024, "make": "Ford", "model": "F-150", "trim": "XLT",
           "title": "2024 Ford F-150 XLT 4WD SuperCrew 5.5' Box",
           "body_style": "Truck"}
    assert "curb_weight_lb" in implausible_extended_spec_fields(car, {"curb_weight_lb": 11580})


def test_v6_ram_1500_loses_the_trx_figure_and_gets_no_curated_replacement():
    # The curated table covers only the 5.7 V8, so a V6 must render blank —
    # never fall back to the nameplate value.
    car = {"year": 2025, "make": "Ram", "model": "1500", "trim": "Big Horn",
           "title": "2025 Ram 1500 Big Horn 4x4 Crew Cab 6'4\" Box",
           "body_style": "Truck", "engine_description": "3.6L V6 24V VVT",
           "cylinders": 6}
    assert "zero_to_60_sec" in implausible_extended_spec_fields(car, {"zero_to_60_sec": 4.9})
    assert curated_zero_to_60_sec(car) is None


def test_hurricane_i6_ram_1500_also_loses_the_trx_figure():
    car = {"year": 2026, "make": "RAM", "model": "1500", "trim": "Laramie",
           "title": "2026 RAM 1500 Laramie 4x4 Crew Cab 5'7\" Box",
           "body_style": "Truck",
           "engine_description": "3.0L I6 24V Twin Turbo Hurricane", "cylinders": 6}
    assert "zero_to_60_sec" in implausible_extended_spec_fields(car, {"zero_to_60_sec": 4.9})


def test_audi_q7_keeps_both_numbers_despite_a_curb_tow_collision():
    # The Q7 row stores 4,400 for curb AND tow. 4,400 lb is a believable curb
    # weight and 5.9 s a believable 0-60, so the collision alone must not blank
    # the page — the earlier row-wide rule cost all 20 sampled Q7s both fields.
    car = {"year": 2022, "make": "Audi", "model": "Q7", "trim": "Premium Plus 55 TFSI quattro",
           "title": "2022 Audi Q7 Premium Plus 55 TFSI quattro",
           "body_style": "SUV", "engine_description": "3.0L V6 Turbo", "cylinders": 6}
    specs = {"curb_weight_lb": 4400, "curb_weight_kg": 1996,
             "tow_capacity_lb": 4400, "zero_to_60_sec": 5.9}
    assert implausible_extended_spec_fields(car, specs) == set()


def test_real_heavy_duty_pickup_keeps_its_curb_weight():
    car = {"year": 2025, "make": "RAM", "model": "2500", "trim": "Tradesman",
           "title": "2025 RAM 2500 Tradesman 4x4 Crew Cab 6'4\" Box",
           "body_style": "Truck", "engine_description": "6.7L I6 Cummins Turbo Diesel"}
    assert implausible_extended_spec_fields(car, {"curb_weight_lb": 8480}) == set()


def test_chassis_cab_and_chevrolet_express_van_stay_exempt():
    for car in (
        {"year": 2024, "make": "RAM", "model": "3500 Chassis Cab", "trim": "Tradesman",
         "body_style": "Truck"},
        {"year": 2023, "make": "Chevrolet", "model": "Express", "trim": "Cargo Van 2500",
         "body_style": "Van"},
        {"year": 2024, "make": "Ford", "model": "F-350 Super Duty", "trim": "Lariat",
         "body_style": "Truck"},
    ):
        assert implausible_extended_spec_fields(car, {"curb_weight_lb": 8480}) == set()


def test_sub_four_second_rule_fires_without_a_horsepower_column():
    # The rows that reached live pages carried NO horsepower — knowledge_engine
    # strips a family-wide value — and sedans are not heavy bodies, so a rule
    # that needed either of those was unreachable.
    for car in (
        {"year": 2016, "make": "Honda", "model": "Accord", "trim": "Sport",
         "title": "2016 Honda Accord Sport CVT", "body_style": "Sedan",
         "engine_description": "2.4L I4", "cylinders": 4},
        {"year": 2023, "make": "BMW", "model": "3 Series", "trim": "330i xDrive",
         "body_style": "Sedan", "engine_description": "2.0L I4 Turbo"},
        {"year": 2023, "make": "Mercedes-Benz", "model": "S-Class", "trim": "S 500 4MATIC",
         "body_style": "Sedan", "engine_description": "3.0L I6 Turbo"},
    ):
        assert "zero_to_60_sec" in implausible_extended_spec_fields(car, {"zero_to_60_sec": 3.7})


def test_plug_in_hybrid_does_not_inherit_the_bev_exemption():
    # An S 580e is 4.4 s and a 750e 4.9 s; both were the last cars still
    # quoting their nameplate's halo figure, kept alive only by "it plugs in".
    for car in (
        {"year": 2025, "make": "Mercedes-Benz", "model": "S-Class", "trim": "S 580e",
         "fuel_type": "Plug-In Hybrid", "body_style": "Sedan"},
        {"year": 2026, "make": "BMW", "model": "7 Series", "trim": "750e xDrive",
         "fuel_type": "Plug-In Hybrid", "body_style": "Sedan"},
    ):
        assert "zero_to_60_sec" in implausible_extended_spec_fields(car, {"zero_to_60_sec": 3.6})


def test_bmw_xm_keeps_its_figure():
    # The one plug-in hybrid with no numeric badge that really does run sub-4.
    car = {"year": 2024, "make": "BMW", "model": "XM", "trim": "Label Red",
           "fuel_type": "Plug-In Hybrid", "body_style": "SUV"}
    assert "zero_to_60_sec" not in implausible_extended_spec_fields(car, {"zero_to_60_sec": 3.6})


def test_pure_bev_still_keeps_a_two_second_figure():
    car = {"year": 2018, "make": "Tesla", "model": "Model S", "trim": "75D AWD",
           "fuel_type": "Electric", "body_style": "Sedan"}
    assert implausible_extended_spec_fields(car, {"zero_to_60_sec": 2.1}) == set()


# ===========================================================================
# NO AI-GENERATED NUMBER MAY REACH A SHOPPER  (2026-08-02)
#
# ``ai_model_specs`` (2,147 rows) and ``ai_engine_specs`` (888 rows) each have
# exactly ONE distinct ``source_host`` — 'ai-research' and 'ai-engine-research'.
# There is no good row in either table, so the only safe wiring is none.
#
# The defect these pin: a 2001 Mercedes-Benz SLK Kompressor rendered 185 hp /
# 200 lb-ft / 3,000 lb with ZERO epa_extended_specs rows for (2001,
# mercedes-benz, slk); ai_model_specs held exactly 185/200/3000 under
# source_host='ai-research'.
# ===========================================================================

_PRODUCTION_ROOTS = (
    "backend/api", "backend/catalog", "backend/db", "backend/enrichment",
    "backend/intelligence", "backend/parsers", "backend/scanner",
    "backend/utils", "backend/vision", "backend/main.py",
)


def _grep_production(pattern: str) -> list[str]:
    """Lines in production code matching *pattern* (tests/scripts excluded)."""
    import re as _re
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    rx = _re.compile(pattern)
    hits: list[str] = []
    for rel in _PRODUCTION_ROOTS:
        target = root / rel
        files = [target] if target.is_file() else sorted(target.rglob("*.py"))
        for path in files:
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            for lineno, line in enumerate(text.splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith("#") or not rx.search(line):
                    continue
                hits.append(f"{path.relative_to(root)}:{lineno}: {stripped}")
    return hits


def test_ai_spec_merge_has_no_production_caller():
    """``_merge_ai_model_specs`` / ``_merge_ai_model_specs_normed`` stay unwired.

    They survive only because ``backend/tests/test_ai_model_specs_merge.py``
    exercises them directly. If a call site reappears, ai_model_specs is back on
    the car page and in the AI chat prompt.
    """
    hits = _grep_production(r"_merge_ai_model_specs(_normed)?\s*\(")
    hits = [h for h in hits if "def _merge_ai_model_specs" not in h]  # the defs themselves
    assert hits == [], "ai_model_specs merged back into a read path:\n" + "\n".join(hits)


def test_ai_engine_specs_has_no_production_caller():
    """The serializer's engine-level hp/torque/tow/0-60 override stays removed."""
    hits = _grep_production(r"lookup_engine_specs\s*\(")
    hits = [h for h in hits if "def lookup_engine_specs" not in h]
    assert hits == [], "ai_engine_specs read back into a render path:\n" + "\n".join(hits)


def test_ai_spec_tables_are_not_queried_by_production_code():
    """No SQL anywhere in production names either AI table."""
    hits = _grep_production(r"FROM\s+ai_(model|engine)_specs")
    hits = [
        h for h in hits
        if "knowledge_engine.py" not in h  # the two retired helpers, unwired above
    ]
    assert hits == [], "a production query reads an AI spec table:\n" + "\n".join(hits)


def test_curb_weight_and_tow_are_never_serialized():
    """Blank beats invented: neither column has a per-trim source.

    tow_capacity_lb: 9,864 rows over 2,037 (year, make, model) groups and ZERO
    of those groups hold more than one distinct value — one rating per
    nameplate, stamped on every trim.
    curb_weight_lb: 2,134 of 23,803 rows are under 2,500 lb; every 2023-2026
    Mazda CX-50 row stores 2,000 for curb weight AND tow capacity (2,000 is the
    tow rating; the car weighs ~3,700).
    """
    from backend.utils.car_serialize import serialize_car_for_api

    out = serialize_car_for_api(
        {"year": 2023, "make": "Ram", "model": "1500", "trim": "Limited",
         "fuel_type": "Gasoline"},
        include_verified=False,
        verified_specs={"curb_weight_lb": 5100, "tow_capacity_lb": 11580},
    )
    assert out["curb_weight_lb"] is None
    assert out["tow_capacity_lb"] is None


def test_merge_verified_specs_blanks_the_unsourceable_fields():
    from backend.enrichment.knowledge_engine_specs import (
        BLANK_EXTENDED_SPEC_FIELDS,
        merge_verified_specs,
    )

    vs = merge_verified_specs(
        {"year": 2001, "make": "Mercedes-Benz", "model": "SLK", "trim": "Kompressor",
         "fuel_type": "Gasoline"}
    )
    for field in BLANK_EXTENDED_SPEC_FIELDS:
        assert vs.get(field) is None, f"{field} rendered a value with no per-trim source"


def test_sourced_extended_specs_returns_nothing_when_the_gate_cannot_run():
    """Unknown provenance is not permission. A blown lookup renders nothing."""
    from backend.enrichment import knowledge_engine_specs as kes

    assert kes.sourced_extended_specs({}) == {}
    assert kes.sourced_extended_specs(None) == {}


def test_sourced_extended_specs_only_returns_the_three_admitted_fields(monkeypatch):
    """Whatever the attribution gate hands back, only hp/torque/0-60 escape."""
    from backend.enrichment import generated_spec_sheet as gss
    from backend.enrichment import knowledge_engine_specs as kes

    monkeypatch.setattr(
        gss,
        "_attributable_extended_specs",
        lambda car: {
            "horsepower": {"value": 203, "source_url": "https://x/", "page_title": "2023 Camry"},
            "battery_kwh": {"value": 100.0, "source_url": "https://x/", "page_title": "2023 Camry"},
            "curb_weight_lb": {"value": 2000, "source_url": "https://x/", "page_title": "2023 Camry"},
        },
    )
    out = kes.sourced_extended_specs({"year": 2023, "make": "Toyota", "model": "Camry"})
    assert out == {"horsepower": 203.0}


def test_merge_verified_specs_does_not_hand_the_ai_chat_agent_a_generated_spec(monkeypatch):
    """The AI chat prompt reads ``merge_verified_specs`` output DIRECTLY.

    ``backend/intelligence/ai/agent.py`` json.dumps this dict into block (4) via
    ``prepare_car_detail_context(car)["verified_specs"]``, so a suppression
    written only in ``car_serialize`` never reaches it. This is the path a
    previous pass missed.
    """
    from backend.enrichment import knowledge_engine_specs as kes

    monkeypatch.setattr(kes, "sourced_extended_specs", lambda car: {})
    vs = kes.merge_verified_specs(
        {"year": 2001, "make": "Mercedes-Benz", "model": "SLK", "trim": "Kompressor",
         "fuel_type": "Gasoline"}
    )
    for field in ("horsepower", "torque_lb_ft", "curb_weight_lb", "zero_to_60_sec",
                  "tow_capacity_lb", "battery_kwh"):
        assert vs.get(field) is None, f"{field} would be printed into the chat prompt"
