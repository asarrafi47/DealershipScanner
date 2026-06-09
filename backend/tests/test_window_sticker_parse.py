"""Regex Monroney parsing (CDJR / Ford) without Claude."""

from __future__ import annotations

from backend.scanner.window_sticker import (
    oem_sticker_parsing_skip_claude,
    parse_sticker_from_text,
)


JEEP_RUBICON_392 = """
2021 MODEL YEAR
WRANGLER UNLIMITED RUBICON 392
Functional/Safety Features
6.4L V8 HEMI MDS VVT Engine
8-Speed Automatic 850RE Transmission
4-Door
LT285/70R17C BSW Off-Road Tires
17-Inch x 7.5-Inch Beadlock-Capable Wheels
Leather-Trimmed Bucket Seats
Exterior Color
Sting-Gray Clear-Coat Exterior Paint
Interior Color
Black Interior with Black / Dark Saddle Leather-Trimmed Bucket Seats
OPTIONAL EQUIPMENT (May Replace Standard Equipment)
Customer Preferred Package 27X ($1,595)
Trailer-Tow Package ($350)
TOTAL PRICE $77,335
"""

FORD_F150 = """
2024 F-150 4X4 SuperCrew
ENGINE 3.5L V6 EcoBoost
TRANSMISSION 10-Speed Automatic
Exterior Color
Avalanche
Interior Color
Black Int w/ Black Leather
275/65R 18 BSW All-Terrain
18" Gloss Black Wheels
OPTIONAL EQUIPMENT
XLT Black Appearance Package ($1,995)
Tow/Haul Package ($490)
TOTAL PRICE $58,950
"""

CHARGER_DAYTONA_EV = """
2024 CHARGER DAYTONA SCAT PACK AWD
Motor: 400V G2500 Front / Rear Electric–Drive Motors
Trans/Gear Box: Front / Rear Offset 1–Speed Gearbox
Exterior Color: Diamond Black Crystal Exterior Paint
Interior Color: Black Interior Color
400V 100.5 kWh Battery Pack
COUNTRY OF ORIGIN:
ENGINE: UNITED STATES
TRANSMISSION: UNITED STATES
EPA
Fuel Economy and Environment Electric Vehicle
Fuel Economy
 78
combined city/hwy
MPGe
 82
city
 73
highway
"""


def test_parse_jeep_rubicon_392_engine_and_specs() -> None:
    parsed = parse_sticker_from_text(JEEP_RUBICON_392)
    assert parsed["engine_display"] == "6.4L V8"
    assert parsed["engine_l"] == 6.4
    assert parsed["cylinders"] == 8
    assert "LT285/70R17C" in (parsed.get("tires") or "")
    assert parsed.get("doors") == "4-door"
    assert any("Customer Preferred Package 27X" in o for o in parsed["options"])
    assert parsed["sticker_specs"]["Engine"] == "6.4L V8"


def test_parse_ford_f150_engine_and_options() -> None:
    parsed = parse_sticker_from_text(FORD_F150)
    assert parsed["engine_display"] == "3.5L V6 Turbo"
    assert parsed.get("drivetrain") == "4WD"
    assert any("XLT Black Appearance Package" in o for o in parsed["options"])
    assert parsed["sticker_specs"]["Engine"] == "3.5L V6 Turbo"


CDJR_2_0_TURBO = """
2024 JEEP WRANGLER 4XE SAHARA
Engine: 2.0L I4 DOHC 16V DI Turbo Engine
TRANSMISSION: 8-Speed Automatic 850RE
TOTAL PRICE $52,450
"""

CDJR_HURRICANE = """
2025 JEEP GRAND CHEROKEE L SUMMIT
Motor: 3.0L I6 Hurricane Twin Turbo HO Engine
Trans/Gear Box: 8-Speed Automatic 850RE Transmission
TOTAL PRICE $68,200
"""

HEMI_SUPERCHARGED = """
2023 DODGE CHALLENGER SRT HELLCAT
Engine: 6.2L Supercharged HEMI V8 SRT Engine
TRANSMISSION: 8-Speed Automatic
TOTAL PRICE $82,500
"""


def test_parse_cdjr_2_0_turbo_engine() -> None:
    parsed = parse_sticker_from_text(CDJR_2_0_TURBO)
    assert parsed["engine_display"] == "2.0L I4 Turbo"
    assert parsed["sticker_specs"]["Engine"] == "2.0L I4 Turbo"


def test_parse_cdjr_hurricane_twin_turbo_engine() -> None:
    parsed = parse_sticker_from_text(CDJR_HURRICANE)
    assert parsed["engine_display"] == "3.0L I6 Twin Turbo"
    assert parsed["sticker_specs"]["Engine"] == "3.0L I6 Twin Turbo"


def test_parse_supercharged_hemi_not_labeled_turbo() -> None:
    parsed = parse_sticker_from_text(HEMI_SUPERCHARGED)
    assert parsed["engine_display"] == "6.2L V8"
    assert "Turbo" not in (parsed.get("engine_display") or "")


def test_parse_charger_daytona_ev_from_motor_not_country_of_origin() -> None:
    parsed = parse_sticker_from_text(CHARGER_DAYTONA_EV)
    assert parsed["fuel_type"] == "Electric"
    assert parsed["cylinders"] == 0
    assert "Electric" in (parsed.get("engine_display") or "")
    assert parsed["sticker_specs"]["Engine"] != "UNITED STATES"
    assert parsed["mpg_city"] == 82
    assert parsed["mpg_highway"] == 73
    assert "Offset" in (parsed.get("transmission") or "")


def test_dodge_charger_daytona_is_bev() -> None:
    from backend.scanner.window_sticker import dodge_charger_daytona_is_bev

    assert dodge_charger_daytona_is_bev(
        {"make": "Dodge", "model": "Charger Daytona", "trim": "Scat Pack", "year": 2024}
    ) is True
    assert dodge_charger_daytona_is_bev(
        {"make": "Dodge", "model": "Charger", "trim": "R/T", "year": 2026}
    ) is False


RAM_ETORQUE_HEMI = """
2022 RAM 1500 BIG HORN
Motor: 5.7L V8 HEMI MDS VVT Engine with eTorque
Trans/Gear Box: 8-Speed Automatic 850RE Transmission
STANDARD EQUIPMENT
5.7L V8 HEMI MDS VVT Engine with eTorque
OPTIONAL EQUIPMENT
Big Horn Level 2 Equipment Group ($1,995)
TOTAL PRICE $52,450
"""

RAM_ETORQUE_V6 = """
2021 RAM 1500 TRADESMAN
Engine: 3.6L V6 24V VVT Engine with Stop/Start and eTorque
TRANSMISSION: 8-Speed Automatic TorqueFlite
TOTAL PRICE $41,200
"""


def test_parse_ram_etorque_hemi_mild_hybrid() -> None:
    parsed = parse_sticker_from_text(RAM_ETORQUE_HEMI)
    assert parsed["engine_display"] == "5.7L V8 Mild Hybrid"
    assert parsed["fuel_type"] == "Hybrid"
    assert parsed["sticker_specs"]["Engine"] == "5.7L V8 Mild Hybrid"


def test_parse_ram_etorque_v6_mild_hybrid() -> None:
    parsed = parse_sticker_from_text(RAM_ETORQUE_V6)
    assert parsed["engine_display"] == "3.6L V6 Mild Hybrid"
    assert parsed["fuel_type"] == "Hybrid"


def test_oem_skip_claude_for_cdjr_and_ford() -> None:
    assert oem_sticker_parsing_skip_claude("1C4RJHBG9SC340097") is True
    assert oem_sticker_parsing_skip_claude("1FAFP45F62F123456") is True
    assert oem_sticker_parsing_skip_claude("WBA3A5C50FD123456") is False


def test_oem_hide_photo_analysis() -> None:
    from backend.scanner.window_sticker import oem_hide_photo_analysis

    assert oem_hide_photo_analysis("1C4JJXSJ5MW755034", "Jeep") is True
    assert oem_hide_photo_analysis("1FAFP45F62F123456", "Ford") is True
    assert oem_hide_photo_analysis(None, "Ford") is True
    assert oem_hide_photo_analysis("WBA3A5C50FD123456", "BMW") is False


def test_sticker_boilerplate_lines_filtered_from_display() -> None:
    from backend.scanner.window_sticker import is_sticker_boilerplate_line, sticker_options_for_display

    junk = [
        "MODEL YEAR COMPASS LATITUDE 4X4",
        "TO: SOLDTO",
        "LABEL IS ADDED TO THIS VEHICLE TO COMPLY WITH FEDERAL LAW. THE LABEL CANNOT BE REMOVED",
        "MODEL INCLUDING DEALER PREPARATION Base Price",
    ]
    for line in junk:
        assert is_sticker_boilerplate_line(line) is True

    pkg = {
        "sticker_options": [
            {"name": "Customer Preferred Package 29J", "price": 1595},
            {"name": junk[0], "price": None},
        ]
    }
    out = sticker_options_for_display(pkg)
    names = [x.get("name") for x in out]
    assert "Customer Preferred Package 29J" in names
    assert junk[0] not in names


def test_sticker_factory_code_ur019_filtered() -> None:
    from backend.scanner.window_sticker import (
        _is_sticker_factory_code_noise,
        parse_sticker_option_items,
        sticker_options_for_display,
    )

    assert _is_sticker_factory_code_noise("019", "UR") is True
    assert _is_sticker_factory_code_noise("UR019") is True
    assert _is_sticker_factory_code_noise("Customer Preferred Package 29J") is False

    ipacket_blob = """
ADDED OPTIONS
UR 019
$395
Customer Preferred Package 29J
$1,595
"""
    items = parse_sticker_option_items(ipacket_blob)
    names = [i.get("name") for i in items]
    assert "Customer Preferred Package 29J" in names
    assert not any("019" == (n or "") for n in names)
    assert not any((n or "").replace(" ", "") == "UR019" for n in names)

    stored = {
        "sticker_options": [
            {"name": "019", "code": "UR", "price": 395},
            {"name": "Power Deployable Running Boards", "price": 995},
        ]
    }
    shown = sticker_options_for_display(stored)
    shown_names = [x.get("name") for x in shown]
    assert "Power Deployable Running Boards" in shown_names
    assert "019" not in shown_names
