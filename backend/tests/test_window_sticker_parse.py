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
    assert parsed["engine_display"] == "3.5L V6"
    assert parsed.get("drivetrain") == "4WD"
    assert any("XLT Black Appearance Package" in o for o in parsed["options"])
    assert parsed["sticker_specs"]["Engine"] == "3.5L V6"


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
