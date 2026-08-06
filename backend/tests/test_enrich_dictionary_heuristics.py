"""Heuristics for enrich_from_dictionary / color extraction.

The color tests used to be gated on importing a legacy top-level
``extract_keffer_colors`` script that no longer exists anywhere, so all three
were permanently skipped on every machine — a gate that could never open. The
function is now vendored in ``backend.dictionary.color_extract`` (the module
``enrich_from_dictionary`` actually calls), so they run unconditionally.
"""

from __future__ import annotations

from backend.dictionary import enrich_from_dictionary as efd
from backend.dictionary.color_extract import extract_color_from_description


def test_combined_listing_text_includes_title_for_transmission():
    car = {
        "title": "2024 Ford F-150 XLT 10-Speed Automatic",
        "trim": "",
        "description": "Clean truck.",
    }
    assert "10-Speed" in efd._combined_listing_text(car)
    t = efd._extract_transmission_from_description(efd._combined_listing_text(car))
    assert "10" in t and "Automatic" in t


def test_extract_transmission_cvt_in_title():
    text = "Used 2022 Honda Civic LX CVT\nLow miles."
    assert efd._extract_transmission_from_description(text).upper().find("CVT") >= 0


def test_extract_color_labeled_exterior():
    body = "Features\nExterior color: Agate Black Metallic\nNice ride."
    assert extract_color_from_description(body) and "Agate" in (extract_color_from_description(body) or "")


def test_extract_color_compound_magnetic():
    text = "2023 Explorer Magnetic Gray Metallic AWD"
    c = extract_color_from_description(text)
    assert c and "Magnetic" in c


def test_extract_color_avoids_bare_black_when_pearlcoat_present():
    text = "Paint: Pearl White Tri-Coat"
    c = extract_color_from_description(text)
    assert c and "Pearl" in c
