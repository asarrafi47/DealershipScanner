"""N21.9 (FP18): text normalization at write time - make casing, a model that carries
its trim, HTML entities, and a page <meta description> stored as the car's description.
Cases are rows read from cars_cleanroom."""
from __future__ import annotations

import pytest

from backend.utils.field_clean import (
    canonical_make,
    clean_car_row_dict,
    is_seo_meta_description,
    strip_trim_from_model,
    unescape_entities,
)


@pytest.mark.parametrize("raw,want", [
    ("LEXUS", "Lexus"), ("Lexus", "Lexus"), ("CADILLAC", "Cadillac"), ("HYUNDAI", "Hyundai"),
    ("Mercedes-benz", "Mercedes-Benz"), ("MERCEDES-BENZ", "Mercedes-Benz"),
    ("Land rover", "Land Rover"), ("LAND ROVER", "Land Rover"), ("MCLAREN", "McLaren"),
    ("Brightdrop", "BrightDrop"), ("Mini", "MINI"), ("bmw", "BMW"), ("Gmc", "GMC"),
    ("RAM", "Ram"), ("Infiniti", "INFINITI"), ("Ineos", "INEOS"),
    ("Unknownmake", "Unknownmake"), (None, None),
])
def test_canonical_make(raw, want):
    assert canonical_make(raw) == want


@pytest.mark.parametrize("model,trim,want", [
    ("CR-V EX-L", "EX-L", "CR-V"),
    ("HR-V Sport", "Sport", "HR-V"),
    ("Sportage EX", "EX", "Sportage"),
    ("Accord Sedan LX", "LX", "Accord Sedan"),
    ("CR-V Hybrid Sport Touring", "Sport Touring", "CR-V Hybrid"),
    ("Ridgeline RTL", "rtl", "Ridgeline"),
    # model designations and body words stay in the model
    ("GLC 300", "300", "GLC 300"),
    ("RX 350h", "350H", "RX 350h"),
    ("Transit Cargo Van", "Cargo Van", "Transit Cargo Van"),
    ("Mustang Mach-E", "Mach-E", "Mustang Mach-E"),
    ("Wrangler Unlimited", "Unlimited", "Wrangler Unlimited"),
    # no word boundary, equal, or no trim: unchanged
    ("Tacoma TRD", "RD", "Tacoma TRD"),
    ("Sport", "Sport", "Sport"),
    ("Camry SE", None, "Camry SE"),
])
def test_strip_trim_from_model(model, trim, want):
    assert strip_trim_from_model(model, trim) == want


def test_entities():
    assert unescape_entities("Town &#038; Country Touring") == "Town & Country Touring"
    assert unescape_entities("Heated &amp; Ventilated") == "Heated & Ventilated"
    assert unescape_entities("a &amp;amp; b") == "a & b"
    assert unescape_entities("AT&T 5 < 6") == "AT&T 5 < 6"
    assert unescape_entities(None) is None


SEO = [
    "Looking for New 2026 Ford F-150? View our inventory of New Ford F-150 for sale at Duval Ford.",
    "New Honda for Sale in Vienna, VA. View our Ourisman Honda of Tysons Corner inventory to find the right vehicle to fit your style and budget!",
    "Trying to find a New Toyota Camry SE for sale in Riverside, CA? We can help! Check out our New Toyota inventory to find the exact one for you.",
]


@pytest.mark.parametrize("text", SEO)
def test_seo_meta_description_detected(text):
    assert is_seo_meta_description(text)
    assert clean_car_row_dict({"description": text})["description"] is None


def test_real_description_kept():
    real = "Certified. L-CERTIFIED, 8-Way Power Driver &amp; Front Passenger Seat Adjusters, Active Cruise Control."
    assert not is_seo_meta_description(real)
    assert clean_car_row_dict({"description": real})["description"].startswith(
        "Certified. L-CERTIFIED, 8-Way Power Driver & Front")
    assert not is_seo_meta_description("One owner, clean history. Visit our inventory page for more." * 10)


def test_real_description_with_trailing_cta_kept():
    real = "2021 Honda Civic one owner, clean Carfax. View our inventory at example.com for more."
    assert not is_seo_meta_description(real)
    assert clean_car_row_dict({"description": real})["description"] == real


def test_clean_row_end_to_end():
    r = clean_car_row_dict({
        "make": "LEXUS", "model": "RX 350h", "trim": "350H",
        "title": "Used 2016 Chrysler Town &#038; Country Touring",
        "model_full_raw": "x", "source_url": "https://x.com/a?b=1&amp;c=2",
    })
    assert r["make"] == "Lexus" and r["model"] == "RX 350h"
    assert r["title"] == "Used 2016 Chrysler Town & Country Touring"
    assert r["source_url"] == "https://x.com/a?b=1&amp;c=2"  # URLs are not text columns
    r = clean_car_row_dict({"make": "Honda", "model": "CR-V EX-L", "trim": "EX-L"})
    assert (r["model"], r["trim"]) == ("CR-V", "EX-L")
    # keys that were absent stay absent
    assert "make" not in clean_car_row_dict({"vin": "1HGBH41JXMN109186"})
