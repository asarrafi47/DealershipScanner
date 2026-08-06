"""
Tests for the gallery OCR layer (backend/vision/image_text.py).

The fixtures below are real text. The highlights slide is exactly what Apple Vision
returned for VIN WBS3U9C58GP969438's gallery; the sticker fixture is transcribed from
that car's actual Monroney. Using real output matters here -- the failure modes this
layer must resist (a neighbouring column bleeding into an option name, a dealer phone
number becoming "equipment", another car's sticker in the gallery) only appear in text
shaped the way OCR actually shapes it.
"""

from __future__ import annotations

from backend.vision.image_text import (
    OcrResult,
    classify_image_text,
    extract_findings,
    summarize_gallery,
)

VIN = "WBS3U9C58GP969438"
OTHER_VIN = "5UX83DP05R9U16883"


def _ocr(lines: list[str], confidence: float = 1.0) -> OcrResult:
    return OcrResult(
        path="/tmp/x.jpg",
        text="\n".join(lines),
        lines=tuple(lines),
        mean_confidence=confidence,
    )


HIGHLIGHTS = [
    "Audi Huntsville",
    "SHOP NOW @ audihuntsville.com",
    "* Questions text or call Eric Hedges",
    "256.529.2280",
    "* Navigation System",
    "* Driver Assistance Plus",
    "* Executive Package",
    "* Lighting Package",
    "* Convertible HardTop",
    "* harman/kardon Surround Sound",
    "System",
    "* Neck Warmer",
    "Vehicle Highlights",
]

STICKER = [
    "2016 BMW M4 Convertible",
    f"VIN: {VIN}",
    "Exterior: Mineral White Metallic (A96)",
    "Base Price $74,200.00",
    "Total Options $12,550.00",
    "Destination Charge $995.00",
    "Total MSRP $87,745.00",
    "EXECUTIVE PACKAGE (ZEC) $2,800.00",
    "LIGHTING PACKAGE (ZLP) $1,900.00",
    "Adaptive M Suspension $1,000.00",
    "Rear Parking Aid, Automatic Parking $500.00",
    "Heated Steering Wheel",
]


# --- classification ---------------------------------------------------------------


def test_highlights_slide_is_classified() -> None:
    assert classify_image_text(_ocr(HIGHLIGHTS)) == "highlights_slide"


def test_window_sticker_is_classified() -> None:
    assert classify_image_text(_ocr(STICKER)) == "window_sticker"


def test_plain_photo_is_not_a_document() -> None:
    assert classify_image_text(_ocr(["M Performance", "160"])) == "photo"


def test_single_sticker_word_is_not_enough() -> None:
    """One marker appears on promo overlays; a Monroney carries several."""
    lines = ["Total Price $32,000", "Great Deal", "Call Today", "Financing", "Apply", "Now"]
    assert classify_image_text(_ocr(lines)) != "window_sticker"


# --- highlights extraction --------------------------------------------------------


def test_highlights_equipment_and_packages_split() -> None:
    f = extract_findings(_ocr(HIGHLIGHTS), expected_vin=VIN)
    assert "Executive Package" in f.packages
    assert "Lighting Package" in f.packages
    assert "Navigation System" in f.equipment
    assert "Neck Warmer" in f.equipment


def test_highlights_drops_contact_and_marketing_noise() -> None:
    f = extract_findings(_ocr(HIGHLIGHTS), expected_vin=VIN)
    joined = " ".join(f.equipment + f.packages).lower()
    assert "eric hedges" not in joined
    assert "shop now" not in joined
    assert "256.529.2280" not in joined
    assert "questions" not in joined


def test_dealer_domain_is_captured_for_attribution() -> None:
    f = extract_findings(_ocr(HIGHLIGHTS), expected_vin=VIN)
    assert "audihuntsville.com" in f.dealer_domains


# --- sticker extraction -----------------------------------------------------------


def test_sticker_msrp_taken_when_vin_matches() -> None:
    f = extract_findings(_ocr(STICKER), expected_vin=VIN)
    assert f.rejected_reason is None
    assert f.msrp == 87745.0


def test_sticker_summary_rows_never_become_options() -> None:
    """Base Price / Total Options / Destination are roll-ups, not equipment."""
    f = extract_findings(_ocr(STICKER), expected_vin=VIN)
    names = " ".join(o["name"] for o in f.priced_options).lower()
    assert "base price" not in names
    assert "total msrp" not in names
    assert "total options" not in names
    assert "destination" not in names
    # No "option" may cost as much as the car.
    assert all(o["price"] < 74_200.0 for o in f.priced_options)


def test_sticker_priced_options_keep_their_prices() -> None:
    f = extract_findings(_ocr(STICKER), expected_vin=VIN)
    priced = {o["name"].lower(): o["price"] for o in f.priced_options}
    assert any("executive package" in k and v == 2800.0 for k, v in priced.items())
    assert any("adaptive m suspension" in k and v == 1000.0 for k, v in priced.items())


# --- contamination guards ---------------------------------------------------------


def test_another_cars_sticker_is_rejected_entirely() -> None:
    """A gallery image showing a different VIN yields nothing at all."""
    f = extract_findings(_ocr(STICKER), expected_vin=OTHER_VIN)
    assert f.rejected_reason is not None
    assert f.rejected_reason.startswith("vin_mismatch")
    assert f.msrp is None
    assert not f.equipment and not f.packages and not f.priced_options


def test_sticker_without_a_vin_yields_equipment_but_no_price() -> None:
    """An unverifiable MSRP is worse than none -- it feeds a fake 'below MSRP' badge."""
    lines = [ln for ln in STICKER if not ln.startswith("VIN:")]
    f = extract_findings(_ocr(lines), expected_vin=VIN)
    assert f.rejected_reason is None
    assert f.msrp is None
    assert f.packages or f.priced_options


def test_low_confidence_image_is_rejected() -> None:
    f = extract_findings(_ocr(HIGHLIGHTS, confidence=0.20), expected_vin=VIN)
    assert f.rejected_reason is not None
    assert f.rejected_reason.startswith("low_confidence")
    assert not f.equipment


def test_failed_ocr_is_rejected_not_silently_empty() -> None:
    bad = OcrResult(path="/tmp/x.jpg", text="", lines=(), mean_confidence=0.0, error="decode_failed")
    f = extract_findings(bad, expected_vin=VIN)
    assert f.rejected_reason == "decode_failed"


# --- regressions from the first real gallery run -----------------------------------
#
# Every fixture below is text that a first pass over Hiley VW's inventory actually
# turned into a bogus "priced option". The originals passed the idealised sticker tests
# above, which is precisely why these exist: clean fixtures proved nothing about OCR
# output shaped the way real Monroneys are shaped.

MAZDA_STICKER = [
    "MAZDA CX-30 2.5 S Select 2023",
    "VIN: 3MVDMBBM6PM 561312",
    "GVWR: 4,345 lbs (1,971 kg)",
    "Standard Equipment",
    "Packages",
    "WEATHER PACKAGE (1WP) $1,100.00",
    "Total MSRP $26,415.00",
    "Destination Charge $1,275.00",
    "EPA Fuel Economy 26 MPG",
]


def test_model_year_is_not_a_price() -> None:
    """'MAZDA CX-30 2.5 S Select 2023' must not yield a $2,023 option."""
    f = extract_findings(_ocr(MAZDA_STICKER), expected_vin="3MVDMBBM6PM561312")
    assert all(o["price"] != 2023.0 for o in f.priced_options)


def test_gvwr_weight_is_not_a_price() -> None:
    f = extract_findings(_ocr(MAZDA_STICKER), expected_vin="3MVDMBBM6PM561312")
    names = " ".join(o["name"].lower() for o in f.priced_options)
    assert "gvwr" not in names
    assert all(o["price"] != 4345.0 for o in f.priced_options)


def test_vin_line_is_not_an_option() -> None:
    f = extract_findings(_ocr(MAZDA_STICKER), expected_vin="3MVDMBBM6PM561312")
    joined = " ".join(o["name"].lower() for o in f.priced_options) + " ".join(f.equipment).lower()
    assert "vin" not in joined


def test_bare_section_headers_are_not_packages() -> None:
    """The word 'Packages' is a heading, not the name of a package."""
    f = extract_findings(_ocr(MAZDA_STICKER), expected_vin="3MVDMBBM6PM561312")
    assert not any(p.strip().lower() in {"package", "packages"} for p in f.packages)
    assert "Standard Equipment" not in f.equipment


def test_mpg_line_is_not_a_price() -> None:
    f = extract_findings(_ocr(MAZDA_STICKER), expected_vin="3MVDMBBM6PM561312")
    assert all("mpg" not in o["name"].lower() for o in f.priced_options)


def test_split_vin_still_confirms_the_car() -> None:
    """OCR wraps VIN blocks; a strict match discarded every real sticker's MSRP."""
    f = extract_findings(_ocr(MAZDA_STICKER), expected_vin="3MVDMBBM6PM561312")
    assert f.rejected_reason is None
    assert f.msrp == 26415.0


def test_real_priced_option_survives_the_tightened_parser() -> None:
    """Tightening must not cost us the genuine ones."""
    f = extract_findings(_ocr(MAZDA_STICKER), expected_vin="3MVDMBBM6PM561312")
    priced = {o["name"].lower(): o["price"] for o in f.priced_options}
    assert any("weather package" in k and v == 1100.0 for k, v in priced.items())


# --- summarisation ----------------------------------------------------------------


def test_summary_reports_rejections_rather_than_hiding_them() -> None:
    good = extract_findings(_ocr(HIGHLIGHTS), expected_vin=VIN, url="a.jpg")
    bad = extract_findings(_ocr(STICKER), expected_vin=OTHER_VIN, url="b.jpg")
    s = summarize_gallery([good, bad])
    assert s["images_read"] == 1
    assert len(s["images_rejected"]) == 1
    assert s["images_rejected"][0]["url"] == "b.jpg"
    assert "vin_mismatch" in s["images_rejected"][0]["reason"]


def test_summary_dedupes_equipment_across_slides() -> None:
    a = extract_findings(_ocr(HIGHLIGHTS), expected_vin=VIN, url="a.jpg")
    b = extract_findings(_ocr(HIGHLIGHTS), expected_vin=VIN, url="b.jpg")
    s = summarize_gallery([a, b])
    assert len(s["equipment"]) == len(set(x.lower() for x in s["equipment"]))


def test_summary_carries_sticker_msrp_and_source_url() -> None:
    f = extract_findings(_ocr(STICKER), expected_vin=VIN, url="sticker.jpg")
    s = summarize_gallery([f])
    assert s["sticker_msrp"] == 87745.0
    assert s["sticker_image_urls"] == ["sticker.jpg"]


# --- image budget -------------------------------------------------------------------


def test_capped_gallery_samples_the_whole_range() -> None:
    """
    Documents are interleaved, not appended: on the gallery that started this work the
    highlights slides sat at 6, 12 and 18 of 40. Head-only and head+tail both missed them.
    """
    from backend.vision.image_text import _select_images

    urls = [f"img{i}" for i in range(40)]
    picked = _select_images(urls, 20)
    assert len(picked) == 20
    assert picked[0] == "img0" and picked[-1] == "img39"   # both ends kept
    middle = {f"img{i}" for i in (6, 12, 18)}
    assert middle & set(picked), "a stride sample must reach the middle of the gallery"


def test_small_gallery_is_untouched() -> None:
    from backend.vision.image_text import _select_images

    urls = [f"img{i}" for i in range(5)]
    assert _select_images(urls, 12) == urls
