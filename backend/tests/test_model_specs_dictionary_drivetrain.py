"""Static fallbacks must not guess drivetrain for mixed-layout nameplates (B-014)."""
import pytest

from backend.enrichment import model_specs_dictionary as msd


@pytest.mark.parametrize(
    "make,model",
    [
        ("hyundai", "tucson"),
        ("toyota", "rav4"),
        ("toyota", "tacoma"),
        ("bmw", "550e"),
        ("bmw", "m5"),
        ("hyundai", "ioniq 5"),
        ("gmc", "canyon"),
    ],
)
def test_mixed_layout_fallback_has_no_drivetrain(make, model):
    assert "drivetrain" not in msd._MODEL_FALLBACK[(make, model)]


def test_single_layout_fallback_keeps_drivetrain():
    assert msd._MODEL_FALLBACK[("hyundai", "elantra")]["drivetrain"] == "FWD"
    assert msd._MODEL_FALLBACK[("jeep", "wrangler 4xe")]["drivetrain"] == "4WD"
