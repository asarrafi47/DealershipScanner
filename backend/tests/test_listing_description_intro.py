"""Tests for dealer description intro stripping and feature clause splitting."""
from backend.utils.listing_description_extract import (
    _split_feature_clauses,
    extract_listing_description,
    strip_dealer_description_intro,
)


def test_strip_dealer_intro_sentence() -> None:
    raw = (
        "This 2023 Mineral White Metallic BMW i4 eDrive40 RWD is well equipped and "
        "includes these features and benefits. Connected Package Pro"
    )
    out = strip_dealer_description_intro(raw)
    assert "well equipped" not in out.lower()
    assert out.startswith("Connected Package Pro")


def test_split_feature_clauses_respects_parentheses() -> None:
    text = (
        "Premium Package (Ambient Lighting, Heated Front Seats, Heated Steering Wheel "
        "and Iconic Sounds Electric), Apple CarPlay Compatibility"
    )
    parts = _split_feature_clauses(text)
    assert any("Premium Package" in p and "Ambient Lighting" in p for p in parts)
    assert any("Apple CarPlay" in p for p in parts)
    assert not any(p.strip() == "Heated Front Seats" for p in parts)


def test_extract_listing_description_drops_intro() -> None:
    parsed = extract_listing_description(
        "This 2024 BMW X3 is well equipped and includes these features and benefits. "
        "Panoramic Moonroof, Heated Front Seats, Apple CarPlay Compatibility."
    )
    feats = parsed.get("standalone_features") or []
    assert feats
    assert not any("well equipped" in f.lower() for f in feats)
