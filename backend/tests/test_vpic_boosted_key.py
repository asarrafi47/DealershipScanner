"""P10C.1: vPIC Turbo flag is exposed as ``boosted`` (Yes/No/other)."""
import pytest

from backend.enrichment.knowledge_engine import _empty_vpic, _normalize_vpic_response


def test_empty_vpic_has_boosted_none():
    assert _empty_vpic()["boosted"] is None


@pytest.mark.parametrize(
    "raw,expected",
    [("Yes", True), ("yes", True), (" YES ", True), ("No", False), ("no", False)],
)
def test_turbo_yes_no(raw, expected):
    assert _normalize_vpic_response({"Turbo": raw})["boosted"] is expected


@pytest.mark.parametrize("raw", ["", None, "Not Applicable", "maybe"])
def test_turbo_blank_or_other_is_none(raw):
    assert _normalize_vpic_response({"Turbo": raw})["boosted"] is None


def test_missing_turbo_key_is_none():
    assert _normalize_vpic_response({})["boosted"] is None
