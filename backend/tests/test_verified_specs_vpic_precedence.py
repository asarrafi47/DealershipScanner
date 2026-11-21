"""P8A.6: read-time drivetrain follows the vPIC rule (targeted fixtures, no full render)."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.enrichment.verified_specs.drivetrain import drivetrain_display, resolve_drivetrain


def _src(dealer="", vpic=None, epa=None, regex=None, dict_specs=None):
    return SimpleNamespace(
        regex=regex or {}, epa=epa or {}, vpic={"drivetrain": vpic} if vpic else {},
        dict_specs=dict_specs, dealer_drive=dealer, title="", trim="", model="", make="Ford",
    )


def test_stored_4wd_vpic_4wd_epa_rwd_gives_4wd():
    assert resolve_drivetrain(_src(dealer="4WD", vpic="4WD", epa={"drivetrain": "RWD"})) == "4WD"


def test_blank_dealer_vpic_4wd_epa_rwd_gives_4wd():
    assert resolve_drivetrain(_src(dealer="", vpic="4WD", epa={"drivetrain": "RWD"})) == "4WD"


def test_dealer_rwd_vpic_4wd_gives_4wd():
    assert resolve_drivetrain(_src(dealer="RWD", vpic="4WD")) == "4WD"


def test_regex_fwd_vpic_awd_gives_awd_even_when_dealer_agrees_with_regex():
    assert resolve_drivetrain(_src(dealer="FWD", vpic="AWD", regex={"drivetrain": "FWD"})) == "AWD"


def test_vpic_none_unchanged():
    assert resolve_drivetrain(_src(dealer="RWD", epa={"drivetrain": "RWD"})) == "RWD"
    assert resolve_drivetrain(_src(dealer="", epa={"drivetrain": "FWD"})) == "FWD"


@pytest.mark.parametrize("raw", ["4x2", "2WD"])
def test_vpic_two_wheel_never_overrides_or_maps_to_an_end(raw):
    assert resolve_drivetrain(_src(dealer="RWD", vpic=raw, epa={"drivetrain": "RWD"})) == "RWD"
    assert resolve_drivetrain(_src(dealer="", vpic=raw)) == raw  # only fills blank, as before


def test_awd_and_4wd_same_wheels_keep_the_cascade_value():
    assert resolve_drivetrain(_src(dealer="AWD", vpic="4WD", epa={"drivetrain": "AWD"})) == "AWD"


def test_display_shows_vpic_value():
    s = _src(dealer="RWD", vpic="4WD")
    label, _ = drivetrain_display(s, resolve_drivetrain(s))
    assert "4" in label and "Rear" not in label
