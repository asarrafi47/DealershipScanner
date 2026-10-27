"""EPA-flag induction mapper (P10A.1)."""
from __future__ import annotations

import pytest

from backend.utils.forced_induction import (
    NA_LABEL,
    SUPERCHARGED,
    TURBO,
    TWIN_TURBO,
    apply_forced_induction_to_engine_display,
    epa_twin_hint,
    forced_induction_from_epa,
    forced_induction_short_suffix,
    has_forced_induction,
)


def _fi(t="", s="", dscr="", model="Camry", atv="", **kw):
    return forced_induction_from_epa(t, s, dscr, model, atv, **kw)


def test_flags():
    assert _fi(t="T") == TURBO
    assert _fi(s="S") == SUPERCHARGED
    assert _fi(t="T", s="S") == TURBO  # D-ET3


def test_blank_ice_is_na_blank_ev_is_empty():
    assert _fi() == NA_LABEL
    assert _fi(atv="EV") == ""
    assert _fi(t="T", atv="EV") == ""  # rule 1 outranks flags


@pytest.mark.parametrize(
    "dscr,expected",
    [
        ("DSL,TRBO", TURBO),
        ("(FFS,TRBO)", TURBO),
        ("(GUZZLER) TRBO)", TURBO),
        ("(FFS) (S-CHARGE)", SUPERCHARGED),
        ("SIDI, S/C", SUPERCHARGED),
        ("(FFS) T/C", TURBO),
        ("(FFS)", NA_LABEL),
    ],
)
def test_eng_dscr_markers(dscr, expected):
    assert _fi(dscr=dscr) == expected


def test_model_badge():
    assert _fi(model="C230 Kompressor") == SUPERCHARGED
    assert _fi(model="Golf Turbo") == TURBO
    assert _fi(model="E-Class Biturbo") == TWIN_TURBO


def test_slash_list_badge_ignored():
    assert _fi(model="240 DL/GL/Turbo") == NA_LABEL


def test_ev_turbo_badge_is_empty():
    assert _fi(model="Taycan Turbo", atv="EV") == ""


def test_twin_helper_and_ecoboost():
    # 2013 F-150 3.5 EcoBoost: EPA says T, classifier silent pre-2016, label decides
    hint = epa_twin_hint(None, "Twin Turbocharged")
    assert hint is True
    assert _fi(t="T", model="F150 Pickup 2WD", twin_hint=hint) == TWIN_TURBO
    assert epa_twin_hint("Turbocharged", "Twin Turbocharged") is False
    assert epa_twin_hint("Twin Turbocharged", None) is True
    assert epa_twin_hint(None, None) is False
    assert _fi(s="S", twin_hint=True) == SUPERCHARGED


def test_has_forced_induction():
    assert not has_forced_induction(NA_LABEL)
    assert not has_forced_induction("")
    assert not has_forced_induction(None)
    assert has_forced_induction(TURBO)


def test_suffix_helpers_add_nothing_for_na():
    assert forced_induction_short_suffix(NA_LABEL) == ""
    assert apply_forced_induction_to_engine_display(
        "2.0L I4", {"forced_induction": NA_LABEL, "make": "Subaru", "model": "WRX"}
    ) == "2.0L I4"
