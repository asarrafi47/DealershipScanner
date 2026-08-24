"""Inventory rarity score — scarcity math and vision-evidence signals."""
from __future__ import annotations

from collections import Counter

from backend.utils.rarity_score import rarity_for_car


def _counts(model=None, trim=None, color=None, color_known=None):
    return {
        "model": Counter(model or {}),
        "trim": Counter(trim or {}),
        "color": Counter(color or {}),
        "color_known": Counter(color_known or {}),
    }


def test_one_of_a_kind_model_scores_high():
    counts = _counts(model={("lamborghini", "urus"): 1})
    out = rarity_for_car({"make": "Lamborghini", "model": "Urus"}, counts=counts)
    assert out is not None
    assert out["score"] >= 3.0
    assert any("Only one" in r for r in out["reasons"])


def test_common_model_common_trim_scores_low():
    counts = _counts(
        model={("toyota", "camry"): 200},
        trim={("toyota", "camry", "se"): 90},
        color={("toyota", "camry", "white"): 60},
        color_known={("toyota", "camry"): 180},
    )
    out = rarity_for_car(
        {"make": "Toyota", "model": "Camry", "trim": "SE", "exterior_color": "White"},
        counts=counts,
    )
    assert out is not None
    assert out["score"] < 2.0
    assert out["label"] == "Common"


def test_rare_trim_and_color_add_up():
    counts = _counts(
        model={("bmw", "m3"): 40},
        trim={("bmw", "m3", "cs"): 1},
        color={("bmw", "m3", "green"): 1},
        color_known={("bmw", "m3"): 35},
    )
    out = rarity_for_car(
        {"make": "BMW", "model": "M3", "trim": "CS", "exterior_color": "Isle of Man Green"},
        counts=counts,
    )
    assert out is not None
    # 40 in fleet (0 model pts) + rarest trim (2) + rarest color family (2)
    assert out["score"] >= 4.0
    assert any("trim" in r.lower() for r in out["reasons"])


def test_vision_evidence_boosts_score():
    counts = _counts(model={("bmw", "m4"): 40})
    base = rarity_for_car({"make": "BMW", "model": "M4"}, counts=counts)
    boosted = rarity_for_car(
        {"make": "BMW", "model": "M4"},
        vision_summary={
            "equipment": [
                "BMW Individual plaque on door sill",
                "Carbon fiber roof",
                "harman/kardon speaker grilles",
            ],
            "priced_options": [{"name": f"opt{i}", "price": 500} for i in range(9)],
        },
        counts=counts,
    )
    assert boosted is not None and base is not None
    assert boosted["score"] >= base["score"] + 3.0  # 1.5 + 0.5 + 0.5 + 1.0
    assert any("Special-order" in r for r in boosted["reasons"])
    assert any("premium audio" in r.lower() for r in boosted["reasons"])


def test_wrap_counts_modestly_and_never_as_color():
    counts = _counts(model={("tesla", "model y"): 60})
    out = rarity_for_car(
        {"make": "Tesla", "model": "Model Y"},
        vision_summary={"notes": "aftermarket wrap over factory paint"},
        counts=counts,
    )
    assert out is not None
    assert any("wrap" in r.lower() for r in out["reasons"])


def test_unscoreable_without_make_model():
    assert rarity_for_car({"make": "", "model": ""}, counts=_counts()) is None
