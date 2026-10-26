"""SEC-A-038: add-on fee description cap and non-finite amount rejection."""
from __future__ import annotations

import pytest

from backend.reviews.guards import ADDON_DESC_MAX, validate_review


def _p(**kw):
    base = {"rating": "4", "body": "Decent visit overall, fair price.",
            "addon_fee_reported": True, "addon_fee_desc": "paint protection",
            "addon_fee_amount": "995"}
    base.update(kw)
    return base


def test_valid_fee_ok():
    assert validate_review(_p())[0]


@pytest.mark.parametrize("amt", ["nan", "inf", "-inf", "NaN"])
def test_non_finite_amount_rejected(amt):
    ok, _ = validate_review(_p(addon_fee_amount=amt))
    assert not ok


def test_desc_length_cap():
    assert not validate_review(_p(addon_fee_desc="x" * (ADDON_DESC_MAX + 1)))[0]


def test_desc_url_and_profanity_rejected():
    assert not validate_review(_p(addon_fee_desc="see www.spam.com"))[0]
    assert not validate_review(_p(addon_fee_desc="what a shit fee"))[0]
