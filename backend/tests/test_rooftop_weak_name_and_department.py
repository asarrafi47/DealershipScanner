"""Rooftop gate, 2026-09-28: weak name tiers never un-list; department rooftops fold into their store."""
from __future__ import annotations

from backend.parsers import resolve_rooftop_attribution


def _vin(i: int) -> str:
    return f"1FTFW1E5{i:09d}"[:17].ljust(17, "0")


def _rows(*labels: str) -> list[dict]:
    return [{"vin": _vin(100 + i), "_rooftop": {"key": lbl, "name": lbl, "site": ""}}
            for i, lbl in enumerate(labels)]


def test_name_token_subset_match_refuses_siblings_with_the_weak_marker():
    # roster "Ted Russell Ford" is a token subset of BOTH rooftops; whichever the
    # tier picks, the other must not be un-listed on that evidence
    # "Ted Russell Ford Parkside" is the only rooftop whose words contain the
    # roster's, so name_token_subset picks it; the other rooftop is refused,
    # but only with the weak marker
    kept, refused = resolve_rooftop_attribution(
        _rows("Ted Russell Ford Parkside", "Ted Russell Ford Parkside", "Rusty Wallace Honda"),
        dealer_id="tedrussellford-net", dealer_name="Ted Russell Ford",
        dealer_url="https://www.tedrussellford.net",
    )
    assert sorted(r["vin"] for r in kept) == [_vin(100), _vin(101)]
    assert [r["vin"] for r in refused] == [_vin(102)]
    assert {r["_rooftop_reject"] for r in refused} == {"sibling_rooftop_weak_tier"}


def test_exact_name_match_still_refuses_siblings_as_evidence():
    kept, refused = resolve_rooftop_attribution(
        _rows("Ted Russell Ford", "Rusty Wallace Honda"),
        dealer_id="tedrussellford-net", dealer_name="Ted Russell Ford",
        dealer_url="https://www.tedrussellford.net",
    )
    assert [r["vin"] for r in kept] == [_vin(100)]
    assert {r["_rooftop_reject"] for r in refused} == {"sibling_rooftop"}


def test_service_rooftop_folds_into_its_store():
    rows = _rows("Terry Labonte Chevrolet", *["Terry Labonte Chevrolet Service"] * 4)
    kept, refused = resolve_rooftop_attribution(
        rows, dealer_id="terrylabontechevy-com", dealer_name="Terry Labonte Chevrolet",
        dealer_url="https://www.terrylabontechevy.com",
    )
    assert len(kept) == 5 and not refused


def test_service_center_of_a_sibling_stays_a_sibling():
    rows = _rows("Audi South Austin", "Audi North Austin Service Center", "Audi North Austin")
    kept, refused = resolve_rooftop_attribution(
        rows, dealer_id="audisouthaustin-com", dealer_name="Audi South Austin",
        dealer_url="https://www.audisouthaustin.com",
    )
    assert [r["vin"] for r in kept] == [_vin(100)]
    assert len(refused) == 2 and {r["_rooftop_reject"] for r in refused} == {"sibling_rooftop"}


def test_lone_service_rooftop_is_not_folded_away():
    # no "X" rooftop beside "X Service": the name tiers still see it as the store
    rows = _rows("Terry Labonte Chevrolet Service", "Terry Labonte Chevrolet Service")
    kept, refused = resolve_rooftop_attribution(
        rows, dealer_id="terrylabontechevy-com", dealer_name="Terry Labonte Chevrolet",
        dealer_url="https://www.terrylabontechevy.com",
    )
    assert len(kept) == 2 and not refused
