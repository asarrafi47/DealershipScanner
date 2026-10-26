"""Discovery merge keeps Google/derived fields (audit B-009)."""
from __future__ import annotations

from backend.discovery.candidate import DealerCandidate
from backend.discovery.merge import _merge_two, merge_and_dedupe


def _pair():
    a = DealerCandidate(name="Acme Toyota", city="Charlotte", state="NC", source_dmv=True,
                        dealer_website_url="https://acmetoyota.com")
    b = DealerCandidate(
        name="Acme Toyota", city="Charlotte", state="NC", source_web=True,
        dealer_website_url="https://acmetoyota.com",
        google_place_id="pid1", google_rating=4.5, google_review_count=120,
        google_primary_type="car_dealer", google_types=["car_dealer"],
        business_status="CLOSED_PERMANENTLY", oem_brand="Toyota", phone="555-1212",
    )
    return a, b


def test_merge_two_keeps_google_fields_either_order():
    a, b = _pair()
    for m in (_merge_two(a, b), _merge_two(b, a)):
        assert m.google_place_id == "pid1"
        assert m.google_rating == 4.5
        assert m.google_review_count == 120
        assert m.google_primary_type == "car_dealer"
        assert m.business_status == "CLOSED_PERMANENTLY"
        assert m.oem_brand == "Toyota"
        assert m.phone == "555-1212"
        assert m.source_dmv and m.source_web


def test_merge_two_non_dealer_and_closed_win():
    a, b = _pair()
    a.business_status = "OPERATIONAL"
    b.is_dealer = False
    m = _merge_two(a, b)
    assert m.is_dealer is False
    assert m.business_status == "CLOSED_PERMANENTLY"


def test_merge_and_dedupe_cluster_keeps_fields():
    a, b = _pair()
    out = merge_and_dedupe([a, b])
    assert len(out) == 1 and out[0].google_place_id == "pid1"
