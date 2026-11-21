"""N6.8 / D-N15: one placeholder-image test for parsers, completeness and the photo alarm."""
import pytest

from backend.parsers.base import _is_placeholder_url
from backend.utils.listing_completeness import listing_missing_field_codes
from backend.utils.placeholder_images import (
    dealer_photo_alarm,
    is_placeholder_image,
    photo_alarms_by_dealer,
    shared_image_context,
)

TOKEN_URLS = [
    "https://cdn.example.com/img/photo_unavailable.jpg",
    "https://cdn.example.com/img/unavailable_stockphoto.png",
    "https://cdn.example.com/img/coming_soon.jpg",
    "https://cdn.example.com/img/comingsoon.jpg",
    "https://chapmanchevy.example.com/assets/default.jpg",
    "/static/placeholder.svg",
]


@pytest.mark.parametrize("url", TOKEN_URLS)
def test_token_is_placeholder_everywhere(url):
    assert is_placeholder_image(url)
    assert _is_placeholder_url(url)


def test_real_photo_not_flagged():
    for u in ("https://cdn.example.com/inv/1HGCV1F1XLA000001/1.jpg",
              "https://images.dealer.com/ddc/abc123.jpg"):
        assert not is_placeholder_image(u)
        assert not _is_placeholder_url(u)
    assert not is_placeholder_image(None) and not is_placeholder_image("")


def test_shared_image_heuristic():
    shared = "https://cdn.example.com/lib/stock-art-77.jpg"
    rows = [{"image_url": shared, "gallery": [shared, f"https://cdn.example.com/c{i}.jpg"]} for i in range(5)]
    ctx = shared_image_context(rows)
    assert is_placeholder_image(shared, ctx)
    assert not is_placeholder_image(shared)  # no context, no token
    assert not is_placeholder_image("https://cdn.example.com/c1.jpg", ctx)
    # below the threshold the same URL is a photo
    assert not is_placeholder_image(shared, shared_image_context(rows[:2]))


def test_completeness_counts_placeholder_only_as_no_photo():
    base = {"vin": "1HGCV1F1XLA000001", "year": 2020, "make": "Honda", "model": "Accord"}
    ph = dict(base, image_url="https://cdn.example.com/unavailable_stockphoto.jpg",
              gallery=["https://cdn.example.com/photo_unavailable.jpg"])
    ok = dict(base, image_url="https://cdn.example.com/a.jpg")
    assert "images" in listing_missing_field_codes(ph, for_public_filter=False, include_non_actionable=True)
    assert "images" not in listing_missing_field_codes(ok, for_public_filter=False, include_non_actionable=True)


def test_dealer_photo_alarm_trips():
    ph = "https://cdn.example.com/unavailable_stockphoto.jpg"
    bad = [{"dealer_id": "nelson", "image_url": ph} for _ in range(12)]
    good = [{"dealer_id": "fine", "image_url": f"https://cdn.example.com/{i}.jpg"} for i in range(12)]
    out = photo_alarms_by_dealer(bad + good)
    assert out["nelson"]["alarm"] and out["nelson"]["placeholder_only"] == 12
    assert not out["fine"]["alarm"]
    # too few cars: no alarm
    assert not dealer_photo_alarm(bad[:3])["alarm"]
