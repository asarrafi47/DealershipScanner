"""Tests for listing deprioritization sort helpers."""

from backend.utils.listings_sort import (
    listing_has_single_photo,
    listing_is_call_for_price,
    listing_sort_key_by_price,
)


def _car(cid: int, *, price=None, photos=2):
    gallery = [f"https://cdn.example.com/{cid}-{i}.jpg" for i in range(photos)]
    return {
        "id": cid,
        "price": price,
        "gallery": gallery,
        "image_url": gallery[0] if gallery else None,
        "photo_count": photos,
    }


def test_call_for_price_sorts_last():
    cars = [
        _car(1, price=None, photos=3),
        _car(2, price=25000, photos=3),
        _car(3, price=0, photos=3),
    ]
    ordered = sorted(cars, key=listing_sort_key_by_price)
    assert [c["id"] for c in ordered] == [2, 1, 3]


def test_single_photo_sorts_after_multi_photo():
    cars = [
        _car(1, price=20000, photos=1),
        _car(2, price=30000, photos=4),
        _car(3, price=15000, photos=2),
    ]
    ordered = sorted(cars, key=listing_sort_key_by_price)
    assert [c["id"] for c in ordered] == [3, 2, 1]


def test_call_for_price_and_single_photo_sort_last():
    cars = [
        _car(1, price=None, photos=1),
        _car(2, price=22000, photos=3),
        _car(3, price=18000, photos=1),
    ]
    ordered = sorted(cars, key=listing_sort_key_by_price)
    assert [c["id"] for c in ordered] == [2, 3, 1]


def test_listing_is_call_for_price():
    assert listing_is_call_for_price({"price": None})
    assert listing_is_call_for_price({"price": 0})
    assert not listing_is_call_for_price({"price": 1})


def test_listing_has_single_photo():
    assert listing_has_single_photo(_car(1, price=1, photos=1))
    assert not listing_has_single_photo(_car(2, price=1, photos=2))
