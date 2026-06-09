"""Shared listing sort helpers: deprioritize call-for-price and thin galleries."""

from __future__ import annotations

from typing import Any


def listing_is_call_for_price(car: dict[str, Any]) -> bool:
    p = car.get("price")
    if p is None:
        return True
    try:
        return float(p) <= 0
    except (TypeError, ValueError):
        return True


def listing_photo_count(car: dict[str, Any]) -> int:
    pc = car.get("photo_count")
    if pc is not None:
        try:
            n = int(pc)
            if n >= 0:
                return n
        except (TypeError, ValueError):
            pass
    from backend.utils.car_serialize import _public_gallery_photo_count

    return _public_gallery_photo_count(car.get("gallery"), image_url=car.get("image_url"))


def listing_has_single_photo(car: dict[str, Any]) -> bool:
    return listing_photo_count(car) <= 1


def listing_sort_depriority(car: dict[str, Any]) -> tuple[int, int]:
    """Lower is better. (call_for_price, single_photo)."""
    return (
        1 if listing_is_call_for_price(car) else 0,
        1 if listing_has_single_photo(car) else 0,
    )


def listing_price_value(car: dict[str, Any]) -> float:
    p = car.get("price")
    if p is None:
        return float("inf")
    try:
        v = float(p)
        return v if v > 0 else float("inf")
    except (TypeError, ValueError):
        return float("inf")


def listing_sort_key_by_price(car: dict[str, Any]) -> tuple:
    return (*listing_sort_depriority(car), listing_price_value(car))
