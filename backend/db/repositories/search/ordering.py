"""Final ``search_cars`` ordering: cap, then price order or nearest-first."""
from __future__ import annotations


def _sort_cars_by_price(cars: list) -> list:
    """Priced + multi-photo first; call-for-price and single-photo listings sink."""
    from backend.utils.listings_sort import listing_sort_key_by_price

    return sorted(cars, key=listing_sort_key_by_price)


def order_results(collected: list[dict], lim: int | None, *, by_distance: bool) -> list[dict]:
    if lim is not None:
        collected = collected[:lim]
    if by_distance:
        from backend.utils.listings_sort import listing_sort_depriority

        return sorted(
            collected,
            key=lambda c: (*listing_sort_depriority(c), c["distance_miles"]),
        )
    return _sort_cars_by_price(collected)
