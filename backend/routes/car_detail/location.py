"""Steps 9 and 13: the dealer map and the hero location line."""

from __future__ import annotations


def dealer_map_for_car(car_raw: dict, dealer_info: dict | None, attribution):
    from backend.listings.dealer_map import build_dealer_map_for_car

    return build_dealer_map_for_car(car_raw, dealer_info, attribution=attribution)


def hero_location(dealer_map, listings_geo: dict):
    from backend.listings.dealer_map import hero_location_for_car

    try:
        return hero_location_for_car(dealer_map, listings_geo.get("zip_code"))
    except Exception:  # a location line must never break the page
        return None
