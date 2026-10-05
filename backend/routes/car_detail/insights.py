"""Steps 10-12: market price + deal score (paid), trim ladder, inventory rarity."""

from __future__ import annotations

from flask import session

from backend.billing import access as paid_access
from backend.billing.catalog import FEATURE_MARKET_INTEL, FEATURE_WINDOW_STICKER
from backend.listings.geo_session import listings_geo_kwargs_from_session

# Per-rung equipment diff ("what this trim adds") — a Premium feature.
TRIM_LADDER_ADDS_FIELDS = (
    "adds", "sticker_adds", "sticker_adds_note",
    "sticker_equipment", "sticker_equipment_note", "sticker_equipment_car_ids",
)


def market_intel_for_viewer(car_raw: dict):
    """``(market_intel, deal_score_detail)`` — both ``None`` unless the viewer pays.

    Paid members only (also with billing off), and only if the market-intel APIs
    would serve this viewer.
    """
    market_intel = None
    deal_score_detail = None
    if paid_access.sees_paid_ui() and paid_access.shows(FEATURE_MARKET_INTEL):
        from backend.utils.market_price import market_price_for_car

        geo = listings_geo_kwargs_from_session(session)
        market_intel = market_price_for_car(
            car_raw,
            zip_code=geo.get("zip_code"),
            radius_miles=geo.get("radius_miles"),
        )
        # Detailed market-price-band breakdown (median/p25/p75, N listings across
        # M dealers) — gated behind FEATURE_MARKET_INTEL like the market intel above.
        try:
            from backend.intelligence.deal_score_cache import detailed_deal_score

            deal_score_detail = detailed_deal_score(car_raw)
        except Exception:
            deal_score_detail = None
    return market_intel, deal_score_detail


def trim_ladder_for_viewer(car_raw: dict):
    """The trim ladder, with the per-rung equipment stripped for non-premium viewers.

    Basic ladder position (name + neighbors) is free on every listing, any
    year — only the per-rung equipment diff ("what this trim adds") is a
    Premium feature. Strip that content server-side for non-premium
    viewers (not just hide it in the template) so the JSON API
    (api_car_detail shares this same context) never leaks it either.
    """
    from backend.enrichment.trim_ladder import resolve_trim_ladder

    trim_ladder = resolve_trim_ladder(
        make=car_raw.get("make"),
        model=car_raw.get("model"),
        year=car_raw.get("year"),
        trim=car_raw.get("trim"),
    )
    # Per-rung equipment comes from window stickers: same gate as the sticker UI.
    if trim_ladder and not paid_access.shows(FEATURE_WINDOW_STICKER):
        trim_ladder = dict(trim_ladder)
        trim_ladder["steps"] = [
            {k: v for k, v in step.items() if k not in TRIM_LADDER_ADDS_FIELDS}
            for step in trim_ladder.get("steps") or []
        ]
    return trim_ladder


def inventory_rarity(car_raw: dict):
    """Inventory rarity — scarcity within our own active fleet plus visible-
    option evidence from the vision scan. Ungated: it is our own data, and
    "one of 2 in our inventory" is a purchase nudge for every viewer.
    """
    rarity = None
    try:
        from backend.utils.rarity_score import rarity_for_car, vision_summary_for_car

        rarity = rarity_for_car(
            car_raw, vision_summary=vision_summary_for_car(car_raw.get("id"))
        )
    except Exception:
        rarity = None
    return rarity
