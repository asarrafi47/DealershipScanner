"""
``serialize_car_for_api`` price/deal policy: implausible-price withholding, the
trusted MSRP block, payment-shaped prices, and the coarse deal score.

Split out of ``serialize.serialize_car_for_api`` (2026-10-01) with no behaviour
change; ``backend/tests/test_serialize_car_for_api_golden.py`` pins the output.
"""
from __future__ import annotations

from typing import Any


def withhold_implausible_price(out: dict[str, Any], c: dict[str, Any]) -> None:
    # price is NOT trusted verbatim from the column. Measured 2026-08-05: of
    # 122,663 active listings, 33 exceed $300k and 212 sit under $500. Most of
    # the >$300k group is genuinely priced (a McLaren 765LT at $699,900 is a
    # real ask) but a few are parser/feed artifacts wearing a plausible-looking
    # number (a 2026 Ford Bronco Base at $449,150 is exactly 9.8x its own
    # 43-car active cohort median of $46,039). Same policy as the
    # ``battery_kwh`` / ``curb_weight_lb`` suppressions (api_withheld): an implausible
    # figure is withheld, never replaced with an invented one. See
    # ``backend/utils/price_plausibility.py`` for the cohort math and its
    # documented gap (thin-cohort exotics/rare trims are left unjudged).
    if out.get("price") is not None:
        try:
            from backend.utils.price_plausibility import implausible_price

            _price_val = float(out["price"])
            if implausible_price(c, _price_val):
                out["price"] = None
        except (TypeError, ValueError):
            pass


def apply_msrp_and_payment(
    out: dict[str, Any], c: dict[str, Any], *, include_extended_display: bool
) -> None:
    # msrp is NOT passed through from the column. ``cars.msrp`` is written
    # verbatim from the dealer feed, and on used inventory the feeds put a
    # marketing "was" price in it: of the 18,350 active listings carrying one,
    # 9,621 are EXACTLY the asking price and 2,439 are below it. Shown as an
    # MSRP those become a fabricated anchor, and the page then subtracts them
    # from the price and calls the remainder a saving. Same shape as the
    # ``battery_kwh`` suppression (api_withheld) — the write path keeps what the feed
    # says, the display layer decides what is credible. ``resolve_display_msrp``
    # admits a figure only from a window sticker we hold for this VIN or from a
    # new, non-CPO listing, and only above the asking price; see
    # ``backend/utils/msrp_trust.py`` for the counts and the rule.
    #
    # ``allow_sticker`` follows ``include_extended_display`` so the bulk callers
    # that never render a price block (listing-completeness gap detection) don't
    # pay for a per-VIN filesystem read and a pdftotext.
    from backend.utils.msrp_trust import resolve_display_msrp

    _msrp = resolve_display_msrp(c, allow_sticker=include_extended_display)
    out["msrp"] = _msrp["msrp"]
    out["msrp_label"] = _msrp["label"]
    out["msrp_source"] = _msrp["source"]
    out["msrp_from_sticker"] = _msrp["from_sticker"]
    out["below_msrp"] = _msrp["savings"]
    # New, non-CPO only (msrp_trust rule b): the dealer's ask over the MSRP.
    out["over_msrp"] = _msrp.get("over_msrp")
    # See serialize_car_for_listings_grid: the hero prints the figure as an
    # advertised payment and shows no MSRP delta or finance estimate against it.
    from backend.utils.market_price import is_payment_shaped_price  # lazy: market_price imports the DB layer

    out["payment_listed"] = is_payment_shaped_price(out.get("price"), year=c.get("year"))
    if out["payment_listed"]:
        out["msrp"] = None
        out["below_msrp"] = None
        out["over_msrp"] = None


def apply_deal_score(out: dict[str, Any], c: dict[str, Any]) -> None:
    # Coarse market deal score (free consumer hook). Scored offline against the
    # in-process market_price_stats cache — no per-car DB round-trip. The detailed
    # band breakdown is gated behind FEATURE_MARKET_INTEL in the route/template.
    try:
        from backend.intelligence.deal_score_cache import public_deal_score

        out["deal_score"] = public_deal_score(c)
    except Exception:
        out["deal_score"] = None
