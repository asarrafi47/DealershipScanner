"""Steps 4-8: the serialized car, its gaps, its registry dealer and its overlays."""

from __future__ import annotations

from backend.utils.listing_completeness import INCOMPLETE_FIELD_LABELS


def serialize_car(main, car_raw: dict, ctx: dict) -> dict:
    return main.serialize_car_for_api(
        car_raw,
        include_verified=False,
        verified_specs=ctx.get("verified_specs") or {},
    )


def listing_incomplete_fields(car_id: int) -> list[dict]:
    from backend.db.incomplete_listings_db import get_missing_field_codes_for_car_id

    _missing_codes = get_missing_field_codes_for_car_id(car_id)
    return [
        {"code": c, "label": INCOMPLETE_FIELD_LABELS.get(c, c.replace("_", " ").title())}
        for c in _missing_codes
    ]


def registry_dealer_info(car_raw: dict) -> dict | None:
    """The dealership-registry row for this listing (URLs normalized), or ``None``."""
    dealer_info = None
    reg_id = car_raw.get("dealership_registry_id")
    if reg_id:
        try:
            from backend.db.dealerships_db import get_dealership_by_id
            from backend.utils.field_clean import normalize_optional_url

            raw_dealer = get_dealership_by_id(int(reg_id))
            if raw_dealer:
                dealer_info = dict(raw_dealer)
                for url_key in ("dealer_website_url", "website_url"):
                    if url_key in dealer_info:
                        dealer_info[url_key] = normalize_optional_url(dealer_info.get(url_key))
        except Exception:
            pass
    return dealer_info


def apply_attribution_overlay(car_id: int, car: dict):
    """What this listing's own photographs say about where the car actually is.

    One batched read keyed by car id (see cars_repo.car_attribution_states); a
    miss — the common case — leaves every dealer surface on this page exactly as
    it was. The public fields go onto the serialized car too (mutates *car*), so
    GET /api/cars/<id> and the compare/save flows that read it carry the same
    caveat the rendered page shows. Returns ``(attribution, attribution_fields)``.
    """
    from backend.db.repositories.cars_repo import car_attribution_states
    from backend.utils.car_serialize.attribution import attribution_public_fields

    attribution = car_attribution_states([car_id]).get(car_id)
    attribution_fields = attribution_public_fields(attribution)
    car.update(attribution_fields)
    return attribution, attribution_fields


def apply_sticker_fact_overlays(car_id: int, car: dict, car_raw: dict) -> None:
    """MSRP and color overlays from sticker facts (mutates *car*).

    When the trusted-MSRP resolver produced nothing (car.msrp is None), two
    sidecar stores may still have something honest to say: a Monroney total an
    agent read off a sticker photographed in this listing's gallery
    (car_image_text), or the range this trim has been observed to sticker at
    (trim_msrp_bands). Same batched-read shape as the attribution overlay; the
    wording lives in car_serialize.msrp_overlay so no surface can present either
    as the feed's MSRP. car_raw carries the identity because the band keys match
    cars.model/cars.trim verbatim, not the BMW display-normalized pair the
    serialized dict may hold.

    Colors the sticker document PRINTS supersede the feed's in the display
    (policy 2026-08-18: the window sticker is the single source of truth), with
    provenance shown and the cars columns untouched. Same batched read as the
    MSRP overlay; photo-observed colors never qualify (see color_overlay).
    """
    from backend.db.repositories.cars_repo import car_sticker_msrp_values
    from backend.utils.car_serialize.color_overlay import color_overlay_public_fields
    from backend.utils.car_serialize.msrp_overlay import msrp_overlay_public_fields

    sticker_facts = car_sticker_msrp_values([car_id]).get(car_id)
    car.update(msrp_overlay_public_fields(car, sticker_facts, identity=car_raw))
    car.update(color_overlay_public_fields(sticker_facts))
