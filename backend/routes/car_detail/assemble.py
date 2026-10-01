"""Final step: the context dict shared by ``car.html``, ``GET /api/cars/<id>`` and
the dev scan lab. Key order is the historical one (the template ignores it; a
JSON consumer reading the raw body would not)."""

from __future__ import annotations

from backend.routes.car_detail.sticker import WindowStickerView


def assemble_view_context(
    *,
    car_raw: dict,
    ctx: dict,
    car: dict,
    sticker: WindowStickerView,
    window_sticker_status: str,
    uid,
    car_is_saved: bool,
    listing_incomplete_fields: list,
    dealer_info: dict | None,
    attribution_fields: dict,
    dealer_map,
    market_intel,
    deal_score_detail,
    trim_ladder,
    rarity,
    listings_geo: dict,
    hero_location,
    generated_spec_sheet,
    unified_options_list: list,
) -> dict:
    from backend.scanner.window_sticker import is_cdjr_stellantis_car
    from backend.utils.dealer_rating_display import dealer_rating_display

    return {
        "car": car,
        "generated_spec_sheet": generated_spec_sheet,
        "unified_options_list": unified_options_list,
        "market_intel": market_intel,
        "deal_score_detail": deal_score_detail,
        "rarity": rarity,
        "trim_ladder": trim_ladder,
        "car_is_saved": car_is_saved,
        "logged_in": bool(uid),
        "listings_geo_zip": listings_geo.get("zip_code") or "",
        "hero_location": hero_location,
        "dealer_rating": dealer_rating_display(dealer_info),
        "listing_incomplete_fields": listing_incomplete_fields,
        "gallery_images": ctx.get("gallery_images") or [],
        "verified_specs": ctx.get("verified_specs") or {},
        "listing_packages_sections": ctx.get("listing_packages_sections") or [],
        "listing_standalone_features": ctx.get("listing_standalone_features") or [],
        "listing_observed_features": ctx.get("listing_observed_features") or [],
        "listing_monroney_options": ctx.get("listing_monroney_options") or [],
        "listing_monroney_standard": ctx.get("listing_monroney_standard") or [],
        "listing_sticker_options": ctx.get("listing_sticker_options") or [],
        "listing_sticker_option_groups": ctx.get("listing_sticker_option_groups") or [],
        "listing_sticker_option_sections": ctx.get("listing_sticker_option_sections") or {},
        "listing_possible_packages": ctx.get("listing_possible_packages") or [],
        "listing_photo_detected_equipment": ctx.get("listing_photo_detected_equipment") or [],
        # Sticker-printed colors: the fetched OEM sticker's parse (ctx) and the
        # photographed-sticker read (car overlay) are the same kind of fact —
        # printed on the document — so either fills the template slot; the
        # photographed read wins only because it was verified against THIS car's
        # own gallery. Both supersede the feed color in the display.
        "sticker_exterior_color": car.get("sticker_exterior_color")
        or ctx.get("sticker_exterior_color"),
        "sticker_interior_color": car.get("sticker_interior_color")
        or ctx.get("sticker_interior_color"),
        "sticker_interior_material": ctx.get("sticker_interior_material"),
        "sticker_spec_lines": ctx.get("sticker_spec_lines") or [],
        "interior_from_listing_description": bool(ctx.get("interior_from_listing_description")),
        "interior_from_llava_vision": bool(ctx.get("interior_from_llava_vision")),
        "packages_panel_has_content": bool(ctx.get("packages_panel_has_content")),
        "llava_interior_section": ctx.get("llava_interior_section"),
        "window_sticker_available": sticker.ready,
        "window_sticker_visual_available": sticker.visual,
        "show_window_sticker_ui": sticker.show_ui,
        "listing_sticker_image_url": sticker.listing_image_url,
        "sticker_fetch_pending": sticker.fetch_pending,
        "is_cdjr_stellantis": is_cdjr_stellantis_car(car_raw),
        "cdjr_window_sticker_eligible": sticker.cdjr_eligible,
        "window_sticker_preview_api_url": sticker.preview_api_url,
        "window_sticker_preview_url": sticker.preview_embed_url,
        # See sticker.rendered_window_sticker_status for the vocabulary.
        "window_sticker_status": window_sticker_status,
        "window_sticker_source_known": bool(sticker.listing_urls or sticker.cdjr_eligible),
        "hide_photo_analysis": bool(ctx.get("hide_photo_analysis")),
        "window_sticker_oem_url": sticker.oem_url,
        "window_sticker_pdf_url": sticker.pdf_url,
        "dealer_info": dealer_info,
        "dealer_map": dealer_map,
        "location_attribution": attribution_fields or None,
    }
