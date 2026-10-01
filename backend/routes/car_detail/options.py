"""Steps 14-15: the generated build sheet and the unified Options-tab list."""

from __future__ import annotations


def generated_spec_sheet_for(car_raw: dict, ctx: dict, *, has_real_sticker: bool):
    """Synthesized build sheet from the data we hold (listing row + verified EPA
    specs + best-effort catalog options) — but ONLY when this car has no real
    window sticker. When a genuine OEM/listing sticker exists (or is being
    fetched), that is authoritative and we never fabricate our own alongside it.
    """
    generated_spec_sheet = None
    if not has_real_sticker:
        try:
            from backend.enrichment.generated_spec_sheet import build_generated_spec_sheet

            generated_spec_sheet = build_generated_spec_sheet(
                car_raw, ctx.get("verified_specs") or {}
            )
        except Exception:
            generated_spec_sheet = None
    return generated_spec_sheet


def unified_options_list_for(ctx: dict, *, show_sticker_ui: bool, generated_spec_sheet) -> list:
    """One de-duplicated, provenance-badged equipment list for the Options tab —
    replaces up to seven separately-rendered, overlapping lists (window
    sticker options, the build-sheet catalog, Monroney data, listing-
    description packages, and photo-analysis guesses) with a single merge.
    See backend/enrichment/generated_spec_sheet.py build_unified_options_list.
    """
    try:
        from backend.enrichment.generated_spec_sheet import build_unified_options_list

        # The parsed-sticker sources are already shown in full in the "Window
        # sticker" embed above this panel when show_sticker_ui is on — folding
        # them in again here would recreate the exact duplication this list
        # exists to remove, so they only feed the unified list when that embed
        # is NOT rendered (the raw-data fallback path the embed itself used).
        return build_unified_options_list(
            catalog=(generated_spec_sheet or {}).get("catalog"),
            sticker_option_sections=(
                None if show_sticker_ui else ctx.get("listing_sticker_option_sections")
            ),
            sticker_option_groups=(
                None if show_sticker_ui else ctx.get("listing_sticker_option_groups")
            ),
            sticker_options=None if show_sticker_ui else ctx.get("listing_sticker_options"),
            monroney_options=ctx.get("listing_monroney_options"),
            monroney_standard=ctx.get("listing_monroney_standard"),
            packages_sections=ctx.get("listing_packages_sections"),
            photo_detected_equipment=ctx.get("listing_photo_detected_equipment"),
            possible_packages=ctx.get("listing_possible_packages"),
            observed_features=ctx.get("listing_observed_features"),
            standalone_features=ctx.get("listing_standalone_features"),
        )
    except Exception:
        return []
