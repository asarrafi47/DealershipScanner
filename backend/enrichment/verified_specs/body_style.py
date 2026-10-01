"""Body style display."""

from __future__ import annotations

from typing import Any

from backend.enrichment.verified_specs.sources import SpecSources


def resolve_body_style(src: SpecSources) -> Any:
    """Wrangler is always "SUV"; otherwise decoder hint > vPIC when the dealer's
    is a placeholder, then the per-car normalizer corrects either; EPA body style
    is the last fallback."""
    from backend.enrichment import knowledge_engine as ke
    from backend.utils.field_clean import is_jeep_wrangler_car, normalize_body_style_for_car

    car, regex, vpic = src.car, src.regex, src.vpic
    make, model, trim, title = src.make, src.model, src.trim, src.title
    body_style_display = None
    if is_jeep_wrangler_car(make, model, trim, title):
        body_style_display = "SUV"
    elif ke._is_na_spec(car.get("body_style")) and regex.get("body_style_hint"):
        body_style_display = regex["body_style_hint"]
    if not body_style_display and ke._is_na_spec(car.get("body_style")) and vpic.get("body_style"):
        body_style_display = vpic["body_style"]
    if not is_jeep_wrangler_car(make, model, trim, title):
        corrected_bs = normalize_body_style_for_car(
            car.get("body_style") if not ke._is_na_spec(car.get("body_style")) else body_style_display,
            make=make,
            model=model,
            trim=trim,
            title=title,
        )
        if corrected_bs:
            body_style_display = corrected_bs
    return body_style_display or src.epa.get("body_style")
