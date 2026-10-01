"""Cylinder count: storage value, display value, and the verified flag."""

from __future__ import annotations

from typing import Any

from backend.enrichment.verified_specs._coerce import int_or_none
from backend.enrichment.verified_specs.sources import SpecSources


def resolve_cylinders(src: SpecSources) -> int | None:
    """Priority: vPIC (when it agrees with the engine text) > linked catalog row >
    trim decoder > EPA > dealer column > model_specs > raw vPIC > trim/engine text.

    Resolved catalog row beats the regex trim decoder — the decoder is era-blind
    ("E 350" decodes to the modern turbo-four) while the link was scored against
    this car's own engine data. Only when the by-id row actually resolved: a
    fuzzy fallback must not inherit this authority.
    """
    regex, epa, vpic, dict_specs = src.regex, src.epa, src.vpic, src.dict_specs
    cyl_ver = src.vpic_cyl if src.vpic_cyl_ok else None
    if cyl_ver is None:
        cyl_ver = int_or_none(src.epa_trim.get("cylinders")) if src.linked_exact else None
    if cyl_ver is None:
        cyl_ver = regex.get("cylinders")
    if cyl_ver is None:
        cyl_ver = epa.get("cylinders")
    if cyl_ver is None:
        di = int_or_none(src.dealer_cyl)
        if di is not None:
            cyl_ver = di
    if cyl_ver is None and dict_specs and dict_specs.get("cylinders") is not None:
        try:
            cyl_ver = int(dict_specs["cylinders"])
        except (TypeError, ValueError):
            cyl_ver = None
    if cyl_ver is None and vpic.get("cylinders") is not None:
        cyl_ver = vpic["cylinders"]
    if cyl_ver is None:
        cyl_ver = _cylinders_from_listing_text(src)
    return cyl_ver


def _cylinders_from_listing_text(src: SpecSources) -> int | None:
    """Last resort: the answer is often sitting in the listing's own text.

    e.g. an Infiniti Q70L whose trim reads "Sedan V-6 cyl". Read the layout
    token from trim/engine text only (skipping BEVs, which have no cylinders and
    never carry a V-N badge).

    The TITLE is deliberately excluded: it carries the model designator, and BMW
    "i8"/"i4"/"i7" and Hummer "H2"/"H3" collide with the V/I/H/W layout pattern
    ("i8" -> 8 cyl, "H2" -> 2 cyl). A genuine cylinder badge lives in the trim or
    engine description, not the model name.
    """
    from backend.utils.engine_consistency import cylinders_from_engine_text, is_bev_fuel

    if not is_bev_fuel(src.dealer_ft or src.epa.get("fuel_type")):
        return cylinders_from_engine_text(
            " ".join(x for x in (src.trim, src.car.get("engine_description")) if x)
        )
    return None


def cylinders_display(
    src: SpecSources, cyl_ver: int | None, is_bev: bool
) -> tuple[Any, Any, bool]:
    """``(cylinders, cylinders_display, cylinders_verified)`` once BEV status is known."""
    from backend.enrichment import knowledge_engine as ke

    dealer_cyl_i = int_or_none(src.dealer_cyl)
    if is_bev:
        cyl_ver = 0
        display_cyl = 0
    elif src.vpic_cyl_ok and src.vpic_cyl:
        # VIN decode outranks the stored column (see resolve_cylinders).
        display_cyl = src.vpic_cyl
    elif dealer_cyl_i is not None and dealer_cyl_i > 0:
        display_cyl = dealer_cyl_i
    elif cyl_ver is not None:
        display_cyl = cyl_ver
    else:
        display_cyl = dealer_cyl_i

    verified = bool(
        display_cyl is not None
        and (
            (src.vpic_cyl_ok and src.vpic_cyl and display_cyl == src.vpic_cyl)
            or (
                (ke._is_na_spec(src.dealer_cyl) or dealer_cyl_i in (None, 0))
                and (src.regex.get("cylinders") is not None or src.epa.get("cylinders") is not None)
            )
        )
    )
    return cyl_ver, display_cyl, verified
