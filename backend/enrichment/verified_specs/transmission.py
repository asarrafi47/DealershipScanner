"""Gears and the transmission display string."""

from __future__ import annotations

import re
from typing import Any

from backend.enrichment.verified_specs.sources import SpecSources


def resolve_gears(src: SpecSources) -> Any:
    return src.regex.get("gears") or src.epa.get("gears")


def _raw_transmission(src: SpecSources) -> Any:
    """Priority: trim-decoder year-aware hint > EPA > vPIC > model_specs > dealer.

    transmission_hint encodes year logic (e.g. Transit pre-/post-2020); must beat
    model_specs.
    """
    from backend.enrichment import knowledge_engine as ke

    regex, epa, vpic, dict_specs = src.regex, src.epa, src.vpic, src.dict_specs
    dealer_trans = src.dealer_trans
    _trans_hint = regex.get("transmission_hint")
    if _trans_hint and not epa.get("transmission") and not (dealer_trans and not ke._is_na_spec(dealer_trans)):
        return _trans_hint
    epa_trany = epa.get("transmission")
    trans_raw = epa_trany or (
        dealer_trans if dealer_trans and not ke._is_na_spec(dealer_trans) else None
    )
    # EPA aggregate is often generic "Automatic" while the VDP lists "8-Speed Automatic".
    dealer_ok = dealer_trans and not ke._is_na_spec(dealer_trans)
    if (
        dealer_ok
        and ke._transmission_has_gear_detail(dealer_trans)
        and not ke._transmission_has_gear_detail(epa_trany or "")
    ):
        trans_raw = dealer_trans
    if not trans_raw and vpic.get("transmission"):
        trans_raw = vpic["transmission"]
    if not trans_raw and dict_specs and dict_specs.get("transmission"):
        _dict_trans = str(dict_specs["transmission"]).strip()
        # model_specs is one row per make+model with no year column -- it reflects
        # whichever generation was scraped/seeded, so a specific gear count (e.g.
        # "10-Speed Automatic") is only safe for that one generation and actively
        # wrong for the rest of a multi-generation nameplate's history (confirmed:
        # 2000-2016 F-150s inherited the 2017+ 10-speed spec this way). A generic
        # value ("Automatic", "CVT") is far less likely to have changed and is safe
        # to keep; a gear-count-specific one is suppressed rather than guessed.
        if not ke._transmission_has_gear_detail(_dict_trans):
            trans_raw = _dict_trans
    return trans_raw


def resolve_transmission(src: SpecSources, gears_ver: Any) -> Any:
    """Display transmission before BEV status is known (see :func:`bev_transmission`)."""
    from backend.enrichment import knowledge_engine as ke

    trans_raw = _raw_transmission(src)
    trans_ver = ke.format_transmission_display(trans_raw) or trans_raw

    # Trim decoder often knows "8-Speed Automatic" while EPA row is generic "Automatic".
    _dec_hint = src.regex.get("transmission_hint")
    if (
        _dec_hint
        and ke._transmission_has_gear_detail(_dec_hint)
        and not ke._transmission_has_gear_detail(str(trans_ver or ""))
    ):
        trans_ver = ke.format_transmission_display(_dec_hint) or _dec_hint
    elif gears_ver is not None and str(trans_ver or "").strip().lower() in ("automatic", "auto"):
        try:
            _ng = int(gears_ver)
            if _ng > 0:
                trans_ver = f"{_ng}-Speed Automatic"
        except (TypeError, ValueError):
            pass
    return trans_ver


def bev_transmission(src: SpecSources, trans_ver: Any, is_bev: bool, sticker_pkg: dict[str, Any]) -> Any:
    """An electric-drive car with no transmission anywhere gets the sticker's or single-speed."""
    from backend.enrichment import knowledge_engine as ke

    if is_bev and not trans_ver and ke._is_na_spec(src.dealer_trans):
        return sticker_pkg.get("transmission") or "Single-speed automatic"
    return trans_ver


def normalize_transmission(src: SpecSources, trans_ver: Any) -> Any:
    """Normalize the display string only when it's a raw shorthand (no speed count).

    normalize_transmission_standard is a bucket classifier; applying it to
    strings that already contain speed info ("10-Speed Automatic") would strip
    the count to just "Automatic".
    """
    from backend.enrichment import knowledge_engine as ke

    if trans_ver and not ke._is_na_spec(str(trans_ver)) and not re.search(r"\b\d+[-\s]?speed\b", trans_ver, re.I):
        from backend.utils.transmission_normalize import normalize_transmission_standard

        vin_raw = src.car.get("vin")
        vin_s = str(vin_raw).strip() if vin_raw not in (None, "") else None
        norm_t, _weak = normalize_transmission_standard(
            trans_ver,
            make=src.make,
            model=src.model,
            trim=src.trim,
            title=src.title_for_decode,
            year=src.year,
            vin=vin_s,
        )
        if norm_t:
            trans_ver = norm_t
    return trans_ver
