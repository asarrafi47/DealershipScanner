"""
``serialize_car_for_api`` spec-sheet precedence: forced induction, fuel type,
cylinders, transmission (persisted bucket vs dealer text vs EPA, then vPIC wins),
drivetrain, fuel economy, model generation and body style.

Split out of ``serialize.serialize_car_for_api`` (2026-10-01) with no behaviour
change; ``backend/tests/test_serialize_car_for_api_golden.py`` pins the output.
"""
from __future__ import annotations

import re
from typing import Any

from backend.utils.field_clean import coerce_drivetrain_stored, is_effectively_empty

from ._common import (
    DISPLAY_DASH,
    _dealer_spec_wins,
    _format_mpg_city_highway,
    _transmission_line_has_gear_count,
    _transmission_phrase_prefer_detail,
    format_display_value,
)
from .engine import _effective_fuel_type_for_display


def apply_forced_induction(out: dict[str, Any], c: dict[str, Any]) -> None:
    """Forced induction: use stored value or compute on the fly."""
    _fi = c.get("forced_induction") or ""
    if not _fi.strip():
        try:
            from backend.utils.forced_induction import classify_forced_induction_from_car_row
            _fi = classify_forced_induction_from_car_row(c) or ""
        except Exception:
            _fi = ""
    out["forced_induction"] = _fi or None


def apply_fuel_type_override(out: dict[str, Any], c: dict[str, Any], engine_disp: Any) -> None:
    ft_override = _effective_fuel_type_for_display(c, engine_disp)
    if ft_override:
        out["fuel_type"] = format_display_value(ft_override)


def fill_cylinders_from_verified(out: dict[str, Any], c: dict[str, Any], vs: dict[str, Any]) -> None:
    vcyl = vs.get("cylinders")
    if vcyl is not None and (c.get("cylinders") is None or str(c.get("cylinders")).strip() == ""):
        try:
            out["cylinders"] = int(vcyl)
        except (TypeError, ValueError):
            out["cylinders"] = vcyl


def resolve_transmission_display(c: dict[str, Any], vs: dict[str, Any]) -> Any:
    """Transmission line before the vPIC override (see :func:`apply_vpic_transmission`).

    Prefer persisted transmission_type bucket when present; else dealer text / EPA / normalize.
    Feeds sometimes label geared automatics (incl. many PHEVs) as "CVT"; trust detailed transmission
    when it clearly normalizes to a non-CVT bucket.
    """
    from backend.utils.transmission_normalize import normalize_transmission_standard

    stored_tt = c.get("transmission_type")
    dealer_t = c.get("transmission")
    y_int = c.get("year") if isinstance(c.get("year"), int) else None
    ignore_stored_cvt_bucket = False
    if (
        isinstance(stored_tt, str)
        and stored_tt.strip() == "CVT"
        and _dealer_spec_wins(dealer_t)
    ):
        d_norm, _d_weak = normalize_transmission_standard(
            dealer_t,
            make=c.get("make"),
            model=c.get("model"),
            trim=c.get("trim"),
            title=c.get("title"),
            year=y_int,
            vin=c.get("vin"),
            log_weak=False,
        )
        if d_norm and d_norm != "CVT":
            ignore_stored_cvt_bucket = True

    if (
        isinstance(stored_tt, str)
        and stored_tt.strip() in ("Automatic", "Manual", "CVT")
        and not ignore_stored_cvt_bucket
    ):
        bucket = stored_tt.strip()
        inferred_td = vs.get("transmission_display")
        if bucket == "Automatic" and isinstance(inferred_td, str) and _transmission_line_has_gear_count(
            inferred_td
        ):
            td = format_display_value(inferred_td.strip())
        elif bucket in ("Automatic", "Manual") and _dealer_spec_wins(dealer_t):
            raw_d = str(dealer_t).strip()
            if raw_d and re.search(r"\b\d+[-\s]?speed\b", raw_d, re.I):
                td = format_display_value(raw_d)
            else:
                td = format_display_value(bucket)
        else:
            td = format_display_value(bucket)
    else:
        inferred_t = vs.get("transmission_display")
        if _dealer_spec_wins(dealer_t):
            td_src = dealer_t
        else:
            td_src = inferred_t or dealer_t

        td_norm, _td_weak = normalize_transmission_standard(
            td_src,
            make=c.get("make"),
            model=c.get("model"),
            trim=c.get("trim"),
            title=c.get("title"),
            year=y_int,
            vin=c.get("vin"),
            log_weak=False,
        )
        pick = _transmission_phrase_prefer_detail(td_src, td_norm)
        td = format_display_value(pick if pick is not None else td_src)
    return td


def resolve_drivetrain_display(c: dict[str, Any], vs: dict[str, Any]) -> Any:
    dealer_d = coerce_drivetrain_stored(c.get("drivetrain"))
    bs_raw = str(c.get("body_style") or "").lower()
    if dealer_d == "FWD" and "pickup" in bs_raw:
        dealer_d = None
    inferred_dd = vs.get("drivetrain_display")
    if _dealer_spec_wins(dealer_d):
        dd = format_display_value(dealer_d)
    else:
        dd = format_display_value(inferred_dd or dealer_d)
    return dd


def apply_vpic_transmission(out: dict[str, Any], c: dict[str, Any], td: Any) -> Any:
    """Set ``transmission_source`` / ``transmission_feed``; return the line to display.

    vPIC TransmissionStyle outranks the feed (DC-7, visual review 2026-09-28):
    it is the per-VIN filing, while feed codes like "DDU" bucket an e-CVT
    hybrid as "Automatic". Kept the feed's line only when both name the same
    family and the feed carries a gear count the decode lacks. When the two
    disagree on family, the feed's own words ride along as
    ``transmission_feed`` so the page can show them muted.
    """
    from backend.utils.vpic_specs import (
        transmission_family,
        vpic_specs_for_vin as _vpic_specs_tx,
        vpic_transmission_label,
    )

    dealer_t = c.get("transmission")
    out["transmission_source"] = None
    out["transmission_feed"] = None
    _vp_tx = vpic_transmission_label(_vpic_specs_tx(c.get("vin")))
    if _vp_tx:
        _fam_feed = transmission_family(td)
        _fam_vpic = transmission_family(_vp_tx)
        _feed_has_gears = isinstance(td, str) and _transmission_line_has_gear_count(td)
        _vpic_has_gears = _transmission_line_has_gear_count(_vp_tx)
        if not (_fam_feed == _fam_vpic and _feed_has_gears and not _vpic_has_gears):
            if _fam_feed != _fam_vpic:
                _feed_raw = str(dealer_t or "").strip() or (td if isinstance(td, str) else "")
                if _feed_raw and _feed_raw not in ("—", "-"):
                    out["transmission_feed"] = _feed_raw
            td = _vp_tx
            out["transmission_source"] = "NHTSA vPIC"
    return td


def apply_fuel_economy(out: dict[str, Any], c: dict[str, Any], vs: dict[str, Any]) -> None:
    fe = vs.get("fuel_economy_display")
    if not fe or (isinstance(fe, str) and fe.strip() in ("", "—", "-")):
        fe = _format_mpg_city_highway(c.get("mpg_city"), c.get("mpg_highway"))
    out["fuel_economy_display"] = format_display_value(fe) if fe else DISPLAY_DASH


def apply_generation(out: dict[str, Any], vs: dict[str, Any]) -> None:
    """Model generation (backend.catalog model_generations — read-time join)."""
    out["generation_code"] = vs.get("generation_code")
    out["generation_years"] = vs.get("generation_years")


def apply_body_style(out: dict[str, Any], c: dict[str, Any], vs: dict[str, Any]) -> None:
    """Verified body style fills a blank, then a trim-decode hint, then normalization."""
    bsd = vs.get("body_style_display")
    if bsd and (is_effectively_empty(c.get("body_style")) or out.get("body_style") == DISPLAY_DASH):
        out["body_style"] = format_display_value(bsd)

    if out.get("body_style") == DISPLAY_DASH or is_effectively_empty(out.get("body_style")):
        try:
            from backend.enrichment.knowledge_engine import decode_trim_logic

            _hints = decode_trim_logic(c.get("make"), c.get("model"), c.get("trim"), c.get("title"))
            _bh = _hints.get("body_style_hint")
            if _bh:
                out["body_style"] = format_display_value(_bh)
        except Exception:
            pass

    from backend.utils.field_clean import normalize_body_style_for_car

    _bs_raw = out.get("body_style")
    if _bs_raw and _bs_raw != DISPLAY_DASH:
        _bs_corrected = normalize_body_style_for_car(
            str(_bs_raw),
            make=c.get("make"),
            model=c.get("model"),
            trim=c.get("trim"),
            title=c.get("title"),
        )
        if _bs_corrected:
            out["body_style"] = format_display_value(_bs_corrected)
