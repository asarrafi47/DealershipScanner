"""
API / UI serialization: last-line defense so junk placeholders never leave the backend.

Use ``serialize_car_for_api`` for JSON responses and embedded listing payloads.
Templates can use Jinja filters ``format_display_value`` / ``engine_display`` registered in ``main``.
"""
from __future__ import annotations

import logging
import math
import os
import re
from typing import Any

from backend.utils.field_clean import (
    clean_car_row_dict,
    is_effectively_empty,
    is_spec_overlay_junk,
    normalize_optional_url,
)

logger = logging.getLogger(__name__)

DISPLAY_DASH = "—"

_BMW_SRC_CPO_MARKERS = (
    "certified-inventory",
    "certified_inventory",
    "/certified/",
    "/cpo/",
    "certifiedused",
    "bmw-certified",
    "-cpo-",
    "cpo-inventory",
)


def _bmw_trace_vin_enabled(vin: str | None) -> bool:
    raw = (os.environ.get("BMW_TRACE_VINS") or "").strip().upper()
    if not raw or not vin:
        return False
    v = str(vin).strip().upper()[:17]
    return v in {x.strip()[:17] for x in raw.split(",") if x.strip()}


def _bmw_resolve_condition_for_display(
    c: dict[str, Any],
    out: dict[str, Any],
    *,
    title_lower: str,
) -> None:
    """
    BMW-only display rules for *condition* (does not invent odometer-based used).

    Priority: explicit Used/New in DB → keep; specific dealer condition strings → keep;
    then ``is_cpo`` / title CPO phrases / certified inventory URL / title Used|New prefix;
    generic DB value ``certified`` → *Certified Pre-Owned*; else leave ``out`` unchanged.
    """
    if (c.get("make") or "").strip().upper() != "BMW":
        return

    dash = DISPLAY_DASH
    raw_cond = c.get("condition")
    rl = str(raw_cond).strip().lower() if raw_cond else ""

    if rl in ("used", "new"):
        return
    if rl and rl not in ("certified",) and "certif" not in rl:
        if out.get("condition") != dash:
            return

    vin_key = str(c.get("vin") or "")[:17]
    trace = _bmw_trace_vin_enabled(vin_key)
    rule: str | None = None

    if c.get("is_cpo") in (1, True, "1"):
        out["condition"] = "Certified Pre-Owned"
        rule = "is_cpo"
    elif title_lower and (
        "bmw certified" in title_lower
        or "bmw cpo" in title_lower
        or "certified pre-owned" in title_lower
        or "certified preowned" in title_lower
    ):
        out["condition"] = "Certified Pre-Owned"
        rule = "title_cpo"
    else:
        su = (c.get("source_url") or "").lower()
        if any(m in su for m in _BMW_SRC_CPO_MARKERS) or ("certified" in su and "inventory" in su):
            out["condition"] = "Certified Pre-Owned"
            rule = "source_url_cpo"
        elif title_lower:
            if title_lower.startswith("used "):
                out["condition"] = "Used"
                rule = "title_used_prefix"
            elif title_lower.startswith("new "):
                out["condition"] = "New"
                rule = "title_new_prefix"

    if rule is None and rl == "certified":
        out["condition"] = "Certified Pre-Owned"
        rule = "db_certified_token"

    if trace:
        logger.info(
            "BMW condition display trace vin=%s rule=%r raw_db_condition=%r is_cpo=%r title=%r source_url=%r out_condition=%r",
            vin_key,
            rule,
            raw_cond,
            c.get("is_cpo"),
            (c.get("title") or "")[:120],
            (c.get("source_url") or "")[:160],
            out.get("condition"),
        )


def _format_mpg_city_highway(mpg_city: Any, mpg_highway: Any) -> str | None:
    """Delegate to field_clean (shared with knowledge_engine verified_specs)."""
    from backend.utils.field_clean import format_mpg_city_highway_display

    return format_mpg_city_highway_display(mpg_city, mpg_highway)


def _dealer_spec_wins(dealer_val: Any) -> bool:
    """True when DB/dealer column has a real value (VDP/listing) vs EPA placeholder."""
    if dealer_val is None:
        return False
    if isinstance(dealer_val, bool):
        return True
    if isinstance(dealer_val, (int, float)):
        if isinstance(dealer_val, float) and math.isnan(dealer_val):
            return False
        return True
    if is_effectively_empty(dealer_val):
        return False
    s = str(dealer_val).strip()
    if not s or is_spec_overlay_junk(s):
        return False
    return True


# Dealer DMS boilerplate → treat as missing in UI/API
_MANUFACTURER_SPEC_RE = re.compile(
    r"see\s+manufacturer|manufacturer\s+specifications|refer\s+to\s+manufacturer",
    re.IGNORECASE,
)

# Listing/VDP text that is only a liter figure (no layout / cylinder / motor words) — merge with inferred cylinders.
_DISP_ONLY_ENGINE_RE = re.compile(r"^\s*(\d+\.\d+|\d+)\s*l?\s*$", re.IGNORECASE)


def _is_displacement_only_engine_text(s: str) -> bool:
    t = (s or "").strip()
    if not t or is_effectively_empty(t):
        return False
    if _MANUFACTURER_SPEC_RE.search(t):
        return False
    if re.search(
        r"(?i)\b(v|i|inline|flat|h|w|twin|single|dual|triple|quad|turbo|supercharg|diesel|"
        r"electric|plug|motor|hp|kw|lb[-\s]?ft|liter|litre|cylinders?|rotary|hybrid)\b",
        t,
    ):
        return False
    return bool(_DISP_ONLY_ENGINE_RE.match(t))


def _effective_cylinder_count(car: dict[str, Any], vs: dict[str, Any]) -> int | None:
    """Prefer dealer cylinders when valid; else merged ``cylinders_display`` / ``cylinders`` from verified specs."""
    raw = car.get("cylinders")
    try:
        di = int(raw) if raw is not None and str(raw).strip() != "" else None
    except (TypeError, ValueError):
        di = None
    if di is not None and di > 0:
        return di
    if not vs:
        return None
    for key in ("cylinders_display", "cylinders"):
        v = vs.get(key)
        if v is None or str(v).strip() == "":
            continue
        try:
            vi = int(v)
            if vi > 0:
                return vi
        except (TypeError, ValueError):
            continue
    return None


def format_display_value(value: Any, *, dash: str = DISPLAY_DASH) -> str:
    """
    Human-facing string for a spec field.
    None / null / N/A / 'None' / manufacturer boilerplate → em dash (—).
    """
    if value is None:
        return dash
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, float) and math.isnan(value):
        return dash
    if isinstance(value, (int, float)):
        if isinstance(value, float) and value == int(value):
            return str(int(value))
        return str(value)
    s = str(value).strip()
    if not s:
        return dash
    low = s.lower()
    if low in ("none", "null", "undefined"):
        return dash
    if is_effectively_empty(s):
        return dash
    if _MANUFACTURER_SPEC_RE.search(s):
        return dash
    return s


def _fuel_word(car: dict[str, Any]) -> str:
    ft = (car.get("fuel_type") or "").strip()
    if not ft or is_effectively_empty(ft):
        return "Gasoline"
    low = ft.lower()
    if "electric" in low and "plug" not in low:
        return "Electric"
    if "plug" in low or "phev" in low:
        return "Plug-in hybrid"
    if "diesel" in low:
        return "Diesel"
    if "hybrid" in low:
        return "Hybrid"
    return ft[:40]


def _cylinder_layout_token(cyl_i: int, *text_hints: str | None) -> str:
    """
    Short layout label (e.g. ``V8``, ``I4``). Prefer tokens found in listing/EPA text;
    otherwise use common heuristics by cylinder count.
    """
    blob = " ".join(
        p.strip() for p in text_hints if isinstance(p, str) and p.strip()
    ).upper()
    if blob:
        m = re.search(r"\bV\s*-?\s*(\d{1,2})\b", blob)
        if m:
            return f"V{int(m.group(1))}"
        m = re.search(r"\bI\s*-?\s*(\d)\b", blob)
        if m:
            return f"I{int(m.group(1))}"
        if re.search(r"\b(FLAT|H)\s*-?\s*4\b", blob) or re.search(r"\bH4\b", blob):
            return "H4"
        if re.search(r"\b(FLAT|H)\s*-?\s*6\b", blob) or re.search(r"\bH6\b", blob):
            return "H6"
        if re.search(r"\bINLINE\s*-?\s*6\b", blob) or re.search(r"\bIN[-\s]?LINE\s*-?\s*6\b", blob):
            return "I6"
    if cyl_i <= 0:
        return ""
    by_count = {
        1: "1-cyl",
        2: "2-cyl",
        3: "I3",
        4: "I4",
        5: "I5",
        6: "V6",
        7: "7-cyl",
        8: "V8",
        10: "V10",
        12: "V12",
    }
    return by_count.get(cyl_i, f"{cyl_i}-cyl")


def parse_engine_displacement_liters(car: dict[str, Any]) -> float | None:
    """
    Best-effort displacement in liters for structured filters (``engine_l`` column first,
    then a ``N`` or ``N.N`` prefix before ``L`` in ``engine_description``).
    """
    raw = car.get("engine_l")
    if raw is not None:
        s = str(raw).strip().lower()
        if s in ("electric", "phev", ""):
            return None
        try:
            v = float(s.replace("l", "").strip())
            if v > 0:
                return v
        except (TypeError, ValueError):
            pass
    ed = car.get("engine_description")
    if isinstance(ed, str) and ed.strip():
        m = re.search(r"(\d+\.\d+|\d+)\s*[lL]\b", ed)
        if m:
            try:
                v = float(m.group(1))
                return v if v > 0 else None
            except ValueError:
                return None
        if _is_displacement_only_engine_text(ed):
            m2 = _DISP_ONLY_ENGINE_RE.match(ed.strip())
            if m2:
                try:
                    v = float(m2.group(1))
                    return v if v > 0 else None
                except ValueError:
                    pass
    return None


def _format_engine_l_numeric_liters(lit: float) -> str:
    s = f"{lit:.3f}".rstrip("0").rstrip(".")
    return s


def infer_engine_l_for_db(car: dict[str, Any]) -> str | None:
    """
    Suggested value for the ``cars.engine_l`` column (TEXT): short displacement (e.g. ``2.0``),
    or ``Electric`` / ``PHEV`` for electrified rows. Used at upsert when the scraper set
    ``engine_description`` / analytics ``engine`` but not ``engine_l``.
    """
    raw = car.get("engine_l")
    if raw is not None and not is_effectively_empty(raw) and not is_spec_overlay_junk(raw):
        s = str(raw).strip()
        low = s.lower()
        if low in ("electric", "phev"):
            return "Electric" if low == "electric" else "PHEV"
        if low in ("n/a", "na", "none", "tbd", "---", "0", "0.0"):
            pass
        else:
            return s[:32]

    try:
        ci = int(car.get("cylinders")) if car.get("cylinders") is not None and str(car.get("cylinders")).strip() != "" else None
    except (TypeError, ValueError):
        ci = None
    if ci == 0:
        ft = str(car.get("fuel_type") or "").lower()
        if "plug" in ft or "phev" in ft:
            return "PHEV"
        if "electric" in ft and "plug" not in ft:
            return "Electric"
        if "hybrid" in ft and "plug" in ft:
            return "PHEV"

    lit = parse_engine_displacement_liters(car)
    if lit is not None and lit > 0:
        return _format_engine_l_numeric_liters(lit)[:32]
    return None


def car_matches_engine_displacement_l_range(
    car: dict[str, Any],
    lo: float | None,
    hi: float | None,
) -> bool:
    """True when parsed liters is within ``[lo, hi]`` (inclusive); unknown liters never match."""
    if lo is None and hi is None:
        return True
    v = parse_engine_displacement_liters(car)
    if v is None:
        return False
    if lo is not None and v < lo:
        return False
    if hi is not None and v > hi:
        return False
    return True


def build_engine_display(car: dict[str, Any], verified_specs: dict[str, Any] | None = None) -> str:
    """
    Priority: rich ``engine_description`` (not manufacturer boilerplate, not displacement-only)
    → inferred ``master_engine_string`` (same rules) →
    ``{liters}L`` + layout (e.g. ``2.0L I4``, ``4.4L V8``) using ``engine_l`` / thin description
    plus effective cylinder count (dealer row, else ``cylinders_display`` / ``cylinders`` from
    verified specs) → partial → —.

    Displacement-only strings (e.g. ``2``, ``2.0``, ``2.0L``) are never shown alone when verified
    specs supply a positive cylinder count — common for BMW VDP rows missing an explicit layout.
    """
    c = clean_car_row_dict(car)
    dash = DISPLAY_DASH
    vs = verified_specs or {}
    mes = vs.get("master_engine_string") if isinstance(vs.get("master_engine_string"), str) else None

    ed = c.get("engine_description")
    if isinstance(ed, str) and ed.strip() and not is_effectively_empty(ed):
        if not _MANUFACTURER_SPEC_RE.search(ed) and not _is_displacement_only_engine_text(ed):
            return format_display_value(ed, dash=dash)

    if isinstance(mes, str) and mes.strip():
        if not _is_displacement_only_engine_text(mes):
            fd = format_display_value(mes, dash=dash)
            if fd != dash:
                return fd

    eng_l = c.get("engine_l")
    cyl_i = _effective_cylinder_count(c, vs)

    if cyl_i == 0:
        return "Electric"

    lit = None
    if eng_l is not None and str(eng_l).strip():
        s = str(eng_l).strip()
        if s.lower() in ("electric", "phev"):
            return "Electric" if s.lower() == "electric" else "Plug-in hybrid"
        try:
            f = float(s.replace("L", "").strip())
            if f > 0:
                lit = f"{f:.1f}L"
        except (TypeError, ValueError):
            if not is_effectively_empty(s):
                return format_display_value(s, dash=dash)

    if lit is None and isinstance(ed, str) and ed.strip() and _is_displacement_only_engine_text(ed):
        try:
            f = float(re.sub(r"(?i)l\s*$", "", ed.strip()))
            if f > 0:
                lit = f"{f:.1f}L"
        except (TypeError, ValueError):
            pass

    layout_hints: tuple[str | None, ...] = ()
    if isinstance(ed, str) and ed.strip():
        layout_hints = (ed.strip(),)
    if cyl_i is not None and cyl_i > 0:
        layout = _cylinder_layout_token(cyl_i, *layout_hints, mes)
    else:
        layout = ""

    if lit and layout:
        return f"{lit} {layout}"
    if lit:
        return lit
    if layout:
        return layout

    return dash


def _bmw_series_trim_from_motor_model(model_raw: str) -> tuple[str | None, str | None]:
    """
    Single-token BMW motor models: 330i -> (3 Series, 330i); M340i -> (3 Series, M340i).
    Does not split X3, i4, or multi-word model strings.
    """
    s = (model_raw or "").strip()
    if not s or " " in s:
        return None, None
    su = s.upper()
    if su.startswith("X") and re.match(r"^X\d", su):
        return None, None
    if su.startswith("Z") and re.match(r"^Z\d", su):
        return None, None
    if re.match(r"^[iI][Xx\d]", s):
        return None, None
    m = re.match(r"^M(\d)(\d{2})([iI])$", s)
    if m:
        return f"{m.group(1)} Series", s
    m2 = re.match(r"^([2-8])(\d{2})([eEiI]+)$", s)
    if m2:
        return f"{m2.group(1)} Series", s
    return None, None


# Strips the series-prefix digit from standard BMW motor trims:
#   330i -> 30i,  540i xDrive -> 40i xDrive,  M340i -> unchanged,  xDrive30i -> unchanged
_BMW_MOTOR_TRIM_PREFIX_RE = re.compile(r"^([2-9])(\d{2}[eEiI]\S*(?:\s+.*)?)$")


def _strip_bmw_trim_series_prefix(trim: str) -> str:
    """330i -> 30i; 540i xDrive -> 40i xDrive. Leaves M340i, xDrive30i, 30i untouched."""
    m = _BMW_MOTOR_TRIM_PREFIX_RE.match(trim.strip())
    return m.group(2) if m else trim


_BMW_MODEL_TAIL = re.compile(
    r"""
    ^(?P<base>
        X\d[A-Za-z]?              # X3, X5M
      | [iI][Xx\d]+               # i4, iX, i7
      | Z\d                       # Z4
      | \d{3,4}[eE]?              # 330i, 530e, 760i
      | \d\s+Series               # 5 Series, 3 Series
    )
    \s+(?P<tail>.+)$
    """,
    re.VERBOSE | re.IGNORECASE,
)


def _bmw_title_suffix_trim(title: str, model: str) -> str | None:
    """If title contains ``… {model} {trim}``, return trim tail (conservative)."""
    if not title or not model:
        return None
    t = title.strip()
    m = model.strip()
    if len(m) < 2:
        return None
    idx = t.upper().rfind(m.upper())
    if idx < 0:
        return None
    rest = t[idx + len(m) :].strip()
    if len(rest) < 2:
        return None
    if not re.search(r"(?i)(s?drive|xdrive|m\s+sport|competition|pure\s+impulse|gran\s+coupe)", rest):
        if not re.match(r"^[A-Za-z0-9][A-Za-z0-9\s\-]{1,60}$", rest):
            return None
    return rest[:80]


def apply_bmw_model_trim_display(car: dict[str, Any]) -> tuple[str, str]:
    """
    Return (model_display, trim_display) for BMW rows only. Does not mutate *car*.
    Conservative: only fills trim from title/model split when trim is missing.
    """
    make = (car.get("make") or "").strip()
    if make.upper() != "BMW":
        return format_display_value(car.get("model")), format_display_value(car.get("trim"))

    model_raw = (car.get("model") or "").strip()
    trim_raw = car.get("trim")
    title = (car.get("title") or "").strip()

    model_base = model_raw
    trim_extra: str | None = None
    series_from_motor, motor_trim = _bmw_series_trim_from_motor_model(model_raw)
    if series_from_motor:
        model_base = series_from_motor

    mm = _BMW_MODEL_TAIL.match(model_raw)
    if mm:
        model_base = mm.group("base").strip()
        trim_extra = mm.group("tail").strip()

    trim_out = trim_raw if isinstance(trim_raw, str) and trim_raw.strip() else None
    if trim_extra:
        trim_out = trim_extra if not trim_out else f"{trim_out} / {trim_extra}"

    if motor_trim and (not trim_out or is_effectively_empty(trim_out)):
        trim_out = motor_trim

    if not trim_out or is_effectively_empty(trim_out):
        from_title = _bmw_title_suffix_trim(title, model_base)
        if from_title:
            trim_out = from_title

    # Normalize: 330i → 30i, 540i xDrive → 40i xDrive (strip series prefix digit).
    if trim_out and not is_effectively_empty(trim_out):
        trim_out = _strip_bmw_trim_series_prefix(trim_out.strip())

    return format_display_value(model_base), format_display_value(trim_out)


def serialize_car_for_api(
    car: dict[str, Any],
    *,
    include_verified: bool = True,
    verified_specs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Shallow copy safe for JSON: strings cleaned + display dashes, numbers preserved.
    Adds ``engine_display``; ``model`` / ``trim`` may be BMW-normalized for display only.

    ``include_verified=False`` skips EPA/trim merge (use for bulk listing payloads).
    Pass ``verified_specs`` when the caller already merged (e.g. car detail page).
    """
    if not car:
        return {}
    c = clean_car_row_dict(dict(car))

    vs: dict[str, Any] = {}
    if verified_specs is not None:
        vs = verified_specs
    elif include_verified:
        try:
            from backend.knowledge_engine import merge_verified_specs

            vs = merge_verified_specs(c)
        except Exception:
            vs = {}

    model_d, trim_d = apply_bmw_model_trim_display(c)
    engine_disp = build_engine_display(c, vs if vs else None)

    out: dict[str, Any] = {}
    for k, v in c.items():
        if k in ("kbb_snapshot_json", "internal_notes", "marked_for_review", "price_provenance_json"):
            continue
        if k in ("gallery", "history_highlights"):
            out[k] = v
            continue
        if k == "packages":
            if is_effectively_empty(v) or str(v).strip() in ("{}", "[]"):
                out[k] = None
            else:
                out[k] = v
            continue
        if k in (
            "price",
            "mileage",
            "year",
            "msrp",
            "cylinders",
            "mpg_city",
            "mpg_highway",
            "id",
            "distance_miles",
            "dealership_registry_id",
            "kbb_fair_purchase",
            "kbb_range_low",
            "kbb_range_high",
            "kbb_private_party",
            "kbb_trade_in",
        ):
            out[k] = v
            continue
        if k == "engine_l":
            out[k] = v
            continue
        if k == "data_quality_score" and isinstance(v, (int, float)):
            out[k] = v
            continue
        if isinstance(v, (dict, list)) and k not in ("gallery", "history_highlights"):
            out[k] = v
            continue
        if isinstance(v, (int, float)):
            out[k] = v
            continue
        out[k] = format_display_value(v)

    for url_key in ("image_url", "source_url", "carfax_url", "dealer_url"):
        if url_key in c:
            out[url_key] = normalize_optional_url(c.get(url_key))

    out["model"] = model_d
    out["trim"] = trim_d
    out["engine_display"] = engine_disp

    vcyl = vs.get("cylinders")
    if vcyl is not None and (c.get("cylinders") is None or str(c.get("cylinders")).strip() == ""):
        try:
            out["cylinders"] = int(vcyl)
        except (TypeError, ValueError):
            out["cylinders"] = vcyl

    # Prefer grounded dealer/VDP columns over EPA-only inference when dealer data is real.
    dealer_t = c.get("transmission")
    inferred_t = vs.get("transmission_display")
    if _dealer_spec_wins(dealer_t):
        td = format_display_value(dealer_t)
    else:
        td = format_display_value(inferred_t or dealer_t)

    dealer_d = c.get("drivetrain")
    inferred_dd = vs.get("drivetrain_display")
    if _dealer_spec_wins(dealer_d):
        dd = format_display_value(dealer_d)
    else:
        dd = format_display_value(inferred_dd or dealer_d)

    out["transmission_display"] = td
    out["drivetrain_display"] = dd

    fe = vs.get("fuel_economy_display")
    if not fe or (isinstance(fe, str) and fe.strip() in ("", "—", "-")):
        fe = _format_mpg_city_highway(c.get("mpg_city"), c.get("mpg_highway"))
    out["fuel_economy_display"] = format_display_value(fe) if fe else DISPLAY_DASH

    bsd = vs.get("body_style_display")
    if bsd and (is_effectively_empty(c.get("body_style")) or out.get("body_style") == DISPLAY_DASH):
        out["body_style"] = format_display_value(bsd)

    if out.get("body_style") == DISPLAY_DASH or is_effectively_empty(out.get("body_style")):
        try:
            from backend.knowledge_engine import decode_trim_logic

            _hints = decode_trim_logic(c.get("make"), c.get("model"), c.get("trim"), c.get("title"))
            _bh = _hints.get("body_style_hint")
            if _bh:
                out["body_style"] = format_display_value(_bh)
        except Exception:
            pass

    fill_derived_condition_for_display(c, out)

    from backend.utils.interior_color_buckets import infer_paint_color_buckets, parse_stored_buckets

    out["exterior_color_families"] = infer_paint_color_buckets(c.get("exterior_color"), c.get("make"))
    _ib = parse_stored_buckets(car.get("interior_color_buckets"))
    out["interior_color_families"] = (
        _ib if _ib else infer_paint_color_buckets(c.get("interior_color"), c.get("make"))
    )

    _attach_kbb_listing_summary(c, out)
    return out


def _fmt_usd0(n: float | int | None) -> str | None:
    if n is None:
        return None
    try:
        x = float(n)
    except (TypeError, ValueError):
        return None
    if x <= 0:
        return None
    return f"${x:,.0f}"


def _attach_kbb_listing_summary(c: dict[str, Any], out: dict[str, Any]) -> None:
    """
    Adds ``kbb`` summary for templates/API: fair purchase + range vs asking price.
    Does not expose raw ``kbb_snapshot_json`` on the public car payload.
    """
    fp = c.get("kbb_fair_purchase")
    lo = c.get("kbb_range_low")
    hi = c.get("kbb_range_high")
    fetched = c.get("kbb_fetched_at")
    has_any = any(
        x is not None
        for x in (fp, lo, hi, c.get("kbb_private_party"), c.get("kbb_trade_in"))
    )
    if not has_any:
        out["kbb"] = None
        return

    price = c.get("price")
    try:
        price_f = float(price) if price is not None and str(price).strip() != "" else None
    except (TypeError, ValueError):
        price_f = None
    if price_f is not None and price_f <= 0:
        price_f = None

    vs = None
    if price_f is not None:
        if lo is not None and hi is not None:
            try:
                lo_f, hi_f = float(lo), float(hi)
                if price_f < lo_f:
                    vs = "below_kbb_range"
                elif price_f > hi_f:
                    vs = "above_kbb_range"
                else:
                    vs = "within_kbb_range"
            except (TypeError, ValueError):
                vs = None
        if vs is None and fp is not None:
            try:
                fpp = float(fp)
                if price_f < fpp * 0.97:
                    vs = "below_kbb_fair_purchase"
                elif price_f > fpp * 1.03:
                    vs = "above_kbb_fair_purchase"
                else:
                    vs = "near_kbb_fair_purchase"
            except (TypeError, ValueError):
                vs = None

    labels = {
        "below_kbb_range": "Below KBB fair market range",
        "within_kbb_range": "Within KBB fair market range",
        "above_kbb_range": "Above KBB fair market range",
        "below_kbb_fair_purchase": "Below KBB typical listing / fair purchase",
        "near_kbb_fair_purchase": "Close to KBB typical listing / fair purchase",
        "above_kbb_fair_purchase": "Above KBB typical listing / fair purchase",
    }

    range_txt = None
    if lo is not None and hi is not None:
        a, b = _fmt_usd0(lo), _fmt_usd0(hi)
        if a and b:
            range_txt = f"{a} – {b}"

    out["kbb"] = {
        "has_data": True,
        "fair_purchase": fp,
        "fair_purchase_display": _fmt_usd0(fp),
        "range_low": lo,
        "range_high": hi,
        "range_display": range_txt,
        "private_party_display": _fmt_usd0(c.get("kbb_private_party")),
        "trade_in_display": _fmt_usd0(c.get("kbb_trade_in")),
        "fetched_at": format_display_value(fetched) if fetched else None,
        "vs_listing_code": vs,
        "vs_listing_label": labels.get(vs) if vs else None,
    }


def fill_derived_condition_for_display(c: dict[str, Any], out: dict[str, Any]) -> None:
    """
    Mutates *out* ``condition`` from title / CPO / mileage / model year heuristics.
    *out* must already contain ``condition`` from ``format_display_value``.
    """
    tit = (c.get("title") or "").strip()
    low = tit.lower()
    if (c.get("make") or "").strip().upper() == "BMW":
        _bmw_resolve_condition_for_display(c, out, title_lower=low)
    elif tit and (is_effectively_empty(c.get("condition")) or out.get("condition") == DISPLAY_DASH):
        if "certified pre-owned" in low or "certified preowned" in low:
            out["condition"] = "Certified Pre-Owned"
        elif "mazda certified" in low:
            out["condition"] = "Certified Pre-Owned"
        elif re.search(r"\bcpo\b", low):
            out["condition"] = "Certified Pre-Owned"
        elif " certified " in f" {low} " or low.startswith("certified "):
            out["condition"] = "Certified"
        elif low.startswith("used "):
            out["condition"] = "Used"
        elif low.startswith("new "):
            out["condition"] = "New"

    _oc = out.get("condition")
    if _oc is None or str(_oc).strip() in ("", DISPLAY_DASH):
        if c.get("is_cpo") in (1, True, "1"):
            out["condition"] = "Certified Pre-Owned"
        else:
            _low = (c.get("title") or "").lower()
            _su = (c.get("source_url") or "").lower()
            # CPO / certified inventory before mileage→Used so listings are not mislabeled.
            if (
                re.search(r"\bcpo\b", _low)
                or "certified pre-owned" in _low
                or "certified preowned" in _low
                or "mazda certified" in _low
                or (" certified " in f" {_low} " and "pre-owned" in _low)
                or _low.startswith("certified ")
                or "/certified" in _su
                or "cpo-inventory" in _su
                or "-cpo-" in _su
                or "certified_inventory" in _su.replace("-", "_")
            ):
                out["condition"] = "Certified Pre-Owned"
            else:
                _mi: int | None
                try:
                    raw_m = c.get("mileage")
                    if raw_m is None or str(raw_m).strip() == "":
                        _mi = None
                    else:
                        _mi = int(float(str(raw_m).replace(",", "")))
                except (TypeError, ValueError):
                    _mi = None
                if _mi is not None and _mi > 0:
                    out["condition"] = "Used"
                elif _mi == 0:
                    _ttl = (c.get("title") or "").lower()
                    if (
                        "/new-inventory" in _su
                        or "/new/" in _su
                        or "newinventory" in _su.replace("-", "").replace("_", "")
                        or _ttl.startswith("new ")
                    ):
                        out["condition"] = "New"

    # Model year <= 2023: almost never new retail; default Pre-Owned if still unknown.
    # Prefer Certified Pre-Owned when listing text/URL still suggests a CPO program.
    _oc3 = out.get("condition")
    if _oc3 is None or str(_oc3).strip() in ("", DISPLAY_DASH):
        try:
            yy = int(c.get("year")) if c.get("year") is not None else None
        except (TypeError, ValueError):
            yy = None
        if yy is not None and yy < 2024:
            low3 = (c.get("title") or "").lower()
            su3 = (c.get("source_url") or "").lower()
            cpo_hint = (
                c.get("is_cpo") in (1, True, "1")
                or re.search(r"\bcpo\b", low3)
                or "certified pre-owned" in low3
                or "certified preowned" in low3
                or "mazda certified" in low3
                or (" certified " in f" {low3} " and "pre-owned" in low3)
                or "/certified" in su3
                or "cpo-inventory" in su3
                or "-cpo-" in su3
            )
            if cpo_hint:
                out["condition"] = "Certified Pre-Owned"
            else:
                out["condition"] = "Pre-Owned"

    # 2024+ with no title/mileage signal: inventory SRP / VDP URL often encodes new vs used.
    _oc4 = out.get("condition")
    if _oc4 is None or str(_oc4).strip() in ("", DISPLAY_DASH):
        try:
            yy4 = int(c.get("year")) if c.get("year") is not None else None
        except (TypeError, ValueError):
            yy4 = None
        if yy4 is not None and yy4 >= 2024:
            su4 = (c.get("source_url") or "").lower()
            if (
                "used-inventory" in su4
                or "used_inventory" in su4
                or "pre-owned" in su4
                or "preowned" in su4
                or "/used/" in su4
            ):
                out["condition"] = "Used"
            elif (
                "new-inventory" in su4
                or "newinventory" in su4.replace("-", "").replace("_", "")
                or "/new/" in su4
            ):
                out["condition"] = "New"


def infer_condition_for_storage(car: dict[str, Any]) -> str | None:
    """
    Return a ``cars.condition`` value to persist when the row has no real condition yet.
    None means leave the column unchanged (caller may still NULL junk via ``clean_car_row_dict``).
    """
    c = clean_car_row_dict(dict(car))
    if not is_effectively_empty(c.get("condition")):
        return None
    out: dict[str, Any] = {"condition": format_display_value(c.get("condition"))}
    fill_derived_condition_for_display(c, out)
    fin = out.get("condition")
    if not fin or str(fin).strip() in ("", DISPLAY_DASH):
        return None
    return str(fin).strip()


def _format_mileage_mi(mileage: Any) -> str:
    if mileage is None or str(mileage).strip() == "":
        return "—"
    try:
        return f"{int(mileage):,} mi"
    except (TypeError, ValueError):
        return "—"


def build_detail_display_snapshot(
    verified_specs: dict[str, Any],
    ser: dict[str, Any],
) -> dict[str, Any]:
    """
    Text the car detail template would show for key spec rows (for /dev/api/car-debug).
    *ser* must be from serialize_car_for_api(..., verified_specs=verified_specs).
    """
    # Raw cylinder count for dev/debug; user-facing copy is folded into ``engine_display``.
    cc = ser.get("cylinders")
    if cc is None or str(cc).strip() == "":
        cyl_render = "—"
    else:
        cyl_render = str(cc)
    return {
        "year": ser.get("year"),
        "make": ser.get("make"),
        "model": ser.get("model"),
        "trim": ser.get("trim"),
        "mileage_mi": _format_mileage_mi(ser.get("mileage")),
        "engine": ser.get("engine_display"),
        "transmission": ser.get("transmission_display"),
        "drivetrain": ser.get("drivetrain_display"),
        "body_style": ser.get("body_style"),
        "fuel_type": ser.get("fuel_type"),
        "condition": ser.get("condition"),
        "cylinders": cyl_render,
        "efficiency": ser.get("fuel_economy_display"),
        "exterior_color": ser.get("exterior_color"),
        "interior_color": ser.get("interior_color"),
        "vin": ser.get("vin"),
        "stock_number": ser.get("stock_number"),
    }
