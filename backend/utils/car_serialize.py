"""
API / UI serialization: last-line defense so junk placeholders never leave the backend.

Use ``serialize_car_for_api`` for JSON responses and embedded listing payloads.
Templates can use Jinja filters ``format_display_value`` / ``engine_display`` registered in ``main``.
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
from typing import Any

from backend.parsers.base import filter_spyne_gallery_variants
from backend.utils.field_clean import (
    clean_car_row_dict,
    coerce_drivetrain_stored,
    is_effectively_empty,
    is_spec_overlay_junk,
    normalize_optional_url,
)
from backend.utils.safe_listing_url import normalize_listing_image_url

logger = logging.getLogger(__name__)

DISPLAY_DASH = "—"

# Omitted from public JSON and redacted in /dev debug endpoints (SEC-087).
SENSITIVE_CAR_ROW_KEYS = frozenset(
    {
        "internal_notes",
        "marked_for_review",
        "price_provenance_json",
        "spec_source_json",
        "recovery_notes",
        "recovery_source",
        "recovery_status",
        "recoverability_score",
        "missing_field_count",
    }
)


def redact_sensitive_car_row(row: dict[str, Any]) -> dict[str, Any]:
    """Copy car row dict without operator-only / provenance columns."""
    return {k: v for k, v in row.items() if k not in SENSITIVE_CAR_ROW_KEYS}

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


_EPA_MODE_AGGREGATE_RE = re.compile(
    r"\(?\s*EPA\s+mode\s+aggregate\s*\)?",
    re.IGNORECASE,
)


def _strip_epa_aggregate_label(s: str) -> str:
    if not s:
        return s
    return _EPA_MODE_AGGREGATE_RE.sub("", s).strip().strip(",").strip()


def _extract_liters_from_engine_text(s: str | None) -> float | None:
    """Pull a positive displacement from Monroney-style or dealer engine copy."""
    if not isinstance(s, str):
        return None
    t = _strip_epa_aggregate_label(_normalize_engine_description(s.strip()))
    if not t:
        return None
    m = re.search(r"(\d+\.\d+|\d+)\s*[lL]\b", t)
    if not m:
        return None
    try:
        v = float(m.group(1))
        return v if v > 0 else None
    except ValueError:
        return None


def _transmission_line_has_gear_count(s: Any) -> bool:
    if s is None:
        return False
    raw = str(s).strip()
    if not raw:
        return False
    if re.search(r"\b\d+[-\s]?speed\b", raw, re.I):
        return True
    if re.match(r"Auto(?:matic)?\s*\(\s*S\d+\s*\)", raw, re.I):
        return True
    if re.match(r"Auto(?:matic)?\s*\(\s*AM-S\d+\s*\)", raw, re.I):
        return True
    if re.match(r"Auto(?:matic)?\s*\(\s*A\d+\s*\)", raw, re.I):
        return True
    return False


def _transmission_phrase_prefer_detail(td_src: Any, td_norm: str | None) -> Any:
    """
    If the source string names a gear count (e.g. '8-Speed Automatic'), show that phrase
    instead of collapsing to the bucket label 'Automatic' / 'Manual'.
    """
    if td_src is None:
        return td_norm if td_norm else td_src
    raw = str(td_src).strip()
    if not raw:
        return td_norm if td_norm else td_src
    if td_norm in ("Automatic", "Manual") and re.search(r"\b\d+[-\s]?speed\b", raw, re.I):
        return raw
    return td_norm if td_norm else td_src


# Dealer DMS boilerplate → treat as missing in UI/API
_MANUFACTURER_SPEC_RE = re.compile(
    r"see\s+manufacturer|manufacturer\s+specifications|refer\s+to\s+manufacturer",
    re.IGNORECASE,
)

# Listing/VDP text that is only a liter figure (no layout / cylinder / motor words) — merge with inferred cylinders.
_DISP_ONLY_ENGINE_RE = re.compile(r"^\s*(\d+\.\d+|\d+)\s*l?\s*$", re.IGNORECASE)

# Injection/valve/program tech codes that add no buyer-facing value.
_ENGINE_JUNK_PARENS_RE = re.compile(
    r"\(\s*(?:SIDI(?:\s*&\s*PFI)?|PFI|GDI|MPFI|DOHC|SOHC|FFV|Stop[- ]Start|ZL[0-9]+)"
    r"(?:[^)]*?)?\s*\)",
    re.IGNORECASE,
)
# Strip "Mild Hybrid" or plain "Hybrid" from inside parentheses but keep the text outside.
_ENGINE_MILD_HYBRID_PARENS_RE = re.compile(
    r"\(\s*(?:[^)]*?;\s*)?(?:Mild\s+)?Hybrid(?:[^)]*?)?\s*\)",
    re.IGNORECASE,
)
# Inline valve/injection codes not in parens: "16V", "MPFI", "DOHC", "PDI", "GDI"
_ENGINE_INLINE_TECH_RE = re.compile(
    r"\b\d{1,2}V\b|\b(?:MPFI|DOHC|SOHC|PDI|GDI)\b",
    re.IGNORECASE,
)
# Hyundai/Kia internal engine-family suffix codes standing alone after layout token.
_ENGINE_FAMILY_CODE_RE = re.compile(
    r"\b(?:CW|PY|Nu|Theta|Gamma|Kappa|Lambda|Tau|Smartstream)\b",
    re.IGNORECASE,
)
# Redundant fuel-quality or type suffixes at end of string.
_ENGINE_FUEL_SUFFIX_RE = re.compile(
    r"\s+(?:Midgrade\s+)?Gasoline\s*$",
    re.IGNORECASE,
)


def _normalize_engine_description(raw: str) -> str:
    """
    Strip injection-tech jargon from an ``engine_description`` string,
    keeping displacement, cylinder layout, and drivetrain qualifiers
    (Diesel, Hybrid, Mild Hybrid, Twin Turbo, Turbocharged).

    Examples:
      "2.0L I4 (SIDI)"                    → "2.0L I4"
      "Hybrid 3.6L V6 (Mild Hybrid)"      → "3.6L V6 Mild Hybrid"
      "Diesel 3.0L V6 Diesel"             → "3.0L V6 Diesel"
      "3.5L V6 24V PDI DOHC Twin Turbo"   → "3.5L V6 Twin Turbo"
      "EV Electricity"                    → "Electric"
      "2L GDI Nu"                         → "2.0L"   (displacement-only; cylinders merged downstream)
    """
    s = raw.strip()
    if not s:
        return s

    low = s.lower()

    if "electricity" in low or low.startswith("ev ") or low == "ev":
        return "Electric"

    # Detect and strip leading type prefixes; track what qualifiers to append.
    hybrid_prefix = False
    mild_hybrid_qual = False
    diesel_qual = False
    ffv_qual = False

    if re.match(r"(?i)^hybrid\s+", s):
        hybrid_prefix = True
        s = re.sub(r"(?i)^hybrid\s+", "", s).strip()
    if re.match(r"(?i)^diesel\s+", s):
        diesel_qual = True
        s = re.sub(r"(?i)^diesel\s+", "", s).strip()
    if re.match(r"(?i)^ffv\s+", s):
        ffv_qual = True
        s = re.sub(r"(?i)^ffv\s+", "", s).strip()

    # Extract "Mild Hybrid" or plain "Hybrid" qualifier from parentheses before stripping them.
    if re.search(r"(?i)\bMild\s+Hybrid\b", s):
        mild_hybrid_qual = True
    elif hybrid_prefix or re.search(r"(?i)\bHybrid\b", s):
        hybrid_prefix = True

    # Strip parenthetical tech groups (SIDI, PFI, Stop-Start, ZL1, FFV, Mild Hybrid, etc.).
    s = _ENGINE_JUNK_PARENS_RE.sub("", s)
    s = _ENGINE_MILD_HYBRID_PARENS_RE.sub("", s)
    # Strip any remaining empty parens.
    s = re.sub(r"\(\s*\)", "", s)

    # Strip inline valve/injection tokens.
    s = _ENGINE_INLINE_TECH_RE.sub("", s)

    # Strip Hyundai/Kia internal engine-family codes.
    s = _ENGINE_FAMILY_CODE_RE.sub("", s)

    # Strip trailing redundant fuel words.
    s = _ENGINE_FUEL_SUFFIX_RE.sub("", s)
    # Deduplicate trailing "Diesel" if already present in core.
    s = re.sub(r"(?i)\s+Diesel$", lambda m: "" if re.search(r"(?i)\bDiesel\b", s[: s.rfind(m.group())]) else m.group(), s)

    # Normalize integer-only displacement tokens: "2L" → "2.0L", "3L" → "3.0L".
    # Do NOT touch already-decimal values like "2.0L", "3.5L".
    s = re.sub(r"(?i)(?<!\d\.)(?<!\d)\b(\d+)L\b", lambda m: f"{int(m.group(1))}.0L", s)

    # Collapse multiple spaces.
    s = re.sub(r"\s{2,}", " ", s).strip()

    # Re-append qualifiers in a consistent order.
    qualifiers: list[str] = []
    if diesel_qual and not re.search(r"(?i)\bDiesel\b", s):
        qualifiers.append("Diesel")
    if mild_hybrid_qual and not re.search(r"(?i)\bMild\s+Hybrid\b", s):
        qualifiers.append("Mild Hybrid")
    elif hybrid_prefix and not mild_hybrid_qual and not re.search(r"(?i)\bHybrid\b", s):
        qualifiers.append("Hybrid")
    if ffv_qual and not re.search(r"(?i)\b(?:FFV|Flex\s+Fuel)\b", s):
        qualifiers.append("Flex Fuel")

    if qualifiers:
        s = f"{s} {' '.join(qualifiers)}".strip()

    return s


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
    if di is not None and di >= 0:
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


def _infer_layout_from_vehicle(
    make: str | None,
    model: str | None,
    trim: str | None,
    title: str | None,
    cylinders: int | None,
) -> str:
    """
    Known flat/boxer engine families when cylinder count matches.

    Overrides generic ``V6`` defaults for Porsche sports cars, Subaru, etc.
    """
    if cylinders is None or cylinders <= 0:
        return ""
    mk = (make or "").strip().lower()
    mo = (model or "").strip().lower()
    tr = (trim or "").strip().lower()
    ti = (title or "").strip().lower()
    blob = " ".join(x for x in (mk, mo, tr, ti) if x)

    if mk == "subaru":
        if cylinders == 4:
            return "Flat-4"
        if cylinders == 6:
            return "Flat-6"

    if mk == "porsche":
        if "taycan" in blob:
            return ""
        # Cayenne / Macan / Panamera use V6/V8/Turbo layouts — not flat-6 sports engines.
        if any(x in blob for x in ("cayenne", "macan", "panamera")):
            return ""
        if cylinders == 6:
            return "Flat-6"
        if cylinders == 4 and re.search(r"\b718\b", blob):
            return "Flat-4"

    if mk in ("ram", "dodge") and cylinders == 6:
        if re.search(r"\b2500\b|\b3500\b|\b4500\b|\b5500\b", blob):
            return "I6"

    if mk == "ford" and cylinders == 6:
        if re.search(r"\bf[\s-]?250\b|\bf[\s-]?350\b|\bf[\s-]?450\b|\bf[\s-]?550\b|super\s+duty", blob):
            return "I6"

    if mk in ("chevrolet", "chevy", "gmc") and cylinders == 6:
        if re.search(r"\b2500\b|\b3500\b|\b4500\b|\b5500\b|silverado|sierra", blob):
            return "I6"

    if cylinders == 4:
        if mo in ("86", "gr86") or re.search(r"\bgr\s*86\b", blob):
            return "Flat-4"
        if mo == "brz" or re.search(r"\bbrz\b", blob):
            return "Flat-4"

    return ""


def _layout_token_from_engine_text(blob: str) -> str:
    """Parse explicit layout tokens from engine description / EPA / VPIC text."""
    if not blob:
        return ""
    up = blob.upper()
    if re.search(
        r"\b(?:CUMMINS|DURAMAX|POWER\s+STROKE|POWERSTROKE)\b",
        up,
    ):
        if re.search(r"\bI\s*-?\s*6\b|\bI6\b|\bINLINE\s*-?\s*6\b", up):
            return "I6"
    if re.search(r"\bINLINE\s*-?\s*6\b", up) or re.search(r"\bIN[-\s]?LINE\s*-?\s*6\b", up):
        return "I6"
    m = re.search(r"\bI\s*-?\s*(\d)\b", up)
    if m:
        return f"I{int(m.group(1))}"
    m = re.search(r"\bV\s*-?\s*(\d{1,2})\b", up)
    if m:
        return f"V{int(m.group(1))}"
    if re.search(r"\b(FLAT|BOXER|H)\s*-?\s*4\b", up) or re.search(r"\bFLAT\s*FOUR\b", up):
        return "Flat-4"
    if re.search(r"\b(FLAT|BOXER|H)\s*-?\s*6\b", up) or re.search(r"\bFLAT\s*SIX\b", up):
        return "Flat-6"
    if re.search(r"\bINLINE\s*-?\s*6\b", up) or re.search(r"\bIN[-\s]?LINE\s*-?\s*6\b", up):
        return "I6"
    if re.search(r"\bHORIZONTALLY\s+OPPOSED\b", up) or re.search(r"\bBOXER\b", up):
        m = re.search(r"\b(\d)\s*-?\s*CYL", up)
        if m:
            n = int(m.group(1))
            if n == 4:
                return "Flat-4"
            if n == 6:
                return "Flat-6"
    return ""


def _cylinder_layout_token(
    cyl_i: int | None,
    *text_hints: str | None,
    car: dict[str, Any] | None = None,
) -> str:
    """
    Short layout label (e.g. ``V8``, ``I4``, ``Flat-6``). Prefer tokens found in listing/EPA text;
    then known flat/boxer families by make/model; otherwise use common heuristics by cylinder count.
    """
    car = car or {}
    identity = _infer_layout_from_vehicle(
        car.get("make"),
        car.get("model"),
        car.get("trim"),
        car.get("title"),
        cyl_i,
    )

    blob = " ".join(
        p.strip() for p in text_hints if isinstance(p, str) and p.strip()
    )
    if car:
        blob = " ".join(
            x
            for x in (
                blob,
                str(car.get("make") or ""),
                str(car.get("model") or ""),
                str(car.get("trim") or ""),
                str(car.get("title") or ""),
            )
            if x.strip()
        )

    from_text = _layout_token_from_engine_text(blob)
    if identity:
        # Dealer listings often mislabel Porsche flat-6 as V6 — trust vehicle identity.
        if not from_text or (
            from_text == "V6"
            and identity == "Flat-6"
            and str(car.get("make") or "").strip().lower() == "porsche"
        ):
            return identity
        if from_text.startswith("V") and identity.startswith("Flat"):
            return identity
        if from_text == "V6" and identity == "I6":
            return identity
    if from_text:
        return from_text

    if identity:
        return identity

    try:
        count = int(cyl_i) if cyl_i is not None else 0
    except (TypeError, ValueError):
        count = 0
    if count <= 0:
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
    return by_count.get(count, f"{count}-cyl")


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


def _effective_fuel_type_for_display(car: dict[str, Any], engine_disp: str) -> str | None:
    """Sticker or eTorque override for buyer-facing fuel type."""
    eng_text = " ".join(
        str(x or "")
        for x in (
            car.get("engine_description"),
            engine_disp,
            car.get("engine_display"),
        )
    ).lower()
    if "diesel" in eng_text:
        ft = str(car.get("fuel_type") or "").strip().lower()
        if ft in ("", "gasoline", "gas", "regular", "unleaded"):
            return "Diesel"
    raw = car.get("packages")
    if raw and not is_effectively_empty(raw):
        try:
            import json

            parsed = json.loads(raw) if isinstance(raw, str) else raw
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = None
        if isinstance(parsed, dict):
            sft = parsed.get("sticker_fuel_type")
            if isinstance(sft, str) and sft.strip():
                return sft.strip()
    if engine_disp and re.search(r"mild\s+hybrid", engine_disp, re.I):
        return "Hybrid"
    try:
        from backend.scanner.window_sticker import car_has_etorque_signal

        if car_has_etorque_signal(car):
            ft = str(car.get("fuel_type") or "").strip().lower()
            if ft in ("", "gasoline", "gas", "regular", "unleaded"):
                return "Hybrid"
    except Exception:
        pass
    return None


def build_engine_display(car: dict[str, Any], verified_specs: dict[str, Any] | None = None) -> str:
    """
    Buyer-facing engine line: displacement + layout when known (e.g. ``3.6L V6``, ``6.4L V8``),
    or ``Electric`` when cylinder count is zero.
    """
    c = clean_car_row_dict(car)
    dash = DISPLAY_DASH
    vs = verified_specs or {}

    def _finish(display: str) -> str:
        if not display or display == dash:
            return dash
        try:
            from backend.scanner.window_sticker import (
                upgrade_engine_display_for_etorque,
                upgrade_engine_display_for_turbo,
            )
            from backend.utils.forced_induction import apply_forced_induction_to_engine_display

            display = upgrade_engine_display_for_turbo(display, c)
            display = upgrade_engine_display_for_etorque(display, c)
            return apply_forced_induction_to_engine_display(display, c)
        except Exception:
            return display

    sticker_disp = _sticker_engine_display_from_packages(c)
    if sticker_disp:
        return _finish(sticker_disp)

    trim_disp = _known_trim_engine_display(c)
    if trim_disp:
        return _finish(trim_disp)

    try:
        from backend.dictionary.epa_engine import resolve_engine_display_from_epa

        epa_disp = resolve_engine_display_from_epa(c, vs)
        if epa_disp:
            return _finish(epa_disp)
    except Exception:
        pass

    # Pure EV / fuel cell: merged specs force cylinders_display to 0, but dealer
    # rows sometimes carry a bogus count (e.g. 4 on a BEV), which
    # _effective_cylinder_count would prefer — so check the merged value first.
    if str(c.get("fuel_type") or "").strip().lower() == "hydrogen":
        return "Hydrogen Fuel Cell"
    try:
        if int(vs.get("cylinders_display")) == 0:
            return "Electric"
    except (TypeError, ValueError):
        pass

    cyl_i = _effective_cylinder_count(c, vs)
    if cyl_i == 0:
        return "Electric"

    lit_f = parse_engine_displacement_liters(c)
    if lit_f is None:
        mes = vs.get("master_engine_string")
        if isinstance(mes, str) and mes.strip():
            lit_f = _extract_liters_from_engine_text(mes)
    if lit_f is None:
        ed = c.get("engine_description")
        if isinstance(ed, str) and ed.strip() and not is_effectively_empty(ed):
            lit_f = _extract_liters_from_engine_text(ed)

    layout = _cylinder_layout_token(
        cyl_i,
        c.get("engine_description"),
        vs.get("master_engine_string"),
        vs.get("epa_engine_description"),
        car=c,
    )

    if lit_f is not None and lit_f > 0:
        if layout:
            return _finish(f"{lit_f:.1f}L {layout}")
        return _finish(f"{lit_f:.1f}L")

    return dash


def _known_trim_engine_display(car: dict[str, Any]) -> str:
    try:
        from backend.scanner.window_sticker import known_oem_engine_from_car

        hit = known_oem_engine_from_car(car)
        disp = hit.get("engine_display")
        return str(disp).strip() if isinstance(disp, str) else ""
    except Exception:
        return ""


def _sticker_engine_display_from_packages(car: dict[str, Any]) -> str:
    raw = car.get("packages")
    if not raw or is_effectively_empty(raw):
        return ""
    try:
        import json

        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError, json.JSONDecodeError):
        return ""
    if not isinstance(parsed, dict):
        return ""
    disp = parsed.get("sticker_engine_display")
    if isinstance(disp, str) and disp.strip():
        from backend.scanner.window_sticker import (
            sticker_engine_display_is_valid,
            upgrade_engine_display_for_etorque,
            upgrade_engine_display_for_turbo,
        )

        if sticker_engine_display_is_valid(disp):
            disp = upgrade_engine_display_for_turbo(disp.strip()[:80], car)
            return upgrade_engine_display_for_etorque(disp, car)
    specs = parsed.get("sticker_specs")
    if isinstance(specs, dict):
        eng = specs.get("Engine")
        if isinstance(eng, str) and eng.strip():
            from backend.scanner.window_sticker import (
                upgrade_engine_display_for_etorque,
                upgrade_engine_display_for_turbo,
            )

            eng = upgrade_engine_display_for_turbo(eng.strip()[:80], car)
            return upgrade_engine_display_for_etorque(eng, car)
    return ""


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


_TCO_DEFAULT_STATE = "NC"
_STATE_ZIP_IN_TEXT_RE = re.compile(r"\b([A-Z]{2})\s+(\d{5})(?:-\d{4})?\b")
_COMMA_STATE_ZIP_RE = re.compile(r",\s*([A-Za-z]{2})\s+(\d{5})(?:-\d{4})?\b")
_TRAILING_COMMA_STATE_RE = re.compile(r",\s*([A-Za-z]{2})\s*$")

_TCO_PREMIUM_ENGINE_KEYWORDS = (
    "turbo",
    "twin turbo",
    "twin-turbo",
    "supercharged",
    "supercharger",
    "v8",
    "v12",
    "v-8",
    "v-12",
)

_TCO_LUXURY_PREMIUM_MAKES = frozenset(
    {
        "bmw",
        "mercedes-benz",
        "mercedes",
        "porsche",
        "audi",
    }
)


def _valid_us_state_code(raw: Any) -> str:
    from backend.discovery.normalize import normalize_us_state_to_code

    code = normalize_us_state_to_code(str(raw).strip() if raw is not None else "")
    return code if len(code) == 2 else ""


def _extract_state_from_location_text(text: str | None) -> str:
    if not text or not str(text).strip():
        return ""
    raw = str(text).strip()
    m = _STATE_ZIP_IN_TEXT_RE.search(raw.upper())
    if m:
        return m.group(1)
    m = _COMMA_STATE_ZIP_RE.search(raw)
    if m:
        code = _valid_us_state_code(m.group(1))
        if code:
            return code
    m = _TRAILING_COMMA_STATE_RE.search(raw)
    if m:
        code = _valid_us_state_code(m.group(1))
        if code:
            return code
    from backend.discovery.normalize import normalize_us_state_to_code

    for segment in reversed([p.strip() for p in raw.split(",") if p.strip()]):
        code = normalize_us_state_to_code(segment)
        if code:
            return code
    return ""


def _state_from_dealership_registry(registry_id: Any) -> str:
    try:
        rid = int(registry_id)
    except (TypeError, ValueError):
        return ""
    if rid <= 0:
        return ""
    try:
        from backend.db.dealerships_db import get_dealership_by_id

        row = get_dealership_by_id(rid)
        if not row:
            return ""
        for key in ("state", "state_code"):
            code = _valid_us_state_code(row.get(key))
            if code:
                return code
        parts = [
            row.get("street_address"),
            row.get("city"),
            row.get("state"),
            row.get("zip_code"),
        ]
        location = ", ".join(str(p).strip() for p in parts if p and str(p).strip())
        return _extract_state_from_location_text(location)
    except Exception:
        return ""


def resolve_car_state_code(car: dict[str, Any]) -> str:
    """
    Two-letter US state for TCO fuel lookup.

    Uses row state, dealership registry/location text, listing ZIP, then ``NC``.
    """
    if not car:
        return _TCO_DEFAULT_STATE
    c = car
    for key in ("state", "state_code", "dealer_state"):
        code = _valid_us_state_code(c.get(key))
        if code:
            return code
    for key in (
        "dealer_location",
        "dealer_address",
        "location",
        "address",
        "dealer_city_state",
    ):
        code = _extract_state_from_location_text(c.get(key))
        if code:
            return code
    code = _state_from_dealership_registry(c.get("dealership_registry_id"))
    if code:
        return code
    zip_raw = c.get("zip_code")
    if zip_raw and str(zip_raw).strip():
        try:
            from backend.db.geo import us_postal_meta_for_zip

            meta = us_postal_meta_for_zip(str(zip_raw).strip())
            if meta and meta.get("state_code"):
                code = _valid_us_state_code(meta["state_code"])
                if code:
                    return code
        except Exception:
            pass
    return _TCO_DEFAULT_STATE


def _normalize_tco_make_key(raw: Any) -> str:
    return re.sub(r"\s+", " ", str(raw or "").strip().lower())


def _engine_spec_text_for_fuel_requirement(
    car: dict[str, Any],
    *,
    engine_display: str | None = None,
    verified_specs: dict[str, Any] | None = None,
) -> str:
    vs = verified_specs or {}
    chunks = [
        car.get("engine_description"),
        car.get("engine"),
        car.get("title"),
        car.get("trim"),
        engine_display,
        vs.get("master_engine_string"),
        vs.get("epa_engine_description"),
        car.get("fuel_type"),
        vs.get("epa_fuel_type"),
    ]
    return " ".join(str(x).strip() for x in chunks if x and str(x).strip()).lower()


def resolve_car_fuel_requirement(
    car: dict[str, Any],
    *,
    engine_display: str | None = None,
    verified_specs: dict[str, Any] | None = None,
) -> str:
    """
    TCO fuel tier: ``premium`` when engine or make signals premium gasoline; else ``regular``.
    """
    if not car:
        return "regular"
    blob = _engine_spec_text_for_fuel_requirement(
        car, engine_display=engine_display, verified_specs=verified_specs
    )
    if "premium" in blob or "premium gasoline" in blob:
        return "premium"
    for kw in _TCO_PREMIUM_ENGINE_KEYWORDS:
        if kw in blob:
            return "premium"
    if re.search(r"\bv\s*[-]?\s*8\b", blob):
        return "premium"
    if re.search(r"\bv\s*[-]?\s*12\b", blob):
        return "premium"
    make_key = _normalize_tco_make_key(car.get("make"))
    if make_key in _TCO_LUXURY_PREMIUM_MAKES:
        return "premium"
    compact = make_key.replace(" ", "").replace("-", "")
    for luxury in _TCO_LUXURY_PREMIUM_MAKES:
        if compact == luxury.replace(" ", "").replace("-", ""):
            return "premium"
    return "regular"


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
            from backend.enrichment.knowledge_engine import merge_verified_specs

            vs = merge_verified_specs(c)
        except Exception:
            vs = {}

    model_d, trim_d = apply_bmw_model_trim_display(c)
    engine_disp = build_engine_display(c, vs if vs else None)

    out: dict[str, Any] = {}
    for k, v in c.items():
        if k in SENSITIVE_CAR_ROW_KEYS:
            continue
        if k == "gallery":
            if isinstance(v, list):
                out[k] = filter_spyne_gallery_variants(v)
            elif isinstance(v, str):
                try:
                    import json as _json
                    parsed = _json.loads(v)
                    out[k] = filter_spyne_gallery_variants(parsed) if isinstance(parsed, list) else v
                except Exception:
                    out[k] = v
            else:
                out[k] = v
            continue
        if k in ("spin_frames", "interior_pano"):
            # Handled explicitly after the loop (contract defaults: [] / null);
            # must not fall through to format_display_value (None -> em-dash).
            continue
        if k == "history_highlights":
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

    # 360 spin assets: always emitted with contract defaults ([] / null).
    _spin = c.get("spin_frames")
    if isinstance(_spin, str):
        try:
            _spin = json.loads(_spin)
        except (TypeError, ValueError):
            _spin = None
    out["spin_frames"] = (
        [u for u in _spin if isinstance(u, str) and u.strip()] if isinstance(_spin, list) else []
    )
    out["interior_pano"] = normalize_optional_url(c.get("interior_pano"))

    from backend.parsers.vdp_urls import resolve_vehicle_source_url

    src_was_placeholder = is_effectively_empty(c.get("source_url"))
    detail_was_placeholder = is_effectively_empty(c.get("_detail_url")) and is_effectively_empty(
        c.get("detail_url")
    )
    listing_vdp = resolve_vehicle_source_url(c)
    if listing_vdp:
        out["listing_vdp_url"] = listing_vdp
        if not src_was_placeholder:
            out["source_url"] = listing_vdp
    else:
        out["listing_vdp_url"] = out.get("source_url") or out.get("dealer_url")

    out["model"] = model_d
    out["trim"] = trim_d
    out["engine_display"] = engine_disp

    # Forced induction: use stored value or compute on the fly.
    _fi = c.get("forced_induction") or ""
    if not _fi.strip():
        try:
            from backend.utils.forced_induction import classify_forced_induction_from_car_row
            _fi = classify_forced_induction_from_car_row(c) or ""
        except Exception:
            _fi = ""
    out["forced_induction"] = _fi or None

    ft_override = _effective_fuel_type_for_display(c, engine_disp)
    if ft_override:
        out["fuel_type"] = format_display_value(ft_override)

    vcyl = vs.get("cylinders")
    if vcyl is not None and (c.get("cylinders") is None or str(c.get("cylinders")).strip() == ""):
        try:
            out["cylinders"] = int(vcyl)
        except (TypeError, ValueError):
            out["cylinders"] = vcyl

    # Prefer persisted transmission_type bucket when present; else dealer text / EPA / normalize.
    # Feeds sometimes label geared automatics (incl. many PHEVs) as "CVT"; trust detailed transmission
    # when it clearly normalizes to a non-CVT bucket.
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

    dealer_d = coerce_drivetrain_stored(c.get("drivetrain"))
    bs_raw = str(c.get("body_style") or "").lower()
    if dealer_d == "FWD" and "pickup" in bs_raw:
        dealer_d = None
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

    # Extended specs (epa_extended_specs, model-level match — not necessarily this exact
    # trim; pass through as-is, no dealer field to reconcile against).
    out["horsepower"] = vs.get("horsepower")
    out["torque_lb_ft"] = vs.get("torque_lb_ft")
    out["curb_weight_lb"] = vs.get("curb_weight_lb")
    out["zero_to_60_sec"] = vs.get("zero_to_60_sec")
    out["fuel_tank_gal"] = vs.get("fuel_tank_gal")
    out["ev_range_miles"] = vs.get("ev_range_miles")
    out["battery_kwh"] = vs.get("battery_kwh")
    out["tow_capacity_lb"] = vs.get("tow_capacity_lb")

    # EV range / battery are ONLY real for battery-electric and plug-in hybrids. The
    # model-level spec match can pull an EV trim's row onto a gas car of the same
    # nameplate (e.g. gas Kona matching Kona Electric), so gate on the car's own fuel
    # type and drop these fields for anything that can't be plugged in.
    _ft = str(out.get("fuel_type") or c.get("fuel_type") or "").strip().lower()
    _ev_capable = ("plug-in" in _ft) or ("plug in" in _ft) or _ft in ("electric", "ev") or (
        "electric" in _ft and "gas" not in _ft and "hybrid" not in _ft
    )
    if not _ev_capable:
        out["ev_range_miles"] = None
        out["battery_kwh"] = None

    # Engine-specific override (ai_engine_specs, currently trucks/HD): the model-level
    # match above can hand a diesel a gas engine's numbers. When we have specs for this
    # car's EXACT engine, the engine-critical fields (hp/torque/tow/0-60) win outright;
    # fuel_tank/curb only fill when the model-level value is missing.
    try:
        from backend.enrichment.knowledge_engine import lookup_engine_specs

        _es = lookup_engine_specs(
            c.get("year"), c.get("make"), c.get("model"),
            c.get("engine_description"), c.get("cylinders"), c.get("fuel_type"),
        )
    except Exception:
        _es = {}
    if _es:
        for _f in ("horsepower", "torque_lb_ft", "tow_capacity_lb", "zero_to_60_sec"):
            if _es.get(_f) is not None:
                out[_f] = _es[_f]
        for _f in ("fuel_tank_gal", "curb_weight_lb"):
            if out.get(_f) is None and _es.get(_f) is not None:
                out[_f] = _es[_f]

    # Factory catalog packages / standalone options (catalog_trims/_options/_packages),
    # matched on this row's own year/make/model/trim — independent of dealer window
    # sticker data above. Most trims have no catalog rows; None when no match.
    try:
        from backend.enrichment.catalog_lookup import lookup_catalog_options_and_packages

        _catalog = lookup_catalog_options_and_packages(
            c.get("year"), c.get("make"), c.get("model"), c.get("trim")
        )
    except Exception:
        _catalog = {}
    out["catalog_packages"] = _catalog.get("packages") or None
    out["catalog_options"] = _catalog.get("options") or None

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

    fill_derived_condition_for_display(c, out)

    from backend.utils.interior_color_buckets import infer_paint_color_buckets, parse_stored_buckets

    out["exterior_color_families"] = infer_paint_color_buckets(c.get("exterior_color"), c.get("make"))
    _ib = parse_stored_buckets(car.get("interior_color_buckets"))
    out["interior_color_families"] = (
        _ib if _ib else infer_paint_color_buckets(c.get("interior_color"), c.get("make"))
    )

    _pkg_names: list[str] = []
    _pkg_raw = out.get("packages")
    if _pkg_raw:
        try:
            _p = json.loads(_pkg_raw) if isinstance(_pkg_raw, str) else _pkg_raw
            for _entry in (_p.get("packages_normalized") or []):
                if isinstance(_entry, dict):
                    _n = (_entry.get("canonical_name") or _entry.get("name") or "").strip()
                    if _n:
                        _pkg_names.append(_n)
            for _n in (_p.get("possible_packages") or []):
                if isinstance(_n, str) and _n.strip():
                    _pkg_names.append(_n.strip())
        except Exception:
            pass
    out["package_names"] = _pkg_names

    out["created_at"] = c.get("first_seen_at") or c.get("scraped_at")
    out["price_history_json"] = _price_history_json_for_vdp(c)
    out["state"] = resolve_car_state_code(c)
    out["fuel_requirement"] = resolve_car_fuel_requirement(
        c, engine_display=engine_disp, verified_specs=vs if vs else None
    )
    from backend.intelligence.tco_fuel_estimates import (
        resolve_fuel_tank_gallons,
        resolve_tco_avg_mpg,
        resolve_tco_ev_efficiency,
    )
    from backend.intelligence.ev_range_estimates import resolve_factory_epa_range

    out["tco_avg_mpg"] = resolve_tco_avg_mpg(c)
    out["tco_ev_efficiency"] = resolve_tco_ev_efficiency(c)
    out["factory_range"] = resolve_factory_epa_range(c)
    out["fuel_tank_gallons"] = round(resolve_fuel_tank_gallons(c), 1)

    # Coarse market deal score (free consumer hook). Scored offline against the
    # in-process market_price_stats cache — no per-car DB round-trip. The detailed
    # band breakdown is gated behind FEATURE_MARKET_INTEL in the route/template.
    try:
        from backend.intelligence.deal_score_cache import public_deal_score

        out["deal_score"] = public_deal_score(c)
    except Exception:
        out["deal_score"] = None

    return out


def _price_history_json_for_vdp(car: dict[str, Any]) -> str:
    """
    JSON array string for VDP negotiation radar: [{date, price}, ...].

    Reads operator ``price_provenance_json`` when it stores sweep history; never
    exposes the raw provenance blob on the public car payload.
    """
    events: list[dict[str, Any]] = []
    raw = car.get("price_provenance_json")
    if raw and str(raw).strip():
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError, ValueError):
            parsed = None
        if isinstance(parsed, list):
            source = parsed
        elif isinstance(parsed, dict):
            source = parsed.get("history") or parsed.get("sweeps") or parsed.get("price_history") or []
        else:
            source = []
        if isinstance(source, list):
            for item in source:
                if not isinstance(item, dict):
                    continue
                when = item.get("date") or item.get("recorded_at") or item.get("scraped_at")
                amt = item.get("price")
                if when is None or amt is None:
                    continue
                try:
                    events.append({"date": str(when), "price": float(amt)})
                except (TypeError, ValueError):
                    continue
    return json.dumps(events)


_PRICE_DROP_RECENT_DAYS = max(1, int(os.environ.get("PRICE_DROP_RECENT_DAYS", "14")))


def _latest_price_drop_for_grid(car: dict[str, Any]) -> tuple[float | None, int | None]:
    """
    ``(amount, days_ago)`` for the most recent price drop within
    :data:`_PRICE_DROP_RECENT_DAYS`, else ``(None, None)``.

    Cheap grid-card signal — compares only the last two ``price_provenance_json``
    snapshots (scanner appends chronologically, so ``[-1]`` is newest). Does not
    parse/expose the full history array; see ``_price_history_json_for_vdp`` for that.
    """
    raw = car.get("price_provenance_json")
    if not raw or not str(raw).strip():
        return None, None
    try:
        history = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError, ValueError):
        return None, None
    if not isinstance(history, list) or len(history) < 2:
        return None, None
    prev, latest = history[-2], history[-1]
    if not isinstance(prev, dict) or not isinstance(latest, dict):
        return None, None
    try:
        prev_price = float(prev.get("price"))
        latest_price = float(latest.get("price"))
    except (TypeError, ValueError):
        return None, None
    if latest_price >= prev_price:
        return None, None
    when = latest.get("date")
    if not when:
        return None, None
    import re as _re
    from datetime import datetime, timezone

    try:
        when_dt = datetime.fromisoformat(_re.sub(r"Z$", "+00:00", str(when)))
        if when_dt.tzinfo is None:
            when_dt = when_dt.replace(tzinfo=timezone.utc)
        days_ago = (datetime.now(timezone.utc) - when_dt).days
    except (TypeError, ValueError):
        return None, None
    if days_ago < 0 or days_ago > _PRICE_DROP_RECENT_DAYS:
        return None, None
    return round(prev_price - latest_price, 2), days_ago


_LISTINGS_GRID_GALLERY_MAX = max(1, int(os.environ.get("LISTINGS_GRID_GALLERY_MAX", "4")))


def _package_names_from_raw(packages_raw: Any) -> list[str]:
    names: list[str] = []
    if is_effectively_empty(packages_raw) or str(packages_raw).strip() in ("{}", "[]"):
        return names
    try:
        pkg = json.loads(packages_raw) if isinstance(packages_raw, str) else packages_raw
    except Exception:
        return names
    if not isinstance(pkg, dict):
        return names
    for entry in pkg.get("packages_normalized") or []:
        if isinstance(entry, dict):
            n = (entry.get("canonical_name") or entry.get("name") or "").strip()
            if n:
                names.append(n)
    for n in pkg.get("possible_packages") or []:
        if isinstance(n, str) and n.strip():
            names.append(n.strip())
    return names


def _listings_grid_gallery(raw: Any) -> list[str]:
    if isinstance(raw, list):
        parsed = raw
    elif isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        if not isinstance(parsed, list):
            return []
    else:
        return []
    out = filter_spyne_gallery_variants(parsed)
    safe = [u for u in out if normalize_listing_image_url(u)]
    return safe[:_LISTINGS_GRID_GALLERY_MAX] if safe else []


def _parse_gallery_url_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        parsed = raw
    elif isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        if not isinstance(parsed, list):
            return []
    else:
        return []
    return [u.strip() for u in parsed if isinstance(u, str) and u.strip()]


def _public_gallery_photo_count(raw: Any, *, image_url: Any = None) -> int:
    """Count public listing photos (same rules as car detail gallery)."""
    from backend.vision.url_heuristics import filter_public_gallery_urls, heuristic_listing_gallery_fluff_url

    urls = filter_public_gallery_urls(_parse_gallery_url_list(raw))
    if not urls and image_url:
        iu = str(image_url).strip()
        if iu and not heuristic_listing_gallery_fluff_url(iu):
            urls = [iu]
    return len(urls)


def serialize_car_for_listings_grid(car: dict[str, Any]) -> dict[str, Any]:
    """
    Compact listings grid JSON: filter/cascade fields + card display only.

    Skips full ``serialize_car_for_api`` (EPA merge, transmission heuristics, etc.)
    to keep ``GET /api/listings/cars`` fast on SQLite (~2.5k rows).
    """
    if not car:
        return {}
    c = clean_car_row_dict(dict(car))
    from backend.utils.field_clean import normalize_body_style_for_car
    from backend.utils.interior_color_buckets import infer_paint_color_buckets, parse_stored_buckets

    model_d, trim_d = apply_bmw_model_trim_display(c)
    ext_fam = infer_paint_color_buckets(c.get("exterior_color"), c.get("make"))
    _ib = parse_stored_buckets(car.get("interior_color_buckets"))
    int_fam = _ib if _ib else infer_paint_color_buckets(c.get("interior_color"), c.get("make"))

    def num(key: str) -> Any:
        return c.get(key)

    dt_stored = coerce_drivetrain_stored(c.get("drivetrain"))
    if dt_stored == "FWD" and "pickup" in str(c.get("body_style") or "").lower():
        dt_stored = None
    fuel_display = _effective_fuel_type_for_display(c, c.get("engine_description") or "") or c.get("fuel_type")

    out: dict[str, Any] = {
        "id": num("id"),
        "title": format_display_value(c.get("title")),
        "year": num("year"),
        "make": format_display_value(c.get("make")),
        "model": model_d,
        "trim": trim_d,
        "price": num("price"),
        "mileage": num("mileage"),
        "fuel_type": format_display_value(fuel_display),
        "cylinders": num("cylinders"),
        "transmission": format_display_value(c.get("transmission")),
        "drivetrain": format_display_value(dt_stored),
        "body_style": format_display_value(
            normalize_body_style_for_car(
                c.get("body_style"),
                make=c.get("make"),
                model=c.get("model"),
                trim=c.get("trim"),
                title=c.get("title"),
            )
            or c.get("body_style")
        ),
        "exterior_color": format_display_value(c.get("exterior_color")),
        "interior_color": format_display_value(c.get("interior_color")),
        "exterior_color_families": ext_fam,
        "interior_color_families": int_fam,
        "engine_l": c.get("engine_l"),
        "engine_description": c.get("engine_description"),
        "image_url": (img_url := normalize_listing_image_url(c.get("image_url"))),
        # Grid cards only need a primary image; omit duplicate gallery URLs from JSON.
        "gallery": [] if img_url else _listings_grid_gallery(c.get("gallery")),
        "photo_count": _public_gallery_photo_count(c.get("gallery"), image_url=c.get("image_url")),
        "dealer_name": format_display_value(c.get("dealer_name")),
        "dealer_url": normalize_optional_url(c.get("dealer_url")),
        "dealership_registry_id": num("dealership_registry_id"),
        "dealer_id": c.get("dealer_id"),
        "package_names": _package_names_from_raw(c.get("packages")),
        "data_quality_score": num("data_quality_score"),
    }
    out["condition"] = format_display_value(c.get("condition"))
    fill_derived_condition_for_display(c, out)
    # Raw column, matching exactly what search_cars(cpo_only=True) filters on server-side —
    # NOT derived from out["condition"], which can read "Certified" (no "Pre-Owned") for some
    # rows even when is_cpo=1, which would disagree with the SQL-side filter.
    out["is_cpo"] = c.get("is_cpo") in (1, True, "1")
    price_drop_amount, price_drop_days_ago = _latest_price_drop_for_grid(c)
    out["price_drop_amount"] = price_drop_amount
    out["price_drop_days_ago"] = price_drop_days_ago
    # Coarse deal score for grid cards (in-memory band cache; no per-card DB hit).
    try:
        from backend.intelligence.deal_score_cache import public_deal_score

        out["deal_score"] = public_deal_score(c)
    except Exception:
        out["deal_score"] = None
    return out


def listings_inventory_is_new(condition: Any) -> bool:
    """True when derived display condition is new retail inventory."""
    return str(condition or "").strip().lower() == "new"


def listings_inventory_is_pre_owned(condition: Any) -> bool:
    """True for used / CPO / pre-owned inventory (anything explicitly not new)."""
    c = str(condition or "").strip().lower()
    if not c or c in ("—", "-", "n/a"):
        return False
    return c != "new"


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

    # Final fallback: mileage=0 on a current/upcoming model year → New.
    # These are new inventory rows scraped without a URL or "New" title prefix.
    _oc5 = out.get("condition")
    if _oc5 is None or str(_oc5).strip() in ("", DISPLAY_DASH):
        try:
            yy5 = int(c.get("year")) if c.get("year") is not None else None
        except (TypeError, ValueError):
            yy5 = None
        if yy5 is not None and yy5 >= 2024:
            try:
                raw_m5 = c.get("mileage")
                mi5 = (
                    int(float(str(raw_m5).replace(",", "")))
                    if raw_m5 is not None and str(raw_m5).strip() != ""
                    else None
                )
            except (TypeError, ValueError):
                mi5 = None
            if mi5 == 0:
                out["condition"] = "New"


def normalize_condition_for_storage(car: dict[str, Any]) -> str | None:
    """
    Normalize non-blank but incorrect stored condition values.

    - "Certified" → "Certified Pre-Owned" when CPO signals present in title/URL
    - "Pre-Owned"  → "Used"

    Returns the corrected value, or None if no change needed.
    """
    cond = (car.get("condition") or "").strip()
    if not cond:
        return None

    low_t = (car.get("title") or "").lower()
    low_u = (car.get("source_url") or "").lower()
    is_cpo_flag = car.get("is_cpo") in (1, True, "1")

    if cond == "Certified":
        if (
            "certified pre-owned" in low_t
            or "certified preowned" in low_t
            or "/certified" in low_u
            or "-cpo-" in low_u
            or "cpo-inventory" in low_u
            or is_cpo_flag
        ):
            return "Certified Pre-Owned"

    if cond == "Pre-Owned":
        return "Used"

    return None


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
