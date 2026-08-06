"""
Engine display: displacement/layout parsing and the buyer-facing engine line.

Split out of the former monolith ``backend/utils/car_serialize.py`` and
re-exported from the package facade so the public import surface is unchanged.
"""
from __future__ import annotations

import re
from typing import Any

from backend.utils.field_clean import (
    clean_car_row_dict,
    is_effectively_empty,
    is_spec_overlay_junk,
)

from ._common import (
    DISPLAY_DASH,
    _MANUFACTURER_SPEC_RE,
    _strip_epa_aggregate_label,
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
    """
    Window-sticker / diesel override for buyer-facing fuel type.

    A 48V mild hybrid (eTorque, MHEV) is deliberately NOT promoted to "Hybrid"
    here. It has no plug and no EV-only mode and burns the same pump gasoline as
    the non-electrified engine, so "Hybrid" oversells it to a shopper. Worse, the
    promotion was display-only: every fuel FILTER (``search_cars``, the facet
    cascade) reads the raw ``cars.fuel_type`` column, so a gas-labelled eTorque
    row rendered as "Hybrid" while still being returned under "Gasoline" — the
    card and the filter disagreed. The 48V hardware is still shown, in the place
    that describes hardware: ``engine_display`` reads "5.7L V8 Mild Hybrid".
    """
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
            if isinstance(sft, str) and sft.strip() and _sticker_fuel_may_override(car, sft):
                return sft.strip()
    return None


def _sticker_fuel_may_override(car: dict[str, Any], sticker_fuel: str) -> bool:
    """
    False when a stored sticker fuel type would only re-add the mild-hybrid promotion.

    ``packages.sticker_fuel_type`` is written by the window-sticker parser, which
    labels every eTorque truck "Hybrid" — the same 48V promotion removed above,
    only persisted. Letting it through would flip a pump-fuel row to a hybrid
    label at DISPLAY time while the fuel filters keep reading the stored column,
    so the card would contradict the search that returned it. A sticker that says
    something the column is not already claiming (e.g. Diesel) still wins.
    """
    from backend.utils.fuel_type_normalize import is_correctable_hybrid_label

    stored = str(car.get("fuel_type") or "").strip()
    if not stored or not is_correctable_hybrid_label(sticker_fuel):
        return True
    # Stored label is itself hybrid-ish (Hybrid / MHEV / "Gasoline / Electric"):
    # the sticker is not changing the fuel class, so it is safe to show.
    return is_correctable_hybrid_label(stored)


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
