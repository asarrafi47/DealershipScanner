"""
Classify engine forced induction type from make/model/trim/engine data.

Returns "Turbocharged", "Twin Turbocharged", "Supercharged", or None (N/A or unknown).
"""
from __future__ import annotations

import re
from typing import Any

_TWIN_TURBO_RE = re.compile(
    r"twin[- ]?turbo|biturbo|bi[- ]turbo|twin[- ]charged|twin[- ]scroll.*turbo",
    re.I,
)
_TURBO_RE = re.compile(
    r"turbo(?:charged)?|\btdi\b|\btfsi\b|\btsi\b|ecoboost|powerboost|ecotec.*turbo"
    r"|\bdieseltec\b|\bcdti\b|\bcdi\b|\bdci\b|\bjtd\b|\bjtdm\b|\bcrdi\b|\bcvvt.*turbo",
    re.I,
)
_TWIN_SC_RE = re.compile(r"twin[- ]?supercharg", re.I)
_SC_RE = re.compile(r"supercharg|kompressor|\btvs\b", re.I)
_T_DISPLACEMENT_TRIM_RE = re.compile(r"\b\d(?:\.\d)?t\b", re.I)
_DISPLAY_FI_RE = re.compile(
    r"\b(?:twin\s+turbo|twin\s+supercharg|biturbo|turbo|supercharg|kompressor)\b",
    re.IGNORECASE,
)


def _classify_from_trim_t_displacement(blob: str) -> str | None:
    """``2.0T``, ``1.5T``, ``3.0t`` in title/trim/model → turbocharged."""
    if not _T_DISPLACEMENT_TRIM_RE.search(blob or ""):
        return None
    if re.search(r"\b3\.0t\b", blob, re.I):
        return "Twin Turbocharged"
    return "Turbocharged"


def display_has_forced_induction_marker(display: str | None) -> bool:
    return bool(_DISPLAY_FI_RE.search(display or ""))


def forced_induction_short_suffix(forced_induction: str | None) -> str:
    """Buyer-facing suffix for ``engine_display`` (e.g. ``Turbo``, ``Supercharged``)."""
    if not forced_induction:
        return ""
    low = forced_induction.strip().lower()
    if "twin turbo" in low or "biturbo" in low:
        return "Twin Turbo"
    if "twin super" in low:
        return "Twin Supercharged"
    if "supercharg" in low or "kompressor" in low:
        return "Supercharged"
    if "turbo" in low:
        return "Turbo"
    return ""


def apply_forced_induction_to_engine_display(
    display: str,
    car: dict[str, Any] | None = None,
) -> str:
    """Append turbo / supercharged hint when rules or dictionary classify the engine."""
    s = (display or "").strip()
    if not s or display_has_forced_induction_marker(s):
        return s
    if car is None:
        return s
    fi = car.get("forced_induction")
    if not isinstance(fi, str) or not fi.strip():
        fi = classify_forced_induction_from_car_row(car) or ""
    if not fi:
        blob = " ".join(
            str(car.get(k) or "") for k in ("title", "trim", "model", "engine_description")
        )
        fi = _classify_from_trim_t_displacement(blob) or ""
    suffix = forced_induction_short_suffix(fi)
    if not suffix:
        return s
    return f"{s} {suffix}"


def extract_forced_induction_from_text(raw: str | None) -> str | None:
    """Extract from raw engine description text (call BEFORE normalization strips keywords)."""
    if not raw:
        return None
    t = raw.strip()
    if _TWIN_TURBO_RE.search(t):
        return "Twin Turbocharged"
    if _TURBO_RE.search(t):
        return "Turbocharged"
    if _TWIN_SC_RE.search(t):
        return "Twin Supercharged"
    if _SC_RE.search(t):
        return "Supercharged"
    return None


def _norm(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip()).lower()


def _has(text: str, *keywords: str) -> bool:
    return any(k in text for k in keywords)


def _classify_by_make_model(
    make: str,
    model: str,
    trim: str,
    year: int | None,
    cylinders: int | None,
    engine_l: float | None,
    fuel_type: str = "",
) -> str | None:
    m = _norm(make)
    mo = _norm(model)
    tr = _norm(trim)
    ft = _norm(fuel_type)
    full = f"{mo} {tr}"
    yr = year or 0
    cyl = cylinders or 0
    el = engine_l or 0.0

    # ── BMW ──────────────────────────────────────────────────────────────────
    if m == "bmw":
        if re.search(r"\bi[3x]\b|\bi[4-9]\b", mo) or _has(tr, "electric"):
            return None
        # M high-perf with twin turbo S-series engines or V8
        if re.search(r"\bm[2-8]\b", full) or re.search(r"x[3-7]\s*m\b|x[3-7]m\b", full):
            return "Twin Turbocharged"
        if cyl == 8 or el >= 4.2:
            return "Twin Turbocharged"
        return "Turbocharged"

    # ── Mercedes-Benz ─────────────────────────────────────────────────────
    if _has(m, "mercedes"):
        if re.search(r"\beq[a-z]|\belectric\b", full):
            return None
        # AMG V8 twin turbo (C63, E63, S63, GLE63, GLS63, G63, SL63, GT63)
        if re.search(r"(?:c|e|s|cl|sl|gle|glc|gls|g|cla|amg\s*gt)\s*63|gt\s*[rsc]\b|gt\s*black", full):
            return "Twin Turbocharged"
        # AMG inline-4 (A35, CLA35, GLA35, GLB35) or inline-6 (C43, E53, GLE53)
        if re.search(r"\b(?:35|43|53)\s*amg|amg\s*(?:35|43|53)\b", full):
            return "Turbocharged"
        return "Turbocharged"

    # ── Audi ─────────────────────────────────────────────────────────────────
    if m == "audi":
        if re.search(r"e-tron|etron\b", mo):
            return None
        if re.search(r"\brs\s*[3-9]\b|\brs\s*q[0-9]\b|\br8\b", full):
            return "Twin Turbocharged"
        # S6/S7/S8 with V8
        if re.search(r"\bs[6-8]\b", mo) and (cyl == 8 or el >= 3.9):
            return "Twin Turbocharged"
        return "Turbocharged"

    # ── Volkswagen ───────────────────────────────────────────────────────────
    if _has(m, "volkswagen") or m == "vw":
        if re.search(r"\bid\.\d|\bid\s\d", mo):
            return None
        return "Turbocharged"

    # ── Porsche ───────────────────────────────────────────────────────────────
    if m == "porsche":
        if _has(mo, "taycan"):
            return None
        if _has(tr, "turbo"):
            return "Twin Turbocharged"
        # GT3, GT3 RS = naturally aspirated
        if re.search(r"\bgt3\b", tr) and "gt2" not in tr:
            return None
        if re.search(r"\bgt2\b", tr):
            return "Twin Turbocharged"
        return "Turbocharged"

    # ── Ford ─────────────────────────────────────────────────────────────────
    if m == "ford":
        if _has(mo, "mustang"):
            if _has(tr, "gt500"):
                return "Supercharged"
            # GT350/GT350R = naturally aspirated flat-plane V8 (Voodoo 5.2L)
            if _has(tr, "gt350"):
                return None
            if _has(tr, "gt350", "dark horse", "mach 1") and cyl == 8:
                return None
            if _has(tr, "ecoboost") or (el and el <= 2.4 and cyl == 4):
                return "Turbocharged"
            return None  # V8 GT = NA
        if _has(mo, "raptor"):
            if cyl == 8 or el >= 5.0:
                return "Supercharged"  # Raptor R
            return "Twin Turbocharged"  # 3.5L V6 EcoBoost
        # 2.7L or 3.5L EcoBoost V6 = twin turbo
        if cyl == 6 and el >= 2.5:
            return "Twin Turbocharged"
        # 4-cyl EcoBoost = single turbo
        if _has(tr, "ecoboost") or (cyl == 4 and el and el <= 2.4):
            return "Turbocharged"
        if _has(tr, " st", "st "):
            return "Turbocharged"
        return None

    # ── Lincoln ───────────────────────────────────────────────────────────────
    if m == "lincoln":
        if cyl == 6 and el >= 2.9:
            return "Twin Turbocharged"  # 3.0T V6
        return "Turbocharged"

    # ── Chevrolet / GMC ───────────────────────────────────────────────────────
    if _has(m, "chevrolet", "chevy", "gmc"):
        if _has(mo, "corvette"):
            if _has(tr, "zr1") and yr >= 2023:
                return "Twin Turbocharged"
            if _has(tr, "zr1"):
                return "Supercharged"
            if _has(tr, "z06") and yr >= 2022:
                return None  # flat-plane NA V8
            return None
        if _has(mo, "camaro") and _has(tr, "zl1"):
            return "Supercharged"
        if _has(mo, "trailblazer", "trax", "encore", "envoy"):
            return "Turbocharged"
        if cyl == 4 and el and 2.4 <= el <= 2.9:
            return "Turbocharged"  # 2.7T 4-cyl
        if _has(tr, "duramax", "diesel") or (cyl == 6 and el and 2.7 <= el <= 3.1 and _has(mo, "silverado", "sierra", "colorado", "canyon")):
            return "Turbocharged"
        if cyl >= 6:
            return None  # NA V6/V8
        return None

    # ── Buick ─────────────────────────────────────────────────────────────────
    if m == "buick":
        if _has(mo, "envision", "encore", "envista"):
            return "Turbocharged"
        return None

    # ── Cadillac ─────────────────────────────────────────────────────────────
    if m == "cadillac":
        if _has(mo, "lyriq", "celestiq"):
            return None
        if _has(tr, "blackwing"):
            return "Twin Turbocharged" if _has(mo, "ct4") else "Supercharged"
        if _has(mo, "escalade") and _has(tr, "-v", " v "):
            return "Supercharged"
        if re.search(r"\bct[45]\b", mo) or _has(mo, "xt4"):
            return "Turbocharged"
        if _has(mo, "xt5", "xt6") and el and el < 3.0:
            return "Turbocharged"
        return None

    # ── Dodge ─────────────────────────────────────────────────────────────────
    if m == "dodge":
        if _has(tr, "hellcat", "hell cat", "redeye", "demon", "jailbreak"):
            return "Supercharged"
        return None

    # ── RAM ───────────────────────────────────────────────────────────────────
    if m == "ram":
        if _has(tr, "trx"):
            return "Supercharged"
        if _has(tr, "ecodiesel", "diesel") or (cyl == 6 and el and 2.8 <= el <= 3.2):
            return "Turbocharged"
        if cyl == 4 and el and el <= 2.2:
            return "Turbocharged"
        return None

    # ── Jeep ──────────────────────────────────────────────────────────────────
    if m == "jeep":
        if _has(tr, "trackhawk"):
            return "Supercharged"
        if _has(tr, "4xe"):
            return "Turbocharged"
        if cyl == 4 and el and 1.8 <= el <= 2.2:
            return "Turbocharged"
        if _has(tr, "ecodiesel", "diesel"):
            return "Turbocharged"
        return None

    # ── Chrysler ─────────────────────────────────────────────────────────────
    if m == "chrysler":
        return None  # Pacifica V6 = NA

    # ── Honda / Acura ─────────────────────────────────────────────────────────
    if m in ("honda", "acura"):
        if _has(mo, "nsx"):
            return "Twin Turbocharged"
        if _has(tr, "type r", "type-r", "si") or _has(mo, "type r"):
            return "Turbocharged"
        if m == "acura" and _has(mo, "rdx", "tlx", "integra"):
            return "Turbocharged"
        if _has(tr, "1.5t", "2.0t", "turbo"):
            return "Turbocharged"
        # Explicit small-displacement turbo: CR-V 1.5T, Civic 1.5T (not hybrids)
        if cyl == 4 and el and el <= 1.6 and yr >= 2016 and not _has(ft, "hybrid"):
            return "Turbocharged"
        # 2.0L non-hybrid = could be turbo (Accord 2.0T Sport) or NA
        # Only flag if trim explicitly says turbo-related OR NOT a hybrid
        if cyl == 4 and el and 1.9 <= el <= 2.1:
            # Hybrid Accord/CR-V use 2.0L Atkinson NA — skip
            if _has(ft, "hybrid"):
                return None
            # Pilot 3.5L V6 = always NA; this branch won't hit (wrong el), but guard anyway
            if _has(tr, "2.0t") or _has(mo, "accord") and _has(tr, "sport 2.0t"):
                return "Turbocharged"
        return None

    # ── Toyota / Lexus ────────────────────────────────────────────────────────
    if m in ("toyota", "lexus"):
        if _has(mo, "supra") and yr >= 2020:
            return "Turbocharged"
        if _has(mo, "corolla") and _has(tr, "gr"):
            return "Turbocharged"
        if m == "lexus" and cyl == 4 and el and el <= 2.1:
            return "Turbocharged"
        if m == "lexus" and re.search(r"\bnx\s*(?:200t|300)\b|200t\b", full):
            return "Turbocharged"
        return None

    # ── Hyundai / Kia ─────────────────────────────────────────────────────────
    if m in ("hyundai", "kia"):
        if re.search(r"\bioniq\b|\bev6\b|\bev9\b|\bniro\s*ev\b|\bkona\s*electric\b", mo):
            return None
        if _has(tr, "n line", " n ", "n ", "gt-line", "turbo", "1.6t", "2.0t", "2.5t") or _has(mo, " n ", "n-line"):
            return "Turbocharged"
        if el and el <= 1.7 and cyl == 4:
            return "Turbocharged"
        return None

    # ── Genesis ───────────────────────────────────────────────────────────────
    if m == "genesis":
        if cyl == 6 and el >= 3.0:
            return "Twin Turbocharged"
        return "Turbocharged"

    # ── INFINITI ──────────────────────────────────────────────────────────────
    if m in ("infiniti",):
        if re.search(r"\b3\.0t\b", full) or _has(tr, "3.0t", "red sport"):
            return "Twin Turbocharged"
        if _has(tr, "2.0t", "turbo"):
            return "Turbocharged"
        if cyl == 6 and el >= 2.9:
            return "Twin Turbocharged"
        if cyl == 4 and el and el <= 2.1:
            return "Turbocharged"
        return None

    # ── Subaru ────────────────────────────────────────────────────────────────
    if m == "subaru":
        if _has(mo, "wrx", "sti") or re.search(r"\bxt\b", tr):
            return "Turbocharged"
        return None

    # ── MINI ──────────────────────────────────────────────────────────────────
    if m == "mini":
        if _has(mo, "electric") or _has(tr, "electric", " se"):
            return None
        return "Turbocharged"

    # ── Volvo ─────────────────────────────────────────────────────────────────
    if m == "volvo":
        if re.search(r"\b(ex30|ex90|ex40|ec40|em90|c40)\b", mo) and (
            _has(ft, "electric") or re.search(r"\b(twin motor|single motor)\b", tr)
        ):
            return None
        if _has(tr, "recharge") and _has(mo, "xc40", "c40") and not _has(mo, "xc40 t"):
            return None  # pure EV
        return "Turbocharged"

    # ── Alfa Romeo ────────────────────────────────────────────────────────────
    if _has(m, "alfa"):
        if re.search(r"quadrifoglio|\bqv\b", tr):
            return "Twin Turbocharged"
        return "Turbocharged"

    # ── Maserati ─────────────────────────────────────────────────────────────
    if m == "maserati":
        if _has(tr, "trofeo") or (cyl == 8 and el >= 3.7):
            return "Twin Turbocharged"
        return "Turbocharged"

    # ── Jaguar ────────────────────────────────────────────────────────────────
    if m == "jaguar":
        if _has(mo, "i-pace"):
            return None
        if _has(mo, "f-type", "f type") and cyl == 8:
            return "Twin Turbocharged" if yr >= 2021 else "Supercharged"
        if _has(tr, "svr"):
            return "Twin Turbocharged" if yr >= 2021 else "Supercharged"
        return "Turbocharged"

    # ── Land Rover ────────────────────────────────────────────────────────────
    if m == "land rover":
        if cyl == 8 and yr < 2022:
            return "Supercharged"
        if cyl == 8 and yr >= 2022:
            return "Twin Turbocharged"
        return "Turbocharged"

    # ── Tesla / EV brands ────────────────────────────────────────────────────
    if m in ("tesla", "rivian", "lucid", "polestar", "fisker", "canoo"):
        return None

    # ── Ferrari / Lamborghini / McLaren / Bentley / Rolls-Royce ─────────────
    if m in ("ferrari", "lamborghini", "mclaren", "bentley"):
        return "Twin Turbocharged"
    if _has(m, "rolls"):
        return "Twin Turbocharged"

    return None


def classify_forced_induction(
    make: str | None,
    model: str | None,
    trim: str | None,
    year: int | None,
    cylinders: int | None,
    engine_l: float | None,
    engine_description: str | None = None,
    fuel_type: str | None = None,
    description: str | None = None,
    title: str | None = None,
) -> str | None:
    """
    Return forced induction label or None.

    Checks raw text first (most reliable), falls back to make/model lookup.
    Electric vehicles always return None.
    """
    ft = _norm(fuel_type)
    if _has(ft, "electric") and "plug" not in ft and "hybrid" not in ft:
        return None

    identity_blob = " ".join(
        x for x in (title, model, trim, engine_description) if x and str(x).strip()
    )
    from_t = _classify_from_trim_t_displacement(identity_blob)
    if from_t:
        return from_t

    # Text patterns from raw engine description (before normalization strips keywords)
    from_text = extract_forced_induction_from_text(engine_description)
    if from_text:
        return from_text

    # Also scan car description body text for turbo signals
    from_desc = extract_forced_induction_from_text(description)
    if from_desc:
        return from_desc

    return _classify_by_make_model(
        make or "",
        model or "",
        trim or "",
        year,
        cylinders,
        engine_l,
        fuel_type=fuel_type or "",
    )


def classify_forced_induction_from_car_row(car: dict[str, Any]) -> str | None:
    """Convenience wrapper for a car dict row."""
    el = None
    raw_el = car.get("engine_l")
    if raw_el is not None:
        try:
            el = float(str(raw_el).split("L")[0].strip())
        except (ValueError, TypeError):
            pass

    cyl = None
    raw_cyl = car.get("cylinders")
    if raw_cyl is not None:
        try:
            cyl = int(raw_cyl)
        except (ValueError, TypeError):
            pass

    yr = None
    raw_yr = car.get("year")
    if raw_yr is not None:
        try:
            yr = int(raw_yr)
        except (ValueError, TypeError):
            pass

    return classify_forced_induction(
        make=car.get("make"),
        model=car.get("model"),
        trim=car.get("trim"),
        year=yr,
        cylinders=cyl,
        engine_l=el,
        engine_description=car.get("engine_description"),
        fuel_type=car.get("fuel_type"),
        description=car.get("description"),
        title=car.get("title"),
    )
