"""Trim-name resolution: model keys, label preservation, canonical names, ordering."""
from __future__ import annotations

import re
from typing import Iterable

from .drivetrain import (
    _DRIVETRAIN_INLINE_RE,
    drivetrain_merge_key,
)
from .tables import (
    GENERIC_TRIM_ORDER,
    MAKE_TRIM_ORDER,
    MODEL_TRIM_ORDER,
    _BMW_EV_TRIM_RE,
    _BMW_MOTOR_TRIM_RE,
)
from .validation import (
    _COLON_SUFFIX_RE,
    _DIMENSION_TAIL_RE,
    _ENGINE_TRIM_RE,
    _FEATURE_FRAGMENT_RE,
    _JUNK_TRIM_EXACT,
    _JUNK_TRIM_SUBSTR_RE,
    _JUNK_TRIM_TOKENS,
    _NON_TRIM_NAME_RE,
    _PAREN_DIMENSION_RE,
    _SENTENCE_FRAGMENT_RE,
    _SHORT_PERFORMANCE_TRIM_RE,
    _TRANSMISSION_TRIM_RE,
    _norm_make_key,
    _norm_model_key,
)

def _resolve_trim_model_key(make: str, model: str | None) -> tuple[str, str]:
    """Map listing model strings to trim-ladder model keys."""
    mk = _norm_make_key(make)
    mod = _norm_model_key(model)
    if mk == "jeep":
        if mod.startswith("wagoneer"):
            return mk, "wagoneer"
        if mod.startswith("grandwagoneer"):
            return mk, "grandwagoneer"
        if "grandcherokee" in mod:
            return mk, "grandcherokee"
    if mk == "mini":
        if mod in {"2door", "4door", "convertible", "cooper", "coopers", "hardtop"}:
            return mk, "cooper"
        if "countryman" in mod:
            return mk, "countryman"
    if mk == "toyota":
        if "crownsignia" in mod or mod == "crownsignia":
            return mk, "crownsignia"
        if mod.startswith("toyotacrown"):
            return mk, "crown"
        if "grsupra" in mod or mod == "supra":
            return mk, "grsupra"
        if mod.startswith("corollahybrid") or mod.startswith("corollahatchback"):
            return mk, "corolla"
        if mod.startswith("bz"):
            return mk, "bz"
        if mod.startswith("tacoma"):
            return mk, "tacoma"
        if mod.startswith("landcruiser"):
            return mk, "landcruiser"
        if mod.startswith("rav4prime"):
            return mk, "rav4prime"
    if mk == "bmw":
        if mod.startswith("2series"):
            return mk, "2series"
        if mod.startswith("4series"):
            return mk, "4series"
        if mod.startswith("8series"):
            return mk, "8series"
        if mod.startswith("z4"):
            return mk, "z4"
        if mod == "m2" or mod.startswith("m2"):
            return mk, "m2"
    if mk == "chevrolet" and "captiva" in mod:
        return mk, "captivasport"
    if mk == "volvo":
        if mod.startswith("ex90"):
            return mk, "ex90"
        if mod.startswith("ex30"):
            return mk, "ex30"
    if mk == "chrysler" and mod == "pacifica":
        return mk, "pacifica"
    if mk == "audi":
        if mod.startswith("r8spyder"):
            return mk, "r8spyder"
        if mod.startswith("r8"):
            return mk, "r8"
        if "q6etron" in mod or (mod.startswith("q6") and "etron" in mod):
            return mk, "q6etron"
        if "q8etron" in mod or (mod.startswith("q8") and "etron" in mod):
            return mk, "q8etron"
        if mod.startswith("a8"):
            return mk, "a8"
        if mod.startswith("q5") or mod.startswith("sq5"):
            return mk, "q5"
        if mod.startswith("a4") or mod.startswith("a5") or mod.startswith("s4") or mod.startswith("s5"):
            return mk, "a4"
        if mod.startswith("a6") or mod.startswith("s6"):
            return mk, "a6"
        if mod.startswith("a3") or mod.startswith("s3"):
            return mk, "a3"
        if mod.startswith("q7"):
            return mk, "q7"
        if mod.startswith("q8"):
            return mk, "q8"
        if mod.startswith("rs6"):
            return mk, "rs6"
        if "etron" in mod:
            return mk, mod
    if mk == "landrover" and "rangerover" in mod:
        return mk, "rangerover"
    if mk == "tesla":
        if mod.startswith("model3"):
            return mk, "model3"
        if mod.startswith("modely"):
            return mk, "modely"
        if mod.startswith("models"):
            return mk, "models"
        if mod.startswith("modelx"):
            return mk, "modelx"
    if mk == "volvo" and mod.startswith("xc90"):
        return mk, "xc90"
    if mk == "gmc" and mod.startswith("yukon"):
        return mk, "yukon"
    if mk == "nissan" and mod.startswith("pathfinder"):
        return mk, "pathfinder"
    if mk == "porsche" and mod.startswith("taycan"):
        return mk, "taycan"
    if mk == "kia" and mod.startswith("k5"):
        return mk, "k5"
    if mk == "mazda" and mod.startswith("mazda3"):
        return mk, "mazda3"
    if mk == "cadillac" and mod.startswith("escalade"):
        return mk, "escalade"
    if mk == "cadillac" and mod.startswith("xt4"):
        return mk, "xt4"
    if mk == "dodge" and mod.startswith("charger"):
        return mk, "charger"
    if mk == "dodge" and mod.startswith("challenger"):
        return mk, "challenger"
    return mk, mod


_EPA_MODEL_SEARCH_NAMES: dict[tuple[str, str], str] = {
    ("mini", "cooper"): "Cooper",
    ("mini", "countryman"): "Countryman",
    ("toyota", "crownsignia"): "Crown Signia",
    ("toyota", "grsupra"): "GR Supra",
    ("toyota", "corolla"): "Corolla",
    ("toyota", "bz"): "bZ",
    ("bmw", "2series"): "2 Series",
    ("bmw", "4series"): "4 Series",
    ("bmw", "8series"): "8 Series",
    ("jeep", "wagoneer"): "Wagoneer",
    ("jeep", "grandwagoneer"): "Grand Wagoneer",
    ("audi", "a8"): "A8",
    ("audi", "r8"): "R8",
    ("audi", "r8spyder"): "R8 Spyder",
    ("audi", "q6etron"): "Q6 e-tron",
    ("audi", "q8etron"): "Q8 e-tron",
    ("audi", "q5"): "Q5",
    ("audi", "a4"): "A4",
    ("audi", "a6"): "A6",
    ("audi", "a3"): "A3",
    ("audi", "q7"): "Q7",
    ("audi", "q8"): "Q8",
    ("audi", "rs6"): "RS 6",
    ("mercedesbenz", "glc"): "GLC-Class",
    ("mercedesbenz", "gle"): "GLE-Class",
    ("mercedesbenz", "gla"): "GLA-Class",
    ("mercedesbenz", "glb"): "GLB-Class",
    ("mercedesbenz", "gls"): "GLS-Class",
    ("mercedesbenz", "sl"): "SL-Class",
    ("mercedesbenz", "slclass"): "SL-Class",
    ("mercedesbenz", "slc"): "SLC-Class",
    ("mercedesbenz", "slk"): "SLK-Class",
    ("mercedesbenz", "cclass"): "C-Class",
    ("mercedesbenz", "eclass"): "E-Class",
    ("mercedesbenz", "cle"): "CLE-Class",
}


AUDI_DEALER_TRIM_TOKENS: tuple[str, ...] = (
    "Premium Plus",
    "Premium",
    "Prestige",
    "S line",
    "Ultra",
    "Progressiv",
    "Technik",
    "Komfort",
    "60 TFSI",
    "55 TFSI",
    "50 TFSI",
    "L",
)


def audi_dealer_trim_tokens(raw: str) -> list[str]:
    """Extract known Audi dealer trim tokens from a compound listing trim string."""
    text = str(raw or "").strip()
    if not text:
        return []
    found: list[str] = []
    for token in sorted(AUDI_DEALER_TRIM_TOKENS, key=len, reverse=True):
        if re.search(rf"\b{re.escape(token)}\b", text, re.I):
            label = "S line" if token.lower() == "s line" else token
            if label not in found:
                found.append(label)
    return found


def epa_model_search_name(make: str, model: str | None) -> str:
    """Model label used when locating EPA CSV files."""
    mk, mod = _resolve_trim_model_key(make, model)
    return _EPA_MODEL_SEARCH_NAMES.get((mk, mod), (model or "").strip())


def _model_trim_order(make: str, model: str | None) -> tuple[str, ...]:
    key = _resolve_trim_model_key(make, model or "")
    return MODEL_TRIM_ORDER.get(key, ())


def _mercedes_motor_trim_label(raw: str) -> str | None:
    """Normalize Mercedes motor badges (SL400, SL63 AMG, GLC300, etc.)."""
    t = re.sub(r"\s+", " ", (raw or "").strip())
    if not t:
        return None

    m = re.match(r"^(?:AMG\s+)?SL([CK]?)\s*(\d{2,3})\s+AMG\b", t, re.I)
    if m:
        body = f"SL{m.group(1).upper()}" if m.group(1) else "SL"
        return f"{body} {m.group(2)} AMG"
    m = re.match(r"^SL([CK]?)(\d{2,3})\s+AMG\b", t, re.I)
    if m:
        body = f"SL{m.group(1).upper()}" if m.group(1) else "SL"
        return f"{body} {m.group(2)} AMG"
    m = re.match(r"^SL([CK]?)\s*(\d{2,3})\b", t, re.I)
    if m:
        body = f"SL{m.group(1).upper()}" if m.group(1) else "SL"
        return f"{body} {m.group(2)}"
    m = re.match(r"^SL([CK]?)(\d{2,3})\b", t, re.I)
    if m:
        body = f"SL{m.group(1).upper()}" if m.group(1) else "SL"
        return f"{body} {m.group(2)}"

    m = re.search(
        r"\b(AMG\s+[A-Z]{1,3}\s?\d{2,3}[a-z]?|GL[CESABLK]\s?\d{2,3}[a-z]?|C\s?\d{3}|E\s?\d{3}|S\s?\d{3})\b",
        t,
        re.I,
    )
    if m:
        return re.sub(r"\s+", " ", m.group(1).strip())
    return None


def preserve_trim_label(raw: str, make: str, model: str | None = None) -> str:
    """Keep motor/series trim tokens that generic canonicalization would drop."""
    t = str(raw or "").strip()
    if not t:
        return ""

    mk_early = _norm_make_key(make)
    if mk_early == "audi":
        if t.upper() in {"AWD", "RWD"}:
            return t.upper()
        spyder_drv = re.match(r"^Spyder\s+(AWD|RWD)$", t, re.I)
        if spyder_drv:
            return f"Spyder {spyder_drv.group(1).upper()}"

    t = _COLON_SUFFIX_RE.sub("", t).strip()
    t = _PAREN_DIMENSION_RE.sub("", t).strip()
    t = _DIMENSION_TAIL_RE.sub("", t).strip()
    for _ in range(4):
        t = _DRIVETRAIN_INLINE_RE.sub(" ", t).strip()
    t = re.sub(r"\s+", " ", t).strip(" -–—")
    if not t:
        return ""

    mk, mod = _resolve_trim_model_key(make, model)
    if mk == "mercedesbenz":
        mb_label = _mercedes_motor_trim_label(t)
        if mb_label:
            return mb_label

    model_trims = _model_trim_order(make, model)
    if model_trims:
        for known in sorted(model_trims, key=len, reverse=True):
            if t.lower() == known.lower():
                return known
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known

    if mk == "bmw" and _BMW_MOTOR_TRIM_RE.match(t.replace(" ", "")):
        if re.match(r"^M60i(?:\s+xDrive)?$", t, re.I):
            return "M60i xDrive" if re.search(r"xDrive", t, re.I) else "M60i"
        if re.match(r"^Alpina\s+XB7$", t, re.I):
            return "Alpina XB7"
        return t
    if mk == "bmw" and mod in {"x1", "x2", "x3", "x4", "x5", "x6", "x7"}:
        sav = re.search(r"\bX\d\s+(\d{2}[ie])\b", t, re.I)
        if sav:
            return sav.group(1).lower()
        sav_e = re.search(r"\bX\d\s+(\d{2}e)\b", t, re.I)
        if sav_e:
            return sav_e.group(1).lower()
    if mk == "bmw" and re.match(r"^[xs]Drive\d{2}[ie]$", t.replace(" ", ""), re.I):
        return t[0].lower() + t[1:] if t[0].isupper() else t
    if mk == "bmw" and re.match(r"^M\d{2,3}i$", t, re.I):
        return t.upper() if t.islower() else t
    if mk == "bmw":
        ev = _BMW_EV_TRIM_RE.search(t)
        if ev:
            label = ev.group(1).replace(" ", "")
            low = label.lower()
            if low.startswith("edrive"):
                return "eDrive" + label[6:]
            if low.startswith("xdrive"):
                return "xDrive" + label[6:]
            if low.startswith("m60xdrive"):
                return "M60 xDrive"
        motor = re.search(r"\b(M?\d{3}[ie]?)\b", t, re.I)
        if motor and (
            mod in {"3series", "5series", "7series"}
            or re.match(r"^\d{3}[ie]?$", mod)
        ):
            s = motor.group(1)
            if s.upper().startswith("M"):
                base = s[0].upper() + s[1:].lower()
            else:
                base = s.lower()
            if re.search(r"\bxDrive\b", t, re.I):
                return f"{base} xDrive"
            return base
        motor = re.search(r"\b(M?\d{3}i)\b", t, re.I)
        if motor:
            s = motor.group(1)
            if s.upper().startswith("M"):
                return s[0].upper() + s[1:].lower()
            return s.lower()
        plain = re.search(r"\b(M?\d{3})\b", t, re.I)
        if plain and not re.search(r"\b\d{4}\b", t):
            s = plain.group(1)
            if s.upper().startswith("M"):
                return s[0].upper() + s[1:].lower()
            return s.lower()

    if mk == "mini" and mod == "cooper":
        m = re.match(r"^C\s+(\d\s+Door)", t, re.I)
        if m:
            return "Cooper " + m.group(1).replace(" ", "")
        m = re.match(r"^S\s+(\d\s+Door)", t, re.I)
        if m:
            return "Cooper S " + m.group(1).replace(" ", "")

    if mk == "mercedesbenz":
        mb_label = _mercedes_motor_trim_label(t)
        if mb_label:
            return mb_label

    if mk == "toyota" and mod in ("bz", "bzwoodland", "corolla", "corollahybrid", "corollahatchback"):
        m = re.match(r"^(AWD\s*)?(Woodland|LIMITED|Hybrid(?:\s+SE|\s+AWD|\s+SE\s+AWD)?|GR\s+Corolla|Hatchback(?:\s+XSE|\s+FX)?|SE|LE|XLE|XSE)\b", t, re.I)
        if m:
            label = re.sub(r"\s+", " ", m.group(0).strip())
            return label.title() if label.isupper() else label

    if mk == "audi":
        for known in ("Premium Plus", "Premium", "Prestige", "Progressiv", "Technik", "Komfort", "S line", "S Line"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return "S line" if known.lower().replace(" ", "") == "sline" else known
        if re.fullmatch(r"L", t, re.I):
            return "L"
        if t.lower() in {"ultra", "quattro"}:
            return t.title()
        if t.upper() in {"AWD", "RWD"}:
            return t.upper()
        m = re.match(r"^(Spyder\s+)?(AWD|RWD)$", t, re.I)
        if m:
            return ("Spyder " if m.group(1) else "") + m.group(2).upper()
        m = re.search(r"\b(RS\s?6|RS6)\b", t, re.I)
        if m:
            return "RS 6" if " " in m.group(1) else "RS6"
        m = re.search(r"\b(\d{2})\s*TFSI\b", t, re.I)
        if m:
            return f"{m.group(1)} TFSI"
        m = re.search(r"\bV10(?:\s+(?:Plus|performance|RWD|quattro))?\b", t, re.I)
        if m:
            return m.group(0).title().replace("Rwd", "RWD").replace("Quattro", "quattro")

    if mk == "jeep" and mod in ("wagoneer", "grandwagoneer"):
        m = re.match(r"^(L Series [IVX]+|Series [IVX]+|Carbide|Obsidian|Launch Edition)", t, re.I)
        if m:
            label = m.group(1)
            parts: list[str] = []
            for part in label.split():
                up = part.upper()
                if up in {"I", "II", "III", "IV", "V", "VI"}:
                    parts.append(up)
                elif part.lower() == "series":
                    parts.append("Series")
                elif part.lower() == "l":
                    parts.append("L")
                else:
                    parts.append(part.title())
            return " ".join(parts)

    if mk == "volvo":
        m = re.match(r"^(T\d+|B\d+)", t, re.I)
        if m:
            return m.group(1).upper()
    if mk == "gmc" and mod == "yukon":
        for known in ("Denali Ultimate", "AT4X", "AT4", "Denali", "SLT", "Elevation", "SLE"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known
    if mk == "tesla":
        for known in ("Plaid", "Performance", "Long Range", "Standard Range"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known
    if mk == "porsche" and mod == "taycan":
        for known in ("Turbo S", "Turbo", "GTS", "4S"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known
    if mk == "toyota" and mod == "tacoma":
        for known in ("TRD Pro", "TRD Off-Road", "TRD Sport", "Limited", "SR5", "SR"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known
    if mk == "landrover" and mod == "rangerover":
        for known in ("Autobiography", "SV", "HSE", "HST", "SE"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known
    if mk == "cadillac" and mod == "escalade":
        if re.fullmatch(r"V\s*AWD", t, re.I):
            return "V-Series"
        for known in ("V-Series", "Platinum", "Premium Luxury", "Luxury", "Sport"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known
    if mk == "cadillac" and mod == "xt4":
        for known in ("Premium Luxury", "Sport", "Luxury"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known

    return ""


def ordered_trim_candidates(make: str, model: str | None = None) -> tuple[str, ...]:
    """Luxury-first order: index 0 = top of ladder (most luxurious)."""
    key = _norm_make_key(make)
    seen: set[str] = set()
    out: list[str] = []
    for name in (*_model_trim_order(make, model), *MAKE_TRIM_ORDER.get(key, ()), *GENERIC_TRIM_ORDER):
        tok = re.sub(r"[^a-z0-9]+", "", name.lower())
        if tok and tok not in seen:
            seen.add(tok)
            out.append(name)
    return tuple(out)


def ordered_trim_candidates_longest_first(make: str, model: str | None = None) -> tuple[str, ...]:
    return tuple(sorted(ordered_trim_candidates(make, model), key=len, reverse=True))


def known_trim_names_for_make(make: str, model: str | None = None) -> frozenset[str]:
    names = {n.lower() for n in (*ordered_trim_candidates(make, model), *GENERIC_TRIM_ORDER)}
    return frozenset(names)


def trim_name_is_acceptable(name: str, make: str, model: str | None = None) -> bool:
    """True when *name* is a known OEM trim or a short plausible performance label."""
    low = (name or "").strip().lower()
    if low in known_trim_names_for_make(make, model):
        return True
    if preserve_trim_label(name, make, model):
        return True
    if not _is_valid_trim_label(name):
        return False
    tok = re.sub(r"[^a-z0-9]+", "", low)
    if tok in _JUNK_TRIM_EXACT:
        return False
    if _JUNK_TRIM_SUBSTR_RE.search(name):
        return False
    if _SHORT_PERFORMANCE_TRIM_RE.match(name.strip()):
        return True
    # Multi-word only when every word is a known trim token (e.g. "Sport S", "TRD Pro").
    words = low.split()
    if 1 < len(words) <= 4:
        known = known_trim_names_for_make(make, model)
        if all(w in known or re.sub(r"[^a-z0-9]+", "", w) in known for w in words):
            return True
    return False


def _is_valid_trim_label(name: str) -> bool:
    if not name or len(name) < 2 or len(name) > 48:
        return False
    if _NON_TRIM_NAME_RE.match(name):
        return False
    if _SENTENCE_FRAGMENT_RE.match(name):
        return False
    if _ENGINE_TRIM_RE.search(name):
        return False
    if _TRANSMISSION_TRIM_RE.search(name):
        return False
    if _FEATURE_FRAGMENT_RE.search(name):
        return False
    words = name.split()
    if len(words) > 6:
        return False
    if name.isupper() and len(name) > 28:
        return False
    if re.search(r"\b(?:in|on|with|for|from|at)\s+the\b", name, re.I):
        return False
    if re.search(r"\b\d{4}\b", name):
        return False
    if _JUNK_TRIM_SUBSTR_RE.search(name):
        return False
    tok = re.sub(r"[^a-z0-9]+", "", name.lower())
    if tok in _JUNK_TRIM_TOKENS or tok in _JUNK_TRIM_EXACT or tok.isdigit():
        return False
    if re.match(r"^[A-Za-z]{1,4}\d{1,2}$", name.strip()):
        return True
    if _SHORT_PERFORMANCE_TRIM_RE.match(name.strip()):
        return True
    alpha = sum(1 for ch in name if ch.isalpha())
    if alpha < max(3, len(name) // 4):
        return False
    return True


def canonical_trim_name(raw: str, make: str, model: str | None = None) -> str:
    """
    Normalize a trim label: drop drivetrain/wheelbase noise and map to a known OEM trim name.
    """
    preserved = preserve_trim_label(raw, make, model)
    if preserved:
        return preserved

    t = str(raw or "").strip()
    if not t:
        return ""
    if _norm_make_key(make) == "bmw":
        m = re.match(r"^(\d{2})i(\s|/|$)", t, re.I)
        if m and 18 <= int(m.group(1)) <= 49:
            t = re.sub(r"^(\d{2})i", rf"3{m.group(1)}i", t, count=1, flags=re.I)
    t = _COLON_SUFFIX_RE.sub("", t).strip()
    t = _PAREN_DIMENSION_RE.sub("", t).strip()
    t = _DIMENSION_TAIL_RE.sub("", t).strip()
    for _ in range(4):
        t = _DRIVETRAIN_INLINE_RE.sub(" ", t).strip()
    t = re.sub(r"\s+", " ", t).strip(" -–—")
    if not t:
        return ""

    for known in ordered_trim_candidates_longest_first(make, model):
        if re.search(rf"\b{re.escape(known)}\b", t, re.I):
            return known

    # Title-case short unknown tokens (e.g. "sr5" from prose)
    if t.isupper() and len(t) <= 12:
        t = t.title()
    elif t == t.lower():
        t = t.title()

    if not _is_valid_trim_label(t):
        return ""
    return t if trim_name_is_acceptable(t, make, model) else ""


def _bmw_luxury_sort_key(trim_name: str) -> tuple[int, int, int, int] | None:
    """
    BMW motor-trim ordering (lower tuple = more luxurious, top of ladder).

    Rules:
    - M / Competition above numeric motor trims
    - Higher motor number above lower (40i above 30i above 35 eDrive)
    - xDrive above sDrive above eDrive when motor tier matches
    - PHEV (30e) above gas (30i) at same motor tier and drivetrain
    """
    t = (trim_name or "").lower()
    compact = re.sub(r"[^a-z0-9]+", "", t)
    if not compact:
        return None

    drive = 0 if "xdrive" in compact else (1 if "sdrive" in compact else (2 if "edrive" in compact else 1))

    if "competition" in t and re.search(r"\bm\b", t):
        return (0, 0, drive, 0)

    m = re.search(r"m(\d{2,3})", compact)
    if m and compact.startswith("m"):
        return (1, -int(m.group(1)), drive, 0)

    core = compact.replace("xdrive", "").replace("sdrive", "")
    sm = re.match(r"^(\d{3})([ie])?$", core)
    if sm:
        num = int(sm.group(1))
        suffix = 0 if sm.group(2) == "e" else (1 if sm.group(2) == "i" else 2)
        return (2, -num, drive, suffix)

    dm = re.search(r"(?:x|s|e)?drive(\d{2})([ie])?", compact)
    if dm:
        num = int(dm.group(1))
        suffix = 0 if dm.group(2) == "e" else (1 if dm.group(2) == "i" else 2)
        return (2, -num, drive, suffix)

    plain = re.search(r"(\d{2})i", compact)
    if plain:
        return (2, -int(plain.group(1)), drive, 1)

    return None


def _mercedes_luxury_sort_key(trim_name: str) -> tuple[int, int, int] | None:
    """Sort Mercedes motor trims luxury-first (Maybach/600 → AMG → higher model numbers)."""
    t = re.sub(r"\s+", " ", (trim_name or "").strip())
    if not t:
        return None
    low = t.lower()

    model_num: int | None = None
    m = re.search(r"\b(?:GL[CESABLK]|CLS?|EQ[ESCB]|SL|G)\s*(\d{2,3})\b", t, re.I)
    if m:
        model_num = int(m.group(1))
    if model_num is None:
        m = re.search(r"\b(?:AMG\s+)?(?:\w+\s+)?(\d{2,3})\b", t, re.I)
        if m:
            model_num = int(m.group(1))

    if "maybach" in low or (model_num is not None and model_num >= 600 and re.search(r"\bGLS\s*600\b", t, re.I)):
        return (0, -(model_num or 600), 0)

    if "amg" in low:
        amg_num = model_num
        if amg_num is None:
            m2 = re.search(r"\bamg\s+(?:\w+\s+)?(\d{2,3})\b", t, re.I)
            amg_num = int(m2.group(1)) if m2 else 63
        return (1, -(amg_num or 63), 0)

    if model_num is not None:
        return (2, -model_num, 0)

    return None


def _mercedes_luxury_rank(trim_name: str) -> int | None:
    key = _mercedes_luxury_sort_key(trim_name)
    if key is None:
        return None
    tier, neg_num, suffix = key
    return tier * 10_000 + (-neg_num) * 100 + suffix


def _bmw_luxury_rank(trim_name: str) -> int | None:
    key = _bmw_luxury_sort_key(trim_name)
    if key is None:
        return None
    tier, neg_motor, drive, suffix = key
    return tier * 10_000 + (-neg_motor) * 100 + drive * 10 + suffix


def luxury_rank(trim_name: str, make: str, model: str | None = None) -> int:
    """Lower rank = more luxurious (top of ladder)."""
    if _norm_make_key(make) == "bmw":
        bmw_rank = _bmw_luxury_rank(trim_name)
        if bmw_rank is not None:
            return bmw_rank
    if _norm_make_key(make) == "mercedesbenz":
        mb_rank = _mercedes_luxury_rank(trim_name)
        if mb_rank is not None:
            return mb_rank
    order = ordered_trim_candidates(make, model)
    rank_map = {n.lower(): i for i, n in enumerate(order)}
    direct = rank_map.get((trim_name or "").lower())
    if direct is not None:
        return direct
    best = 9_999
    for known in ordered_trim_candidates_longest_first(make, model):
        if re.search(rf"\b{re.escape(known)}\b", trim_name or "", re.I):
            best = min(best, rank_map.get(known.lower(), 9_999))
    return best


def normalize_ladder_steps(
    steps: list[dict],
    make: str,
    *,
    model: str | None = None,
    limit: int = 14,
) -> list[dict]:
    """
    Canonicalize trim names, merge drivetrain duplicates, sort luxury → base (top → bottom).
    """
    by_key: dict[str, dict] = {}

    for step in steps or []:
        raw_name = str(step.get("name") or "")
        name = canonical_trim_name(raw_name, make, model) or preserve_trim_label(raw_name, make, model)
        if not name or not trim_name_is_acceptable(name, make, model):
            continue
        key = drivetrain_merge_key(name, make, model) or re.sub(r"[^a-z0-9]+", "", name.lower())
        aliases: list[str] = []
        for a in step.get("aliases") or []:
            raw = str(a).strip()
            if not raw:
                continue
            canonical = canonical_trim_name(raw, make, model)
            aliases.append(canonical or raw)
        aliases = [a for a in aliases if a.lower() != name.lower()]
        adds = [str(a).strip() for a in (step.get("adds") or []) if str(a).strip()]
        step_ymin = int(step.get("year_min") or 0)
        step_ymax = int(step.get("year_max") or 9999)
        price_note = str(step.get("inventory_price_note") or "").strip()

        if key not in by_key:
            by_key[key] = {
                "name": name,
                "aliases": list(dict.fromkeys(aliases)),
                "adds": [],
                "year_min": step_ymin,
                "year_max": step_ymax,
                "inventory_price_note": price_note,
            }
        entry = by_key[key]
        if price_note and not entry.get("inventory_price_note"):
            entry["inventory_price_note"] = price_note
        if step_ymin:
            entry["year_min"] = max(int(entry.get("year_min") or 0), step_ymin)
        if step_ymax < 9999:
            entry["year_max"] = min(int(entry.get("year_max") or 9999), step_ymax)
        for alias in aliases:
            if alias not in entry["aliases"]:
                entry["aliases"].append(alias)
        seen_adds = {a.lower() for a in entry["adds"]}
        for line in adds:
            if line.lower() not in seen_adds:
                seen_adds.add(line.lower())
                entry["adds"].append(line)

    ordered_keys = sorted(
        by_key.keys(),
        key=lambda k: (
            (0, _bmw_luxury_sort_key(by_key[k]["name"]))
            if _norm_make_key(make) == "bmw" and _bmw_luxury_sort_key(by_key[k]["name"]) is not None
            else (
                (0, _mercedes_luxury_sort_key(by_key[k]["name"]))
                if _norm_make_key(make) == "mercedesbenz"
                and _mercedes_luxury_sort_key(by_key[k]["name"]) is not None
                else (1, luxury_rank(by_key[k]["name"], make, model), by_key[k]["name"].lower())
            )
        ),
    )
    out: list[dict] = []
    for key in ordered_keys[:limit]:
        entry = by_key[key]
        if not entry["adds"]:
            pass
        out.append(
            {
                "name": entry["name"],
                "aliases": entry["aliases"][:6],
                "adds": entry["adds"][:6],
                **({"year_min": int(entry["year_min"])} if int(entry.get("year_min") or 0) else {}),
                **({"year_max": int(entry["year_max"])} if int(entry.get("year_max") or 9999) < 9999 else {}),
                **(
                    {"inventory_price_note": str(entry.get("inventory_price_note") or "").strip()}
                    if str(entry.get("inventory_price_note") or "").strip()
                    else {}
                ),
            }
        )
    return out if len(out) >= 2 else []


def extract_trims_from_text(
    make: str,
    text: str,
    *,
    model: str | None = None,
    limit: int = 12,
) -> list[str]:
    """Find known trim names in a blob of dictionary / listing text."""
    if not text:
        return []
    found: list[str] = []
    seen: set[str] = set()
    for name in ordered_trim_candidates(make, model):
        if len(name) < 2:
            continue
        if re.search(rf"\b{re.escape(name)}\b", text, re.I):
            tok = re.sub(r"[^a-z0-9]+", "", name.lower())
            if tok not in seen:
                seen.add(tok)
                found.append(name)
        if len(found) >= limit:
            break
  # Do not split arbitrary prose (wiki CSV blobs); only known OEM trim tokens.
    return found[:limit]


def merge_trim_names(
    *groups: Iterable[str],
    make: str = "",
    model: str | None = None,
    limit: int = 12,
) -> list[str]:
    """Merge trim name lists; order by OEM knowledge when possible."""
    order = ordered_trim_candidates(make, model)
    rank = {n.lower(): i for i, n in enumerate(order)}
    seen: set[str] = set()
    merged: list[str] = []

    def add(name: str) -> None:
        n = canonical_trim_name(name, make, model) or preserve_trim_label(name, make, model)
        if not n:
            return
        tok = re.sub(r"[^a-z0-9]+", "", n.lower())
        if tok in seen:
            return
        seen.add(tok)
        merged.append(n)

    for g in groups:
        for name in g:
            add(name)

    merged.sort(key=lambda n: rank.get(n.lower(), 10_000 + len(merged)))
    return merged[:limit]
