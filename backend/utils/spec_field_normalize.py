"""
Heuristic extraction/normalization for drivetrain, cylinders, engine, trim, transmission type.

Used by ``collect_row_storage_repairs`` and maintenance repair scripts. Complements
EPA/trim merge (``merge_verified_specs``) for raw listing text.
"""
from __future__ import annotations

import re
from typing import Any

from backend.utils.car_serialize import _dealer_spec_wins
from backend.utils.field_clean import clean_car_row_dict, coerce_drivetrain_stored, is_effectively_empty
from backend.utils.transmission_normalize import normalize_transmission_standard

_VALID_TRANSMISSION_TYPES = frozenset({"Automatic", "Manual", "CVT"})

_JUNK_ENGINE_PHRASES = [
    r"intercooled\s+turbo",
    r"twin\s+turbo",
    r"turbocharged",
    r"supercharged",
    r"regular\s+unleaded",
    r"premium\s+unleaded",
    r"unleaded",
    r"dual\s+overhead\s+cam",
    r"\bdohc\b",
    r"\bsohc\b",
    r"\bohv\b",
    r"\bgasoline\b",
    r"\bdiesel\b",
    r"\bflex\s+fuel\b",
    r"\bcylinder\s+engine\b",
    r"(?<![A-Za-z])\bengine\b",
]

_TRIM_NOISE_TAIL = re.compile(
    r"\s+(?:AWD|FWD|RWD|4WD|4X4|4X2|"
    r"\d+[\s-]*SPEED\s+AUTOMATIC|"
    r"\d+[\s-]*SPEED|"
    r"AUTOMATIC|MANUAL|CVT)\s*$",
    re.I,
)

_TRIM_EXPANSIONS = {
    # Cadillac V-series variations
    r"^V-ser\b": "V-Series",
    r"^V-Ser\b": "V-Series",
    r"^V-series\b": "V-Series",
    r"^V-SERIES\b": "V-Series",
    r"^V Ser\b": "V-Series",
    r"^V Ser\b": "V-Series",
    r"^V-S\b": "V-Series",
}


def normalize_trim_text(trim: Any) -> str | None:
    """Expand common trim abbreviations (e.g., 'V-ser' -> 'V-Series')."""
    if not trim or is_effectively_empty(trim):
        return None

    trim_str = str(trim).strip()
    for pattern, expansion in _TRIM_EXPANSIONS.items():
        if re.match(pattern, trim_str, re.I):
            return re.sub(pattern, expansion, trim_str, flags=re.I)

    return trim_str if trim_str else None


def normalize_drivetrain_from_fields(
    drivetrain: Any,
    *,
    title: str | None = None,
    trim: str | None = None,
) -> str | None:
    """Return FWD/RWD/AWD/4WD using column text first, then title/trim context."""
    direct = coerce_drivetrain_stored(drivetrain)
    if direct:
        return direct
    blob = f"{title or ''} {trim or ''}".strip()
    if not blob:
        return None
    return drivetrain_from_blob(blob)


def drivetrain_from_blob(blob: str) -> str | None:
    """Ordered keyword rules (aligned with inventory repair / nationwide feeds)."""
    u = blob.upper()
    if re.search(r"\bFOUR[\s-]WHEEL[\s-]DRIVE\b", u):
        return "4WD"
    if re.search(r"\b4WD\b", u):
        return "4WD"
    if re.search(r"\b4X4\b", u):
        return "AWD"
    if re.search(r"\bALL[\s-]WHEEL[\s-]DRIVE\b", u) or re.search(r"\bAWD\b", u):
        return "AWD"
    if re.search(r"\b(?:XDRIVE|4MATIC|QUATTRO|SH-AWD)\b", u):
        return "AWD"
    if re.search(r"\bFRONT[\s-]WHEEL[\s-]DRIVE\b", u) or re.search(r"\bFWD\b", u):
        return "FWD"
    if re.search(r"\b4X2\b", u):
        return "FWD"
    if re.search(r"\bREAR[\s-]WHEEL[\s-]DRIVE\b", u) or re.search(r"\bRWD\b", u):
        return "RWD"
    return None


def extract_cylinder_count(
    engine_description: Any,
    cylinders_raw: Any,
    *,
    fuel_type: Any = None,
) -> int | None:
    """Parse cylinder count from engine / cylinder text; BEV hints → 0."""
    parts: list[str] = []
    if engine_description:
        parts.append(str(engine_description))
    if cylinders_raw is not None and not isinstance(cylinders_raw, bool):
        parts.append(str(cylinders_raw))
    blob = " ".join(parts).strip()
    if not blob:
        return None

    ft = str(fuel_type or "").strip().lower()
    if ft == "electric":
        return 0
    if re.search(r"\belectric\b", blob, re.I) and (
        ft == "electric"
        or re.search(r"\b(?:battery|kwh|bev|motor)\b", blob, re.I)
    ):
        return 0

    for rx in (
        re.compile(r"\bV[-\s]?(\d{1,2})\b", re.I),
        re.compile(r"\bI[-\s]?(\d{1,2})\b", re.I),
        re.compile(r"\bW[-\s]?(\d{1,2})\b", re.I),
        re.compile(r"\binline[-\s]?(\d{1,2})\b", re.I),
    ):
        m = rx.search(blob)
        if m:
            n = int(m.group(1))
            if 1 <= n <= 16:
                return n

    m = re.search(r"\b(\d{1,2})\s*(?:CYL|CYLINDER)S?\b", blob, re.I)
    if m:
        n = int(m.group(1))
        if 0 <= n <= 16:
            return n
    return None


def normalize_engine_description_storage(raw: Any) -> str | None:
    """
    Compress noisy VDP engine strings to ``[displacement]L [layout]`` when possible
    (e.g. ``3.5L V6``, ``2.0L I4``).
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or is_effectively_empty(s):
        return None

    work = s
    for pat in _JUNK_ENGINE_PHRASES:
        work = re.sub(pat, " ", work, flags=re.I)
    work = re.sub(r"\s+", " ", work).strip()

    lit: float | None = None
    m = re.search(r"(\d+\.\d+)\s*L(?:/\d+)?\b", work, re.I)
    if m:
        lit = float(m.group(1))
    else:
        m2 = re.search(r"\b(\d+\.\d+)\s*(?:Liter|litre)\b", work, re.I)
        if m2:
            lit = float(m2.group(1))

    layout: str | None = None
    for rx, prefix in (
        (re.compile(r"\bV[-\s]?(\d{1,2})\b", re.I), "V"),
        (re.compile(r"\bI[-\s]?(\d{1,2})\b", re.I), "I"),
        (re.compile(r"\bW[-\s]?(\d{1,2})\b", re.I), "W"),
    ):
        mm = rx.search(work)
        if mm:
            layout = f"{prefix}{int(mm.group(1))}"
            break

    out: str | None = None
    if lit is not None and layout:
        out = f"{lit:.1f}L {layout}"
    elif layout:
        out = layout
    elif lit is not None:
        out = f"{lit:.1f}L"

    if not out or out == s:
        return None
    return out


def infer_trim_from_name_title(
    name: Any,
    title: Any,
    *,
    make: Any,
    model: Any,
) -> str | None:
    """When ``trim`` is empty, take tokens after ``{year} {make} {model}`` in title/name."""
    blob: str | None = None
    for candidate in (title, name):
        if candidate and str(candidate).strip():
            blob = str(candidate).strip()
            break
    if not blob:
        return None

    make_s = str(make or "").strip()
    model_s = str(model or "").strip()
    if not model_s:
        return None

    work = re.sub(r"^\s*\d{4}\s+", "", blob)
    if make_s:
        work = re.sub(re.escape(make_s), "", work, count=1, flags=re.I).strip()

    pos = work.lower().find(model_s.lower())
    if pos < 0:
        return None
    tail = work[pos + len(model_s) :].strip()
    tail = re.split(r"[|,]", tail, maxsplit=1)[0].strip()
    tail = tail.split("(", maxsplit=1)[0].strip()
    tail = _TRIM_NOISE_TAIL.sub("", tail).strip()
    tail = re.sub(r"^#\s*\S+\s*", "", tail).strip()

    if tail and len(tail) <= 60 and not is_effectively_empty(tail):
        return tail
    return None


def normalize_title_text(title: Any) -> str | None:
    """Clean title by removing redundant door count patterns (e.g., '4 Dr.', '4 Door')."""
    if not title or is_effectively_empty(title):
        return None

    title_str = str(title).strip()

    # Remove redundant door count patterns at the end
    # Patterns: "4 Dr.", "4 Door", "2 Dr.", "2 Door", etc.
    title_str = re.sub(r'\s+\d\s+(?:Dr\.|Door|door|dr\.)\s*$', '', title_str)

    return title_str.strip() if title_str else None


def infer_transmission_type_bucket(car: dict[str, Any]) -> str | None:
    """Map detailed transmission text to Automatic / Manual / CVT."""
    raw_tt = car.get("transmission_type")
    if isinstance(raw_tt, str):
        tts = raw_tt.strip()
        if tts in _VALID_TRANSMISSION_TYPES:
            return tts

    trans = car.get("transmission")
    if not trans or is_effectively_empty(trans):
        return None

    year = car.get("year")
    y_int = year if isinstance(year, int) else None

    label, _weak = normalize_transmission_standard(
        trans,
        make=car.get("make"),
        model=car.get("model"),
        trim=car.get("trim"),
        title=car.get("title"),
        year=y_int,
        vin=car.get("vin"),
        log_weak=False,
    )
    if label and label in _VALID_TRANSMISSION_TYPES:
        return label
    return None


def collect_raw_spec_heuristic_updates(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Column patches derived from raw strings (regex / layout). Runs after EPA merge in repair.

    Does not stomp solid dealer integers for cylinders (only fills NULL/0 placeholders).
    """
    c = clean_car_row_dict(dict(raw))
    out: dict[str, Any] = {}

    drv_merged = c.get("drivetrain")
    abbrev = coerce_drivetrain_stored(drv_merged)
    if abbrev:
        cur_cmp = str(drv_merged).strip().upper() if drv_merged is not None else ""
        abb_cmp = str(abbrev).strip().upper()
        if not cur_cmp or cur_cmp != abb_cmp:
            out["drivetrain"] = abbrev
    elif not _dealer_spec_wins(drv_merged):
        nd = normalize_drivetrain_from_fields(drv_merged, title=c.get("title"), trim=c.get("trim"))
        if nd:
            out["drivetrain"] = nd

    cyl_now = c.get("cylinders")
    try:
        cyl_i = int(cyl_now) if cyl_now is not None and str(cyl_now).strip() != "" else None
    except (TypeError, ValueError):
        cyl_i = None

    extracted = extract_cylinder_count(
        raw.get("engine_description") or c.get("engine_description"),
        raw.get("cylinders"),
        fuel_type=c.get("fuel_type"),
    )
    if extracted is not None:
        if cyl_i is None:
            out["cylinders"] = extracted
        elif cyl_i == 0:
            ft = str(c.get("fuel_type") or "").strip().lower()
            if ft == "electric" and extracted > 0:
                pass
            else:
                out["cylinders"] = extracted

    eng = c.get("engine_description")
    if eng and str(eng).strip():
        ne = normalize_engine_description_storage(eng)
        if ne:
            out["engine_description"] = ne

    if is_effectively_empty(c.get("trim")):
        tr = infer_trim_from_name_title(
            raw.get("name"),
            c.get("title"),
            make=c.get("make"),
            model=c.get("model"),
        )
        if tr:
            out["trim"] = tr
    else:
        # Normalize trim abbreviations/truncations
        normalized = normalize_trim_text(c.get("trim"))
        if normalized and normalized != c.get("trim"):
            out["trim"] = normalized

    tt = infer_transmission_type_bucket({**c, "transmission_type": raw.get("transmission_type")})
    cur_tt = c.get("transmission_type")
    cur_ok = isinstance(cur_tt, str) and cur_tt.strip() in _VALID_TRANSMISSION_TYPES
    if tt and not cur_ok:
        out["transmission_type"] = tt

    # Normalize title to remove redundant door count patterns
    title = c.get("title")
    if title:
        normalized_title = normalize_title_text(title)
        if normalized_title and normalized_title != title:
            out["title"] = normalized_title

    return out
