"""Resolve buyer-facing engine lines from DICTIONARY EPA CSV rows."""

from __future__ import annotations

import re
from typing import Any

_FOUR_CYL_RE = re.compile(
    r"\b(?:four[\s-]?cyl(?:inder)?s?|4[\s-]?cyl(?:inder)?s?|inline[\s-]?4|i[\s-]?4)\b",
    re.IGNORECASE,
)
_SIX_CYL_V6_RE = re.compile(
    r"\b(?:v[\s-]?6|six[\s-]?cyl(?:inder)?s?)\b",
    re.IGNORECASE,
)
_INLINE_SIX_RE = re.compile(
    r"\b(?:i[\s-]?6|inline[\s-]?6|in[\s-]?line[\s-]?6)\b",
    re.IGNORECASE,
)
_EIGHT_CYL_V8_RE = re.compile(
    r"\b(?:v[\s-]?8|eight[\s-]?cyl(?:inder)?s?|8[\s-]?cyl(?:inder)?s?)\b",
    re.IGNORECASE,
)
_TFSI_RE = re.compile(r"\btfsi\b", re.IGNORECASE)
_DISP_LAYOUT_RE = re.compile(
    r"([\d.]+)\s*L\s+(I\d|V\d{1,2}|Flat-\d)",
    re.IGNORECASE,
)


def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _parse_float(val: Any) -> float | None:
    if val is None or str(val).strip() == "":
        return None
    try:
        v = float(val)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _parse_int(val: Any) -> int | None:
    if val is None or str(val).strip() == "":
        return None
    try:
        v = int(float(val))
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _layout_from_cylinders(
    cyl: int | None,
    make: str | None = None,
    engine_text: str | None = None,
) -> str:
    if cyl is None or cyl <= 0:
        return ""
    mk = (make or "").strip().lower()
    eng = (engine_text or "").upper()
    if cyl == 6 and mk == "bmw":
        return "I6"
    if cyl == 6 and re.search(r"\b(?:CUMMINS|DURAMAX|POWER\s+STROKE|POWERSTROKE)\b", eng):
        return "I6"
    if cyl == 6 and mk in ("ram", "dodge", "ford", "chevrolet", "chevy", "gmc"):
        if re.search(r"\b(?:CUMMINS|DURAMAX|POWER\s+STROKE|POWERSTROKE|DIESEL)\b", eng):
            return "I6"
    mapping = {3: "I3", 4: "I4", 5: "I5", 6: "V6", 8: "V8", 10: "V10", 12: "V12"}
    return mapping.get(cyl, f"{cyl}-cyl")


def _correct_parsed_layout(
    layout: str,
    *,
    make: str | None,
    cyl: int | None,
) -> str:
    mk = (make or "").strip().lower()
    if cyl == 6 and mk == "bmw" and layout.upper().startswith("V"):
        return "I6"
    return layout


def catalog_engine_fields(row: dict[str, Any]) -> dict[str, str]:
    """Buyer-facing engine catalog fields for a dictionary EPA row."""
    base = _format_epa_engine_base(row)
    ctx = _epa_row_car_context(row)
    display = format_epa_engine_display(row, ctx) if base else ""
    try:
        from backend.utils.forced_induction import classify_forced_induction_from_car_row

        fi = (row.get("forcedInduction") or "").strip()
        if not fi:
            fi = classify_forced_induction_from_car_row(ctx) or ""
    except Exception:
        fi = (row.get("forcedInduction") or "").strip()
    out: dict[str, str] = {}
    if display:
        out["engineDisplay"] = display
    if fi:
        out["forcedInduction"] = fi
    return out


def best_engine_row_for_car(car: dict[str, Any], epa_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Pick the best EPA engine row for a listing (trim match, then listing signals)."""
    if not epa_rows:
        return None
    from backend.dictionary.enrich_from_dictionary import _best_row

    try:
        from backend.utils.car_serialize import (
            _effective_cylinder_count,
            parse_engine_displacement_liters,
        )

        car_lit = parse_engine_displacement_liters(car)
        car_cyl = _effective_cylinder_count(car, {})
        picked = pick_epa_engine_row(epa_rows, car, car_lit=car_lit, car_cyl=car_cyl)
        if picked:
            return picked
    except Exception:
        pass
    return _best_row(epa_rows, str(car.get("trim") or ""))


def engine_updates_from_dictionary_row(
    car: dict[str, Any],
    row: dict[str, Any],
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Build inventory column updates from a matched dictionary EPA row."""
    from backend.dictionary.enrich_from_dictionary import _is_empty

    updates: dict[str, Any] = {}
    catalog = catalog_engine_fields(row)
    disp = catalog.get("engineDisplay") or _format_epa_engine_base(row)
    fi = catalog.get("forcedInduction") or ""

    for db_col, raw in (
        ("engine_l", row.get("displacement")),
        ("cylinders", row.get("cylinders")),
        ("engine_description", row.get("engineOptions")),
        ("forced_induction", fi),
    ):
        if raw is None or str(raw).strip() in ("", "0", "0.0"):
            continue
        if not overwrite and not _is_empty(car.get(db_col)):
            continue
        if db_col == "engine_l":
            try:
                val = float(raw)
                if val <= 0:
                    continue
                updates[db_col] = val
            except (TypeError, ValueError):
                continue
        elif db_col == "cylinders":
            try:
                val = int(float(raw))
                if val <= 0:
                    continue
                updates[db_col] = val
            except (TypeError, ValueError):
                continue
        else:
            updates[db_col] = str(raw).strip()

    if disp and (overwrite or _is_empty(car.get("engine_description"))):
        if _is_empty(updates.get("engine_description")) and _is_empty(row.get("engineOptions")):
            updates["engine_description"] = disp
    return updates


def enrich_car_engine_from_dictionary(
    car: dict[str, Any],
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Fill or correct engine columns from DICTIONARY EPA data."""
    year = car.get("year")
    make = (car.get("make") or "").strip()
    model = (car.get("model") or "").strip()
    if not year or not make or not model:
        return {}
    try:
        from backend.dictionary.enrich_from_dictionary import _load_epa_csv
    except ImportError:
        return {}
    epa_rows = _load_epa_csv(int(year), make, model)
    row = best_engine_row_for_car(car, epa_rows)
    if not row:
        return {}
    return engine_updates_from_dictionary_row(car, row, overwrite=overwrite)


def _format_epa_engine_base(row: dict[str, Any]) -> str:
    """Displacement + layout only (no turbo / supercharged suffix)."""
    make = row.get("Make")
    eng = (row.get("engineOptions") or "").strip()
    m = _DISP_LAYOUT_RE.search(eng)
    if m:
        try:
            lit = float(m.group(1))
            layout = _correct_parsed_layout(
                m.group(2).upper().replace(" ", ""),
                make=str(make or ""),
                cyl=_parse_int(row.get("cylinders")),
            )
            return f"{lit:.1f}L {layout}"
        except ValueError:
            pass
    layout_from_eng = ""
    try:
        from backend.utils.car_serialize import _layout_token_from_engine_text

        layout_from_eng = _layout_token_from_engine_text(eng)
    except ImportError:
        layout_from_eng = ""
    disp = _parse_float(row.get("displacement"))
    cyl = _parse_int(row.get("cylinders"))
    if disp is not None:
        layout = layout_from_eng or _layout_from_cylinders(cyl, str(make or ""), eng)
        if layout:
            return f"{disp:.1f}L {layout}"
        return f"{disp:.1f}L"
    return eng[:48] if eng else ""


def _epa_row_car_context(row: dict[str, Any], car: dict[str, Any] | None = None) -> dict[str, Any]:
    ctx = dict(car or {})
    ctx.setdefault("make", row.get("Make"))
    ctx.setdefault("model", row.get("Model"))
    ctx.setdefault("trim", row.get("Trim"))
    ctx.setdefault("year", row.get("Year"))
    ctx.setdefault("cylinders", row.get("cylinders"))
    ctx.setdefault("engine_l", row.get("displacement"))
    ctx.setdefault("engine_description", row.get("engineOptions"))
    ctx.setdefault("fuel_type", row.get("fuelType"))
    return ctx


def format_epa_engine_display(
    row: dict[str, Any],
    car: dict[str, Any] | None = None,
) -> str:
    """Format ``2.0L I4 Turbo`` from an EPA CSV row and make/model induction rules."""
    base = _format_epa_engine_base(row)
    if not base:
        return ""
    try:
        from backend.utils.forced_induction import apply_forced_induction_to_engine_display

        return apply_forced_induction_to_engine_display(base, _epa_row_car_context(row, car))
    except Exception:
        return base


def _listing_blob(car: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in ("title", "trim", "description", "engine_description"):
        raw = car.get(key)
        if isinstance(raw, str) and raw.strip():
            parts.append(raw.strip())
    return "\n".join(parts)


def displacement_layout_inconsistent(
    liters: float | None,
    layout: str,
    make: str | None,
) -> bool:
    """True when displacement and layout are unlikely together for this make."""
    if liters is None or liters <= 0 or not layout:
        return False
    mk = (make or "").strip().lower()
    lay = layout.upper()
    if lay == "I4" and liters >= 2.5:
        if mk == "audi":
            return liters > 2.1
        if mk in ("bmw", "mercedes-benz", "mercedes", "porsche"):
            return liters > 3.1
        return liters > 2.5
    if lay == "V6" and liters <= 2.3:
        if mk == "audi":
            return True
        return liters < 2.0
    if lay == "V6" and liters >= 5.0:
        return True
    return False


def _score_epa_engine_row(
    row: dict[str, Any],
    car: dict[str, Any],
    *,
    combo: str,
    car_lit: float | None,
    car_cyl: int | None,
) -> int:
    score = 0
    disp = _parse_float(row.get("displacement"))
    cyl = _parse_int(row.get("cylinders"))
    eng = (row.get("engineOptions") or "").lower()
    blob = combo.lower()

    if car_lit is not None and disp is not None and abs(car_lit - disp) < 0.15:
        score += 2
    if car_cyl is not None and cyl is not None and car_cyl == cyl:
        score += 3

    if cyl == 4 and _FOUR_CYL_RE.search(blob):
        score += 6
    if cyl == 6 and _INLINE_SIX_RE.search(blob):
        score += 8
    if cyl == 6 and _SIX_CYL_V6_RE.search(blob):
        score += 6
    if cyl == 8 and _EIGHT_CYL_V8_RE.search(blob):
        score += 6

    if disp is not None:
        if 1.9 <= disp <= 2.1 and re.search(r"\b2\.0\s*l\b", blob):
            score += 4
        if 2.9 <= disp <= 3.1 and re.search(r"\b3\.0\s*l\b|\bv6\b", blob):
            score += 4

    if cyl == 4 and _TFSI_RE.search(blob) and disp is not None and disp <= 2.1:
        score += 3
    if "i4" in eng and _FOUR_CYL_RE.search(blob):
        score += 2
    if "v6" in eng and _SIX_CYL_V6_RE.search(blob):
        score += 2

    trim = _normalize(str(car.get("trim") or ""))
    epa_trim = _normalize(str(row.get("Trim") or ""))
    if trim and epa_trim and (trim in epa_trim or epa_trim in trim):
        score += 2

    dt = _normalize(str(car.get("drivetrain") or ""))
    epa_dt = _normalize(str(row.get("drivetrainOptions") or ""))
    if dt and epa_dt:
        if ("awd" in dt or "4wd" in dt or "quattro" in blob) and "all-wheel" in epa_dt:
            score += 1
        if "fwd" in dt and "front-wheel" in epa_dt:
            score += 1

    return score


def _distinct_epa_engine_rows(epa_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[float | None, int | None]] = set()
    out: list[dict[str, Any]] = []
    for row in epa_rows:
        key = (_parse_float(row.get("displacement")), _parse_int(row.get("cylinders")))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def pick_epa_engine_row(
    epa_rows: list[dict[str, Any]],
    car: dict[str, Any],
    *,
    car_lit: float | None = None,
    car_cyl: int | None = None,
) -> dict[str, Any] | None:
    """
    Pick the best EPA engine row when a YMM has multiple engine options.

    Returns ``None`` when there is only one engine choice or confidence is low.
    """
    distinct = _distinct_epa_engine_rows(epa_rows)
    if len(distinct) <= 1:
        return None

    combo = _listing_blob(car)
    scored = [
        (_score_epa_engine_row(row, car, combo=combo, car_lit=car_lit, car_cyl=car_cyl), row)
        for row in distinct
    ]
    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_row = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else 0
    if best_score >= 4 and best_score - second_score >= 2:
        return best_row
    if best_score >= 6:
        return best_row
    return None


def resolve_engine_display_from_epa(
    car: dict[str, Any],
    verified_specs: dict[str, Any] | None = None,
) -> str | None:
    """
    Return a corrected engine display (e.g. ``2.0L I4``) from EPA data when listing
    signals or stored displacement/cylinder data conflict.
    """
    year = car.get("year")
    make = (car.get("make") or "").strip()
    model = (car.get("model") or "").strip()
    if not year or not make or not model:
        return None

    try:
        from backend.dictionary.enrich_from_dictionary import _load_epa_csv
        from backend.utils.car_serialize import (
            _effective_cylinder_count,
            _cylinder_layout_token,
            parse_engine_displacement_liters,
        )
    except ImportError:
        return None

    epa_rows = _load_epa_csv(int(year), make, model)
    if not epa_rows:
        return None

    vs = verified_specs or {}
    car_lit = parse_engine_displacement_liters(car)
    car_cyl = _effective_cylinder_count(car, vs)
    naive_layout = _cylinder_layout_token(
        car_cyl,
        car.get("engine_description"),
        vs.get("master_engine_string"),
        vs.get("epa_engine_description"),
        car=car,
    )
    naive_display = ""
    if car_lit is not None and car_lit > 0 and naive_layout:
        naive_display = f"{car_lit:.1f}L {naive_layout}"
    inconsistent = displacement_layout_inconsistent(car_lit, naive_layout, make)

    picked = pick_epa_engine_row(epa_rows, car, car_lit=car_lit, car_cyl=car_cyl)
    if picked is None:
        if not inconsistent:
            return None
        picked = pick_epa_engine_row(
            epa_rows,
            car,
            car_lit=None,
            car_cyl=car_cyl,
        )
        if picked is None:
            combo = _listing_blob(car)
            if car_cyl == 4 and _FOUR_CYL_RE.search(combo):
                for row in _distinct_epa_engine_rows(epa_rows):
                    if _parse_int(row.get("cylinders")) == 4:
                        picked = row
                        break
            elif car_cyl == 6 and _INLINE_SIX_RE.search(combo):
                for row in _distinct_epa_engine_rows(epa_rows):
                    eng = (row.get("engineOptions") or "").upper()
                    if _parse_int(row.get("cylinders")) == 6 and (
                        "I6" in eng or "INLINE" in eng or "CUMMINS" in eng
                    ):
                        picked = row
                        break
            elif car_cyl == 6 and _SIX_CYL_V6_RE.search(combo):
                for row in _distinct_epa_engine_rows(epa_rows):
                    if _parse_int(row.get("cylinders")) == 6:
                        picked = row
                        break

    if picked is None:
        return None
    epa_base = _format_epa_engine_base(picked)
    epa_display = format_epa_engine_display(picked, car)
    if not epa_display:
        return None
    if not inconsistent and epa_base == naive_display:
        return None
    if inconsistent or epa_base != naive_display:
        return epa_display
    return None


def engine_fields_need_correction(
    car: dict[str, Any],
    epa_row: dict[str, Any],
    *,
    car_lit: float | None = None,
    car_cyl: int | None = None,
) -> bool:
    """True when stored engine fields disagree with a high-confidence EPA engine row."""
    try:
        from backend.utils.car_serialize import (
            _cylinder_layout_token,
            parse_engine_displacement_liters,
        )
    except ImportError:
        return False

    if car_lit is None:
        car_lit = parse_engine_displacement_liters(car)
    if car_cyl is None:
        try:
            from backend.utils.car_serialize import _effective_cylinder_count

            car_cyl = _effective_cylinder_count(car, {})
        except ImportError:
            car_cyl = None

    epa_lit = _parse_float(epa_row.get("displacement"))
    epa_cyl = _parse_int(epa_row.get("cylinders"))
    layout = _cylinder_layout_token(
        car_cyl,
        car.get("engine_description"),
        car=car,
    )
    if displacement_layout_inconsistent(car_lit, layout, car.get("make")):
        return True
    if epa_lit is not None and car_lit is not None and abs(car_lit - epa_lit) >= 0.15:
        if epa_cyl is not None and car_cyl is not None and car_cyl == epa_cyl:
            return True
    if epa_cyl is not None and car_cyl is not None and car_cyl != epa_cyl:
        combo = _listing_blob(car)
        if _FOUR_CYL_RE.search(combo) or _SIX_CYL_V6_RE.search(combo):
            return True
    return False
