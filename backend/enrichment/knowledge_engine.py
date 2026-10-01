"""
Automotive Knowledge Engine: EPA master data + regex trim decoder.
Fills cylinders, gears, drivetrain when dealer data is missing or generic.
"""
from __future__ import annotations

import csv
import os
import re
from functools import lru_cache
from typing import Any

from backend.db.inventory_db import get_conn
from backend.vehicle_facts import extended_specs as _xspecs
from backend.vehicle_facts.drivetrain import normalize_drivetrain
from backend.vehicle_facts.electrification import LEGACY_CODE, vpic_electrification


def _conn():
    return get_conn()


@lru_cache(maxsize=8192)
def catalog_model_is_trimless(year: int, make: str, model: str) -> bool:
    """
    True when the catalog shows this (year, make, model) has no meaningful trim
    variants — a standalone model (BMW XM/M6/i8, base F-250) whose only
    ``epa_master`` trim is the model name or where there is a single trim. Such
    a car with a blank dealer trim is NOT missing data; the model has no trim to
    capture. Returns False when the model isn't in the catalog (can't tell).
    """
    if not year or not make or not model:
        return False
    conn = None
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT trim FROM epa_master "
            "WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?)",
            (int(year), str(make).strip(), str(model).strip()),
        )
        trims = [(r[0] or "").strip() for r in cur.fetchall()]
    except Exception:
        return False
    finally:
        if conn is not None:
            conn.close()
    trims = [t for t in trims if t]
    if not trims:
        return False
    # Standalone signal: every catalog trim IS the model name (BMW XM→"XM",
    # i8→"i8", Mirai→"Mirai"). A model with a distinctly-named single trim
    # (Land Cruiser→"1958") is NOT trimless — a blank there is a real gap.
    mnorm = "".join(ch for ch in str(model).lower() if ch.isalnum())
    return all("".join(ch for ch in t.lower() if ch.isalnum()) == mnorm for t in trims)


def _bmw_has_30i_suffix(blob: str) -> bool:
    """
    330i / 530i / xDrive30i / sDrive30i — ``\\d30I`` matches three-series sedans;
    SAV trims glue ``30I`` after xDrive/sDrive (``E30I`` not ``\\d30I``).
    Also handles new G-body naming: "30 xDrive" / "30 sDrive" (space before drive suffix).
    """
    u = (blob or "").upper()
    if re.search(r"\d30I\b", u):
        return True
    if re.search(r"(?:XDRIVE|SDRIVE)30I\b", u):
        return True
    # G-body: "30 xDrive" / "30 sDrive" (e.g. 2026 X3 30 xDrive)
    return bool(re.search(r"\b30\s+(?:XDRIVE|SDRIVE)\b", u))


def _bmw_has_40i_suffix(blob: str) -> bool:
    """
    40i motors: 740i (740I), M340i (M340I), xDrive40i (E40I in XDRIVE40I).
    \\b40I\\b fails when 40i is glued to xDrive (E and 4 are both \\w).
    Also handles G-body naming: "M50 xDrive" (X3 M50), "40 xDrive" patterns.
    """
    u = (blob or "").upper()
    if re.search(r"\d40I\b", u):
        return True
    if re.search(r"\D40I\b", u):
        return True
    # G-body: "M50 xDrive" → treat as 40i-class (inline-6 turbo SAV)
    if re.search(r"\bM50\s+(?:XDRIVE|SDRIVE)\b", u):
        return True
    # G-body: "40 xDrive" (e.g. future naming)
    return bool(re.search(r"\b40\s+(?:XDRIVE|SDRIVE)\b", u))


def _bmw_model_is_x5_x7_or_5_7_series(model: str) -> bool:
    """True for X5, X7, 5 Series, 7 Series (incl. short '5' / '7' model names)."""
    m = (model or "").strip().upper()
    if re.search(r"\bX5\b", m):
        return True
    if re.search(r"\bX7\b", m):
        return True
    if re.search(r"\b5\s+SERIES\b", m) or m in ("5", "5 SERIES"):
        return True
    if re.search(r"\b7\s+SERIES\b", m) or m in ("7", "7 SERIES"):
        return True
    return False


def _bmw_model_is_sav_x3_x7(model: str) -> bool:
    """X3–X7 Sports Activity Vehicles (40i mild-hybrid I6 same family as larger SAVs)."""
    m = (model or "").strip().upper()
    return bool(re.match(r"^X[3-7]\b", m))


def _bmw_body_style_hint(model: str | None, blob_upper: str) -> str | None:
    """Best-effort body style from BMW model token + title (when inventory omits body_style)."""
    mu = (model or "").strip().upper()
    if not mu:
        return None
    if mu.startswith("X") and re.match(r"^X\d", mu):
        return "SUV"
    if mu.startswith("Z") and re.match(r"^Z\d", mu):
        return "Roadster"
    if "GRAN COUPE" in blob_upper or "GRANCOUPE" in blob_upper.replace(" ", ""):
        return "Gran Coupe"
    if re.match(r"^M\d{3}I\b", mu):
        return "Sedan"
    if re.match(r"^M[234]\b", mu) and "GRAN" not in blob_upper:
        return "Coupe"
    if re.match(r"^2\d{2}", mu):
        return "Coupe"
    if re.match(r"^4\d{2}", mu):
        return "Coupe"
    if re.match(r"^[3567]\d{2}[EI]", mu):
        return "Sedan"
    return None


def _apply_bmw_gas_fallbacks(out: dict[str, Any], model: str | None, blob_upper: str) -> None:
    """
    Non-EV BMW: default RWD for sedan/coupe motor codes without xDrive; typical automatic;
    body style when missing.
    """
    if out.get("cylinders") == 0 or (out.get("fuel_type_hint") or "").lower() == "electric":
        return
    mo_u = (model or "").strip().upper()
    if out.get("drivetrain") is None and not mo_u.startswith("X") and not mo_u.startswith("IX"):
        if re.search(r"(?:XDRIVE|4MATIC)\d", blob_upper) or re.search(
            r"\b(XDRIVE|4MATIC)\b", blob_upper
        ):
            out["drivetrain"] = "AWD"
        elif (
            re.search(
                r"\b(230I|330I|430I|530I|630I|730I|230E|330E|430E|530E|630E|M240I|M340I|M440I|540I|640I|740I|840I)\b",
                blob_upper,
            )
            or _bmw_has_30i_suffix(blob_upper)
            or _bmw_has_40i_suffix(blob_upper)
            or re.match(r"^M\d{3}[EI]\b", mo_u)
        ):
            out["drivetrain"] = "RWD"
    if not out.get("body_style_hint"):
        bsh = _bmw_body_style_hint(model, blob_upper)
        if bsh:
            out["body_style_hint"] = bsh
    if out.get("cylinders") not in (None, 0) and not out.get("transmission_hint"):
        out["transmission_hint"] = "8-Speed Automatic"


def decode_trim_logic(
    make: str | None,
    model: str | None,
    trim: str | None,
    title: str | None,
) -> dict[str, Any]:
    """
    Regex-based spec hints from brand naming (BMW, Mercedes-Benz, etc.).
    Returns cylinders, gears, drivetrain, optional fuel_type_hint (e.g. Mild Hybrid / Gas for 40i).
    """
    make = (make or "").strip()
    model = (model or "").strip()
    trim = (trim or "").strip()
    title = (title or "").strip()
    # Title + trim first for drivetrain (per product spec); model included for model-specific rules
    blob = f"{title} {trim} {model}".upper()
    trim_title = f"{trim} {title}".upper()
    make_u = make.upper()

    out: dict[str, Any] = {
        "cylinders": None,
        "gears": None,
        "drivetrain": None,
        "fuel_type_hint": None,
        "body_style_hint": None,
        "transmission_hint": None,
    }

    # xDrive / 4MATIC in title or trim → AWD (handles "xDrive40i" where \bXDRIVE\b fails: E+4 are both \w)
    if re.search(r"\b(XDRIVE|4MATIC)\b", trim_title) or re.search(
        r"(?:XDRIVE|4MATIC)\d", trim_title
    ):
        out["drivetrain"] = "AWD"
    elif re.search(r"\b(XDRIVE|4MATIC)\b", blob) or re.search(
        r"(?:XDRIVE|4MATIC)\d", blob
    ):
        out["drivetrain"] = "AWD"
    elif re.search(r"\b(QUATTRO)\b", trim_title) or re.search(r"\b(QUATTRO)\b", blob):
        out["drivetrain"] = "AWD"
    elif re.search(r"\bALL[\s-]?WHEEL\b", blob) or re.search(r"\bALL\s+WHEEL\b", blob):
        out["drivetrain"] = "AWD"
    elif re.search(
        r"\b(AWD|4WD|4X4|SH-AWD|INTELLIGENT AWD)\b",
        blob,
    ):
        out["drivetrain"] = "AWD"
    elif re.search(r"\bSDRIVE\d", trim_title) or re.search(
        r"\b(?:SDRIVE)(?:30|40|50)I\b", blob
    ):
        # Rear-biased sDrive (common X1/X2/X3 / Z4 markets)
        out["drivetrain"] = "RWD"

    # Gears from "8-Speed", "9-Speed Automatic", etc.
    m_gear = re.search(r"\b(\d{1,2})\s*[-]?\s*(SPEED|SPD)\b", blob, re.I)
    if m_gear:
        try:
            out["gears"] = int(m_gear.group(1))
        except ValueError:
            pass

    # --- Dodge / Stellantis BEV nameplates ---
    if make_u == "DODGE" and "DAYTONA" in blob and "CHARGER" in blob:
        out["cylinders"] = 0
        out["fuel_type_hint"] = "Electric"
        if out.get("drivetrain") is None and re.search(r"\bAWD\b", blob):
            out["drivetrain"] = "AWD"
        if out.get("body_style_hint") is None and re.search(r"\b2\s*DOOR\b", blob):
            out["body_style_hint"] = "Coupe"

    # --- BMW (order: BEV → M50i/M60i / 50i / 60i → 40i/M40i → 30i) ---
    if "BMW" in make_u or make_u == "MINI":
        # BEV: i4 / i5 / i7 / iX — 0 cylinders, electric (word boundaries avoid matching '40i')
        if re.search(r"\b(I4|I5|I7|IX)\b", blob) or re.search(
            r"\bI\s*PERFORMANCE\b|\bELECTRIC\b", blob
        ):
            out["cylinders"] = 0
            out["fuel_type_hint"] = "Electric"
            # Listing/VDP often omit drive — infer BMW BEV (eDrive = RWD, xDrive / M50 = AWD)
            if out.get("drivetrain") is None:
                if re.search(r"\bXDRIVE\b", blob) or re.search(r"\bM50\b", blob) or re.search(
                    r"\bM60\b", blob
                ):
                    out["drivetrain"] = "AWD"
                elif re.search(r"\bEDRIVE\d", blob) or re.search(r"\bEDRIVE\b", blob):
                    out["drivetrain"] = "RWD"
            # Body style when inventory row has no body_style (common on CPO EV)
            if re.search(r"\bI4\b", blob):
                out["body_style_hint"] = "Gran Coupe"
            elif re.search(r"\bI7\b", blob):
                out["body_style_hint"] = "Sedan"
            elif re.search(r"\bI5\b", blob):
                out["body_style_hint"] = "Sedan"
            elif re.search(r"\bIX\b", blob):
                out["body_style_hint"] = "SUV"
        # M50i / M60i → V8
        elif re.search(r"\b(M50I|M60I)\b", blob):
            out["cylinders"] = 8
        # 760i, etc. (60i V12 / V8 naming — rule: 8 cyl per product spec)
        elif re.search(r"\d60I\b", blob) and not re.search(r"\bM60I\b", blob):
            out["cylinders"] = 8
        # 550i, 750i, Alpina B7 (50i) — not M50i/M60i
        elif re.search(r"\d50I\b", blob) and not re.search(r"\b(M50I|M60I)\b", blob):
            out["cylinders"] = 8
        # X3–X7 SAV + 40i / M40i or 5/7 Series + 40i (mild-hybrid I6)
        elif (
            (_bmw_model_is_sav_x3_x7(model) or _bmw_model_is_x5_x7_or_5_7_series(model))
            and (_bmw_has_40i_suffix(blob) or re.search(r"\bM40I\b", blob))
        ):
            out["cylinders"] = 6
            out["fuel_type_hint"] = "Gas / Mild Hybrid"
        # PHEV: 330e / 530e / 630e — turbo I-4 + motor (same I4 core as 30i)
        elif re.search(r"\b(230E|330E|430E|530E|630E)\b", blob):
            out["cylinders"] = 4
            out["fuel_type_hint"] = "Plug-In Hybrid"
        # 40e SAV PHEV: xDrive40e / sDrive40e (X5 40e etc.) — 4-cyl + electric
        elif re.search(r"(?:XDRIVE|SDRIVE)40E\b", blob):
            out["cylinders"] = 4
            out["fuel_type_hint"] = "Plug-In Hybrid"
        # 30i: 330i, 430i, 530i, xDrive30i — 4 cyl
        elif _bmw_has_30i_suffix(blob) or re.search(
            r"\b(230I|330I|430I|530I|630I|730I)\b", blob
        ):
            out["cylinders"] = 4
        # 35i suffix (older inline-6 turbo: 335i, 435i, 535i, 135i) — 6 cyl
        elif re.search(r"\d35I\b", blob):
            out["cylinders"] = 6
        # Remaining 40i / M40i: 540i, 740i, M340i, xDrive40i — 6 cyl + mild hybrid
        elif _bmw_has_40i_suffix(blob) or re.search(r"\bM40I\b", blob):
            out["cylinders"] = 6
            out["fuel_type_hint"] = "Gas / Mild Hybrid"

        _apply_bmw_gas_fallbacks(out, model, blob)

    # --- Mazda (Skyactiv — transmission/body; drive only when AWD/FWD explicit in blob)
    if make_u == "MAZDA":
        mu = (model or "").strip().upper()
        if re.match(r"^CX-\d", mu):
            out["body_style_hint"] = "SUV"
        elif re.match(r"^MAZDA\s*3\b", mu) or mu in ("MAZDA3", "3"):
            out["body_style_hint"] = (
                "Hatchback" if re.search(r"HATCH|HATCHBACK", blob) else "Sedan"
            )
        elif re.match(r"^MX-\d", mu):
            out["body_style_hint"] = "Convertible"
        if re.search(r"\b(AWD|I-ACTIV\s*AWD)\b", blob, re.I):
            out["drivetrain"] = "AWD"
        elif re.search(r"\bFWD\b", blob):
            out["drivetrain"] = "FWD"
        if re.search(r"\b(MANUAL|6MT|6-SPEED\s*MANUAL)\b", blob):
            out["transmission_hint"] = "Manual"
        elif out.get("transmission_hint") is None:
            out["transmission_hint"] = "Automatic"

    # --- Mercedes-Benz ---
    if "MERCEDES" in make_u:
        # S580, GLS 580, AMG 63, etc.
        if re.search(r"\b(580|600|63\s|AMG\s*63)\b", blob):
            out["cylinders"] = 8
        # E450, GLC 450, CLE 450 — typical 6 cyl
        elif re.search(r"\b450\b", blob):
            out["cylinders"] = 6
        # C300, GLC 300, E 350 — common 4-cyl turbo trims (rule-based override when dealer omits)
        elif re.search(r"\b300\b", blob) or re.search(r"\b350\b", blob):
            out["cylinders"] = 4

    # --- Jeep: Wrangler / Wrangler Unlimited are SUVs (not convertibles) ---
    if make_u in ("JEEP", "WRANGLER") or re.search(r"\bWRANGLER\b", blob):
        from backend.utils.field_clean import is_jeep_wrangler_car

        if is_jeep_wrangler_car(make, model, trim, title):
            out["body_style_hint"] = "SUV"

    # --- Ford (body_style_hint when not set by rules above) ---
    if make_u == "FORD" and not out.get("body_style_hint"):
        m = (model or "").strip().upper()
        m_alnum = re.sub(r"[^A-Z0-9]", "", m)
        truck = bool(
            re.search(r"\bF[-\s]?(150|250|350|450|550|650)\b", m)
            or re.match(r"^F(150|250|350|450|550|650)\b", m_alnum)
            or m_alnum.startswith("F150")
            or m_alnum.startswith("F250")
            or m_alnum.startswith("F350")
            or m_alnum == "RANGER"
            or re.search(r"\bRANGER\b", m)
        )
        suv = bool(
            re.search(
                r"\b(EXPLORER|ESCAPE|EDGE|BRONCO|EXPEDITION|EXCURSION)\b",
                m,
            )
        )
        if truck:
            out["body_style_hint"] = "Truck"
        elif suv:
            out["body_style_hint"] = "SUV"

    # Last-resort: V-config cylinder count from trim string ("SV V6", "SLT V8", etc.)
    # Use trim only (not full title) to avoid false positives from body copy.
    trim_u_check = (trim or "").upper()
    if out.get("cylinders") is None:
        m_vc = re.search(r"\bV(\d{1,2})\b", trim_u_check)
        if m_vc:
            try:
                n = int(m_vc.group(1))
                if n in (4, 6, 8, 10, 12, 16):
                    out["cylinders"] = n
            except (TypeError, ValueError):
                pass

    # Title-driven transmission (Ford/GM marketing copy; vPIC often omits TransmissionStyle for trucks).
    if out.get("transmission_hint") is None and re.search(
        r"\bTEN[\s-]*SPEED\s+AUTOMATIC\b", blob, re.I
    ):
        out["transmission_hint"] = "10-Speed Automatic"
        if out.get("gears") is None:
            out["gears"] = 10
    if out.get("transmission_hint") is None and out.get("gears") and re.search(
        r"\b(AUTOMATIC|AUTO\.?\s*TRANS)\b", blob, re.I
    ):
        out["transmission_hint"] = f"{out['gears']}-Speed Automatic"
    if out.get("transmission_hint") is None and re.search(r"\bCVT\b", blob, re.I):
        out["transmission_hint"] = "CVT"

    # Ford/Ram HD truck transmission — vPIC & EPA don't cover these commercial vehicles.
    if out.get("transmission_hint") is None and make_u == "FORD":
        mo_u = model.upper()
        try:
            yr = int(title[:4]) if title and title[:4].isdigit() else None
        except (TypeError, ValueError):
            yr = None
        # F-250 / F-350 / F-450 / F-550 / Super Duty
        _is_sd = bool(
            re.match(r"F-?[2345]\d{2}\b", mo_u)
            or re.match(r"SUPER\s+DUTY", mo_u)
            # blob match only for models that start with F (not E-series)
            or (re.search(r"\bSUPER\s+DUTY\b", blob) and re.match(r"F", mo_u))
        )
        if _is_sd:
            if yr is not None and yr >= 2020:
                out["transmission_hint"] = "10-Speed Automatic"
                out["gears"] = 10
            else:
                out["transmission_hint"] = "6-Speed Automatic"
                out["gears"] = 6
        # Transit van (not Transit Connect)
        elif re.match(r"TRANSIT\b", mo_u) and "CONNECT" not in mo_u:
            if yr is not None and yr >= 2020:
                out["transmission_hint"] = "10-Speed Automatic"
                out["gears"] = 10
            else:
                out["transmission_hint"] = "6-Speed Automatic"
                out["gears"] = 6
        # E-Series (E-350, E-350 Super Duty, etc.)
        elif re.match(r"E-?[34]\d{2}\b", mo_u) or re.match(r"E\s*[34]\d{2}", mo_u):
            out["transmission_hint"] = "6-Speed Automatic"
            out["gears"] = 6

    if out.get("transmission_hint") is None and make_u == "RAM":
        mo_u = model.upper()
        # Ram 2500 / 3500 / 4500 / 5500
        if re.match(r"[2345]\d{3}\b", mo_u) or re.search(r"\b[2345]\d{3}\b", mo_u):
            # Diesel (Cummins): Aisin 6-speed; Gas (HEMI 6.4L): 8-speed Torqueflite
            if re.search(r"\bDIESEL\b|\b6\.7\b|CUMMINS", blob, re.I):
                out["transmission_hint"] = "6-Speed Automatic"
                out["gears"] = 6
            else:
                out["transmission_hint"] = "8-Speed Automatic"
                out["gears"] = 8

    return out


def _norm_drive_epa(d: str | None) -> str | None:
    """EPA ``drive`` -> FWD/RWD/AWD/4WD (``vehicle_facts.normalize_drivetrain``).

    "2-Wheel Drive" is None (unknown end); "4-Wheel Drive" / "Four-Wheel Drive"
    are 4WD (they used to leak through raw and part-time 4WD vs plain 4WD split).
    """
    if not d:
        return None
    return normalize_drivetrain(d, "epa")


def _gears_from_trany(trany: str | None) -> int | None:
    if not trany:
        return None
    m = re.search(r"(\d{1,2})\s*[-]?\s*(speed|spd|sp)", trany, re.I)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    m2 = re.search(r"[Ss](\d{1,2})\b", trany)
    if m2:
        try:
            return int(m2.group(1))
        except ValueError:
            return None
    return None


def _epa_trim_lookup_key(
    year: int | None,
    make: str | None,
    model: str | None,
    trim: str | None,
) -> tuple[int, str, str, str] | None:
    if not year or not make or not model or not trim:
        return None
    try:
        y = int(year)
    except (TypeError, ValueError):
        return None
    mk = (make or "").strip()
    md = (model or "").strip()
    tr = (trim or "").strip()
    if not y or not mk or not md or not tr:
        return None
    return y, mk, md, tr


@lru_cache(maxsize=16384)
def _lookup_epa_by_trim_cached(key: tuple[int, str, str, str]) -> frozenset[tuple[str, Any]]:
    year, make, model, trim_clean = key
    result = _lookup_epa_by_trim_uncached(year, make, model, trim_clean)
    return frozenset(result.items())


def clear_epa_trim_lookup_cache() -> None:
    _lookup_epa_by_trim_cached.cache_clear()


def lookup_epa_by_trim(
    year: int | None,
    make: str | None,
    model: str | None,
    trim: str | None,
) -> dict[str, Any]:
    """
    Exact per-trim row from epa_master (populated by build_epa_master.py).

    Returns the best-matching trim row, or an empty dict when no match found.
    Falls back to partial trim substring match when no exact match exists.
    """
    key = _epa_trim_lookup_key(year, make, model, trim)
    if key is None:
        return {}
    return dict(_lookup_epa_by_trim_cached(key))


@lru_cache(maxsize=4096)
def _lookup_epa_master_by_id_cached(epa_master_id: int) -> frozenset:
    conn = None
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT cylinders, drive, trany, displacement,
                   city08, highway08, fuel_type, atv_type, body_style, engine_description
            FROM epa_master WHERE id=? LIMIT 1
            """,
            (int(epa_master_id),),
        )
        row = cur.fetchone()
        if row:
            return frozenset(_epa_row_to_dict(row).items())
    except Exception:
        pass
    finally:
        if conn is not None:
            conn.close()
    return frozenset()


def lookup_epa_master_by_id(epa_master_id: Any) -> dict[str, Any]:
    """
    Catalog row for a resolved ``cars.epa_master_id`` link — the authoritative
    per-trim source (same shape as :func:`lookup_epa_by_trim`, but no fuzzy
    matching; the resolver already picked the row).
    """
    try:
        mid = int(epa_master_id)
    except (TypeError, ValueError):
        return {}
    if mid <= 0:
        return {}
    return dict(_lookup_epa_master_by_id_cached(mid))


def _extended_family_suspicious(cur, year: int, make: str, model: str) -> bool:
    """
    True when EVERY trim of a (year, make, model) family scraped to the same
    horsepower — a scrape bug fingerprint (e.g. 2011 E-Class: E350, E550 and
    the 518-hp E63 all stored as 375). Such hp/torque must not be displayed, and
    as of 2026-08-02 nothing fills in behind them — the field goes blank.

    Reads through ``vehicle_facts.extended_specs`` (*cur* is accepted for the old
    signature and unused).
    """
    del cur
    return _xspecs.hp_family_suspicious(int(year), str(make or "").strip(), str(model or "").strip())


def _plausible_extended(row: dict[str, Any] | None, year: Any, make: Any, model: Any) -> dict[str, Any]:
    """Spec columns of an ``epa_extended_specs`` row with the knowledge-engine guards:
    out-of-band hp/torque dropped, same-hp-for-every-trim families dropped."""
    if not row:
        return {}
    result = {k: row[k] for k in _EXTENDED_SPECS_COLUMNS if row.get(k) is not None}
    if result.get("horsepower") is not None and not (60 <= result["horsepower"] <= 1600):
        result.pop("horsepower")
    if result.get("torque_lb_ft") is not None and not (40 <= result["torque_lb_ft"] <= 1500):
        result.pop("torque_lb_ft")
    if (result.get("horsepower") is not None or result.get("torque_lb_ft") is not None) and year:
        if _extended_family_suspicious(None, int(year), str(make or ""), str(model or "")):
            result.pop("horsepower", None)
            result.pop("torque_lb_ft", None)
    return result


def _lookup_extended_by_master_id_cached(epa_master_id: int) -> frozenset:
    """Row via ``vehicle_facts.extended_specs`` (its cache), family check keyed on
    the ``epa_master`` row's (year, make, model) as before."""
    row = _xspecs.row_by_master_id(epa_master_id)
    if not row:
        return frozenset()
    ymm = _epa_master_ymm(int(epa_master_id))
    if ymm is None:
        result = _plausible_extended(row, None, None, None)
    else:
        result = _plausible_extended(row, *ymm)
    # NO ``ai_model_specs`` FALLBACK HERE. See the block comment above
    # ``_merge_ai_model_specs``: every row of that table is a number a model
    # wrote. A still-missing field stays missing.
    return frozenset(result.items())


_lookup_extended_by_master_id_cached.cache_clear = _xspecs.clear_cache  # type: ignore[attr-defined]


@lru_cache(maxsize=8192)
def _epa_master_ymm(epa_master_id: int) -> tuple[int, str, str] | None:
    conn = None
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("SELECT year, make, model, trim FROM epa_master WHERE id=?", (int(epa_master_id),))
        r = cur.fetchone()
        return (int(r[0]), str(r[1] or ""), str(r[2] or "")) if r else None
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


def lookup_epa_extended_specs_by_master_id(epa_master_id: Any) -> dict[str, Any]:
    """hp/tq/0-60 joined directly on the resolved catalog link (exact FK, no guessing)."""
    try:
        mid = int(epa_master_id)
    except (TypeError, ValueError):
        return {}
    if mid <= 0:
        return {}
    return dict(_lookup_extended_by_master_id_cached(mid))


def _lookup_epa_by_trim_uncached(
    year: int,
    make: str,
    model: str,
    trim_clean: str,
) -> dict[str, Any]:
    conn = None
    try:
        conn = _conn()
        cur = conn.cursor()
        # Exact trim match
        cur.execute(
            """
            SELECT cylinders, drive, trany, displacement,
                   city08, highway08, fuel_type, atv_type, body_style, engine_description
            FROM epa_master
            WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?) AND lower(trim)=lower(?)
            LIMIT 1
            """,
            (year, make.strip(), model.strip(), trim_clean),
        )
        row = cur.fetchone()
        if not row:
            # Substring match: trim starts with the EPA trim token or vice-versa
            cur.execute(
                """
                SELECT cylinders, drive, trany, displacement,
                       city08, highway08, fuel_type, atv_type, body_style, engine_description
                FROM epa_master
                WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?)
                  AND (lower(?) LIKE '%' || lower(trim) || '%'
                       OR lower(trim) LIKE '%' || lower(?) || '%')
                LIMIT 1
                """,
                (year, make.strip(), model.strip(), trim_clean, trim_clean),
            )
            row = cur.fetchone()
        # Try normalized model names (e.g. "Silverado 1500" → "Silverado")
        if not row:
            for fallback_model in epa_model_candidates(make, model, strategy="by_trim"):
                cur.execute(
                    """
                    SELECT cylinders, drive, trany, displacement,
                           city08, highway08, fuel_type, atv_type, body_style, engine_description
                    FROM epa_master
                    WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?) AND lower(trim)=lower(?)
                    LIMIT 1
                    """,
                    (year, make.strip(), fallback_model, trim_clean),
                )
                row = cur.fetchone()
                if not row:
                    cur.execute(
                        """
                        SELECT cylinders, drive, trany, displacement,
                               city08, highway08, fuel_type, atv_type, body_style, engine_description
                        FROM epa_master
                        WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?)
                          AND (lower(?) LIKE '%' || lower(trim) || '%'
                               OR lower(trim) LIKE '%' || lower(?) || '%')
                        LIMIT 1
                        """,
                        (year, make.strip(), fallback_model, trim_clean, trim_clean),
                    )
                    row = cur.fetchone()
                if row:
                    break
        if row:
            return _epa_row_to_dict(row)
    except Exception:
        pass
    finally:
        if conn is not None:
            conn.close()
    return _lookup_epa_from_dictionary_csv(year, make, model, trim_clean)


def _epa_row_to_dict(row: tuple) -> dict[str, Any]:
    out: dict[str, Any] = {}
    cyl, drive, trany, disp, c08, h08, fuel_type, atv_type, body_style, engine_desc = row
    if cyl is not None:
        out["cylinders"] = int(cyl)
    out["drivetrain"] = _norm_drive_epa(drive)
    out["transmission"] = trany
    out["gears"] = _gears_from_trany(trany)
    if disp is not None:
        out["displacement"] = float(disp)
    if c08 is not None and float(c08) > 0:
        out["city08"] = float(c08)
    if h08 is not None and float(h08) > 0:
        out["highway08"] = float(h08)
    if fuel_type:
        out["fuel_type"] = fuel_type
    if atv_type:
        out["atv_type"] = atv_type
    if body_style:
        out["body_style"] = body_style
    if engine_desc:
        out["engine_description"] = engine_desc
    return out


_EXTENDED_SPECS_COLUMNS = _xspecs.SPEC_COLUMNS


def _extended_specs_row_to_dict(row: tuple) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, val in zip(_EXTENDED_SPECS_COLUMNS, row):
        if val is not None:
            out[key] = val
    return out


def _lookup_epa_extended_specs_cached(key: tuple[int, str, str, str]) -> frozenset[tuple[str, Any]]:
    year, make, model, trim_clean = key
    result = _lookup_epa_extended_specs_uncached(year, make, model, trim_clean)
    return frozenset(result.items())


_lookup_epa_extended_specs_cached.cache_clear = _xspecs.clear_cache  # type: ignore[attr-defined]


def clear_epa_extended_specs_lookup_cache() -> None:
    _lookup_epa_extended_specs_cached.cache_clear()


def lookup_epa_extended_specs(
    year: int | None,
    make: str | None,
    model: str | None,
    trim: str | None,
) -> dict[str, Any]:
    """
    Horsepower/torque/curb weight/0-60/tow capacity/EV range from ``epa_extended_specs``
    (populated 2026-07-06 from an imported dump; see ``backend/db/inventory_pg.py``).

    Exact (year, make, model, trim) match first, falling back to any row for the same
    (year, make, model) when no trim-level match exists. Returns {} when nothing found.
    """
    key = _epa_trim_lookup_key(year, make, model, trim)
    if key is None:
        return {}
    return dict(_lookup_epa_extended_specs_cached(key))


def _lookup_epa_extended_specs_uncached(
    year: int,
    make: str,
    model: str,
    trim_clean: str,
) -> dict[str, Any]:
    """Exact (year, make, model, trim) row, else any row for (year, make, model),
    via ``vehicle_facts.extended_specs``; implausible / family-suspicious hp and
    torque dropped. Nothing fills in behind a dropped value (no ``ai_model_specs``
    fallback here either)."""
    row = _xspecs.row_by_ymmt(year, make, model, trim_clean)
    return _plausible_extended(row, year, make, model)


# ---------------------------------------------------------------------------
# THE AI SPEC TABLES ARE NOT WIRED INTO ANY READ PATH. (2026-08-02)
#
# ``ai_model_specs`` (2,147 rows) and ``ai_engine_specs`` (888 rows) each have
# exactly ONE distinct ``source_host`` — 'ai-research' and 'ai-engine-research'
# respectively (counted 2026-08-02 against the live inventory database). There
# is no subset of either table that came from a document. Every value in them is
# a number a model wrote, so there is no query that can separate a good row from
# a bad one, and a specification is a measurement.
#
# ``_merge_ai_model_specs`` and ``_merge_ai_model_specs_normed`` below used to be
# called at the end of ``_lookup_epa_extended_specs_uncached`` and
# ``_lookup_extended_by_master_id_cached``, filling every still-missing key of
# ``_AI_SPEC_COLUMNS`` — horsepower, torque, curb weight, 0-60 and tow capacity —
# which then flowed into ``merge_verified_specs`` and onto the car page and into
# the AI chat prompt. Both call sites are gone.
#
# The two functions and ``_AI_SPEC_COLUMNS`` are retained ONLY because
# ``backend/tests/test_ai_model_specs_merge.py`` exercises them directly and that
# file is not mine to edit. They have NO production caller — verified by
# ``test_spec_guardrails.py::test_ai_spec_merge_has_no_production_caller``, which
# greps the tree and fails if one reappears. Delete both functions and that test
# module together; do not re-wire them.
#
# ``lookup_engine_specs`` (``ai_engine_specs``) is in the same position: its last
# production caller, the serializer's engine-level hp/torque/tow/0-60 override,
# was removed the same day.
# ---------------------------------------------------------------------------

#: Columns ai_model_specs supplied (subset of _EXTENDED_SPECS_COLUMNS). Unwired.
_AI_SPEC_COLUMNS = (
    "horsepower", "torque_lb_ft", "torque_nm", "curb_weight_lb",
    "curb_weight_kg", "zero_to_60_sec", "fuel_tank_gal", "tow_capacity_lb",
)


#: Engine-specific columns (ai_engine_specs). hp/torque/tow/0-60 are engine-critical.
_ENGINE_SPEC_COLUMNS = (
    "horsepower", "torque_lb_ft", "tow_capacity_lb",
    "zero_to_60_sec", "fuel_tank_gal", "curb_weight_lb",
)


@lru_cache(maxsize=16384)
def _lookup_engine_specs_cached(key: tuple) -> frozenset:
    year, make, model, eng, cyl, fuel = key
    conn = None
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT {", ".join(_ENGINE_SPEC_COLUMNS)}
            FROM ai_engine_specs
            WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?)
              AND lower(btrim(engine_description))=lower(btrim(?))
              AND COALESCE(cylinders,-1)=COALESCE(?,-1)
              AND lower(btrim(fuel_type))=lower(btrim(?))
            LIMIT 1
            """,
            (year, make, model, eng, cyl, fuel),
        )
        row = cur.fetchone()
    except Exception:
        return frozenset()
    finally:
        if conn is not None:
            conn.close()
    if not row:
        return frozenset()
    return frozenset((k, v) for k, v in zip(_ENGINE_SPEC_COLUMNS, row) if v is not None)


def lookup_engine_specs(
    year: int | None, make: str | None, model: str | None,
    engine_description: str | None, cylinders: int | None, fuel_type: str | None,
) -> dict[str, Any]:
    """Engine-matched specs from ``ai_engine_specs`` (empty dict when no exact-engine row)."""
    if not year or not make or not model:
        return {}
    key = (int(year), str(make).strip(), str(model).strip(),
           str(engine_description or "").strip(),
           int(cylinders) if cylinders not in (None, "") else None,
           str(fuel_type or "").strip())
    return dict(_lookup_engine_specs_cached(key))


def clear_engine_specs_lookup_cache() -> None:
    _lookup_engine_specs_cached.cache_clear()


def _merge_ai_model_specs_normed(
    cur, year: int, make: str, name_targets: tuple[str, ...], result: dict[str, Any]
) -> dict[str, Any]:
    """
    Like :func:`_merge_ai_model_specs` but matches ai_model_specs.model against
    any of *name_targets* alphanumeric-normalized ("E350" == "E 350").
    """
    def _n(s: str) -> str:
        return "".join(ch for ch in s.lower() if ch.isalnum())

    targets = {_n(t) for t in name_targets if t}
    if not targets:
        return result
    try:
        cur.execute(
            f"""
            SELECT model, {", ".join(_AI_SPEC_COLUMNS)}
            FROM ai_model_specs
            WHERE year=? AND lower(make)=lower(?)
            """,
            (year, make.strip()),
        )
        rows = cur.fetchall()
    except Exception:
        return result
    for row in rows:
        if _n(str(row[0] or "")) in targets:
            for key, val in zip(_AI_SPEC_COLUMNS, row[1:]):
                if val is not None:
                    result.setdefault(key, val)
            break
    return result


def _merge_ai_model_specs(cur, year: int, make: str, model: str, result: dict[str, Any]) -> dict[str, Any]:
    """Fill still-missing fields from ai_model_specs; existing keys are never overwritten."""
    try:
        cur.execute(
            f"""
            SELECT {", ".join(_AI_SPEC_COLUMNS)}
            FROM ai_model_specs
            WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?)
            LIMIT 1
            """,
            (year, make.strip(), model.strip()),
        )
        ai_row = cur.fetchone()
    except Exception:
        return result  # table absent or query failed — real data still returned
    if ai_row:
        for key, val in zip(_AI_SPEC_COLUMNS, ai_row):
            if val is not None:
                result.setdefault(key, val)
    return result


def _lookup_epa_from_dictionary_csv(
    year: int | None,
    make: str | None,
    model: str | None,
    trim: str | None,
) -> dict[str, Any]:
    """Fallback when epa_master is empty or missing a row: read dictionary *_EPA.csv."""
    if not year or not make or not model:
        return {}
    from backend.enrichment.trim_ladder import _find_epa_csv

    path = _find_epa_csv(make, model, year)
    if not path:
        return {}
    trim_clean = (trim or "").strip()
    try:
        with path.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
    except OSError:
        return {}

    def _row_matches(row: dict[str, str]) -> bool:
        if not trim_clean:
            return True
        row_trim = (row.get("Trim") or "").strip()
        if not row_trim:
            return False
        tl = trim_clean.lower()
        rl = row_trim.lower()
        return tl == rl or tl in rl or rl in tl

    candidates = [r for r in rows if _row_matches(r)]
    if not candidates and trim_clean:
        return {}
    if not candidates:
        candidates = rows

    def _score(row: dict[str, str]) -> tuple[int, int]:
        try:
            row_year = int((row.get("Year") or "").strip())
        except (TypeError, ValueError):
            row_year = year or 0
        year_dist = abs(row_year - int(year))
        trim_exact = 0 if trim_clean and (row.get("Trim") or "").strip().lower() == trim_clean.lower() else 1
        return (trim_exact, year_dist)

    best = min(candidates, key=_score)
    try:
        city = float((best.get("mpg_city") or "").strip()) if (best.get("mpg_city") or "").strip() else None
    except (TypeError, ValueError):
        city = None
    try:
        hwy = float((best.get("mpg_highway") or "").strip()) if (best.get("mpg_highway") or "").strip() else None
    except (TypeError, ValueError):
        hwy = None

    out: dict[str, Any] = {}
    if city is not None and city > 0:
        out["city08"] = city
    if hwy is not None and hwy > 0:
        out["highway08"] = hwy
    try:
        cyl = int((best.get("cylinders") or "").strip()) if (best.get("cylinders") or "").strip() else None
        if cyl is not None:
            out["cylinders"] = cyl
    except (TypeError, ValueError):
        pass
    try:
        disp = float((best.get("displacement") or "").strip()) if (best.get("displacement") or "").strip() else None
        if disp is not None:
            out["displacement"] = disp
    except (TypeError, ValueError):
        pass
    drive = (best.get("drivetrainOptions") or "").strip()
    if drive:
        out["drivetrain"] = _norm_drive_epa(drive)
    trany = (best.get("transmissionOptions") or "").strip()
    if trany:
        out["transmission"] = trany
        out["gears"] = _gears_from_trany(trany)
    fuel = (best.get("fuelType") or "").strip()
    if fuel:
        out["fuel_type"] = fuel
    body = (best.get("bodyStyle") or "").strip()
    if body:
        out["body_style"] = body
    engine = (best.get("engineOptions") or best.get("engineDisplay") or "").strip()
    if engine:
        out["engine_description"] = engine
    return out


def format_transmission_display(trany: str | None) -> str | None:
    """
    EPA trany codes → readable labels, e.g. Auto(S8) → 8-Speed Automatic, Auto(AM-S7) → 7-Speed DCT.
    """
    if not trany:
        return None
    s = trany.strip()
    m = re.match(r"Auto(?:matic)?\s*\(\s*S(\d+)\s*\)", s, re.I)
    if m:
        return f"{int(m.group(1))}-Speed Automatic"
    m = re.match(r"Auto(?:matic)?\s*\(\s*AM-S(\d+)\s*\)", s, re.I)
    if m:
        return f"{int(m.group(1))}-Speed DCT"
    m = re.match(r"Auto(?:matic)?\s*\(\s*A(\d+)\s*\)", s, re.I)
    if m:
        return f"{int(m.group(1))}-Speed Automatic"
    m = re.match(r"Auto(?:matic)?\s*\(\s*CVT\s*\)", s, re.I)
    if m:
        return "CVT"
    if re.search(r"CVT", s, re.I) and "Auto" not in s:
        return "CVT"
    return s


def _transmission_has_gear_detail(trany: str | None) -> bool:
    """True if *trany* encodes a gear count (dealer phrase or EPA Auto(Sn) style after formatting)."""
    if not trany:
        return False
    s = str(trany).strip()
    if re.search(r"\b\d+[-\s]?speed\b", s, re.I):
        return True
    fmt = format_transmission_display(s)
    return bool(fmt and re.search(r"\b\d+[-\s]?speed\b", fmt, re.I))


def _fmt_liter(displ: float | None, default_liters: str) -> str:
    if displ is not None and displ > 0:
        return f"{displ:.1f}L"
    return f"{default_liters}L"


def _bmw_power_torque(blob: str, epa_displ: float | None) -> tuple[int | None, int | None, str]:
    """
    BMW HP / torque / engine description (mapping 'brain'); liters prefer EPA displacement.
    """
    u = blob.upper()
    # iX M60 (BEV performance)
    if re.search(r"\bIX\b", u) and re.search(r"\bM60\b", u):
        return 610, 811, "Dual Electric Motors"
    if re.search(r"\bM60I\b", u):
        return 523, 553, f"{_fmt_liter(epa_displ, '4.4')} V8 M TwinPower Turbo"
    if re.search(r"\bM50I\b", u):
        return 523, 553, f"{_fmt_liter(epa_displ, '4.4')} V8 TwinPower Turbo"
    if re.search(r"\b(230E|330E|430E|530E|630E)\b", u):
        lit = _fmt_liter(epa_displ, "2.0")
        return 288, 310, f"{lit} Inline-4 TwinPower Turbo plug-in hybrid"
    if _bmw_has_40i_suffix(u) or re.search(r"\bM40I\b", u):
        return 375, 398, f"{_fmt_liter(epa_displ, '3.0')} Inline-6 TwinPower Turbo"
    if _bmw_has_30i_suffix(u) or re.search(r"\b(230I|330I|430I|530I|630I|730I)\b", u):
        return 255, 295, f"{_fmt_liter(epa_displ, '2.0')} Inline-4 TwinPower Turbo"
    return None, None, ""


def _detect_phev_hybrid(blob: str, epa: dict[str, Any]) -> bool:
    u = blob.upper()
    atv = (epa.get("atv_type") or "").strip().upper()
    fuel = (epa.get("fuel_type") or "").upper()
    if atv in ("PHEV", "HYBRID"):
        return True
    if "PLUG" in fuel or "PHEV" in fuel:
        return True
    if re.search(r"\b(50E|45E|40E|30E)\b", u) or re.search(r"\b(230E|330E|430E|530E|630E)\b", u):
        return True
    return False


def build_master_engine_string(
    make: str | None,
    model: str | None,
    trim: str | None,
    title: str | None,
    regex: dict[str, Any],
    epa: dict[str, Any],
) -> str | None:
    """Monroney-style engine line: [Electric +] [liters] config, HP / TQ."""
    blob = f"{title or ''} {trim or ''} {model or ''}".strip()
    u = blob.upper()
    make_u = (make or "").strip().upper()
    epa_displ = epa.get("displacement")
    if isinstance(epa_displ, str):
        try:
            epa_displ = float(epa_displ)
        except (TypeError, ValueError):
            epa_displ = None

    prefix = ""
    if _detect_phev_hybrid(blob, epa):
        prefix = "Electric + "

    if "BMW" in make_u or make_u == "MINI":
        hp, tq, desc = _bmw_power_torque(u, epa_displ)
        if hp is not None and tq is not None and desc:
            if "plug-in" in desc.lower():
                return f"{desc}, {hp} HP / {tq} lb-ft"
            return f"{prefix}{desc}, {hp} HP / {tq} lb-ft"
        if regex.get("cylinders") == 0 or re.search(r"\b(I4|I5|I7|IX)\b", u):
            return "Electric motor(s) — output per manufacturer; see MPGe below."

    cyl = epa.get("cylinders")
    disp = epa_displ
    if disp is not None and float(disp) > 0:
        return f"{float(disp):.1f}L"
    return None


def format_fuel_economy_display(epa: dict[str, Any], is_bev: bool) -> str | None:
    c = epa.get("city08")
    h = epa.get("highway08")
    # BEV MPGe lives in city08 / highway08 in epa_master; city_e is kWh/100 mi.
    if is_bev and c is not None and h is not None and c > 0 and h > 0:
        return f"{round(c)} MPGe City / {round(h)} MPGe Hwy"
    if is_bev:
        return None
    if c is not None and h is not None and (c > 0 or h > 0):
        return f"{round(c)} City / {round(h)} Hwy"
    return None


def _drivetrain_ui_label(drive_display: str, make: str | None, blob: str) -> str:
    if not drive_display:
        return ""
    if drive_display.upper() != "AWD":
        return drive_display
    mu = (make or "").upper()
    bu = blob.upper()
    if "BMW" in mu or "MINI" in mu:
        if "XDRIVE" in bu:
            return "AWD (xDrive)"
        return "AWD"
    if "MERCEDES" in mu and "4MATIC" in bu:
        return "AWD (4MATIC)"
    if "QUATTRO" in bu:
        return "AWD (quattro)"
    return "AWD"


def _bmw_epa_model_like_pattern(model: str | None) -> str | None:
    """
    Dealer DB stores short model names ('X3', 'X5', 'X1') but EPA uses full trim strings
    like 'X3 xDrive30i'. Return a LIKE pattern so lookup_epa_aggregate can fall back to
    a fuzzy match when exact lookup returns nothing.
    I-series BEV/short (``i4``, ``i5``, ``i7``, ``iX``) map to epa model strings
    like ``i4 eDrive40 Gran Coupe ...``.
    """
    m = (model or "").strip().upper()
    if not m:
        return None
    # I3 / I4 / I5 / I7 / I8 / iX: dealer "i4" + trim "eDrive40" in lookup_epa_aggregate
    if " " not in m and re.match(r"^I[34578]\b", m):
        return f"{(model or '').strip()}%"
    if " " not in m and m.startswith("IX"):
        return f"{(model or '').strip()}%"
    # Only applies to short SAV/sedan codes without spaces (X3, X5, X1, 530e, M340, etc.)
    if " " not in m and re.match(r"^(X[1-9]|[0-9]|M[0-9]|Z[0-9])", m):
        return f"{model.strip()}%"
    return None


def _bmw_bev_epa_model_narrow_substring(title: str | None, trim: str | None) -> str | None:
    """
    For BMW BEV, EPA model strings include ``eDrive40``, ``M50``, etc.
    When title/trim name the variant, add ``AND lower(model) LIKE %token%`` to the SQL.
    """
    t = f"{title or ''} {trim or ''}"
    c = re.sub(r"[^a-z0-9]", "", t.lower())
    if re.search(r"e-?\s*drive\s*50|edrive50", t, re.I) or "edrive50" in c:
        return "edrive50"
    if re.search(r"e-?\s*drive\s*40|edrive40", t, re.I) or "edrive40" in c:
        return "edrive40"
    if re.search(r"e-?\s*drive\s*35|edrive35", t, re.I) or "edrive35" in c:
        return "edrive35"
    if re.search(r"e-?\s*drive\s*30|edrive30", t, re.I) or "edrive30" in c:
        return "edrive30"
    if re.search(r"(?<![0-9a-z/])m50(?![0-9a-z])", t, re.I):
        return "m50"
    if re.search(r"(?<![0-9a-z/])m60(?![0-9a-z])", t, re.I):
        return "m60"
    return None


def _ford_epa_pickup_like_pattern(make: str | None, model: str | None) -> str | None:
    """
    Dealer DMS often lists ``F-150`` / ``F150 XLT`` while EPA uses ``F150 Pickup 2WD`` / ``F150 Pickup 4WD``.
    Return a LIKE pattern (``F150 Pickup%``) or None.
    """
    mk = (make or "").strip().upper()
    if mk != "FORD":
        return None
    alnum = re.sub(r"[^A-Z0-9]", "", (model or "").upper())
    m = re.match(r"^F(150|250|350|450|550|650)", alnum)
    if not m:
        return None
    return f"F{m.group(1)} Pickup%"


# Listing model -> EPA model fallbacks moved to ``backend.vehicle_facts.epa_model``;
# the two EPA lookups read them as ``epa_model_candidates(strategy="by_trim" /
# "aggregate")``. Re-exported here for old importers.
from backend.vehicle_facts.epa_model import (  # noqa: E402,F401
    _EPA_MODEL_NOISE_SUFFIX_RE,
    _model_epa_fallbacks,
    epa_model_candidates,
)


# vPIC DriveType is normalized by ``vehicle_facts.normalize_drivetrain`` ("4x2" /
# "2WD" is two-wheel drive of UNKNOWN end -> None: vPIC stamps it on FWD Camrys,
# HR-Vs and Pilots as much as on RWD trucks. Mapping it to RWD (pre-2026-09-23)
# made the resolver reject every FWD catalog row for those cars.)

_VPIC_BODY_MAP = {
    "sedan": "Sedan", "coupe": "Coupe", "convertible": "Convertible",
    "hatchback": "Hatchback",
    "sport utility vehicle (suv)/multi-purpose vehicle (mpv)": "SUV",
    "suv": "SUV", "crossover utility vehicle (cuv)": "SUV",
    "pickup": "Truck", "pickup truck": "Truck",
    "van": "Van", "cargo van": "Van", "passenger van": "Van", "minivan": "Minivan",
    "wagon": "Wagon",
}


# nhtsa_vpic_cache is a reference cache populated out-of-band (backfill scripts);
# within a serving process it is read-only, so per-vin memoization is safe. Backed by
# a plain dict (rather than lru_cache) so a batch job can bulk-prime every vin with a
# single query — see ``prime_vpic_cache`` — instead of one round-trip per car.
_VPIC_MEMO: dict[str | None, dict[str, Any]] = {}


def _empty_vpic() -> dict[str, Any]:
    return {
        "transmission": None, "drivetrain": None, "cylinders": None,
        "engine_l": None, "fuel_type": None, "body_style": None, "trim": None,
        # 'ev' | 'phev' | 'hybrid' | None (None = vPIC did not say; NOT "not a hybrid")
        "electrification": None,
    }


def clear_vpic_lookup_cache() -> None:
    _VPIC_MEMO.clear()


def lookup_vpic_from_cache(vin: str | None) -> dict[str, Any]:
    """
    Read the cached nhtsa_vpic_cache row for *vin* and return a normalized spec dict.

    Keys returned (all may be None):
      transmission, drivetrain, cylinders, engine_l, fuel_type, body_style, trim
    """
    if vin in _VPIC_MEMO:
        return dict(_VPIC_MEMO[vin])
    out = _lookup_vpic_from_cache_uncached(vin)
    _VPIC_MEMO[vin] = out
    return dict(out)


def prime_vpic_cache(vins) -> None:
    """
    Bulk-load nhtsa_vpic_cache for *vins* into the memo with batched ``IN`` queries,
    so a subsequent per-vin :func:`lookup_vpic_from_cache` is a pure in-memory hit.
    Result is byte-for-byte identical to the per-vin path (same normalization); it just
    collapses N round-trips into ceil(N/batch). vins already memoized are skipped.
    """
    import json as _json

    todo: list[str] = []
    seen: set[str] = set()
    for v in vins:
        if not v or v in _VPIC_MEMO or v in seen:
            continue
        seen.add(v)
        todo.append(v)
    if not todo:
        return
    conn = None
    try:
        conn = _conn()
        for start in range(0, len(todo), 900):
            batch = todo[start:start + 900]
            placeholders = ",".join(["?"] * len(batch))
            rows = conn.execute(
                f"SELECT vin, response_json FROM nhtsa_vpic_cache WHERE vin IN ({placeholders})",
                tuple(batch),
            ).fetchall()
            for row in rows:
                v = row[0]
                try:
                    r = _json.loads(row[1]).get("Results", [{}])[0]
                    _VPIC_MEMO[v] = _normalize_vpic_response(r)
                except Exception:
                    _VPIC_MEMO[v] = _empty_vpic()
            for v in batch:
                _VPIC_MEMO.setdefault(v, _empty_vpic())
    except Exception:
        # On any failure leave the memo as-is; per-vin lookups still work (they query).
        pass
    finally:
        if conn is not None:
            conn.close()


def _lookup_vpic_from_cache_uncached(vin: str | None) -> dict[str, Any]:
    if not vin:
        return _empty_vpic()
    conn = None
    try:
        import json as _json
        conn = _conn()
        row = conn.execute(
            "SELECT response_json FROM nhtsa_vpic_cache WHERE vin=?", (vin,)
        ).fetchone()
        if not row:
            return _empty_vpic()
        r = _json.loads(row[0]).get("Results", [{}])[0]
    except Exception:
        return _empty_vpic()
    finally:
        if conn is not None:
            conn.close()
    return _normalize_vpic_response(r)


def _normalize_vpic_response(r: dict[str, Any]) -> dict[str, Any]:
    out = _empty_vpic()
    ts = (r.get("TransmissionStyle") or "").strip()
    speeds = (r.get("TransmissionSpeeds") or "").strip()
    if ts and ts.lower() not in ("", "not applicable"):
        if speeds and speeds.isdigit():
            out["transmission"] = f"{speeds}-Speed {ts.title()}"
        else:
            out["transmission"] = ts.title()

    out["drivetrain"] = normalize_drivetrain(r.get("DriveType"), "vpic")

    cyl_raw = (r.get("EngineCylinders") or "").strip()
    if cyl_raw and cyl_raw.isdigit():
        out["cylinders"] = int(cyl_raw)

    disp = (r.get("DisplacementL") or "").strip()
    try:
        out["engine_l"] = float(disp) if disp else None
    except ValueError:
        pass

    ft = (r.get("FuelTypePrimary") or "").strip()
    if ft and ft.lower() not in ("", "not applicable"):
        ft_l = ft.lower()
        if "electric" in ft_l and "natural" not in ft_l:
            out["fuel_type"] = "Electric"
        elif "diesel" in ft_l:
            out["fuel_type"] = "Diesel"
        elif "gasoline" in ft_l or "petrol" in ft_l:
            out["fuel_type"] = "Gasoline"
        elif "hybrid" in ft_l:
            out["fuel_type"] = "Hybrid"
        else:
            out["fuel_type"] = ft

    # vehicle_facts.vpic_electrification: BEV / PHEV / HEV / FCEV; a MILD hybrid
    # (48V assist: EPA and dealers disagree on the label) is no override -> None.
    _el = vpic_electrification(r)
    out["electrification"] = LEGACY_CODE.get(_el) if _el else None
    bc = (r.get("BodyClass") or "").strip()
    out["body_style"] = _VPIC_BODY_MAP.get(bc.lower())

    trim_vpic = (r.get("Trim") or "").strip()
    if trim_vpic and trim_vpic.lower() not in ("", "not applicable"):
        out["trim"] = trim_vpic

    return out


@lru_cache(maxsize=16384)
def _lookup_epa_aggregate_cached(
    year: int | None,
    make: str | None,
    model: str | None,
    title: str | None,
    trim: str | None,
    prefer_cylinders: int | None,
) -> dict[str, Any]:
    # Static reference table (epa_master, rebuilt by scripts): safe to memoize for
    # the process lifetime. The public wrapper hands out a fresh shallow copy so no
    # caller can mutate the shared cached dict (all values are scalars).
    return _lookup_epa_aggregate_uncached(
        year, make, model, title=title, trim=trim, prefer_cylinders=prefer_cylinders
    )


def clear_epa_aggregate_lookup_cache() -> None:
    _lookup_epa_aggregate_cached.cache_clear()


def lookup_epa_aggregate(
    year: int | None,
    make: str | None,
    model: str | None,
    *,
    title: str | None = None,
    trim: str | None = None,
    prefer_cylinders: int | None = None,
) -> dict[str, Any]:
    """
    Mode row from epa_master: cylinders, drive, transmission, MPG, displacement, atv_type.

    *title* / *trim* refine BMW I-series and similar short-model EPA matches (e.g. eDrive40).
    *prefer_cylinders* (dealer-reported count) picks the matching engine family when a
    model spans several (e.g. GLE 350 2.0L I4 vs GLE 450 3.0L I6) instead of the mode row.
    """
    # ``title`` / ``trim`` are consulted ONLY inside the BMW fuzzy-match branch of the
    # uncached implementation. For every other make they do not affect the result, so
    # drop them from the memo key — otherwise the near-unique ``title`` makes the cache
    # miss on every car and re-query a model that many listings share.
    if (make or "").strip().upper() != "BMW":
        title = None
        trim = None
    return dict(
        _lookup_epa_aggregate_cached(year, make, model, title, trim, prefer_cylinders)
    )


def _lookup_epa_aggregate_uncached(
    year: int | None,
    make: str | None,
    model: str | None,
    *,
    title: str | None = None,
    trim: str | None = None,
    prefer_cylinders: int | None = None,
) -> dict[str, Any]:
    """
    Mode row from epa_master: cylinders, drive, transmission, MPG, displacement, atv_type.

    *title* / *trim* refine BMW I-series and similar short-model EPA matches (e.g. eDrive40).
    *prefer_cylinders* (dealer-reported count) picks the matching engine family when a
    model spans several (e.g. GLE 350 2.0L I4 vs GLE 450 3.0L I6) instead of the mode row.
    """
    out: dict[str, Any] = {
        "cylinders": None,
        "drivetrain": None,
        "gears": None,
        "transmission": None,
        "displacement": None,
        "city08": None,
        "highway08": None,
        "city_e": None,
        "highway_e": None,
        "atv_type": None,
        "fuel_type": None,
    }
    if not year or not make or not model:
        return out
    conn = None
    try:
        conn = _conn()
        cur = conn.cursor()
        sql_mode = """
            SELECT cylinders, drive, trany, COUNT(*) AS n,
                   AVG(displacement) AS disp,
                   AVG(city08) AS c08,
                   AVG(highway08) AS h08,
                   AVG(city_e) AS ce,
                   AVG(highway_e) AS he,
                   GROUP_CONCAT(DISTINCT atv_type) AS atv_cat,
                   GROUP_CONCAT(DISTINCT fuel_type) AS fuel_cat
            FROM epa_master
            WHERE year = ? AND lower(make) = lower(?) AND {model_clause}
            GROUP BY cylinders, drive, trany
            ORDER BY n DESC
            """

        def _pick(rows: list) -> tuple | None:
            """Group matching the dealer cylinder count when one exists, else the mode row."""
            if not rows:
                return None
            if prefer_cylinders is not None:
                for r in rows:
                    if r[0] is not None and int(r[0]) == prefer_cylinders:
                        return r
            return rows[0]

        cur.execute(
            sql_mode.format(model_clause="lower(model) = lower(?)"),
            (year, make.strip(), model.strip()),
        )
        row = _pick(cur.fetchall())
        if not row:
            like_pat = _ford_epa_pickup_like_pattern(make, model)
            if like_pat:
                cur.execute(
                    sql_mode.format(model_clause="model LIKE ?"),
                    (year, make.strip(), like_pat),
                )
                row = _pick(cur.fetchall())
        # BMW fuzzy fallback: dealer stores 'X3' but EPA has 'X3 xDrive30i'; I-series
        # short codes need optional trim/title (e.g. eDrive40 on ``i4``).
        if not row and (make or "").strip().upper() == "BMW":
            bmw_pat = _bmw_epa_model_like_pattern(model)
            if bmw_pat:
                extra = _bmw_bev_epa_model_narrow_substring(title, trim)
                if extra:
                    cur.execute(
                        sql_mode.format(
                            model_clause="model LIKE ? AND lower(model) LIKE ?"
                        ),
                        (year, make.strip(), bmw_pat, f"%{extra}%"),
                    )
                    row = _pick(cur.fetchall())
                if not row:
                    cur.execute(
                        sql_mode.format(model_clause="model LIKE ?"),
                        (year, make.strip(), bmw_pat),
                    )
                    row = _pick(cur.fetchall())
        # Generic model-name normalization fallbacks (Silverado 1500 → Silverado, GLC 300 → GLC-Class, etc.)
        if not row:
            for fallback_model in epa_model_candidates(make, model, strategy="aggregate"):
                cur.execute(
                    sql_mode.format(model_clause="lower(model) = lower(?)"),
                    (year, make.strip(), fallback_model),
                )
                row = _pick(cur.fetchall())
                if row:
                    break
        if not row:
            return out
        cyl, drive, trany, _n, disp, c08, h08, ce, he, atv_cat, fuel_cat = row
        if cyl is not None:
            out["cylinders"] = int(cyl)
        out["drivetrain"] = _norm_drive_epa(drive)
        out["transmission"] = trany
        out["gears"] = _gears_from_trany(trany)
        if disp is not None:
            out["displacement"] = float(disp)
        for key, val in (
            ("city08", c08),
            ("highway08", h08),
            ("city_e", ce),
            ("highway_e", he),
        ):
            if val is not None and (isinstance(val, (int, float)) and val > 0):
                out[key] = float(val)
        if atv_cat:
            cats = [a.strip() for a in atv_cat.split(",") if a.strip()]
            # A blended group (gas GLC 300 + 350e PHEV rows concat to "EV,Hybrid")
            # must not report EV/FCV when any member burns fuel.
            fuels_lower = (fuel_cat or "").lower()
            if "gas" in fuels_lower or "diesel" in fuels_lower:
                cats = [c for c in cats if c.upper() not in ("EV", "FCV")]
            out["atv_type"] = cats[0] if cats else None
        if fuel_cat:
            out["fuel_type"] = fuel_cat.split(",")[0].strip() or None
    except Exception:
        if conn is not None:
            conn.close()
            conn = None
        try:
            conn = _conn()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT cylinders, drive, trany, COUNT(*) AS n
                FROM epa_master
                WHERE year = ? AND lower(make) = lower(?) AND lower(model) = lower(?)
                GROUP BY cylinders, drive, trany
                ORDER BY n DESC
                LIMIT 1
                """,
                (year, make.strip(), model.strip()),
            )
            row = cur.fetchone()
            if not row:
                like_pat = _ford_epa_pickup_like_pattern(make, model)
                if like_pat:
                    cur.execute(
                        """
                        SELECT cylinders, drive, trany, COUNT(*) AS n
                        FROM epa_master
                        WHERE year = ? AND lower(make) = lower(?) AND model LIKE ?
                        GROUP BY cylinders, drive, trany
                        ORDER BY n DESC
                        LIMIT 1
                        """,
                        (year, make.strip(), like_pat),
                    )
                    row = cur.fetchone()
            if row:
                cyl, drive, trany, _ = row
                if cyl is not None:
                    out["cylinders"] = int(cyl)
                out["drivetrain"] = _norm_drive_epa(drive)
                out["transmission"] = trany
                out["gears"] = _gears_from_trany(trany)
        except Exception:
            pass
    finally:
        if conn is not None:
            conn.close()
    return out


def _is_na_spec(v: Any) -> bool:
    """True when dealer DMS sent a placeholder instead of a real spec (includes ``--``, em dash)."""
    from backend.utils.field_clean import is_effectively_empty

    if v is None:
        return True
    if isinstance(v, bool):
        return is_effectively_empty(v)
    if isinstance(v, (int, float)):
        return False
    if is_effectively_empty(v):
        return True
    s = str(v).strip().upper()
    return s in ("N/A", "NA", "UNKNOWN", "NULL")


# ---------------------------------------------------------------------------
# merge_verified_specs / prepare_car_detail_context and their private helper
# _sticker_specs_from_packages were mechanically extracted into
# backend.enrichment.knowledge_engine_specs. Re-export them here so the
# historical import surface (from backend.enrichment.knowledge_engine import
# merge_verified_specs / prepare_car_detail_context / ...) is preserved.
from backend.enrichment.knowledge_engine_specs import (  # noqa: E402,F401
    _sticker_specs_from_packages,  # noqa: F401
    merge_verified_specs,  # noqa: F401
    prepare_car_detail_context,  # noqa: F401
)
