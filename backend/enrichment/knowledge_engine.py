"""
Automotive Knowledge Engine: EPA master data + regex trim decoder.
Fills cylinders, gears, drivetrain when dealer data is missing or generic.
"""
from __future__ import annotations

import csv
import os
import re
import sqlite3
from functools import lru_cache
from typing import Any

from backend.db.inventory_db import DB_PATH


def _conn():
    return sqlite3.connect(DB_PATH)


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
    if not d:
        return None
    u = d.strip().upper()
    if "4WD" in u or "ALL" in u or "AWD" in u:
        return "AWD"
    if "PART" in u and "4" in u:
        return "4WD"
    if "FWD" in u or "FRONT" in u:
        return "FWD"
    if "RWD" in u or "REAR" in u:
        return "RWD"
    return d


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


def _ensure_epa_columns(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(epa_master)")
    have = {row[1] for row in cur.fetchall()}
    for col, typ in [
        ("city08", "REAL"),
        ("highway08", "REAL"),
        ("city_e", "REAL"),
        ("highway_e", "REAL"),
        ("atv_type", "TEXT"),
        ("trim", "TEXT"),
        ("body_style", "TEXT"),
        ("engine_description", "TEXT"),
    ]:
        if col not in have:
            try:
                cur.execute(f"ALTER TABLE epa_master ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError:
                pass
    conn.commit()


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


def _lookup_epa_by_trim_uncached(
    year: int,
    make: str,
    model: str,
    trim_clean: str,
) -> dict[str, Any]:
    try:
        conn = _conn()
        _ensure_epa_columns(conn)
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
            for fallback_model in _model_epa_fallbacks(make, model):
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
        conn.close()
        if row:
            return _epa_row_to_dict(row)
    except sqlite3.OperationalError:
        pass
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


def _model_epa_fallbacks(make: str | None, model: str | None) -> list[str]:
    """
    Alternative EPA model strings to try when exact model match fails.

    Many dealers store extended model names (e.g. "Silverado 1500", "GLC 300") while
    EPA uses base names ("Silverado", "GLC-Class"). Returns a list ordered most→least specific.
    """
    mk = (make or "").strip().upper()
    mo = (model or "").strip()
    mo_u = mo.upper()
    fallbacks: list[str] = []

    # --- Chevrolet: Silverado 1500 / Silverado 2500 HD / Silverado 3500 HD Chassis Cab → Silverado ---
    if mk == "CHEVROLET":
        m = re.match(r"^(Silverado|Colorado)\s+\S", mo, re.I)
        if m:
            fallbacks.append(m.group(1))
        # Corvette Stingray / Corvette Z06 → Corvette
        if re.match(r"^Corvette\s+\S", mo, re.I):
            fallbacks.append("Corvette")

    # --- GMC: Sierra 1500 / Sierra 2500 HD / Sierra 3500 HD → Sierra (skip Sierra EV — different arch) ---
    if mk == "GMC":
        m = re.match(r"^(Sierra|Canyon|Yukon|Acadia)\s+\S", mo, re.I)
        if m and "EV" not in mo_u:
            fallbacks.append(m.group(1))
        # HUMMER EV SUV → Hummer EV
        if re.match(r"^HUMMER\s+EV\s+SUV\b", mo, re.I):
            fallbacks.append("Hummer EV")

    # --- Buick: Encore GX → Encore ---
    if mk == "BUICK":
        if re.match(r"^Encore\s+GX\b", mo, re.I):
            fallbacks.append("Encore")

    # --- Kia: Sportage Hybrid / Niro PHEV → base model ---
    if mk == "KIA":
        stripped = re.sub(r"\s+(Hybrid|Plug[-\s]?In\s+Hybrid|PHEV|EV)\s*$", "", mo, flags=re.I).strip()
        if stripped.lower() != mo.lower():
            fallbacks.append(stripped)

    # --- Toyota: "Tundra Hybrid" → "Tundra", "Tacoma i-FORCE MAX" → "Tacoma", etc. ---
    if mk == "TOYOTA":
        stripped = re.sub(r"\s+(Hybrid|Plug[-\s]?In\s+Hybrid|i-FORCE\s+MAX)\s*$", "", mo, flags=re.I).strip()
        if stripped.lower() != mo.lower():
            fallbacks.append(stripped)

    # --- Hyundai: Kona N → Kona, Ioniq 5 N → Ioniq 5 ---
    if mk == "HYUNDAI":
        stripped = re.sub(r"\s+N\s*$", "", mo, flags=re.I).strip()
        if stripped.lower() != mo.lower():
            fallbacks.append(stripped)

    # --- Ram: 1500 Classic → 1500; ProMaster City variants ---
    if mk == "RAM":
        m = re.match(r"^(1500|2500|3500)\s+Classic\b", mo, re.I)
        if m:
            fallbacks.append(m.group(1))
        if re.match(r"^ProMaster\s+City\b", mo, re.I):
            fallbacks.append("Promaster City")

    # --- Chrysler: "Town & Country" → "Town and Country" ---
    if mk == "CHRYSLER":
        if re.match(r"^Town\s*&\s*Country\b", mo, re.I):
            fallbacks.append("Town and Country")

    # --- Volvo: "XC40 Recharge Pure Electric" / "XC60 Recharge" → base model ---
    if mk == "VOLVO":
        m = re.match(r"^(XC40|XC60|XC90|S60|S90|V60|V90)\s+\S", mo, re.I)
        if m:
            fallbacks.append(m.group(1))

    # --- Nissan: Kicks Play → Kicks; NV200 Compact Cargo → NV200 Cargo Van ---
    if mk == "NISSAN":
        if re.match(r"^Kicks\s+Play\b", mo, re.I):
            fallbacks.append("Kicks")
        if re.match(r"^NV200\s+Compact\s+Cargo\b", mo, re.I):
            fallbacks.append("NV200 Cargo Van")

    # --- Jeep: Wrangler Unlimited / Wrangler JK Unlimited → Wrangler; Wagoneer S → Wagoneer ---
    if mk == "JEEP":
        if re.match(r"^Wrangler\s+(Unlimited|JK\s+Unlimited)\b", mo, re.I):
            fallbacks.append("Wrangler")
        if re.match(r"^Wagoneer\s+S\b", mo, re.I):
            fallbacks.append("Wagoneer")

    # --- Mercedes-Benz: "GLC 300" → "GLC-Class", "E 350" → "E-Class", "AMG GLC63" → "GLC-Class" ---
    if "MERCEDES" in mk:
        seg_map = {
            "A": "A-Class", "C": "C-Class", "CLA": "CLA-Class", "CLE": "CLE-Class",
            "CLS": "CLS-Class", "E": "E-Class", "G": "G-Class",
            "GLA": "GLA-Class", "GLB": "GLB-Class", "GLC": "GLC-Class",
            "GLE": "GLE-Class", "GLS": "GLS-Class", "S": "S-Class", "SL": "SL-Class",
        }
        m = re.match(r"^([A-Z]+)\s+\d{3}\b", mo_u)
        if m:
            epa_cls = seg_map.get(m.group(1))
            if epa_cls:
                fallbacks.append(epa_cls)
        # "AMG GLC 63" form
        m2 = re.match(r"^AMG\s+([A-Z]+)\s*\d{2}", mo_u)
        if m2:
            epa_cls = seg_map.get(m2.group(1))
            if epa_cls:
                fallbacks.append(epa_cls)

    # --- Mitsubishi: Outlander Phev / Outlander Sport → Outlander ---
    if mk == "MITSUBISHI":
        if re.match(r"^Outlander\s+(Phev|Sport)\b", mo, re.I):
            fallbacks.append("Outlander")

    # --- Volkswagen: Atlas Cross Sport → Atlas ---
    if mk == "VOLKSWAGEN":
        if re.match(r"^Atlas\s+Cross\s+Sport\b", mo, re.I):
            fallbacks.append("Atlas")

    # --- Mazda: "MX-5 MIATA" → "MX-5"; "Mazda CX-9" / "Mazda3" → strip "Mazda" prefix ---
    if mk == "MAZDA":
        if re.match(r"^MX-5\s+MIATA\b", mo, re.I):
            fallbacks.append("MX-5")
        else:
            m = re.match(r"^Mazda\s+(.+)", mo, re.I)
            if m:
                fallbacks.append(m.group(1).strip())

    # --- BMW: numeric sedan/coupe → Series name (330i → "3 Series", M3 → "M", etc.) ---
    if mk == "BMW":
        mo_stripped = mo.strip()
        mo_s_u = mo_stripped.upper()
        # Standalone M models: M3, M4, M5, M8
        if re.match(r"^M[3458]\b", mo_s_u) and " " not in mo_stripped:
            fallbacks.append("M")
        # X3 M / X5 M / X6 M → try "M" then base X-series
        elif re.match(r"^X([3-6])\s+M\b", mo_s_u):
            fallbacks.append("M")
            base = re.match(r"^(X[3-6])\b", mo_s_u).group(1).title()
            fallbacks.append(base)
        # Standard numeric: 228i, 330i, 435i, 530e, 740i, 840i → "{n} Series"
        elif re.match(r"^[2-8]\d{2}[iIeE]?\b", mo_s_u) and " " not in mo_stripped:
            fallbacks.append(f"{mo_s_u[0]} Series")
        # M-prefix numeric: M235i, M340i, M440i → "{n} Series"
        elif re.match(r"^M([2-8])\d{2}[iI]\b", mo_s_u) and " " not in mo_stripped:
            s = re.match(r"^M([2-8])", mo_s_u).group(1)
            fallbacks.append(f"{s} Series")

    # --- Audi: dealer model strings vs EPA base names ---
    if mk == "AUDI":
        mo_compact = mo_u.replace("-", "")
        if re.match(r"^A8\b", mo, re.I):
            fallbacks.extend(["A8", "A8 L"])
        if re.match(r"^Q6\b", mo, re.I) and "ETRON" in mo_compact:
            fallbacks.extend(["Q6 e-tron", "Q6"])
        if re.match(r"^Q8\b", mo, re.I) and "ETRON" in mo_compact:
            fallbacks.extend(["Q8 e-tron", "Q8"])
        for base in ("A3", "A4", "A5", "A6", "A7", "A8", "Q3", "Q5", "Q6", "Q7", "Q8"):
            if re.match(rf"^{base}\b", mo, re.I):
                fallbacks.append(base)
        stripped = re.sub(r"\s+(Sportback|Sedan|Coupe|allroad)\s*$", "", mo, flags=re.I).strip()
        if stripped and stripped.lower() != mo.lower():
            fallbacks.append(stripped)

    return fallbacks


_VPIC_DRIVE_MAP = {
    "fwd": "FWD", "front-wheel drive": "FWD", "front wheel drive": "FWD",
    "rwd": "RWD", "rear-wheel drive": "RWD", "rear wheel drive": "RWD", "4x2": "RWD",
    "awd": "AWD", "all-wheel drive": "AWD", "all wheel drive": "AWD",
    "4wd": "4WD", "4x4": "4WD", "four-wheel drive": "4WD", "four wheel drive": "4WD",
}

_VPIC_BODY_MAP = {
    "sedan": "Sedan", "coupe": "Coupe", "convertible": "Convertible",
    "hatchback": "Hatchback",
    "sport utility vehicle (suv)/multi-purpose vehicle (mpv)": "SUV",
    "suv": "SUV", "crossover utility vehicle (cuv)": "SUV",
    "pickup": "Truck", "pickup truck": "Truck",
    "van": "Van", "cargo van": "Van", "passenger van": "Van", "minivan": "Minivan",
    "wagon": "Wagon",
}


def lookup_vpic_from_cache(vin: str | None) -> dict[str, Any]:
    """
    Read the cached nhtsa_vpic_cache row for *vin* and return a normalized spec dict.

    Keys returned (all may be None):
      transmission, drivetrain, cylinders, engine_l, fuel_type, body_style, trim
    """
    out: dict[str, Any] = {
        "transmission": None, "drivetrain": None, "cylinders": None,
        "engine_l": None, "fuel_type": None, "body_style": None, "trim": None,
    }
    if not vin:
        return out
    try:
        import json as _json
        with _conn() as conn:
            row = conn.execute(
                "SELECT response_json FROM nhtsa_vpic_cache WHERE vin=?", (vin,)
            ).fetchone()
        if not row:
            return out
        r = _json.loads(row[0]).get("Results", [{}])[0]
    except Exception:
        return out

    ts = (r.get("TransmissionStyle") or "").strip()
    speeds = (r.get("TransmissionSpeeds") or "").strip()
    if ts and ts.lower() not in ("", "not applicable"):
        if speeds and speeds.isdigit():
            out["transmission"] = f"{speeds}-Speed {ts.title()}"
        else:
            out["transmission"] = ts.title()

    dt = (r.get("DriveType") or "").strip()
    out["drivetrain"] = _VPIC_DRIVE_MAP.get(dt.lower()) or (
        "AWD" if "all" in dt.lower() or "awd" in dt.lower()
        else "4WD" if "4wd" in dt.lower() or "4x4" in dt.lower()
        else "FWD" if "fwd" in dt.lower() or "front" in dt.lower()
        else "RWD" if ("rwd" in dt.lower() or "rear" in dt.lower() or "4x2" in dt.lower())
        else None
    )

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

    bc = (r.get("BodyClass") or "").strip()
    out["body_style"] = _VPIC_BODY_MAP.get(bc.lower())

    trim_vpic = (r.get("Trim") or "").strip()
    if trim_vpic and trim_vpic.lower() not in ("", "not applicable"):
        out["trim"] = trim_vpic

    return out


def lookup_epa_aggregate(
    year: int | None,
    make: str | None,
    model: str | None,
    *,
    title: str | None = None,
    trim: str | None = None,
) -> dict[str, Any]:
    """
    Mode row from epa_master: cylinders, drive, transmission, MPG, displacement, atv_type.

    *title* / *trim* refine BMW I-series and similar short-model EPA matches (e.g. eDrive40).
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
    try:
        conn = _conn()
        _ensure_epa_columns(conn)
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
            LIMIT 1
            """
        cur.execute(
            sql_mode.format(model_clause="lower(model) = lower(?)"),
            (year, make.strip(), model.strip()),
        )
        row = cur.fetchone()
        if not row:
            like_pat = _ford_epa_pickup_like_pattern(make, model)
            if like_pat:
                cur.execute(
                    sql_mode.format(model_clause="model LIKE ?"),
                    (year, make.strip(), like_pat),
                )
                row = cur.fetchone()
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
                    row = cur.fetchone()
                if not row:
                    cur.execute(
                        sql_mode.format(model_clause="model LIKE ?"),
                        (year, make.strip(), bmw_pat),
                    )
                    row = cur.fetchone()
        # Generic model-name normalization fallbacks (Silverado 1500 → Silverado, GLC 300 → GLC-Class, etc.)
        if not row:
            for fallback_model in _model_epa_fallbacks(make, model):
                cur.execute(
                    sql_mode.format(model_clause="lower(model) = lower(?)"),
                    (year, make.strip(), fallback_model),
                )
                row = cur.fetchone()
                if row:
                    break
        conn.close()
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
            out["atv_type"] = atv_cat.split(",")[0].strip() or None
        if fuel_cat:
            out["fuel_type"] = fuel_cat.split(",")[0].strip() or None
    except sqlite3.OperationalError:
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
            conn.close()
            if row:
                cyl, drive, trany, _ = row
                if cyl is not None:
                    out["cylinders"] = int(cyl)
                out["drivetrain"] = _norm_drive_epa(drive)
                out["transmission"] = trany
                out["gears"] = _gears_from_trany(trany)
        except sqlite3.OperationalError:
            pass
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


def _sticker_specs_from_packages(car: dict[str, Any]) -> dict[str, Any]:
    """Read merged Monroney fields from ``cars.packages`` JSON."""
    import json

    out: dict[str, Any] = {}
    raw = car.get("packages")
    if not raw or str(raw).strip() in ("{}", "[]", "null"):
        return out
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError, json.JSONDecodeError):
        return out
    if not isinstance(parsed, dict):
        return out

    from backend.scanner.window_sticker import sticker_engine_display_is_valid

    disp = parsed.get("sticker_engine_display")
    if isinstance(disp, str) and sticker_engine_display_is_valid(disp):
        from backend.scanner.window_sticker import upgrade_engine_display_for_etorque

        out["engine_display"] = upgrade_engine_display_for_etorque(disp.strip(), car)
    ft = parsed.get("sticker_fuel_type")
    if isinstance(ft, str) and ft.strip():
        out["fuel_type"] = ft.strip()
    trans = parsed.get("sticker_transmission")
    if isinstance(trans, str) and sticker_engine_display_is_valid(trans):
        out["transmission"] = trans.strip()
    for key, pkg_key in (("mpg_city", "sticker_mpg_city"), ("mpg_highway", "sticker_mpg_highway")):
        val = parsed.get(pkg_key)
        if val is not None:
            try:
                out[key] = int(val)
            except (TypeError, ValueError):
                pass
    return out


def merge_verified_specs(car: dict[str, Any]) -> dict[str, Any]:
    """
    Combine dealer row with regex decoder + EPA lookup.
    Prefer: regex (brand trim) > EPA aggregate > dealer fields.
    When dealer omits or sends N/A, show inferred values as verified.
    """
    from backend.utils.field_clean import clean_car_row_dict

    car = clean_car_row_dict(dict(car))
    make = car.get("make") or ""
    model = car.get("model") or ""
    trim = car.get("trim") or ""
    title = car.get("title") or ""
    year = car.get("year")
    try:
        y = int(year) if year is not None else None
    except (TypeError, ValueError):
        y = None

    def _int_or_none(v: Any) -> int | None:
        if v is None or v == "":
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    dealer_cyl = car.get("cylinders")
    dealer_drive = (car.get("drivetrain") or "").strip()
    dealer_trans = (car.get("transmission") or "").strip()

    title_for_decode = (title or "").strip()
    dealer_ft = (car.get("fuel_type") or "").strip()
    if (make or "").strip().upper() == "BMW" and dealer_ft:
        title_for_decode = f"{title_for_decode} {dealer_ft}".strip()
    regex = decode_trim_logic(make, model, trim, title_for_decode)
    # Per-trim lookup first (exact match from build_epa_master.py data), then aggregate fallback
    epa_trim = lookup_epa_by_trim(y, make, model, trim) if trim else {}
    epa = lookup_epa_aggregate(y, make, model, title=title_for_decode, trim=trim)
    # Merge: per-trim values win over aggregate for any key they provide
    epa = {**epa, **{k: v for k, v in epa_trim.items() if v is not None}}

    from backend.enrichment.model_specs_dictionary import lookup_model_specs_dictionary

    dict_specs = lookup_model_specs_dictionary(make, model)
    vpic = lookup_vpic_from_cache(car.get("vin"))

    cyl_ver = regex.get("cylinders")
    if cyl_ver is None:
        cyl_ver = epa.get("cylinders")
    if cyl_ver is None:
        di = _int_or_none(dealer_cyl)
        if di is not None:
            cyl_ver = di
    if cyl_ver is None and dict_specs and dict_specs.get("cylinders") is not None:
        try:
            cyl_ver = int(dict_specs["cylinders"])
        except (TypeError, ValueError):
            cyl_ver = None
    if cyl_ver is None and vpic.get("cylinders") is not None:
        cyl_ver = vpic["cylinders"]

    drive_ver = regex.get("drivetrain") or epa.get("drivetrain")
    if not drive_ver and dealer_drive and not _is_na_spec(dealer_drive):
        drive_ver = dealer_drive
    if not drive_ver and dict_specs and dict_specs.get("drivetrain"):
        drive_ver = str(dict_specs["drivetrain"]).strip()
    if not drive_ver and vpic.get("drivetrain"):
        drive_ver = vpic["drivetrain"]

    gears_ver = regex.get("gears") or epa.get("gears")
    # Priority: trim-decoder year-aware hint > EPA > vPIC > model_specs > dealer.
    # transmission_hint encodes year logic (e.g. Transit pre-/post-2020); must beat model_specs.
    _trans_hint = regex.get("transmission_hint")
    if _trans_hint and not epa.get("transmission") and not (dealer_trans and not _is_na_spec(dealer_trans)):
        trans_raw = _trans_hint
    else:
        epa_trany = epa.get("transmission")
        trans_raw = epa_trany or (
            dealer_trans if dealer_trans and not _is_na_spec(dealer_trans) else None
        )
        # EPA aggregate is often generic "Automatic" while the VDP lists "8-Speed Automatic".
        dealer_ok = dealer_trans and not _is_na_spec(dealer_trans)
        if (
            dealer_ok
            and _transmission_has_gear_detail(dealer_trans)
            and not _transmission_has_gear_detail(epa_trany or "")
        ):
            trans_raw = dealer_trans
        if not trans_raw and vpic.get("transmission"):
            trans_raw = vpic["transmission"]
        if not trans_raw and dict_specs and dict_specs.get("transmission"):
            trans_raw = str(dict_specs["transmission"]).strip()
    trans_ver = format_transmission_display(trans_raw) or trans_raw

    # Trim decoder often knows "8-Speed Automatic" while EPA row is generic "Automatic".
    _dec_hint = regex.get("transmission_hint")
    if (
        _dec_hint
        and _transmission_has_gear_detail(_dec_hint)
        and not _transmission_has_gear_detail(str(trans_ver or ""))
    ):
        trans_ver = format_transmission_display(_dec_hint) or _dec_hint
    elif gears_ver is not None and str(trans_ver or "").strip().lower() in ("automatic", "auto"):
        try:
            _ng = int(gears_ver)
            if _ng > 0:
                trans_ver = f"{_ng}-Speed Automatic"
        except (TypeError, ValueError):
            pass

    dealer_cyl_i = _int_or_none(dealer_cyl)

    blob_full = f"{title} {trim} {model}".strip()
    # Dealer-supplied fuel_type "Electric" / "Electricity" (pure BEV, not PHEV/hybrid)
    _dealer_ft_lower = (car.get("fuel_type") or "").strip().lower()
    _dealer_is_pure_ev = (
        "electric" in _dealer_ft_lower
        and not any(x in _dealer_ft_lower for x in ("gas", "gasoline", "hybrid", "plug"))
    )
    is_bev = (
        regex.get("cylinders") == 0
        or (regex.get("fuel_type_hint") or "").strip().lower() == "electric"
        or (epa.get("atv_type") or "").strip().upper() == "EV"
        or "electric" in (epa.get("fuel_type") or "").lower()
        or _dealer_is_pure_ev
    )
    try:
        from backend.scanner.window_sticker import dodge_charger_daytona_is_bev

        if dodge_charger_daytona_is_bev(car):
            is_bev = True
    except Exception:
        pass

    sticker_pkg = _sticker_specs_from_packages(car)
    if sticker_pkg.get("fuel_type") == "Electric":
        is_bev = True
    if is_bev:
        cyl_ver = 0
        display_cyl = 0
    elif dealer_cyl_i is not None and dealer_cyl_i > 0:
        display_cyl = dealer_cyl_i
    elif cyl_ver is not None:
        display_cyl = cyl_ver
    else:
        display_cyl = dealer_cyl_i

    cylinders_verified = bool(
        display_cyl is not None
        and (_is_na_spec(dealer_cyl) or dealer_cyl_i in (None, 0))
        and (regex.get("cylinders") is not None or epa.get("cylinders") is not None)
    )

    # Regex/EPA first so xDrive/4MATIC in title wins over dealer placeholders.
    if drive_ver:
        display_drive = drive_ver
    elif not _is_na_spec(dealer_drive):
        display_drive = dealer_drive
    else:
        display_drive = ""

    drivetrain_verified = bool(
        drive_ver
        and (display_drive or "").strip().upper() == (drive_ver or "").strip().upper()
        and _is_na_spec(dealer_drive)
    )

    blob_full = f"{title} {trim} {model}".strip()
    display_drive_ui = _drivetrain_ui_label(display_drive, make, blob_full)

    if is_bev and not trans_ver and _is_na_spec(dealer_trans):
        trans_ver = sticker_pkg.get("transmission") or "Single-speed automatic"

    body_style_display = None
    from backend.utils.field_clean import is_jeep_wrangler_car, normalize_body_style_for_car

    if is_jeep_wrangler_car(make, model, trim, title):
        body_style_display = "SUV"
    elif _is_na_spec(car.get("body_style")) and regex.get("body_style_hint"):
        body_style_display = regex["body_style_hint"]
    if not body_style_display and _is_na_spec(car.get("body_style")) and vpic.get("body_style"):
        body_style_display = vpic["body_style"]
    if not is_jeep_wrangler_car(make, model, trim, title):
        corrected_bs = normalize_body_style_for_car(
            car.get("body_style") if not _is_na_spec(car.get("body_style")) else body_style_display,
            make=make,
            model=model,
            trim=trim,
            title=title,
        )
        if corrected_bs:
            body_style_display = corrected_bs
    fuel_economy_display = format_fuel_economy_display(epa, is_bev)
    if is_bev and sticker_pkg.get("mpg_city") and sticker_pkg.get("mpg_highway"):
        fuel_economy_display = format_fuel_economy_display(
            {
                "city08": sticker_pkg.get("mpg_city"),
                "highway08": sticker_pkg.get("mpg_highway"),
            },
            True,
        )
    if not fuel_economy_display and not is_bev:
        from backend.utils.field_clean import format_mpg_city_highway_display

        fuel_economy_display = format_mpg_city_highway_display(
            car.get("mpg_city"), car.get("mpg_highway")
        )
    master_engine_string = build_master_engine_string(make, model, trim, title, regex, epa)

    sources = []
    if (
        regex.get("cylinders") is not None
        or regex.get("drivetrain")
        or regex.get("fuel_type_hint")
        or regex.get("body_style_hint")
        or regex.get("transmission_hint")
    ):
        sources.append("Trim decoder")
    if epa.get("cylinders") is not None or epa.get("drivetrain") or epa.get("gears") or epa.get("transmission"):
        sources.append("EPA dataset")
    if dict_specs:
        sources.append("Model specs dictionary")

    # Normalize display string only when it's a raw shorthand (no speed count already present).
    # normalize_transmission_standard is a bucket classifier; applying it to strings that already
    # contain speed info ("10-Speed Automatic") would strip the count to just "Automatic".
    if trans_ver and not _is_na_spec(str(trans_ver)) and not re.search(r"\b\d+[-\s]?speed\b", trans_ver, re.I):
        from backend.utils.transmission_normalize import normalize_transmission_standard
        vin_raw = car.get("vin")
        vin_s = str(vin_raw).strip() if vin_raw not in (None, "") else None
        norm_t, _weak = normalize_transmission_standard(
            trans_ver,
            make=make,
            model=model,
            trim=trim,
            title=title_for_decode,
            year=y,
            vin=vin_s,
        )
        if norm_t:
            trans_ver = norm_t

    return {
        "cylinders": cyl_ver,
        "cylinders_display": display_cyl,
        "cylinders_verified": cylinders_verified,
        "drivetrain": drive_ver,
        "drivetrain_display": display_drive_ui,
        "drivetrain_verified": drivetrain_verified,
        "gears": gears_ver,
        "transmission_display": trans_ver,
        "fuel_type_hint": regex.get("fuel_type_hint"),
        "sources": sources,
        "dealer_cylinders": dealer_cyl,
        "master_engine_string": master_engine_string,
        "fuel_economy_display": fuel_economy_display,
        "epa_displacement": epa.get("displacement") or vpic.get("engine_l"),
        "body_style_display": body_style_display or epa.get("body_style"),
        # Per-trim EPA fields (populated from DICTIONARY via build_epa_master.py)
        "epa_fuel_type": epa.get("fuel_type"),
        "epa_engine_description": epa_trim.get("engine_description"),
        "epa_city08": epa.get("city08"),
        "epa_highway08": epa.get("highway08"),
    }


def prepare_car_detail_context(car: dict[str, Any]) -> dict[str, Any]:
    """Attach verified_specs + normalized gallery list for templates."""
    import json

    if not car:
        return {}
    g = car.get("gallery")
    if isinstance(g, str):
        try:
            g = json.loads(g)
        except (TypeError, ValueError):
            g = []
    if not isinstance(g, list):
        g = []
    from backend.vision.url_heuristics import (
        filter_public_gallery_urls,
        heuristic_listing_gallery_fluff_url,
        prefer_full_gallery_url,
    )

    urls = filter_public_gallery_urls([u for u in g if u and isinstance(u, str)])
    if not urls and car.get("image_url"):
        iu = car.get("image_url")
        if isinstance(iu, str) and iu.strip() and not heuristic_listing_gallery_fluff_url(iu):
            urls = [prefer_full_gallery_url(iu.strip())]
    verified = merge_verified_specs(car)

    listing_packages_sections: list[dict[str, Any]] = []
    listing_standalone_features: list[str] = []
    listing_observed_features: list[str] = []
    listing_monroney_options: list[str] = []
    listing_monroney_standard: list[str] = []
    listing_sticker_options: list[str] = []
    listing_sticker_option_groups: list[dict[str, Any]] = []
    listing_sticker_option_sections: dict[str, Any] = {}
    listing_possible_packages: list[str] = []
    listing_photo_detected_equipment: list[str] = []
    sticker_exterior_color: str | None = None
    sticker_interior_color: str | None = None
    sticker_interior_material: str | None = None
    sticker_spec_lines: list[dict[str, str]] = []
    interior_from_listing_description = False
    interior_from_llava_vision = False
    llava_interior_section: dict[str, Any] | None = None
    hide_photo_analysis = False

    def _normalized_pkg_title(entry: dict[str, Any]) -> str:
        for key in ("name", "canonical_name", "name_verbatim"):
            v = entry.get(key)
            if v is not None and str(v).strip():
                return str(v).strip()[:200]
        return ""

    pkg_raw = car.get("packages")
    pj: dict[str, Any] | None = None
    if pkg_raw and str(pkg_raw).strip() not in ("{}", "[]", "null"):
        try:
            parsed = json.loads(pkg_raw) if isinstance(pkg_raw, str) else pkg_raw
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = None
        if isinstance(parsed, dict):
            pj = parsed

    try:
        from backend.enrichment.window_sticker_service import should_skip_photo_package_analysis

        hide_photo_analysis = should_skip_photo_package_analysis(car, pj)
    except Exception:
        hide_photo_analysis = False
    if not hide_photo_analysis:
        try:
            from backend.scanner.window_sticker import oem_hide_photo_analysis

            hide_photo_analysis = oem_hide_photo_analysis(
                str(car.get("vin") or ""),
                str(car.get("make") or ""),
            )
        except Exception:
            hide_photo_analysis = False

    if pj is not None:
        liv = pj.get("llava_interior_cabin")
        if isinstance(liv, dict) and liv and not hide_photo_analysis:
            ib = liv.get("interior_buckets") or []
            bucket_list = [str(x).strip() for x in ib if str(x).strip()][:24]
            llava_interior_section = {
                "guess": str(liv.get("interior_guess_text") or "").strip()[:240],
                "buckets": bucket_list,
                "evidence": str(liv.get("evidence") or "").strip()[:400],
                "confidence": liv.get("confidence"),
            }
        trim_low = str(car.get("trim") or "").strip().lower()
        seen_titles: set[str] = set()
        for entry in pj.get("packages_normalized") or []:
            if not isinstance(entry, dict):
                continue
            title = _normalized_pkg_title(entry)
            if not title:
                continue
            low = title.lower()
            if low in seen_titles:
                continue
            feats = entry.get("features") or []
            feat_list = [str(x).strip() for x in feats if isinstance(x, str) and str(x).strip()]
            evidence = entry.get("evidence_spans") or []
            evidence_list = [
                str(x).strip() for x in evidence if isinstance(x, str) and str(x).strip()
            ][:8]
            # Trim names are not option packages — skip empty accordions that only repeat trim.
            if trim_low and low == trim_low and not feat_list and not evidence_list:
                continue
            seen_titles.add(low)
            listing_packages_sections.append(
                {
                    "name": title,
                    "features": feat_list[:30],
                    "evidence": evidence_list,
                    "source": "listing",
                    "from_listing_description": True,
                    "from_vision": False,
                }
            )

        mo = pj.get("monroney_options")
        if isinstance(mo, list):
            try:
                from backend.scanner.window_sticker import is_sticker_boilerplate_line
            except ImportError:
                is_sticker_boilerplate_line = None
            for x in mo:
                s = str(x).strip()[:500]
                if not s:
                    continue
                if is_sticker_boilerplate_line and is_sticker_boilerplate_line(s):
                    continue
                try:
                    from backend.scanner.window_sticker import _is_sticker_factory_code_noise
                except ImportError:
                    _is_sticker_factory_code_noise = None
                if _is_sticker_factory_code_noise and _is_sticker_factory_code_noise(s):
                    continue
                listing_monroney_options.append(s)
        listing_monroney_options = listing_monroney_options[:60]

        mstd = pj.get("monroney_standard_highlights")
        if isinstance(mstd, list):
            try:
                from backend.scanner.window_sticker import is_sticker_boilerplate_line as _is_boilerplate
            except ImportError:
                _is_boilerplate = None
            for x in mstd:
                s = str(x).strip()[:500]
                if not s:
                    continue
                if _is_boilerplate and _is_boilerplate(s):
                    continue
                listing_monroney_standard.append(s)
        listing_monroney_standard = listing_monroney_standard[:40]

        so = pj.get("sticker_options")
        listing_sticker_option_groups: list[dict[str, Any]] = []
        listing_sticker_option_sections: dict[str, Any] = {}
        try:
            from backend.scanner.window_sticker import (
                group_sticker_options_for_display,
                sticker_option_sections_for_display,
                sticker_options_for_display,
            )

            listing_sticker_options = sticker_options_for_display(pj)
            listing_sticker_option_groups = group_sticker_options_for_display(listing_sticker_options)
            listing_sticker_option_sections = sticker_option_sections_for_display(pj)
        except Exception:
            listing_sticker_options = []
            listing_sticker_option_groups = []
            listing_sticker_option_sections = {}
        if not listing_sticker_options and isinstance(so, list):
            seen_so: set[str] = set()
            for x in so:
                s = str(x).strip()[:200]
                if not s:
                    continue
                k = s.lower()
                if k in seen_so:
                    continue
                seen_so.add(k)
                listing_sticker_options.append({"name": s, "price": None, "label": s})
        listing_sticker_options = listing_sticker_options[:80]

        sec = pj.get("sticker_exterior_color")
        if isinstance(sec, str) and sec.strip():
            sticker_exterior_color = re.sub(r"^:\s*", "", sec.strip())[:120]
        sic = pj.get("sticker_interior_color")
        if isinstance(sic, str) and sic.strip():
            sticker_interior_color = re.sub(r"^:\s*", "", sic.strip())[:120]
        sim = pj.get("sticker_interior_material")
        if isinstance(sim, str) and sim.strip():
            sticker_interior_material = sim.strip()[:120]

        ss = pj.get("sticker_specs")
        if isinstance(ss, dict):
            for label, val in ss.items():
                if not isinstance(label, str) or not isinstance(val, str):
                    continue
                s = re.sub(r"^:\s*", "", val.strip())
                if not s:
                    continue
                sticker_spec_lines.append({"label": label.strip()[:40], "value": s[:160]})
        if not sticker_spec_lines:
            for label, key in (
                ("Engine", "sticker_engine_display"),
                ("Transmission", "sticker_transmission"),
                ("Drivetrain", "sticker_drivetrain"),
                ("Doors", "sticker_doors"),
                ("Seating", "sticker_seating"),
                ("Tires", "sticker_tires"),
                ("Wheels", "sticker_wheels"),
            ):
                val = pj.get(key)
                if isinstance(val, str) and val.strip():
                    v = re.sub(r"^:\s*", "", val.strip())
                    if v:
                        sticker_spec_lines.append({"label": label, "value": v[:160]})

        _mk = str(car.get("make") or "").strip().lower()
        _hide_possible = hide_photo_analysis and _mk in {"jeep", "chrysler", "dodge", "ram"}
        if not _hide_possible and not listing_packages_sections:
            for raw in pj.get("possible_packages") or []:
                if not isinstance(raw, str):
                    continue
                label = raw.strip()[:200]
                if not label:
                    continue
                low = label.lower()
                if low in seen_titles:
                    continue
                seen_titles.add(low)
                listing_possible_packages.append(label)
        else:
            listing_possible_packages = []

        seen_sf: set[str] = set()
        sf = pj.get("standalone_features_from_description")
        if isinstance(sf, list):
            for x in sf:
                s = str(x).strip()[:200]
                if not s:
                    continue
                k = s.lower()
                if k in seen_sf:
                    continue
                seen_sf.add(k)
                listing_standalone_features.append(s)
        listing_standalone_features = listing_standalone_features[:40]

        if listing_sticker_options:
            try:
                from backend.scanner.window_sticker import (
                    build_sticker_option_dedupe_keys,
                    normalize_option_dedupe_key,
                    option_name_overlaps_sticker,
                )

                sticker_keys = build_sticker_option_dedupe_keys(listing_sticker_options)
                base_sec = (listing_sticker_option_sections or {}).get("base") or {}
                for feat in base_sec.get("features") or []:
                    if isinstance(feat, dict):
                        fn = str(feat.get("name") or "").strip()
                        if fn:
                            sticker_keys.add(normalize_option_dedupe_key(fn))
                if sticker_keys:
                    filtered_sections: list[dict[str, Any]] = []
                    for sec in listing_packages_sections:
                        title = str(sec.get("name") or "").strip()
                        if title and option_name_overlaps_sticker(title, sticker_keys):
                            continue
                        feats = [
                            f
                            for f in (sec.get("features") or [])
                            if isinstance(f, str)
                            and not option_name_overlaps_sticker(f, sticker_keys)
                        ]
                        evidence = [
                            e
                            for e in (sec.get("evidence") or [])
                            if isinstance(e, str) and str(e).strip()
                        ]
                        if not feats and not evidence:
                            continue
                        sec_copy = dict(sec)
                        sec_copy["features"] = feats[:30]
                        sec_copy["evidence"] = evidence[:8]
                        filtered_sections.append(sec_copy)
                    listing_packages_sections = filtered_sections
                    listing_monroney_options = [
                        x
                        for x in listing_monroney_options
                        if not option_name_overlaps_sticker(x, sticker_keys)
                    ]
                    listing_standalone_features = [
                        x
                        for x in listing_standalone_features
                        if not option_name_overlaps_sticker(x, sticker_keys)
                    ]
                    listing_monroney_standard = [
                        x
                        for x in listing_monroney_standard
                        if not option_name_overlaps_sticker(x, sticker_keys)
                    ]
            except Exception:
                pass

        seen_obs: set[str] = set()
        obs = pj.get("observed_features")
        if isinstance(obs, list) and not hide_photo_analysis:
            for x in obs:
                s = str(x).strip()[:200]
                if not s:
                    continue
                k = s.lower()
                if k in seen_obs:
                    continue
                seen_obs.add(k)
                listing_observed_features.append(s)
        listing_observed_features = listing_observed_features[:40]

        try:
            from backend.vision.equipment_vision import collect_photo_detected_equipment

            photo_only = [] if hide_photo_analysis else collect_photo_detected_equipment(pj)
        except Exception:
            photo_only = [] if hide_photo_analysis else list(dict.fromkeys(
                listing_observed_features + listing_possible_packages
            ))[:48]
        try:
            from backend.utils.listing_description_extract import collect_equipment_options_from_packages

            listing_photo_detected_equipment = collect_equipment_options_from_packages(
                pj,
                photo_equipment=photo_only,
                include_vision=not hide_photo_analysis,
            )
        except Exception:
            listing_photo_detected_equipment = photo_only
    spec_raw = car.get("spec_source_json")
    if spec_raw and str(spec_raw).strip():
        try:
            sj = json.loads(spec_raw) if isinstance(spec_raw, str) else spec_raw
        except (TypeError, ValueError, json.JSONDecodeError):
            sj = None
        if isinstance(sj, dict):
            ic = sj.get("interior_color")
            if isinstance(ic, dict) and str(ic.get("source") or "").strip().lower() == "listing_description":
                interior_from_listing_description = True
            if isinstance(ic, dict) and str(ic.get("source") or "").strip().lower() == "llava_vision":
                interior_from_llava_vision = True
            icv = sj.get("interior_cabin_vision")
            if isinstance(icv, dict) and str(icv.get("source") or "").strip().lower() == "llava_vision":
                interior_from_llava_vision = True

    packages_panel_has_content = bool(
        listing_packages_sections
        or listing_standalone_features
        or listing_observed_features
        or listing_photo_detected_equipment
        or listing_monroney_options
        or listing_monroney_standard
        or listing_sticker_options
        or listing_possible_packages
        or sticker_exterior_color
        or sticker_interior_color
        or sticker_interior_material
        or sticker_spec_lines
        or llava_interior_section
    )

    return {
        "gallery_images": urls,
        "verified_specs": verified,
        "listing_packages_sections": listing_packages_sections,
        "listing_standalone_features": listing_standalone_features,
        "listing_observed_features": listing_observed_features,
        "listing_monroney_options": listing_monroney_options,
        "listing_monroney_standard": listing_monroney_standard,
        "listing_sticker_options": listing_sticker_options,
        "listing_sticker_option_groups": listing_sticker_option_groups,
        "listing_sticker_option_sections": listing_sticker_option_sections,
        "listing_possible_packages": listing_possible_packages,
        "listing_photo_detected_equipment": listing_photo_detected_equipment,
        "sticker_exterior_color": sticker_exterior_color,
        "sticker_interior_color": sticker_interior_color,
        "sticker_interior_material": sticker_interior_material,
        "sticker_spec_lines": sticker_spec_lines,
        "interior_from_listing_description": interior_from_listing_description,
        "interior_from_llava_vision": interior_from_llava_vision,
        "packages_panel_has_content": packages_panel_has_content,
        "llava_interior_section": llava_interior_section,
        "hide_photo_analysis": hide_photo_analysis,
    }
