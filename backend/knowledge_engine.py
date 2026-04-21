"""
Automotive Knowledge Engine: EPA master data + regex trim decoder.
Fills cylinders, gears, drivetrain when dealer data is missing or generic.
"""
from __future__ import annotations

import os
import re
import sqlite3
from typing import Any

DB_PATH = os.environ.get("INVENTORY_DB_PATH", "inventory.db")


def _conn():
    return sqlite3.connect(DB_PATH)


def _bmw_has_30i_suffix(blob: str) -> bool:
    """
    330i / 530i / xDrive30i / sDrive30i — ``\\d30I`` matches three-series sedans;
    SAV trims glue ``30I`` after xDrive/sDrive (``E30I`` not ``\\d30I``).
    """
    u = (blob or "").upper()
    if re.search(r"\d30I\b", u):
        return True
    return bool(re.search(r"(?:XDRIVE|SDRIVE)30I\b", u))


def _bmw_has_40i_suffix(blob: str) -> bool:
    """
    40i motors: 740i (740I), M340i (M340I), xDrive40i (E40I in XDRIVE40I).
    \\b40I\\b fails when 40i is glued to xDrive (E and 4 are both \\w).
    """
    if re.search(r"\d40I\b", blob):
        return True
    return bool(re.search(r"\D40I\b", blob))


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
        # 30i: 330i, 430i, 530i, xDrive30i — 4 cyl
        elif _bmw_has_30i_suffix(blob) or re.search(
            r"\b(230I|330I|430I|530I|630I|730I)\b", blob
        ):
            out["cylinders"] = 4
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
    ]:
        if col not in have:
            try:
                cur.execute(f"ALTER TABLE epa_master ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError:
                pass
    conn.commit()


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
    if cyl is not None and int(cyl) > 0 and disp is not None and float(disp) > 0:
        n = int(cyl)
        layout = {3: "I3", 4: "I4", 5: "I5", 6: "V6", 8: "V8", 10: "V10", 12: "V12"}.get(
            n, f"{n}-cyl"
        )
        return f"{float(disp):.1f}L {layout} (EPA mode aggregate)"
    if cyl is not None and int(cyl) > 0:
        n = int(cyl)
        layout = {3: "I3", 4: "I4", 5: "I5", 6: "V6", 8: "V8", 10: "V10", 12: "V12"}.get(
            n, f"{n}-cyl"
        )
        return f"{layout} (EPA mode aggregate)"
    return None


def format_fuel_economy_display(epa: dict[str, Any], is_bev: bool) -> str | None:
    if is_bev:
        ce = epa.get("city_e")
        he = epa.get("highway_e")
        if ce is not None and he is not None and (ce > 0 or he > 0):
            return f"{round(ce)} MPGe City / {round(he)} MPGe Hwy"
        return None
    c = epa.get("city08")
    h = epa.get("highway08")
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


def lookup_epa_aggregate(year: int | None, make: str | None, model: str | None) -> dict[str, Any]:
    """
    Mode row from epa_master: cylinders, drive, transmission, MPG, displacement, atv_type.
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
    if v is None:
        return True
    if isinstance(v, (int, float)):
        return False
    from backend.utils.field_clean import is_effectively_empty

    if is_effectively_empty(v):
        return True
    s = str(v).strip().upper()
    return s in ("N/A", "NA", "UNKNOWN", "NULL")


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
    epa = lookup_epa_aggregate(y, make, model)

    cyl_ver = regex.get("cylinders")
    if cyl_ver is None:
        cyl_ver = epa.get("cylinders")
    if cyl_ver is None:
        di = _int_or_none(dealer_cyl)
        if di is not None:
            cyl_ver = di

    drive_ver = regex.get("drivetrain") or epa.get("drivetrain")
    if not drive_ver and dealer_drive and not _is_na_spec(dealer_drive):
        drive_ver = dealer_drive

    gears_ver = regex.get("gears") or epa.get("gears")
    trans_raw = epa.get("transmission") or (
        dealer_trans if dealer_trans and not _is_na_spec(dealer_trans) else None
    )
    trans_ver = format_transmission_display(trans_raw) or trans_raw

    dealer_cyl_i = _int_or_none(dealer_cyl)
    # Prefer dealer when present and valid; else regex/EPA aggregate
    if dealer_cyl_i is not None and dealer_cyl_i > 0:
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
    is_bev = (
        regex.get("cylinders") == 0
        or (regex.get("fuel_type_hint") or "").strip().lower() == "electric"
        or (epa.get("atv_type") or "").strip().upper() == "EV"
        or "electric" in (epa.get("fuel_type") or "").lower()
    )
    if is_bev and not trans_ver and _is_na_spec(dealer_trans):
        trans_ver = "Single-speed automatic"
    elif (
        not is_bev
        and not trans_ver
        and _is_na_spec(dealer_trans)
        and (regex.get("transmission_hint") or "").strip()
    ):
        trans_ver = str(regex["transmission_hint"]).strip()
    body_style_display = None
    if _is_na_spec(car.get("body_style")) and regex.get("body_style_hint"):
        body_style_display = regex["body_style_hint"]
    fuel_economy_display = format_fuel_economy_display(epa, is_bev)
    if not fuel_economy_display:
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
        "epa_displacement": epa.get("displacement"),
        "body_style_display": body_style_display,
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
    urls = [u for u in g if u and isinstance(u, str)]
    if not urls and car.get("image_url"):
        urls = [car["image_url"]]
    verified = merge_verified_specs(car)

    listing_packages_sections: list[dict[str, Any]] = []
    listing_standalone_features: list[str] = []
    listing_observed_features: list[str] = []
    interior_from_listing_description = False
    interior_from_llava_vision = False
    llava_interior_section: dict[str, Any] | None = None

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

    if pj is not None:
        liv = pj.get("llava_interior_cabin")
        if isinstance(liv, dict) and liv:
            ib = liv.get("interior_buckets") or []
            bucket_list = [str(x).strip() for x in ib if str(x).strip()][:24]
            llava_interior_section = {
                "guess": str(liv.get("interior_guess_text") or "").strip()[:240],
                "buckets": bucket_list,
                "evidence": str(liv.get("evidence") or "").strip()[:400],
                "confidence": liv.get("confidence"),
            }
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
            seen_titles.add(low)
            feats = entry.get("features") or []
            feat_list = [str(x).strip() for x in feats if isinstance(x, str) and str(x).strip()]
            listing_packages_sections.append(
                {
                    "name": title,
                    "features": feat_list[:30],
                    "source": "listing",
                    "from_listing_description": True,
                    "from_vision": False,
                }
            )

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
            listing_packages_sections.append(
                {
                    "name": label,
                    "features": [],
                    "source": "vision_possible",
                    "from_listing_description": False,
                    "from_vision": True,
                }
            )

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

        seen_obs: set[str] = set()
        obs = pj.get("observed_features")
        if isinstance(obs, list):
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
        or llava_interior_section
    )

    return {
        "gallery_images": urls,
        "verified_specs": verified,
        "listing_packages_sections": listing_packages_sections,
        "listing_standalone_features": listing_standalone_features,
        "listing_observed_features": listing_observed_features,
        "interior_from_listing_description": interior_from_listing_description,
        "interior_from_llava_vision": interior_from_llava_vision,
        "packages_panel_has_content": packages_panel_has_content,
        "llava_interior_section": llava_interior_section,
    }
