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
    """330i / 530i / xDrive30i — digit + 30I at end of motor code."""
    return bool(re.search(r"\d30I\b", blob))


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
        # M50i / M60i → V8
        elif re.search(r"\b(M50I|M60I)\b", blob):
            out["cylinders"] = 8
        # 760i, etc. (60i V12 / V8 naming — rule: 8 cyl per product spec)
        elif re.search(r"\d60I\b", blob) and not re.search(r"\bM60I\b", blob):
            out["cylinders"] = 8
        # 550i, 750i, Alpina B7 (50i) — not M50i/M60i
        elif re.search(r"\d50I\b", blob) and not re.search(r"\b(M50I|M60I)\b", blob):
            out["cylinders"] = 8
        # X5/X7/5 Series/7 Series + 40i / M40i (mild-hybrid I6)
        elif _bmw_model_is_x5_x7_or_5_7_series(model) and (
            _bmw_has_40i_suffix(blob) or re.search(r"\bM40I\b", blob)
        ):
            out["cylinders"] = 6
            out["fuel_type_hint"] = "Gas / Mild Hybrid"
        # 30i: 330i, 430i, 530i, xDrive30i — 4 cyl
        elif _bmw_has_30i_suffix(blob) or re.search(
            r"\b(230I|330I|430I|530I|630I|730I)\b", blob
        ):
            out["cylinders"] = 4
        # Remaining 40i / M40i: 540i, 740i, M340i, xDrive40i — 6 cyl + mild hybrid
        elif _bmw_has_40i_suffix(blob) or re.search(r"\bM40I\b", blob):
            out["cylinders"] = 6
            out["fuel_type_hint"] = "Gas / Mild Hybrid"

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
    m = re.match(r"Auto\s*\(\s*S(\d+)\s*\)", s, re.I)
    if m:
        return f"{int(m.group(1))}-Speed Automatic"
    m = re.match(r"Auto\s*\(\s*AM-S(\d+)\s*\)", s, re.I)
    if m:
        return f"{int(m.group(1))}-Speed DCT"
    m = re.match(r"Auto\s*\(\s*A(\d+)\s*\)", s, re.I)
    if m:
        return f"{int(m.group(1))}-Speed Automatic"
    m = re.match(r"Auto\s*\(\s*CVT\s*\)", s, re.I)
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
    if re.search(r"\b(50E|45E|40E)\b", u):
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
            return f"{prefix}{desc}, {hp} HP / {tq} lb-ft"
        if regex.get("cylinders") == 0 or re.search(r"\b(I4|I5|I7|IX)\b", u):
            return "Electric motor(s) — output per manufacturer; see MPGe below."

    return "See manufacturer specifications"


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
        cur.execute(
            """
            SELECT cylinders, drive, trany, COUNT(*) AS n,
                   AVG(displacement) AS disp,
                   AVG(city08) AS c08,
                   AVG(highway08) AS h08,
                   AVG(city_e) AS ce,
                   AVG(highway_e) AS he,
                   GROUP_CONCAT(DISTINCT atv_type) AS atv_cat,
                   GROUP_CONCAT(DISTINCT fuel_type) AS fuel_cat
            FROM epa_master
            WHERE year = ? AND lower(make) = lower(?) AND lower(model) = lower(?)
            GROUP BY cylinders, drive, trany
            ORDER BY n DESC
            LIMIT 1
            """,
            (year, make.strip(), model.strip()),
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
    if v is None:
        return True
    s = str(v).strip().upper()
    return s in ("", "N/A", "NA", "—", "-", "UNKNOWN", "NULL")


def merge_verified_specs(car: dict[str, Any]) -> dict[str, Any]:
    """
    Combine dealer row with regex decoder + EPA lookup.
    Prefer: regex (brand trim) > EPA aggregate > dealer fields.
    When dealer omits or sends N/A, show inferred values as verified.
    """
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

    regex = decode_trim_logic(make, model, trim, title)
    epa = lookup_epa_aggregate(y, make, model)

    cyl_ver = regex.get("cylinders")
    if cyl_ver is None:
        cyl_ver = epa.get("cylinders")
    if cyl_ver is None:
        di = _int_or_none(dealer_cyl)
        if di is not None:
            cyl_ver = di

    drive_ver = regex.get("drivetrain") or epa.get("drivetrain")
    if not drive_ver and dealer_drive and dealer_drive.upper() not in ("N/A", "NA"):
        drive_ver = dealer_drive

    gears_ver = regex.get("gears") or epa.get("gears")
    trans_raw = epa.get("transmission") or (
        dealer_trans if dealer_trans and dealer_trans.upper() != "N/A" else None
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

    # Regex/EPA first so xDrive/4MATIC in title wins over dealer "N/A"
    display_drive = drive_ver or dealer_drive or ""
    if _is_na_spec(dealer_drive) or (dealer_drive or "").strip().upper() in ("N/A", "NA", ""):
        if drive_ver:
            display_drive = drive_ver
    elif (display_drive or "").strip().upper() in ("N/A", "NA", "") and drive_ver:
        display_drive = drive_ver

    drivetrain_verified = bool(
        drive_ver
        and (display_drive or "").strip().upper() == (drive_ver or "").strip().upper()
        and (
            _is_na_spec(dealer_drive)
            or (dealer_drive or "").strip().upper() in ("N/A", "NA", "")
        )
    )

    blob_full = f"{title} {trim} {model}".strip()
    display_drive_ui = _drivetrain_ui_label(display_drive, make, blob_full)
    is_bev = (
        regex.get("cylinders") == 0
        or (regex.get("fuel_type_hint") or "").strip().lower() == "electric"
        or (epa.get("atv_type") or "").strip().upper() == "EV"
        or "electric" in (epa.get("fuel_type") or "").lower()
    )
    fuel_economy_display = format_fuel_economy_display(epa, is_bev)
    master_engine_string = build_master_engine_string(make, model, trim, title, regex, epa)

    sources = []
    if (
        regex.get("cylinders") is not None
        or regex.get("drivetrain")
        or regex.get("fuel_type_hint")
    ):
        sources.append("Trim decoder")
    if epa.get("cylinders") is not None or epa.get("drivetrain") or epa.get("gears"):
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
    return {"gallery_images": urls, "verified_specs": verified}
