#!/usr/bin/env python3
"""Scan lab: per-dealer log of timing, scan issues, incomplete fields and
dictionary-vs-dealer discrepancies for a controlled set of dealers.

Two phases, same dealers:

  --phase pre   snapshot the dealer's active rows BEFORE the scan (counts, missing
                field tally, discrepancy tally) -> <out>/pre.json
  --phase post  after the scan: scan_runs timing per dealer, WARNING/ERROR lines
                from the scanner log, the same tallies on rows written by this
                run, and a before/after diff -> <out>/<dealer>.md, <out>/README.md,
                <out>/post.json

"Dictionary" = the reference stores the site renders from: the vPIC VIN decode
(nhtsa_vpic_cache), the linked catalog row (cars.epa_master_id -> epa_master)
and the car's own engine text. A discrepancy is logged, never fixed here.

Usage:
  python -m backend.scripts.scan_lab_report --phase pre  --dealers a,b --out workspace/scan_lab/lab_YYYYMMDD
  python -m backend.scripts.scan_lab_report --phase post --dealers a,b --out ... \
      --since 2026-09-22T17:00:00+00:00 --log workspace/scanlogs/lab_YYYYMMDD.log
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.catalog.resolver import _drive_bucket, _fuel_bucket  # noqa: E402
from backend.db import inventory_pg  # noqa: E402
from backend.db.inventory_db import get_conn as inv_get_conn  # noqa: E402
from backend.enrichment.knowledge_engine import (  # noqa: E402
    _conn as ref_conn,
    lookup_epa_master_by_id,
    merge_verified_specs,
    prime_vpic_cache,
)
from backend.utils.engine_consistency import (  # noqa: E402
    catalog_row_conflicts_with_engine_text,
    cylinders_from_engine_text,
    liters_from_engine_text,
)
from backend.utils.model_aliases import makes_equivalent, models_equivalent  # noqa: E402
from backend.utils.listing_completeness import (  # noqa: E402
    INCOMPLETE_FIELD_LABELS,
    listing_missing_field_codes,
)

DISCREPANCY_LABELS: dict[str, str] = {
    "vpic_make": "make differs from VIN decode",
    "vpic_model": "model differs from VIN decode",
    "vpic_year": "year differs from VIN decode",
    "vpic_cylinders_vs_column": "stored cylinders differ from VIN decode",
    "vpic_cylinders_vs_text": "dealer engine text cylinders differ from VIN decode",
    "vpic_displacement_vs_text": "dealer engine text liters differ from VIN decode",
    "vpic_fuel": "fuel type differs from VIN decode",
    "vpic_drive": "drivetrain differs from VIN decode",
    "vpic_trim": "trim differs from VIN decode (informational)",
    "catalog_engine": "linked catalog row contradicts dealer engine text",
    "catalog_drive": "linked catalog row drivetrain differs",
    "catalog_fuel": "linked catalog row fuel differs",
    "catalog_year": "linked catalog row year differs",
    "catalog_trim": "linked catalog row trim differs (informational)",
    "catalog_unlinked": "no catalog link (epa_master_id NULL)",
    "column_cylinders_vs_text": "stored cylinders contradict dealer engine text",
    "electric_label_combustion_text": "fuel says electric, engine text has liters",
    "trim_quarantined": "trim held in cars_trim_quarantine",
}
INFORMATIONAL = {"vpic_trim", "catalog_trim", "catalog_unlinked", "trim_quarantined", "catalog_year",
                 # the EPA row is model-level (one drive/fuel per model line); the VIN decode
                 # outranks it, so a catalog mismatch is a note, not an inaccuracy (2026-09-26:
                 # 80 / 76 dealers were "inaccurate" on nothing but these)
                 "catalog_drive", "catalog_fuel"}  # catalog_year: EPA lags a model year; the link borrows the prior year on purpose

_CAR_COLS = (
    "id, vin, year, make, model, trim, price, mileage, fuel_type, cylinders, transmission, "
    "drivetrain, exterior_color, interior_color, image_url, gallery, dealer_id, dealer_name, "
    "stock_number, scraped_at, first_seen_at, source_url, body_style, engine_description, "
    "engine_l, condition, msrp, listing_active, listing_removed_at, epa_master_id, "
    "epa_match_confidence, spec_source_json, title, forced_induction"
)


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


def _rows(conn, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _to_float(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None


def _to_int(v: Any) -> int | None:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _vpic_raw(vins: list[str]) -> dict[str, dict[str, Any]]:
    """VIN -> selected raw vPIC fields (make/model/year included, which the
    normalized helper drops)."""
    keep = (
        "Make", "Model", "ModelYear", "Trim", "EngineCylinders", "DisplacementL",
        "FuelTypePrimary", "FuelTypeSecondary", "DriveType", "BodyClass", "ElectrificationLevel",
    )
    out: dict[str, dict[str, Any]] = {}
    todo = [v for v in vins if v]
    if not todo:
        return out
    conn = ref_conn()
    try:
        for start in range(0, len(todo), 800):
            batch = todo[start:start + 800]
            ph = ",".join(["?"] * len(batch))
            for vin, raw in conn.execute(
                f"SELECT vin, response_json FROM nhtsa_vpic_cache WHERE vin IN ({ph})", tuple(batch)
            ).fetchall():
                try:
                    r = json.loads(raw).get("Results", [{}])[0]
                except Exception:  # noqa: BLE001
                    continue
                out[vin] = {k: (str(r.get(k) or "").strip() or None) for k in keep}
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass
    return out


def _catalog_rows(ids: list[int]) -> dict[int, dict[str, Any]]:
    """epa_master rows by id, including trim/atv_type (the cached read-time
    lookup drops trim, which is the only hybrid marker on some EPA rows)."""
    out: dict[int, dict[str, Any]] = {}
    todo = sorted({int(i) for i in ids if i})
    if not todo:
        return out
    conn = ref_conn()
    try:
        for start in range(0, len(todo), 800):
            batch = todo[start:start + 800]
            ph = ",".join(["?"] * len(batch))
            cur = conn.execute(
                f"SELECT id, year, make, model, trim, cylinders, displacement, drive, fuel_type, atv_type "
                f"FROM epa_master WHERE id IN ({ph})",
                tuple(batch),
            )
            cols = [d[0] for d in cur.description]
            for r in cur.fetchall():
                row = dict(zip(cols, r))
                out[int(row["id"])] = row
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass
    return out


def _vpic_fuel_bucket(v: dict[str, Any]) -> str:
    """'' when vPIC does not state electrification: a blank or 'Mild HEV' level says
    nothing about whether the dealer's 'Hybrid' label is right."""
    el = (v.get("ElectrificationLevel") or "").upper()
    ft = _fuel_bucket(v.get("FuelTypePrimary"))
    if "PHEV" in el:
        return "hybrid"  # plug-in: vPIC lists FuelTypePrimary "Electric" for these
    if "BEV" in el or (ft == "ev" and not el):
        return "ev"
    if "MILD" in el:
        return ""
    if "HEV" in el:
        return "hybrid"
    if ft == "diesel":
        return "diesel"
    return ""


def _catalog_fuel_bucket(row: dict[str, Any]) -> str:
    """EPA fuel_type is the pump fuel; electrification lives in atv_type."""
    atv = str(row.get("atv_type") or "").lower()
    trim = str(row.get("trim") or "").lower()
    if atv == "ev" or ("electric" in atv and "hybrid" not in atv):
        return "ev"
    # EPA leaves atv_type blank on some hybrid rows (2026 Tacoma "Hybrid 4WD");
    # the trim text is the tie-breaker, not a mismatch.
    fuel = str(row.get("fuel_type") or "").lower()
    if "hybrid" in atv or "hybrid" in trim or "phev" in trim or "/ electricity" in fuel:
        return "hybrid"  # "Regular Gasoline / Electricity" = plug-in hybrid, EPA flags it EV
    return _fuel_bucket(row.get("fuel_type"))


def _drive_same(a: str, b: str) -> bool:
    if not a or not b:
        return True
    if {a, b} <= {"AWD", "4WD"}:
        return True  # EPA/vPIC split hairs dealers do not
    return a == b


def _quarantined_ids(conn, dealer_id: str) -> set[int]:
    try:
        rows = _rows(
            conn,
            "SELECT q.car_id FROM cars_trim_quarantine q JOIN cars c ON c.id = q.car_id WHERE c.dealer_id = ?",
            (dealer_id,),
        )
    except Exception:  # noqa: BLE001
        return set()
    return {int(r["car_id"]) for r in rows}


# --------------------------------------------------------------------------
# per-car evaluation
# --------------------------------------------------------------------------

def evaluate_car(car: dict[str, Any], vpic: dict[str, Any] | None, quarantined: bool,
                 catalog_row: dict[str, Any] | None = None) -> tuple[list[str], dict[str, str]]:
    """Return (missing field codes, {discrepancy code: detail})."""
    vs = merge_verified_specs(car, include_extended_specs=False)
    missing = listing_missing_field_codes(car, for_public_filter=False, detail_ctx={"verified_specs": vs})

    d: dict[str, str] = {}
    eng_text = car.get("engine_description") or ""
    text_l = liters_from_engine_text(eng_text)
    text_cyl = cylinders_from_engine_text(eng_text)
    col_cyl = _to_int(car.get("cylinders"))
    dealer_fuel = _fuel_bucket(car.get("fuel_type"))
    dealer_drive = _drive_bucket(car.get("drivetrain"))

    if vpic:
        vm, cm = _norm(vpic.get("Make")), _norm(car.get("make"))
        if vm and cm and vm != cm and vm not in cm and cm not in vm and not makes_equivalent(car.get("make"), vpic.get("Make")):
            d["vpic_make"] = f"dealer={car.get('make')!r} vpic={vpic.get('Make')!r}"
        if vpic.get("Model") and car.get("model") and not models_equivalent(
            car.get("make"), car.get("model"), car.get("trim"), vpic.get("Model")
        ):
            d["vpic_model"] = f"dealer={car.get('model')!r} vpic={vpic.get('Model')!r}"
        vy, cy = _to_int(vpic.get("ModelYear")), _to_int(car.get("year"))
        if vy and cy and vy != cy:
            d["vpic_year"] = f"dealer={cy} vpic={vy}"
        vcyl = _to_int(vpic.get("EngineCylinders"))
        if vcyl and col_cyl and vcyl != col_cyl:
            d["vpic_cylinders_vs_column"] = f"column={col_cyl} vpic={vcyl}"
        if vcyl and text_cyl and vcyl != text_cyl:
            d["vpic_cylinders_vs_text"] = f"text={text_cyl} ({eng_text[:40]!r}) vpic={vcyl}"
        vl = _to_float(vpic.get("DisplacementL"))
        if vl and text_l and abs(vl - text_l) > 0.15:
            d["vpic_displacement_vs_text"] = f"text={text_l}L ({eng_text[:40]!r}) vpic={vl}L"
        vf = _vpic_fuel_bucket(vpic)
        if vf and dealer_fuel and vf != dealer_fuel:
            d["vpic_fuel"] = f"dealer={car.get('fuel_type')!r} vpic={vpic.get('FuelTypePrimary')!r}/{vpic.get('ElectrificationLevel')!r}"
        vd = _drive_bucket(vpic.get("DriveType"))
        if not _drive_same(vd, dealer_drive):
            d["vpic_drive"] = f"dealer={car.get('drivetrain')!r} vpic={vpic.get('DriveType')!r}"
        vt, ct = _norm(vpic.get("Trim")), _norm(car.get("trim"))
        if vt and ct and vt not in ct and ct not in vt:
            d["vpic_trim"] = f"dealer={car.get('trim')!r} vpic={vpic.get('Trim')!r}"

    emid = car.get("epa_master_id")
    if emid:
        row = catalog_row if catalog_row is not None else (lookup_epa_master_by_id(emid) or {})
        # The link is made against VIN-backed facts (resolver.apply_vin_facts), so
        # judge it against those too: a catalog row that follows the VIN where the
        # dealer label is wrong is correct, and the dealer error is already logged
        # under vpic_drive / vpic_fuel.
        try:
            from backend.catalog.resolver import apply_vin_facts

            vin_car = apply_vin_facts(car)
        except Exception:  # noqa: BLE001
            vin_car = car
        dealer_fuel = _fuel_bucket(vin_car.get("fuel_type"))
        dealer_drive = _drive_bucket(vin_car.get("drivetrain"))
        if row:
            why = catalog_row_conflicts_with_engine_text(eng_text, row.get("displacement"), row.get("cylinders"))
            if why:
                d["catalog_engine"] = f"{why}: text={eng_text[:40]!r} row={row.get('displacement')}L/{row.get('cylinders')}cyl"
            rd = _drive_bucket(row.get("drive"))
            if not _drive_same(rd, dealer_drive):
                d["catalog_drive"] = f"car={vin_car.get('drivetrain')!r} row={row.get('drive')!r}"
            rf = _catalog_fuel_bucket(row)
            if rf and dealer_fuel and rf != dealer_fuel:
                d["catalog_fuel"] = f"car={vin_car.get('fuel_type')!r} row={row.get('fuel_type')!r}/{row.get('atv_type')!r}"
            ry, cy = _to_int(row.get("year")), _to_int(car.get("year"))
            if ry and cy and ry != cy:
                d["catalog_year"] = f"dealer={cy} row={ry}"
            rt, ct = _norm(row.get("trim")), _norm(car.get("trim"))
            if rt and ct and rt not in ct and ct not in rt:
                d["catalog_trim"] = f"dealer={car.get('trim')!r} row={row.get('trim')!r}"
    else:
        d["catalog_unlinked"] = f"{car.get('year')} {car.get('make')} {car.get('model')}"

    if col_cyl and text_cyl and col_cyl != text_cyl:
        d["column_cylinders_vs_text"] = f"column={col_cyl} text={text_cyl} ({eng_text[:40]!r})"
    if dealer_fuel == "ev" and text_l:
        d["electric_label_combustion_text"] = f"fuel={car.get('fuel_type')!r} text={eng_text[:40]!r}"
    if quarantined:
        d["trim_quarantined"] = f"trim={car.get('trim')!r}"
    return missing, d


# --------------------------------------------------------------------------
# tallies
# --------------------------------------------------------------------------

# Feeds whose replayed payload has no msrp key at all (findings doc section 2,
# P5: jazel, overfuel, wp_vehicles_index, autowall, dealermasters; chapman only
# on in-transit units). msrp on these is platform-genuine, not a gap.
NO_MSRP_PROVIDERS = frozenset({"jazel", "overfuel", "wp_vehicles_index", "autowall", "dealermasters"})
# Feeds with one image and no description, and no VDP recipe yet (P4).
NO_DESCRIPTION_GALLERY_PROVIDERS = frozenset({"chapman", "autowall", "wp_vehicles_index"})
_VIN_SHAPE_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")
_ALLOCATION_WMI = ("2T", "4T", "5T", "JT")
_VPIC_DERIVED_CODES = frozenset({"engine", "cylinders", "transmission", "drivetrain", "fuel_type", "body_style"})


def is_allocation_vin(vin: Any) -> bool:
    """Placeholder / allocation VIN: fails the VIN shape, or a Toyota / Lexus
    WMI with a letter in serial positions 13-17 (JTEABFAJ9V136AJ97,
    4T1DAACK1VU42O521). They decode with ErrorCode 1,400 and carry no stock
    number, msrp or photos until the unit lands (section 2, V4 / A3)."""
    v = str(vin or "").strip().upper()
    if not v:
        return False
    if not _VIN_SHAPE_RE.match(v):
        return True
    return v[:2] in _ALLOCATION_WMI and any(ch.isalpha() for ch in v[12:17])


def _is_ev(car: dict[str, Any], vpic: dict[str, Any] | None) -> bool:
    if _fuel_bucket(car.get("fuel_type")) == "ev":
        return True
    if vpic:
        el = str(vpic.get("ElectrificationLevel") or "").lower()
        ft = str(vpic.get("FuelTypePrimary") or "").lower()
        if "bev" in el or (ft == "electric" and "hybrid" not in el and "phev" not in el):
            return True
    return False


def _is_new(car: dict[str, Any]) -> bool:
    cond = str(car.get("condition") or "").strip().lower()
    return cond.startswith("new") or cond in ("ctp", "demo")


def fixable_missing(car: dict[str, Any], vpic: dict[str, Any] | None, missing: list[str],
                    provider_hint: str | None = None) -> tuple[list[str], dict[str, str]]:
    """Split ``missing`` (codes from listing_missing_field_codes) into the codes a
    parser / heal fix could fill and the ones section 2 of the findings doc
    calls legitimately null. Returns (fixable, {code: reason}). Pure.

    Exemptions: engine + cylinders on EVs; every vPIC-derived field and the
    stock number on allocation VINs; mileage on New rows; trim when the decode
    has a Trim / Series or the catalog says the model is trim-less
    (listing_missing_field_codes already applies the catalog rule).
    """
    exempt: dict[str, str] = {}
    ev = _is_ev(car, vpic)
    alloc = is_allocation_vin(car.get("vin"))
    new = _is_new(car)
    vpic_trim = ""
    if vpic:
        vpic_trim = str(vpic.get("Trim") or vpic.get("Series") or "").strip()
    for code in missing:
        if ev and code in ("engine", "cylinders"):
            exempt[code] = "ev"
        elif alloc and (code in _VPIC_DERIVED_CODES or code in ("stock_number", "msrp", "images", "vin")):
            exempt[code] = "allocation_vin"
        elif code == "mileage" and new:
            exempt[code] = "new_row"
        elif code == "trim" and vpic_trim:
            exempt[code] = f"vpic_trim:{vpic_trim[:40]}"
    fixable = [c for c in missing if c not in exempt]
    return fixable, exempt


def msrp_expected(car: dict[str, Any], provider_hint: str | None = None) -> bool:
    """msrp counts as a gap only on New rows, never on allocation VINs and never
    on providers whose feed carries no msrp key (section 2 / section 7)."""
    if not _is_new(car):
        return False
    if is_allocation_vin(car.get("vin")):
        return False
    return (provider_hint or "") not in NO_MSRP_PROVIDERS


def _provider_hint(conn, dealer_id: str) -> str | None:
    try:
        rows = _rows(conn, "SELECT provider_hint FROM dealer_recipes WHERE dealer_id = ? AND COALESCE(stale, 0) = 0 "
                           "ORDER BY id DESC LIMIT 1", (dealer_id,))
    except Exception:  # noqa: BLE001 - table absent (tests) or column missing
        return None
    return str(rows[0].get("provider_hint") or "") or None if rows else None


def tally_dealer(conn, dealer_id: str, since: str | None) -> dict[str, Any]:
    where = "dealer_id = ? AND COALESCE(listing_active, 1) = 1"
    params: list[Any] = [dealer_id]
    if since:
        where += " AND scraped_at >= ?"
        params.append(since)
    cars = _rows(conn, f"SELECT {_CAR_COLS} FROM cars WHERE {where} ORDER BY id", tuple(params))
    quarantined = _quarantined_ids(conn, dealer_id)
    vins = [c.get("vin") for c in cars]
    prime_vpic_cache(vins)
    raw = _vpic_raw([v for v in vins if v])
    catalog = _catalog_rows([c.get("epa_master_id") for c in cars])
    provider_hint = _provider_hint(conn, dealer_id)
    miss = Counter()
    miss_raw = Counter()
    exempt_counts = Counter()
    disc = Counter()
    ex_m: dict[str, list] = defaultdict(list)
    ex_d: dict[str, list] = defaultdict(list)
    incomplete_rows = 0
    incomplete_rows_raw = 0
    msrp_expected_rows = 0
    msrp_missing_rows = 0
    car_issues: list[dict[str, Any]] = []
    with inventory_pg.borrow_read_connection():
        for c in cars:
            m_raw, d = evaluate_car(
                c, raw.get(c.get("vin") or ""), int(c["id"]) in quarantined,
                catalog.get(int(c["epa_master_id"])) if c.get("epa_master_id") else None,
            )
            # Section 2 / 7 of docs/DATA_COMPLETENESS_FINDINGS_2026_09_28.md: count
            # only the gaps a fix could fill; the raw codes stay under *_raw.
            m, exempt = fixable_missing(c, raw.get(c.get("vin") or ""), m_raw, provider_hint)
            if msrp_expected(c, provider_hint):
                msrp_expected_rows += 1
                if not c.get("msrp"):
                    msrp_missing_rows += 1
            label = f"{c.get('year')} {c.get('make')} {c.get('model')} {c.get('trim') or ''}".strip()
            if m_raw:
                incomplete_rows_raw += 1
            if m:
                incomplete_rows += 1
            if m or d:
                car_issues.append({
                    "id": c.get("id"), "vin": c.get("vin"), "car": label,
                    "year": c.get("year"), "make": c.get("make"), "model": c.get("model"), "trim": c.get("trim"),
                    "price": c.get("price"), "fuel": c.get("fuel_type"), "drive": c.get("drivetrain"),
                    "engine": c.get("engine_description"), "url": c.get("source_url"),
                    "missing": m, "missing_exempt": exempt, "disc": d,
                })
            for code in m_raw:
                miss_raw[code] += 1
            for code, why in exempt.items():
                exempt_counts[f"{code}:{why.split(':', 1)[0]}"] += 1
            for code in m:
                miss[code] += 1
                if len(ex_m[code]) < 5:
                    ex_m[code].append({"vin": c.get("vin"), "car": label})
            for code, why in d.items():
                disc[code] += 1
                if len(ex_d[code]) < 5:
                    ex_d[code].append({"vin": c.get("vin"), "car": label, "why": why})
    active_total = _rows(
        conn, "SELECT COUNT(*) AS n FROM cars WHERE dealer_id = ? AND COALESCE(listing_active, 1) = 1", (dealer_id,)
    )[0]["n"]
    vdp_fields = _vdp_field_coverage(conn, dealer_id, since)
    return {
        "dealer_id": dealer_id,
        "active_total": int(active_total),
        "rows": len(cars),
        "vpic_cached": sum(1 for v in vins if v in raw),
        "incomplete_rows": incomplete_rows,
        "missing": dict(miss.most_common()),
        "missing_examples": ex_m,
        # raw counts before the section-2 exemptions, and what was exempted
        "incomplete_rows_raw": incomplete_rows_raw,
        "missing_raw": dict(miss_raw.most_common()),
        "missing_exempt": dict(exempt_counts.most_common()),
        "provider_hint": provider_hint,
        "msrp_expected_rows": msrp_expected_rows,
        "msrp_missing_rows": msrp_missing_rows,
        "discrepancies": dict(disc.most_common()),
        "discrepancy_examples": ex_d,
        "quarantined_trims": len(quarantined),
        "vdp_fields": vdp_fields,
        "car_issues": car_issues,
    }


def _vdp_field_coverage(conn, dealer_id: str, since: str | None) -> dict[str, int]:
    """Fields the SRP/feed never carries; they only arrive from a VDP visit.
    The incomplete index does not flag them, so count them here."""
    where = "dealer_id = ? AND COALESCE(listing_active, 1) = 1"
    params: list[Any] = [dealer_id]
    if since:
        where += " AND scraped_at >= ?"
        params.append(since)
    r = _rows(
        conn,
        f"""SELECT COUNT(*) AS rows,
              COUNT(*) FILTER (WHERE gallery IS NOT NULL AND gallery <> '' AND gallery <> '[]') AS gallery,
              COUNT(*) FILTER (WHERE image_url IS NOT NULL AND image_url <> '') AS image_url,
              COUNT(*) FILTER (WHERE carfax_url IS NOT NULL AND carfax_url <> '') AS carfax_url,
              COUNT(*) FILTER (WHERE description IS NOT NULL AND description <> '') AS description,
              COUNT(*) FILTER (WHERE packages IS NOT NULL AND packages <> '' AND packages <> '[]') AS packages,
              COUNT(*) FILTER (WHERE window_sticker_url IS NOT NULL AND window_sticker_url <> '') AS window_sticker_url,
              COUNT(*) FILTER (WHERE stock_number IS NOT NULL AND stock_number <> '') AS stock_number,
              COUNT(*) FILTER (WHERE msrp IS NOT NULL) AS msrp,
              COUNT(*) FILTER (WHERE interior_color IS NOT NULL AND interior_color <> '') AS interior_color
            FROM cars WHERE {where}""",
        tuple(params),
    )
    return {k: int(v or 0) for k, v in (r[0] if r else {}).items()}


# --------------------------------------------------------------------------
# post-phase inputs: scan_runs + log
# --------------------------------------------------------------------------

def log_vdp_visits(log_path: Path | None, dealer_name: str | None) -> int | None:
    """Count 'VDP: <name> — visiting' lines; scan_runs.vdps_visited is zeroed when
    the VDP phase hits its cap, this survives."""
    if not log_path or not dealer_name or not log_path.exists():
        return None
    needle = f"VDP: {dealer_name} — visiting"
    n = 0
    with log_path.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if needle in line:
                n += 1
    return n


def scan_runs_since(conn, dealer_id: str, since: str) -> list[dict[str, Any]]:
    rows = _rows(
        conn,
        "SELECT id, dealer_id, dealer_name, provider, finished_at, duration_seconds, inventory_rows, deduped_rows, "
        "upserted, vdps_visited, vehicles_vdp_enriched, error, summary_json "
        "FROM scan_runs WHERE dealer_id = ? AND finished_at >= ? ORDER BY id",
        (dealer_id, since),
    )
    for r in rows:
        try:
            r["summary"] = json.loads(r.pop("summary_json") or "{}")
        except (TypeError, ValueError):
            r["summary"] = {}
    return rows


_LOG_RE = re.compile(r"^(\S+ \S+) - (\w+) - (.*)$")
_NOTE_RE = re.compile(
    r"Recipe fetch|Recipes \[|VDP recipes|VDP prefetch|HTTP-only|Inventory paths|Inventory recovery|"
    r"Inventory feed sufficient|reconcile|low_coverage|description probe|phase cap|enrichment enabled|"
    r"VDP pool|Site profile|Intercepting|Dealer complete|phase summary",
    re.I,
)


def recipe_notes(log_path: Path | None, dealer_id: str, dealer_name: str | None, limit: int = 60) -> list[str]:
    """The log lines that say how this dealer was captured: which recipes replayed,
    what HTTP-first and the VDP recipes filled, what the browser was still asked to
    do, and the reconcile verdict. This is the raw material for improving recipes."""
    if not log_path or not log_path.exists():
        return []
    needles = [n for n in (dealer_id, dealer_name) if n]
    out: list[str] = []
    with log_path.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if not any(n in line for n in needles):
                continue
            if not _NOTE_RE.search(line):
                continue
            m = _LOG_RE.match(line.rstrip("\n"))
            msg = m.group(3) if m else line.strip()
            ts = (m.group(1)[11:19] if m else "")
            out.append(f"{ts} {msg[:260]}")
            if len(out) >= limit:
                break
    return out


def log_issues(log_path: Path, dealer_ids: list[str]) -> dict[str, Any]:
    """WARNING/ERROR lines per dealer (matched by dealer_id substring) + global ones."""
    per: dict[str, Counter] = {d: Counter() for d in dealer_ids}
    per_first: dict[str, dict[str, str]] = {d: {} for d in dealer_ids}
    glob = Counter()
    tracebacks = 0
    first_ts = last_ts = None
    if not log_path.exists():
        return {"missing_log": str(log_path)}
    with log_path.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if line.startswith("Traceback"):
                tracebacks += 1
            m = _LOG_RE.match(line)
            if not m:
                continue
            ts, level, msg = m.groups()
            first_ts = first_ts or ts
            last_ts = ts
            if level not in ("WARNING", "ERROR", "CRITICAL"):
                continue
            key = re.sub(r"\d+", "#", msg)[:110]
            hit = False
            for d in dealer_ids:
                if d in msg or d.split("-")[0] in msg:
                    per[d][f"{level} {key}"] += 1
                    per_first[d].setdefault(f"{level} {key}", msg[:300])
                    hit = True
            if not hit:
                glob[f"{level} {key}"] += 1
    return {
        "first_ts": first_ts,
        "last_ts": last_ts,
        "tracebacks": tracebacks,
        "per_dealer": {d: dict(c.most_common(15)) for d, c in per.items()},
        "per_dealer_first": per_first,
        "global": dict(glob.most_common(15)),
    }


# --------------------------------------------------------------------------
# rendering
# --------------------------------------------------------------------------

def _pct(n: int, d: int) -> str:
    return f"{(100.0 * n / d):.1f}%" if d else "-"


def _tally_table(t: dict[str, Any], key: str, labels: dict[str, str], ex_key: str) -> list[str]:
    out = ["| code | rows | share | example |", "|---|---:|---:|---|"]
    for code, n in t.get(key, {}).items():
        ex = t.get(ex_key, {}).get(code, [])
        first = ex[0] if ex else {}
        why = first.get("why", "")
        sample = f"{first.get('car', '')} `{first.get('vin', '')}` {why}".strip()
        out.append(f"| `{code}` {labels.get(code, '')} | {n} | {_pct(n, t['rows'])} | {sample[:160]} |")
    if len(out) == 2:
        out.append("| none | 0 | - | |")
    return out


def render_dealer(dealer_id: str, pre: dict[str, Any] | None, post: dict[str, Any], runs: list[dict[str, Any]],
                  issues: dict[str, Any], since: str, log_path: str | None = None) -> str:
    L = [f"# {dealer_id} — scan lab {since[:10]}", ""]
    L.append("## Timing (scan_runs)")
    if not runs:
        L.append(f"No scan_runs row finished after {since}. Scan did not complete for this dealer.")
    for r in runs:
        s = r["summary"]
        ph = s.get("phase_secs") or {}
        rec = s.get("reconcile") or {}
        cov = s.get("recipe_coverage")
        L += [
            "",
            f"- run `{r['id']}` provider `{r.get('provider')}` finished {r.get('finished_at')} in **{(r.get('duration_seconds') or 0) / 60:.1f} min**",
            f"- phases (s): " + ", ".join(f"{k}={v}" for k, v in ph.items()),
            f"- inventory rows {r.get('inventory_rows')} → deduped {r.get('deduped_rows')} → upserted {r.get('upserted')}; "
            f"VDPs visited {r.get('vdps_visited')} (log shows {r.get('vdp_visits_log')}), VDP-enriched {r.get('vehicles_vdp_enriched')}",
            f"- reconcile: {json.dumps(rec)}",
            f"- recipe coverage: {json.dumps(cov)}",
            f"- recovery strategy: {s.get('recovery_winning_strategy')}",
        ]
        if r.get("error"):
            L.append(f"- **error:** `{str(r['error'])[:300]}`")
    cc = (runs[-1]["summary"].get("capture_coverage") if runs else None) or {}
    if cc.get("n"):
        L += ["", f"## Capture coverage before upsert ({cc['n']} rows, what the network delivered this run)", "",
              "| field | share |", "|---|---:|"]
        for k, val in cc.items():
            if k in ("n", "missing_examples"):
                continue
            ex = (cc.get("missing_examples") or {}).get(k) or []
            L.append(f"| {k} | {float(val) * 100:.1f}%{'  (missing e.g. ' + ', '.join(ex) + ')' if ex and float(val) < 1 else ''} |")
        pf = runs[-1]["summary"].get("vdp_prefetch") or {}
        if pf:
            L += ["", f"- prefetch: {json.dumps(pf, default=str)[:600]}"]
        L.append(f"- recipe_fetch: {runs[-1]['summary'].get('recipe_fetch')}; http_only: {runs[-1]['summary'].get('http_only')}")
    notes = recipe_notes(Path(log_path) if log_path else None, dealer_id, runs[-1].get("dealer_name") if runs else None)
    if notes:
        L += ["", "## Recipe notes (capture path, from the scanner log)", ""]
        L += [f"- `{n}`" for n in notes]
    L += ["", "## Scan issues (log WARNING/ERROR lines naming this dealer)"]
    pd = issues.get("per_dealer", {}).get(dealer_id, {})
    if not pd:
        L.append("none")
    for k, n in pd.items():
        L.append(f"- {n}× {k}")
    L += ["", "## Inventory before / after"]
    if pre:
        L += [
            f"- active rows before: **{pre['active_total']}**, after: **{post['active_total']}**",
            f"- rows written by this run: **{post['rows']}** (VIN decode cached for {post['vpic_cached']})",
            f"- incomplete rows before (all active): {pre['incomplete_rows']}/{pre['rows']} ({_pct(pre['incomplete_rows'], pre['rows'])}); "
            f"after (rows from this run): {post['incomplete_rows']}/{post['rows']} ({_pct(post['incomplete_rows'], post['rows'])})",
        ]
    else:
        L.append(f"- active rows after: **{post['active_total']}**; rows from this run: **{post['rows']}**")
    L += ["", "## Incomplete fields (rows written by this run)"]
    L += _tally_table(post, "missing", INCOMPLETE_FIELD_LABELS, "missing_examples")
    if pre:
        L += ["", "Before (all active rows, same codes):", ""]
        L += _tally_table(pre, "missing", INCOMPLETE_FIELD_LABELS, "missing_examples")
    L += ["", "## VDP-only fields (not flagged by the incomplete index)", "",
          "| field | rows filled | share |", "|---|---:|---:|"]
    vf = post.get("vdp_fields") or {}
    pvf = (pre or {}).get("vdp_fields") or {}
    for k, v in vf.items():
        if k == "rows":
            continue
        before = f" (before {_pct(pvf.get(k, 0), pvf.get('rows', 0))})" if pvf else ""
        L.append(f"| {k} | {v} | {_pct(v, vf.get('rows', 0))}{before} |")
    L += ["", "## Dictionary vs dealer discrepancies (rows written by this run)",
          "", "Codes marked informational are not errors by themselves (trim wording, unlinked catalog).", ""]
    L += _tally_table(post, "discrepancies", DISCREPANCY_LABELS, "discrepancy_examples")
    if pre:
        L += ["", "Before (all active rows):", ""]
        L += _tally_table(pre, "discrepancies", DISCREPANCY_LABELS, "discrepancy_examples")
    L += ["", "## Examples", ""]
    for code, exs in post.get("discrepancy_examples", {}).items():
        if code in INFORMATIONAL:
            continue
        L.append(f"### `{code}` — {DISCREPANCY_LABELS.get(code, '')}")
        for e in exs:
            L.append(f"- {e['car']} `{e['vin']}`: {e['why']}")
        L.append("")
    return "\n".join(L) + "\n"


def render_summary(dealers: list[str], pre_all: dict[str, Any], post_all: dict[str, Any],
                   runs_all: dict[str, list], issues: dict[str, Any], since: str) -> str:
    L = [f"# Scan lab {since[:10]} — summary", "",
         f"Log window: {issues.get('first_ts')} → {issues.get('last_ts')}; tracebacks in log: {issues.get('tracebacks', 0)}", "",
         "| dealer | provider | minutes | inv rows | upserted | VDPs | active before→after | incomplete before→after | gallery filled | hard discrepancies (rows) | error |",
         "|---|---|---:|---:|---:|---:|---|---|---:|---:|---|"]
    for d in dealers:
        runs = runs_all.get(d, [])
        r = runs[-1] if runs else {}
        pre = pre_all.get(d) or {}
        post = post_all[d]
        hard = sum(n for c, n in post["discrepancies"].items() if c not in INFORMATIONAL)
        L.append(
            f"| {d} | {r.get('provider') or '-'} | {((r.get('duration_seconds') or 0) / 60):.1f} | {r.get('inventory_rows', '-')} | "
            f"{r.get('upserted', '-')} | {r.get('vdp_visits_log', r.get('vdps_visited', '-'))} | {pre.get('active_total', '-')}→{post['active_total']} | "
            f"{_pct(pre.get('incomplete_rows', 0), pre.get('rows', 0))}→{_pct(post['incomplete_rows'], post['rows'])} | "
            f"{_pct((post.get('vdp_fields') or {}).get('gallery', 0), post['rows'])} | {hard} | {str(r.get('error') or '')[:60]} |"
        )
    L += ["", "## Missing fields across the run (rows written by this run)", ""]
    tot = Counter()
    rows = 0
    for d in dealers:
        tot.update(post_all[d]["missing"])
        rows += post_all[d]["rows"]
    L += ["| field | rows | share |", "|---|---:|---:|"]
    for code, n in tot.most_common():
        L.append(f"| {INCOMPLETE_FIELD_LABELS.get(code, code)} | {n} | {_pct(n, rows)} |")
    L += ["", "## Discrepancies across the run", "", "| code | rows | share |", "|---|---:|---:|"]
    tot = Counter()
    for d in dealers:
        tot.update(post_all[d]["discrepancies"])
    for code, n in tot.most_common():
        flag = " (info)" if code in INFORMATIONAL else ""
        L.append(f"| `{code}` {DISCREPANCY_LABELS.get(code, '')}{flag} | {n} | {_pct(n, rows)} |")
    L += ["", "## Global log issues (not tied to one dealer)", ""]
    for k, n in issues.get("global", {}).items():
        L.append(f"- {n}× {k}")
    L += ["", "Per-dealer detail: " + ", ".join(f"[{d}]({d}.md)" for d in dealers), ""]
    return "\n".join(L)


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--phase", choices=("pre", "post"), required=True)
    ap.add_argument("--dealers", required=True, help="comma-separated dealer ids")
    ap.add_argument("--out", required=True, help="output directory")
    ap.add_argument("--since", help="post: ISO timestamp the scan started (UTC)")
    ap.add_argument("--log", help="post: scanner log file")
    args = ap.parse_args()

    dealers = [d.strip() for d in args.dealers.split(",") if d.strip()]
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    conn = inv_get_conn()
    try:
        if args.phase == "pre":
            pre = {d: tally_dealer(conn, d, None) for d in dealers}
            for d in dealers:
                pre[d].pop("car_issues", None)
            pre["_taken_at"] = datetime.now(timezone.utc).isoformat()
            (out / "pre.json").write_text(json.dumps(pre, indent=1, default=str), encoding="utf-8")
            for d in dealers:
                t = pre[d]
                print(f"{d:32s} active={t['active_total']:5d} incomplete={t['incomplete_rows']:5d} "
                      f"hard_disc={sum(n for c, n in t['discrepancies'].items() if c not in INFORMATIONAL):5d}")
            return 0

        if not args.since:
            ap.error("--since is required for --phase post")
        pre_all: dict[str, Any] = {}
        if (out / "pre.json").exists():
            pre_all = json.loads((out / "pre.json").read_text(encoding="utf-8"))
        issues = log_issues(Path(args.log), dealers) if args.log else {}
        post_all: dict[str, Any] = {}
        runs_all: dict[str, list] = {}
        for d in dealers:
            post_all[d] = tally_dealer(conn, d, args.since)
            runs_all[d] = scan_runs_since(conn, d, args.since)
            for r in runs_all[d]:
                r["vdp_visits_log"] = log_vdp_visits(Path(args.log) if args.log else None, r.get("dealer_name"))
            (out / f"{d}.md").write_text(
                render_dealer(d, pre_all.get(d), post_all[d], runs_all[d], issues, args.since, args.log), encoding="utf-8"
            )
        (out / "cars.json").write_text(
            json.dumps({d: post_all[d].pop("car_issues", []) for d in dealers}, indent=0, default=str), encoding="utf-8"
        )
        (out / "post.json").write_text(
            json.dumps({"since": args.since, "tallies": post_all, "runs": runs_all, "issues": issues}, indent=1, default=str),
            encoding="utf-8",
        )
        summary = render_summary(dealers, pre_all, post_all, runs_all, issues, args.since)
        (out / "README.md").write_text(summary, encoding="utf-8")
        print(summary)
        return 0
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    sys.exit(main())
