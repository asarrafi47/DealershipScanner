"""Ingest U.S. EPA Fuel Economy records for selected BMW series (source-backed, no brochure colors)."""
from __future__ import annotations

import time
from datetime import datetime
from typing import Callable

from vehicle_reference.ingestion.bundle import ingest_vehicle_bundle
from vehicle_reference.utils.mpg import format_epa_mpg_ratings
from vehicle_reference.sources import epa_client

EPA_EXTERNAL_SOURCE = "epa_fueleconomy"
EPA_SOURCE_SPEC = {
    "label": "U.S. EPA Fuel Economy (FuelEconomy.gov vehicle record)",
    "url": "https://www.fueleconomy.gov/feg/ws/index.shtml",
    "source_group_key": "epa_fueleconomy_gov",
    "notes": "Ratings, drivetrain, transmission, engine description from EPA test vehicle configuration.",
}

# Phase-2 target lines (EPA baseModel values for U.S. BMW).
TARGET_BASE_MODELS = frozenset(
    {
        "2 Series",
        "3 Series",
        "4 Series",
        "5 Series",
        "X1",
        "X2",
        "X3",
        "X5",
        "X7",
        "i4",
        "iX",
    }
)

BODY_SUFFIXES: tuple[str, ...] = (
    " Sports Activity Coupe",
    " Sports Activity Vehicle",
    " Gran Coupe",
    " Convertible",
    " Coupe",
    " Sedan",
    " Wagon",
    " Hatchback",
    " SAV",
    " Roadster",
)


def split_epa_model_name(model: str) -> tuple[str, str | None]:
    for suf in BODY_SUFFIXES:
        if model.endswith(suf):
            return model[: -len(suf)].strip(), suf.strip()
    return model.strip(), None


def infer_body_from_vclass(vclass: str) -> tuple[str, str | None]:
    vc = (vclass or "").strip()
    if not vc:
        return "", "EPA VClass missing; body style not inferred."
    u = vc.upper()
    if "SPORT UTILITY" in u or "SUV" in u:
        return "SAV", "Body style summarized from EPA vehicle class (not BMW marketing name)."
    if "STATION WAGON" in u:
        return "Wagon", "Body style summarized from EPA vehicle class."
    if "COUPE" in u and "GRAND" not in u:
        return "Coupe", "Body style summarized from EPA vehicle class."
    if "MINICOMPACT" in u or "SUBCOMPACT" in u or "COMPACT" in u or "MID-SIZE" in u or "LARGE" in u:
        return "Car", "Body style summarized from EPA passenger car class (generic)."
    return vc, "Body style taken verbatim from EPA VClass."


def build_fuel_type(veh: dict) -> str:
    atv = (str(veh.get("atvType") or "")).strip()
    f1 = (str(veh.get("fuelType1") or "")).strip()
    f2 = (str(veh.get("fuelType2") or "")).strip()
    if atv == "Plug-in Hybrid":
        parts = [x for x in (f1, f2) if x]
        if parts:
            return f"Plug-in Hybrid ({' + '.join(parts)})"
        return "Plug-in Hybrid"
    if atv == "EV" or f1 == "Electricity":
        return "Electric"
    return f1 or atv or ""


def should_skip_model(model_menu: str) -> str | None:
    m = model_menu.strip()
    compact = m.lower().replace(" ", "")
    if "ix3" in compact:
        return "Excluded iX3-style entry (non-U.S. or not in scope for this build)."
    return None


def delete_epa_vehicles(conn, brand_id: int) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM ref_vehicle
        WHERE brand_id = ? AND external_source = ?
        """,
        (brand_id, EPA_EXTERNAL_SOURCE),
    )
    return cur.rowcount


def ingest_epa_bmw_range(
    conn,
    *,
    brand_code: str,
    year_from: int,
    year_to: int,
    sleep_s: float = 0.1,
    log: Callable[[str], None] | None = None,
) -> tuple[int, int]:
    """
    Returns (vehicles_inserted, vehicles_skipped).
    Deletes prior epa_fueleconomy rows for this brand first.
    """
    from vehicle_reference.ingestion.bundle import _get_brand_id

    def _log(msg: str) -> None:
        if log:
            log(msg)

    cur = conn.cursor()
    brand_id = _get_brand_id(cur, brand_code)
    deleted = delete_epa_vehicles(conn, brand_id)
    conn.commit()
    _log(f"Removed {deleted} prior EPA vehicle rows for brand_id={brand_id}")

    inserted = 0
    skipped = 0

    for year in range(year_from, year_to + 1):
        models = epa_client.fetch_with_retries(epa_client.menu_models, year, "BMW", sleep_s=sleep_s)
        for model_menu, _model_val in models:
            reason = should_skip_model(model_menu)
            if reason:
                skipped += 1
                continue
            opts = epa_client.fetch_with_retries(
                epa_client.menu_options, year, "BMW", model_menu, sleep_s=sleep_s
            )
            for opt_text, vid_s in opts:
                try:
                    vid = int(vid_s)
                except ValueError:
                    skipped += 1
                    continue
                veh = epa_client.fetch_with_retries(epa_client.vehicle_record, vid, sleep_s=sleep_s)
                if not veh:
                    skipped += 1
                    continue
                base = (str(veh.get("baseModel") or "")).strip()
                if not base:
                    skipped += 1
                    continue
                if base not in TARGET_BASE_MODELS:
                    skipped += 1
                    continue

                variant, body_suffix = split_epa_model_name(str(veh.get("model") or model_menu))
                uncertainty_parts: list[str] = []
                if body_suffix:
                    body_style = body_suffix
                else:
                    body_style, u_body = infer_body_from_vclass(str(veh.get("VClass") or ""))
                    if u_body:
                        uncertainty_parts.append(u_body)

                eng = (opt_text or "").strip()
                displ = str(veh.get("displ") or "").strip()
                cyl = str(veh.get("cylinders") or "").strip()
                if eng and displ and cyl:
                    eng = f"{eng}; {displ} L, {cyl} cyl (EPA)"
                elif eng:
                    eng = f"{eng} (EPA)"
                elif displ or cyl:
                    eng = f"{displ} L, {cyl} cyl (EPA)" if displ and cyl else ""

                mpg_text = format_epa_mpg_ratings(veh)
                fuel = build_fuel_type(veh)

                bundle = {
                    "model_year": year,
                    "market": "US",
                    "series_name": base,
                    "variant_name": variant or None,
                    "trim_line": None,
                    "body_style": body_style or None,
                    "engine": eng or None,
                    "transmission": (str(veh.get("trany") or "").strip() or None),
                    "drivetrain": (str(veh.get("drive") or "").strip() or None),
                    "fuel_type": fuel or None,
                    "mpg_text": mpg_text or None,
                    "passenger_seating": None,
                    "uncertainty_notes": (" ".join(uncertainty_parts).strip() or None),
                    "source": dict(EPA_SOURCE_SPEC),
                    "external_source": EPA_EXTERNAL_SOURCE,
                    "external_record_id": str(vid),
                    "internal_notes": f"EPA vehicle id={vid}; menu_model={model_menu!r}; option={opt_text!r}",
                    "packages": [],
                    "exterior_colors": [],
                    "interior_colors": [],
                }
                ingest_vehicle_bundle(conn, bundle, brand_code=brand_code, commit=False)
                inserted += 1
                if inserted % 200 == 0:
                    _log(f"{year}: inserted {inserted} EPA rows so far…")

        _log(f"Finished EPA year {year} ({len(models)} model menus); committed batch")
        conn.commit()

    return inserted, skipped


def default_year_range() -> tuple[int, int]:
    y = datetime.now().year
    return 2000, max(y, 2025)
