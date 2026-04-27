"""
Vehicle spec backfill: cylinders, MPG, optional transmission/drivetrain.

Pipeline: (1) trim decoder + ``epa_master`` aggregate in SQLite, (2) optional
``MasterCatalog`` when Postgres catalog is indexed, (3) Playwright VDP URL,
(4) Google Programmable Search → fueleconomy.gov page parse.

Persist via ``inventory_db.update_car_row_partial`` + ``spec_source_json`` provenance.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

from backend.db.inventory_db import (
    get_car_by_id,
    get_conn,
    refresh_car_data_quality_score,
    update_car_row_partial,
)
from backend.knowledge_engine import decode_trim_logic, lookup_epa_aggregate
from backend.utils.field_clean import is_effectively_empty
from backend.utils.spec_provenance import merge_spec_source_json

log = logging.getLogger(__name__)


@dataclass
class SpecBackfillResult:
    car_id: int
    ok: bool
    updated_fields: list[str] = field(default_factory=list)
    tiers: list[str] = field(default_factory=list)
    message: str = ""


def _int_year(car: dict[str, Any]) -> int | None:
    y = car.get("year")
    if y is None:
        return None
    try:
        return int(y)
    except (TypeError, ValueError):
        return None


def _is_ev_row(car: dict[str, Any]) -> bool:
    eng = str(car.get("engine_l") or "").strip().lower()
    ft = str(car.get("fuel_type") or "").strip().lower()
    if eng in ("electric", "phev"):
        return True
    if "electric" in ft and "plug" not in ft:
        return True
    return False


def car_needs_spec_backfill(car: dict[str, Any]) -> bool:
    """True when cylinders and/or paired MPG are missing for a non-placeholder row."""
    if not car or not car.get("id"):
        return False
    if _is_ev_row(car):
        return car.get("mpg_city") is None or car.get("mpg_highway") is None
    cyl = car.get("cylinders")
    cyl_missing = True
    if cyl is not None and str(cyl).strip() != "":
        try:
            ci = int(cyl)
            cyl_missing = ci <= 0
        except (TypeError, ValueError):
            cyl_missing = True
    mpg_missing = car.get("mpg_city") is None or car.get("mpg_highway") is None
    return bool(cyl_missing or mpg_missing)


def _title_for_decode(car: dict[str, Any]) -> str:
    title = (car.get("title") or "").strip()
    if (car.get("make") or "").strip().upper() == "BMW" and (car.get("fuel_type") or "").strip():
        return f"{title} {car.get('fuel_type')}".strip()
    return title


def tier_a_trim_and_epa(car: dict[str, Any]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """SQLite EPA aggregate + trim decoder (same signals as ``merge_verified_specs``)."""
    found: dict[str, Any] = {}
    prov: dict[str, dict[str, Any]] = {}
    y = _int_year(car)
    make = car.get("make") or ""
    model = car.get("model") or ""
    trim = car.get("trim") or ""
    regex = decode_trim_logic(make, model, trim, _title_for_decode(car))
    title_ep = (car.get("title") or "").strip()
    if (make or "").strip().upper() == "BMW" and (car.get("fuel_type") or "").strip():
        title_ep = f"{title_ep} {car.get('fuel_type')}".strip()
    epa = lookup_epa_aggregate(y, make, model, title=title_ep, trim=trim)

    cyl = regex.get("cylinders")
    src_cyl = "trim_decoder"
    if cyl is None:
        cyl = epa.get("cylinders")
        src_cyl = "epa_master"
    if cyl is not None:
        try:
            found["cylinders"] = int(cyl)
            prov["cylinders"] = {"source": src_cyl, "detail": f"{y} {make} {model}".strip()}
        except (TypeError, ValueError):
            pass

    is_bev = (
        regex.get("cylinders") == 0
        or (regex.get("fuel_type_hint") or "").strip().lower() == "electric"
        or (epa.get("atv_type") or "").strip().upper() == "EV"
        or "electric" in (epa.get("fuel_type") or "").lower()
    )
    if is_bev:
        # BEV: MPGe is in city08 / highway08; city_e is kWh/100 mi in this catalog.
        c8 = epa.get("city08")
        h8 = epa.get("highway08")
        if c8 is not None and h8 is not None and c8 > 0 and h8 > 0:
            found["mpg_city"] = int(round(float(c8)))
            found["mpg_highway"] = int(round(float(h8)))
            prov["mpg_city"] = {"source": "epa_master", "detail": "MPGe city (EPA city08)"}
            prov["mpg_highway"] = {"source": "epa_master", "detail": "MPGe hwy (EPA highway08)"}
    else:
        c8 = epa.get("city08")
        h8 = epa.get("highway08")
        if c8 is not None and h8 is not None and c8 > 0 and h8 > 0:
            found["mpg_city"] = int(round(float(c8)))
            found["mpg_highway"] = int(round(float(h8)))
            prov["mpg_city"] = {"source": "epa_master", "detail": "city08 aggregate"}
            prov["mpg_highway"] = {"source": "epa_master", "detail": "highway08 aggregate"}

    tr = epa.get("transmission")
    if isinstance(tr, str) and tr.strip() and is_effectively_empty(car.get("transmission")):
        found["transmission"] = tr.strip()[:200]
        prov["transmission"] = {"source": "epa_master"}
    dr = epa.get("drivetrain")
    if isinstance(dr, str) and dr.strip() and is_effectively_empty(car.get("drivetrain")):
        found["drivetrain"] = dr.strip()[:120]
        prov["drivetrain"] = {"source": "epa_master"}

    return found, prov


def tier_a_master_catalog(car: dict[str, Any]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Optional pgvector EPA catalog (heavy import — skipped when collection missing)."""
    found: dict[str, Any] = {}
    prov: dict[str, dict[str, Any]] = {}
    if (os.environ.get("SPEC_BACKFILL_USE_MASTER_CATALOG") or "1").strip() in ("0", "false", "no"):
        return found, prov
    try:
        from backend.vector.catalog_service import MasterCatalog
    except ImportError:
        return found, prov
    catalog = MasterCatalog()
    if not catalog.collection_exists():
        return found, prov
    lk = catalog.lookup_car(
        {
            "year": car.get("year"),
            "make": car.get("make"),
            "model": car.get("model"),
            "trim": car.get("trim"),
        },
        n_results=5,
    )
    if not lk.get("ok"):
        return found, prov
    best = lk.get("best") or {}
    if best.get("cylinders") is not None and not _is_ev_row(car):
        try:
            found.setdefault("cylinders", int(best["cylinders"]))
            prov.setdefault("cylinders", {"source": "master_catalog", "detail": lk.get("query", "")[:200]})
        except (TypeError, ValueError):
            pass
    if best.get("mpg_city") is not None:
        found.setdefault("mpg_city", int(best["mpg_city"]))
        prov.setdefault("mpg_city", {"source": "master_catalog"})
    if best.get("mpg_highway") is not None:
        found.setdefault("mpg_highway", int(best["mpg_highway"]))
        prov.setdefault("mpg_highway", {"source": "master_catalog"})
    if best.get("transmission") and is_effectively_empty(car.get("transmission")):
        found.setdefault("transmission", str(best["transmission"])[:200])
        prov.setdefault("transmission", {"source": "master_catalog"})
    if best.get("drivetrain") and is_effectively_empty(car.get("drivetrain")):
        found.setdefault("drivetrain", str(best["drivetrain"])[:120])
        prov.setdefault("drivetrain", {"source": "master_catalog"})
    return found, prov


def _merge_found(into: dict[str, Any], pinto: dict[str, dict], found: dict, prov: dict) -> None:
    for k, v in found.items():
        if v is None:
            continue
        if k not in into or into[k] is None:
            into[k] = v
            if k in prov:
                pinto[k] = prov[k]


def _pick_detail_url(car: dict[str, Any]) -> str | None:
    for key in ("source_url", "dealer_url"):
        u = car.get(key)
        if isinstance(u, str) and u.strip().lower().startswith("http"):
            return u.strip()
    return None


def tier_b_vdp(car: dict[str, Any]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    from backend.vdp_spec_extract import extract_specs_from_vdp_url

    url = _pick_detail_url(car)
    if not url:
        return {}, {}
    raw = extract_specs_from_vdp_url(url)
    if not raw:
        return {}, {}
    prov: dict[str, dict[str, Any]] = {}
    for k in list(raw.keys()):
        prov[k] = {"source": "vdp_playwright", "url": url[:500]}
    return raw, prov


def tier_c_search(car: dict[str, Any], *, pause_s: float = 1.2) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    from backend.spec_search_client import (
        build_spec_search_query,
        fetch_url_html,
        google_custom_search_links,
        parse_fueleconomy_gov_html,
    )

    found: dict[str, Any] = {}
    prov: dict[str, dict[str, Any]] = {}
    y = _int_year(car)
    make = car.get("make")
    model = car.get("model")
    trim = car.get("trim")

    def run_query(intent: str) -> None:
        q = build_spec_search_query(y, make, model, trim, intent=intent)
        links = google_custom_search_links(q, num=5)
        time.sleep(max(0.0, pause_s))
        for item in links[:3]:
            html = fetch_url_html(item["url"])
            if not html:
                continue
            chunk = parse_fueleconomy_gov_html(html)
            u = item["url"]
            for fk, fv in chunk.items():
                if fv is None:
                    continue
                if fk not in found or found[fk] is None:
                    found[fk] = fv
                    prov[fk] = {"source": "google_cse+fueleconomy.gov", "url": u[:500]}

    if car.get("mpg_city") is None or car.get("mpg_highway") is None:
        run_query("mpg")
    cyl_missing = car.get("cylinders") is None or str(car.get("cylinders")).strip() in ("", "0")
    if cyl_missing and not _is_ev_row(car) and found.get("cylinders") is None:
        run_query("cylinders")
    return found, prov


def _conservative_updates(
    car: dict[str, Any],
    found: dict[str, Any],
    prov: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Only fill NULL / empty / invalid dealer columns."""
    updates: dict[str, Any] = {}
    cyl = car.get("cylinders")
    cyl_bad = True
    if cyl is not None and str(cyl).strip() != "":
        try:
            ci = int(cyl)
            if _is_ev_row(car):
                cyl_bad = False
            else:
                cyl_bad = ci <= 0
        except (TypeError, ValueError):
            cyl_bad = True
    else:
        cyl_bad = not _is_ev_row(car)

    if found.get("cylinders") is not None and cyl_bad:
        try:
            fc = int(found["cylinders"])
        except (TypeError, ValueError):
            fc = None
        if fc is not None:
            if _is_ev_row(car):
                if fc == 0:
                    updates["cylinders"] = 0
            elif fc > 0:
                updates["cylinders"] = fc

    for k in ("mpg_city", "mpg_highway"):
        if car.get(k) is None and found.get(k) is not None:
            try:
                updates[k] = int(found[k])
            except (TypeError, ValueError):
                continue
    for k in ("transmission", "drivetrain"):
        if is_effectively_empty(car.get(k)) and found.get(k):
            updates[k] = str(found[k]).strip()[:240 if k == "transmission" else 120]

    prov_out = {k: dict(prov[k]) for k in updates if k in prov}
    return updates, prov_out


def run_spec_backfill_for_car(
    car_id: int,
    *,
    use_vdp: bool = True,
    use_search: bool = True,
    dry_run: bool = False,
    search_pause_s: float = 1.2,
) -> SpecBackfillResult:
    car = get_car_by_id(car_id)
    if not car:
        return SpecBackfillResult(car_id=car_id, ok=False, message="not_found")
    if not car_needs_spec_backfill(car):
        return SpecBackfillResult(car_id=car_id, ok=True, message="already_complete")

    merged: dict[str, Any] = {}
    prov_all: dict[str, dict[str, Any]] = {}
    tiers: list[str] = []

    f1, p1 = tier_a_trim_and_epa(car)
    _merge_found(merged, prov_all, f1, p1)
    tiers.append("a_trim_epa")

    f1b, p1b = tier_a_master_catalog(car)
    _merge_found(merged, prov_all, f1b, p1b)
    if f1b:
        tiers.append("a_master_catalog")

    def _virtual_row() -> dict[str, Any]:
        v = dict(car)
        for k, val in merged.items():
            if val is not None:
                v[k] = val
        return v

    if use_vdp and car_needs_spec_backfill(_virtual_row()):
        fv, pv = tier_b_vdp(car)
        if fv:
            tiers.append("b_vdp")
            _merge_found(merged, prov_all, fv, pv)

    if use_search and car_needs_spec_backfill(_virtual_row()):
        fs, ps = tier_c_search(car, pause_s=search_pause_s)
        if fs:
            tiers.append("c_search")
            _merge_found(merged, prov_all, fs, ps)

    updates, prov_patch = _conservative_updates(car, merged, prov_all)
    if not updates:
        log.info("spec_backfill no-op car_id=%s tiers=%s", car_id, tiers)
        return SpecBackfillResult(
            car_id=car_id,
            ok=True,
            tiers=tiers,
            message="no_fillable_fields",
        )

    keys = list(updates.keys())
    if dry_run:
        return SpecBackfillResult(
            car_id=car_id,
            ok=True,
            updated_fields=keys,
            tiers=tiers,
            message="dry_run",
        )

    spec_json = merge_spec_source_json(car.get("spec_source_json"), prov_patch)
    payload = {**updates, "spec_source_json": spec_json}
    update_car_row_partial(car_id, payload)
    refresh_car_data_quality_score(car_id)
    log.info("spec_backfill car_id=%s fields=%s tiers=%s", car_id, keys, tiers)
    return SpecBackfillResult(
        car_id=car_id,
        ok=True,
        updated_fields=keys,
        tiers=tiers,
        message="updated",
    )


def iter_candidate_car_ids(*, limit: int | None = None) -> list[int]:
    conn = get_conn()
    try:
        cur = conn.cursor()
        sql = """
            SELECT id FROM cars
            WHERE
                (mpg_city IS NULL OR mpg_highway IS NULL
                 OR cylinders IS NULL
                 OR (cylinders <= 0 AND LOWER(COALESCE(TRIM(engine_l), '')) NOT IN ('electric', 'phev')))
            ORDER BY id
        """
        if limit is not None:
            sql += " LIMIT ?"
            cur.execute(sql, (limit,))
        else:
            cur.execute(sql)
        return [int(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()
