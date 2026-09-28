"""NHTSA vPIC facts outrank the dealer feed for drivetrain and electrification.

Policy (2026-09-23): when the VIN decode STATES a drivetrain or an electrification
level, that value is stored in ``cars.drivetrain`` / ``cars.fuel_type`` and shown on
the page. A blank decode changes nothing. The lab measured why: on 1,611 fresh rows
the dealer feed contradicted the VIN on drivetrain 44 times (4x4 Tacomas listed
"RWD", Ridgelines listed "FWD") and labelled 17 hybrid-only cars "Gasoline"; the
VIN was right every time we checked.

Three call sites share this module:
  * scan write path  (``dealer_run``): override in memory before the upsert for
    VINs already in ``nhtsa_vpic_cache``;
  * post-scan        (``post_scan_vpic``): decode the run's new VINs, then heal
    their rows;
  * fleet heal       (``backend/scripts/heal_from_vpic.py``).

Provenance goes into ``spec_source_json`` under source ``nhtsa_vpic``.

Deliberately NOT overridden:
  * AWD vs 4WD: vPIC says "4WD" for Toyota's AWD crossovers; both mean all four
    wheels, so they are treated as equivalent and the dealer's word stands.
  * 48V mild hybrids: vPIC reports "Mild HEV" or nothing; dealers say "Hybrid",
    EPA says gasoline. No override either way.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Iterable

logger = logging.getLogger("scanner")

VPIC_FUEL_LABEL = {"ev": "Electric", "phev": "Plug-In Hybrid", "hybrid": "Hybrid"}
_VPIC_BATCH_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/"
_VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")
_BATCH = 50
_PAUSE_S = 0.6


def vin_facts_enabled() -> bool:
    return (os.environ.get("SCANNER_VIN_FACTS") or "1").strip().lower() not in ("0", "false", "no", "off")


def _drive_bucket(s: Any) -> str:
    from backend.catalog.resolver import _drive_bucket as _b

    return _b(s)


def _fuel_bucket(s: Any) -> str:
    from backend.catalog.resolver import _fuel_bucket as _b

    return _b(s)


def _drive_same(a: str, b: str) -> bool:
    if not a or not b:
        return True
    return a == b or {a, b} <= {"AWD", "4WD"}


def vin_overrides(row: dict[str, Any], vpic: dict[str, Any] | None) -> dict[str, tuple[Any, str]]:
    """{field: (old, new)} the decode would change on *row*. Empty when vPIC is
    silent or agrees. Pure: does not mutate."""
    if not vpic:
        return {}
    out: dict[str, tuple[Any, str]] = {}
    vd = vpic.get("drivetrain")
    if vd:
        cur_d = _drive_bucket(row.get("drivetrain"))
        if not cur_d or not _drive_same(cur_d, _drive_bucket(vd)):
            out["drivetrain"] = (row.get("drivetrain"), vd)  # fill a blank, or fix a contradiction
    el = vpic.get("electrification")
    if el in VPIC_FUEL_LABEL:
        want = VPIC_FUEL_LABEL[el]
        cur = str(row.get("fuel_type") or "").strip()
        if _fuel_bucket(cur) != _fuel_bucket(want):
            out["fuel_type"] = (cur or None, want)
    elif el is None and str(vpic.get("fuel_type") or "") == "Diesel":
        cur = str(row.get("fuel_type") or "").strip()
        if _fuel_bucket(cur) in ("gas", ""):
            out["fuel_type"] = (cur or None, "Diesel")
    # EngineCylinders: the stored column is the weakest fact on the row (a 2027
    # Buick Enclave 2.5T carried 6, a 2026 Sierra 2.7L I4 carried 8 — both from a
    # model-level backfill; verified 2026-09-25 on hendrickbuickgmccary-com /
    # longofathens-com). The decode is per VIN. EVs have no cylinders: skip when
    # vPIC says it is an EV (electrification 'ev').
    try:
        vc = int(str(vpic.get("cylinders") or "").strip() or 0)
    except ValueError:
        vc = 0
    if 2 <= vc <= 16 and el != "ev":
        try:
            cur_c = int(row.get("cylinders") or 0)
        except (TypeError, ValueError):
            cur_c = 0
        if cur_c != vc:
            out["cylinders"] = (row.get("cylinders"), vc)
    return out


def apply_vin_facts_to_row(row: dict[str, Any], vpic: dict[str, Any] | None) -> dict[str, tuple[Any, str]]:
    """Mutate *row* in place; returns what changed."""
    ch = vin_overrides(row, vpic)
    for field, (_old, new) in ch.items():
        row[field] = new
        row[f"_{field}_source"] = "nhtsa_vpic"
    return ch


def override_vehicles(vehicles: list[dict[str, Any]]) -> dict[str, int]:
    """Scan write path: apply the decode to every vehicle whose VIN is cached."""
    # Every key vin_overrides() can return must exist here: when the cylinder
    # override landed (2026-09-25) this dict lacked "cylinders", the first such
    # row raised KeyError, dealer_run logged "VIN facts failed … 'cylinders'" and
    # skipped the whole pass, so rescans wrote the feed's drivetrain/fuel/cylinders
    # back over healed rows (449 log lines by 2026-09-28).
    stats = {"vehicles": len(vehicles), "cached": 0, "drivetrain": 0, "fuel_type": 0, "cylinders": 0}
    if not vin_facts_enabled() or not vehicles:
        return stats
    from backend.enrichment.knowledge_engine import lookup_vpic_from_cache, prime_vpic_cache

    vins = [str(v.get("vin") or "").strip().upper() for v in vehicles]
    prime_vpic_cache([v for v in vins if _VIN_RE.match(v)])
    for v, vin in zip(vehicles, vins):
        if not _VIN_RE.match(vin):
            continue
        vp = lookup_vpic_from_cache(vin)
        if not vp or not any(vp.get(k) for k in ("drivetrain", "electrification", "fuel_type", "cylinders")):
            continue
        stats["cached"] += 1
        for field in apply_vin_facts_to_row(v, vp):
            stats[field] = stats.get(field, 0) + 1
    return stats


# --------------------------------------------------------------------------
# decode + heal (post-scan and fleet)
# --------------------------------------------------------------------------

def fetch_vpic_batch(vins: list[str]) -> dict[str, dict]:
    """``{vin: {"Results": [row]}}`` shaped like a single-VIN decode so the cache
    stays byte-compatible with ``vpic_specs``."""
    import requests

    resp = requests.post(_VPIC_BATCH_URL, data={"format": "json", "data": ";".join(vins)}, timeout=90)
    resp.raise_for_status()
    out: dict[str, dict] = {}
    for row in (resp.json() or {}).get("Results") or []:
        if not isinstance(row, dict):
            continue
        vin = str(row.get("VIN") or "").strip().upper()
        if vin:
            out[vin] = {"Count": 1, "Message": "Batch decode", "Results": [row]}
    return out


def decode_missing_vins(conn, vins: Iterable[str]) -> dict[str, int]:
    """Decode VINs absent from ``nhtsa_vpic_cache``. Idempotent."""
    todo = sorted({str(v or "").strip().upper() for v in vins if _VIN_RE.match(str(v or "").strip().upper())})
    stats = {"requested": len(todo), "already_cached": 0, "stored": 0, "failed": 0}
    if not todo:
        return stats
    cur = conn.cursor()
    have: set[str] = set()
    for i in range(0, len(todo), 900):
        chunk = todo[i:i + 900]
        cur.execute(
            "SELECT vin FROM nhtsa_vpic_cache WHERE vin IN (" + ",".join("?" * len(chunk)) + ")", tuple(chunk)
        )
        have.update(r[0] for r in cur.fetchall())
    stats["already_cached"] = len(have)
    missing = [v for v in todo if v not in have]
    for i in range(0, len(missing), _BATCH):
        chunk = missing[i:i + _BATCH]
        try:
            decoded = fetch_vpic_batch(chunk)
        except Exception as exc:  # noqa: BLE001 - one bad batch must not stop the rest
            stats["failed"] += len(chunk)
            logger.warning("vPIC batch %d failed: %s", i // _BATCH, str(exc)[:120])
            time.sleep(_PAUSE_S)
            continue
        for vin, payload in decoded.items():
            try:
                cur.execute(
                    "INSERT INTO nhtsa_vpic_cache (vin, response_json, fetched_at) VALUES (?, ?, ?) "
                    "ON CONFLICT (vin) DO NOTHING",
                    (vin, json.dumps(payload), time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
                )
                stats["stored"] += 1
            except Exception as exc:  # noqa: BLE001
                logger.debug("vPIC store %s failed: %s", vin, exc)
        conn.commit()
        time.sleep(_PAUSE_S)
    return stats


def _vpic_flat_rows(conn, vins: list[str]) -> dict[str, dict[str, str]]:
    """``{vin: Results[0]}`` straight from ``nhtsa_vpic_cache`` for the VINs given
    (the normalised lookup drops the engine / EV fields the derivation needs)."""
    out: dict[str, dict[str, str]] = {}
    if not vins:
        return out
    cur = conn.cursor()
    for i in range(0, len(vins), 900):
        chunk = vins[i:i + 900]
        cur.execute(
            "SELECT vin, response_json FROM nhtsa_vpic_cache WHERE vin IN (" + ",".join("?" * len(chunk)) + ")",
            tuple(chunk),
        )
        for vin, raw in cur.fetchall():
            try:
                payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
                row0 = (payload.get("Results") or [{}])[0]
            except (ValueError, AttributeError, IndexError, TypeError):
                continue
            if isinstance(row0, dict):
                out[str(vin).upper()] = {str(k): ("" if v is None else str(v)) for k, v in row0.items()}
    return out


def _ev_engine_description(flat: dict[str, str]) -> str | None:
    """"Electric motor, Dual Motor, 300 hp / 224 kW" from the EV fields vPIC
    carries (EVDriveUnit / EngineHP / EngineKW); None when it carries none."""
    unit = (flat.get("EVDriveUnit") or "").strip()
    hp = (flat.get("EngineHP") or "").strip()
    kw = (flat.get("EngineKW") or "").strip()
    parts = ["Electric motor"]
    if unit and unit.lower() not in ("not applicable", "n/a"):
        parts.append(unit)
    power = []
    for val, suffix in ((hp, "hp"), (kw, "kW")):
        try:
            f = float(val)
        except ValueError:
            continue
        if f > 0:
            power.append(f"{int(round(f))} {suffix}")
    if power:
        parts.append(" / ".join(power))
    if len(parts) == 1:
        return None
    return ", ".join(parts)[:240]


def derived_fills(row: dict[str, Any], vp: dict[str, Any] | None, flat: dict[str, str] | None) -> dict[str, tuple[Any, str]]:
    """Fill-only derivations from the decode for a row whose engine_description /
    body_style is empty: never overwrites the dealer's own engine text (the
    catalog and the decode's synthesis never outrank it), never touches a
    non-empty body_style. EVs get a synthesised motor line when vPIC has the
    EV fields (F06, 2026-09-28: 6,338 engine-null non-EV rows with
    DisplacementL + EngineCylinders cached; 1,219 body-null rows with BodyClass)."""
    out: dict[str, tuple[Any, str]] = {}
    vp = vp or {}
    flat = flat or {}
    cur_engine = str(row.get("engine_description") or "").strip()
    if not cur_engine and flat:
        from backend.enrichment.nhtsa_vpic import _build_engine_description

        is_ev = vp.get("electrification") == "ev" or _fuel_bucket(row.get("fuel_type")) == "ev"
        text = _ev_engine_description(flat) if is_ev else _build_engine_description(flat)
        if text:
            out["engine_description"] = (row.get("engine_description"), text)
    cur_body = str(row.get("body_style") or "").strip()
    if not cur_body:
        body = vp.get("body_style")
        if not body:
            bc = (flat.get("BodyClass") or "").strip()
            if bc and bc.lower() not in ("", "not applicable"):
                body = bc[:120]
        if body:
            out["body_style"] = (row.get("body_style"), body)
    return out


_HEAL_FLAT_CHUNK = 500  # rows whose raw vPIC decodes are held at once in heal_rows


def heal_rows(conn, *, vins: Iterable[str] | None = None, dealers: Iterable[str] | None = None,
              dry_run: bool = False) -> dict[str, Any]:
    """Store the decode's drivetrain / fuel / cylinders on active rows where it
    differs, and fill empty engine_description / body_style from the decode."""
    from backend.enrichment.knowledge_engine import clear_vpic_lookup_cache, lookup_vpic_from_cache, prime_vpic_cache
    from backend.utils.spec_provenance import merge_spec_source_json

    cur = conn.cursor()
    where = "COALESCE(listing_active,1)=1 AND vin IS NOT NULL AND length(vin)=17"
    params: list[Any] = []
    vin_list = sorted({str(v or "").strip().upper() for v in (vins or []) if v})
    dealer_list = [d for d in (dealers or []) if d]
    if vin_list:
        where += " AND vin IN (" + ",".join("?" * len(vin_list)) + ")"
        params += vin_list
    if dealer_list:
        where += " AND dealer_id IN (" + ",".join("?" * len(dealer_list)) + ")"
        params += dealer_list
    cur.execute(f"SELECT c.id, c.vin, c.drivetrain, c.fuel_type, c.cylinders, c.spec_source_json, e.drive, "
                f"c.engine_description, c.body_style FROM cars c "
                f"LEFT JOIN epa_master e ON e.id = c.epa_master_id WHERE {where.replace('listing_active', 'c.listing_active').replace(' vin', ' c.vin').replace('dealer_id', 'c.dealer_id')}", tuple(params))
    rows = [dict(zip(("id", "vin", "drivetrain", "fuel_type", "cylinders", "spec_source_json", "catalog_drive",
                      "engine_description", "body_style"), r)) for r in cur.fetchall()]
    stats: dict[str, Any] = {"rows": len(rows), "decoded": 0, "drivetrain": 0, "fuel_type": 0, "cylinders": 0,
                             "engine_description": 0, "body_style": 0, "examples": []}
    clear_vpic_lookup_cache()
    prime_vpic_cache([r["vin"] for r in rows])
    for start in range(0, len(rows), _HEAL_FLAT_CHUNK):
        chunk = rows[start:start + _HEAL_FLAT_CHUNK]
        # Raw decode rows (Results[0], ~140 keys each) only for the chunk's rows
        # that have something to derive, fetched per chunk so a whole-fleet heal
        # never holds every decode at once.
        need_flat = sorted({r["vin"] for r in chunk
                            if not str(r.get("engine_description") or "").strip() or not str(r.get("body_style") or "").strip()})
        flats = _vpic_flat_rows(conn, need_flat)
        _heal_chunk(cur, chunk, flats, stats, dry_run, lookup_vpic_from_cache, merge_spec_source_json)
    if not dry_run:
        conn.commit()
    return stats


def _heal_chunk(cur, rows: list[dict[str, Any]], flats: dict[str, dict[str, str]], stats: dict[str, Any],
                dry_run: bool, lookup_vpic_from_cache, merge_spec_source_json) -> None:
    """heal_rows' per-row work for one chunk; ``flats`` holds only this chunk's raw decodes."""
    for r in rows:
        vp = lookup_vpic_from_cache(r["vin"]) or {}
        flat = flats.get(r["vin"])
        if not any(vp.get(k) for k in ("drivetrain", "electrification", "fuel_type")) and not flat:
            continue
        stats["decoded"] += 1
        ch = vin_overrides(r, vp)
        ch.update(derived_fills(r, vp, flat))
        # A "4x2" decode says two driven wheels and nothing about the end; the
        # feed's "4x2"/"2WD" used to be written as FWD (468 Tacomas / Grand
        # Cherokees on RWD catalog rows, 2026-09-26). When the decode is silent on
        # the end, the linked catalog row's drive is the best evidence there is.
        if "drivetrain" not in ch and str(vp.get("drive_type_raw") or vp.get("drivetrain_raw") or "").lower() in ("4x2", "2wd") or (
            not vp.get("drivetrain") and str(r.get("drivetrain") or "").upper() in ("2WD", "FWD", "RWD", "")):
            cat = _drive_bucket(r.get("catalog_drive"))
            cur_d = _drive_bucket(r.get("drivetrain"))
            if cat in ("FWD", "RWD") and cur_d != cat and cur_d not in ("AWD", "4WD"):
                ch["drivetrain"] = (r.get("drivetrain"), cat)
                stats["drivetrain_catalog_tiebreak"] = stats.get("drivetrain_catalog_tiebreak", 0) + 1
        if not ch:
            continue
        for field in ch:
            stats[field] += 1
        if len(stats["examples"]) < 12:
            stats["examples"].append({"vin": r["vin"], **{k: f"{o} -> {n}" for k, (o, n) in ch.items()}})
        if dry_run:
            continue
        prov = {k: {"source": ("epa_catalog_tiebreak" if (k == "drivetrain" and n == _drive_bucket(r.get("catalog_drive")) and not vp.get("drivetrain")) else "nhtsa_vpic"),
                    "detail": f"DecodeVINValuesBatch; feed said {o!r}"} for k, (o, n) in ch.items()}
        new_src = merge_spec_source_json(r["spec_source_json"] if isinstance(r["spec_source_json"], str) else None, prov)
        sets = ", ".join(f"{k} = ?" for k in ch) + ", spec_source_json = ?"
        cur.execute(f"UPDATE cars SET {sets} WHERE id = ?", tuple(n for (_o, n) in ch.values()) + (new_src, r["id"]))


def post_scan_vpic(vins: list[str]) -> dict[str, Any]:
    """Post-scan step: decode the run's new VINs, then heal their rows."""
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        out: dict[str, Any] = {"decode": decode_missing_vins(conn, vins)}
        out["heal"] = heal_rows(conn, vins=vins)
        out["heal"].pop("examples", None)
        return out
    finally:
        conn.close()
