"""
Heal stock-code contamination on scanned cars.

One dealer feed (McPeek's Chrysler Dodge Jeep Ram Anaheim) stamped the same
stock-code-shaped token (e.g. ``R1111``, ``FR040``, ``T0386``) identically across
``drivetrain``, ``transmission``, ``exterior_color`` and ``interior_color`` while
``stock_number`` stayed NULL. This script:

1. Selects rows where >= 2 of those four fields carry the *same* stock-code-shaped
   token (covers the full 4-field signature and partial rows where the remaining
   fields are NULL/empty or hold a genuine value like a real color). ``mileage`` is
   part of the same corruption: the parser ran the token through a digit extractor,
   so e.g. token ``UK0005`` produced ``mileage = 5``. A row's mileage is flagged when
   ``int(digits-of-token) == mileage``.
   A *residual* path also selects rows a previous run of this script already healed
   (four text fields nulled, token moved into ``stock_number``) but whose mileage
   still equals the token's digits: ``stock_number`` token-shaped, ``interior_color``
   NULL/empty (colors are never VIN-refillable, so healed rows keep it NULL), and
   ``mileage = int(digits-of-stock_number)``. Verified fleet-wide: zero rows outside
   the affected dealer match this signature.
2. Per DB-safety rules, snapshots every affected row's prior values to a JSON file
   under the scratchpad ``db-backups/`` directory AND to a timestamped backup table
   (``CREATE TABLE ... AS SELECT``), then updates ONLY these columns:
   ``stock_number`` (set to the token when NULL), NULLs out exactly the fields
   that carry the token (genuine values such as a real exterior color are kept),
   and NULLs ``mileage`` where it equals the token's digits (a NULL mileage is
   truthful; a stock-code mileage is not — it refills on the next scan).
3. Re-derives ``drivetrain`` / ``transmission`` from the VIN via the NHTSA vPIC
   batch decoder (``DecodeVINValuesBatch``, <= 50 VINs per POST, polite sleep
   between batches) and fills them only when vPIC returns a confident value.
   Colors are NOT VIN-derivable and stay NULL.
4. Refreshes ``data_quality_score`` for every healed row via the existing helper.

Default is ``--dry-run`` (no writes). Use ``--apply`` to execute.

Usage:
    python -m backend.scripts.heal_stock_code_contamination            # dry run
    python -m backend.scripts.heal_stock_code_contamination --apply
    python -m backend.scripts.heal_stock_code_contamination --apply --limit 10
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)))

from backend.utils.project_env import load_project_dotenv  # noqa: E402

load_project_dotenv()

from backend.db.inventory_db import get_conn, refresh_car_data_quality_score  # noqa: E402
from backend.enrichment.nhtsa_vpic import (  # noqa: E402
    decode_vpic_http_response,
    flat_vpic_result_to_car_patch,
    looks_like_decode_vin,
)

log = logging.getLogger("heal_stock_code_contamination")

# The four columns the dealer feed contaminated with the stock token.
CONTAMINATED_FIELDS = ("drivetrain", "transmission", "exterior_color", "interior_color")

# Stock-code shape observed on the affected rows: R1111, FR040, T0386, U0104,
# DTS1373, DTT0071, FP164T, DTS1310T, T0417AA, DTDTS1214, UK0005.
STOCK_TOKEN_RE = re.compile(r"^[A-Z]{1,5}\d{3,5}[A-Z]{0,2}$")

# Only trust vPIC drivetrain when it normalizes to a canonical layout.
CONFIDENT_DRIVETRAINS = frozenset({"AWD", "4WD", "FWD", "RWD"})

VPIC_BATCH_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/"
VPIC_BATCH_MAX = 50  # documented vPIC batch limit

BACKUP_DIR = (
    "/private/tmp/claude-501/-Users-asarrafi-Projects/"
    "0fd58203-ffeb-4591-b16d-3f7f838acd51/scratchpad/db-backups"
)


@dataclass
class HealPlan:
    car_id: int
    vin: str
    token: str
    prior: dict[str, Any]
    fields_to_null: tuple[str, ...]
    set_stock_number: bool
    null_mileage: bool = False
    # filled during apply / vPIC phase
    vpic_drivetrain: str | None = None
    vpic_transmission: str | None = None
    notes: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------- selection

def detect_contamination_token(values: dict[str, Any]) -> tuple[str, tuple[str, ...]] | None:
    """
    Given the four field values, return ``(token, fields_carrying_it)`` when >= 2 of the
    non-empty fields hold the same stock-code-shaped token, else None.
    """
    trimmed = {f: (str(v).strip() if v is not None else "") for f, v in values.items()}
    nonempty = [v for v in trimmed.values() if v]
    if len(nonempty) < 2:
        return None
    tok, n = Counter(nonempty).most_common(1)[0]
    if n < 2 or not STOCK_TOKEN_RE.match(tok):
        return None
    carriers = tuple(f for f in CONTAMINATED_FIELDS if trimmed.get(f) == tok)
    return tok, carriers


def token_digits(token: str) -> int | None:
    """``int`` of the token's digit substring (``UK0005`` -> 5), or None if no digits."""
    digits = re.sub(r"\D", "", token or "")
    return int(digits) if digits else None


def mileage_matches_token(mileage: Any, token: str) -> bool:
    """True when the stored mileage is exactly the token's digit substring as an int."""
    if mileage is None:
        return False
    d = token_digits(token)
    if d is None:
        return False
    try:
        return int(mileage) == d
    except (TypeError, ValueError):
        return False


def find_contaminated_rows(conn, limit: int | None = None) -> list[HealPlan]:
    """Scan cars for the contamination signature (any dealer; the shape check is the gate)."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, vin, stock_number, drivetrain, transmission, exterior_color, interior_color,
               mileage, dealer_name
        FROM cars
        WHERE (
              (drivetrain    IS NOT NULL AND TRIM(drivetrain)    <> '' AND drivetrain    = transmission)
           OR (drivetrain    IS NOT NULL AND TRIM(drivetrain)    <> '' AND drivetrain    = exterior_color)
           OR (drivetrain    IS NOT NULL AND TRIM(drivetrain)    <> '' AND drivetrain    = interior_color)
           OR (transmission  IS NOT NULL AND TRIM(transmission)  <> '' AND transmission  = exterior_color)
           OR (transmission  IS NOT NULL AND TRIM(transmission)  <> '' AND transmission  = interior_color)
           OR (exterior_color IS NOT NULL AND TRIM(exterior_color) <> '' AND exterior_color = interior_color)
           OR (
                -- residual path: previously healed rows (token moved to stock_number,
                -- interior_color left NULL — colors are never VIN-refillable) whose
                -- mileage still equals the stock token's digit substring.
                stock_number IS NOT NULL AND TRIM(stock_number) <> ''
                AND mileage IS NOT NULL
                AND (interior_color IS NULL OR TRIM(interior_color) = '')
              )
        )
        ORDER BY id
        """,
    )
    plans: list[HealPlan] = []
    for row in cur.fetchall():
        if isinstance(row, dict):
            rid = row["id"]
            vin = row["vin"]
            stock = row["stock_number"]
            mileage = row["mileage"]
            vals = {f: row[f] for f in CONTAMINATED_FIELDS}
        else:
            rid, vin, stock = row[0], row[1], row[2]
            vals = dict(zip(CONTAMINATED_FIELDS, row[3:7]))
            mileage = row[7]
        hit = detect_contamination_token(vals)
        if hit:
            token, carriers = hit
            set_stock = stock is None or not str(stock).strip()
            null_mileage = mileage_matches_token(mileage, token)
        else:
            # Residual path: signature lives in stock_number + digit-matching mileage.
            token = str(stock).strip() if stock is not None else ""
            if not STOCK_TOKEN_RE.match(token) or not mileage_matches_token(mileage, token):
                continue
            carriers = ()
            set_stock = False
            null_mileage = True
        plans.append(
            HealPlan(
                car_id=int(rid),
                vin=(vin or "").strip().upper(),
                token=token,
                prior={"stock_number": stock, "mileage": mileage, **vals},
                fields_to_null=carriers,
                set_stock_number=set_stock,
                null_mileage=null_mileage,
            )
        )
        if limit is not None and len(plans) >= limit:
            break
    return plans


# --------------------------------------------------------------------------- backups

def write_json_snapshot(plans: list[HealPlan], ts: str) -> str:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    path = os.path.join(BACKUP_DIR, f"heal_stock_code_contamination_{ts}.json")
    payload = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "script": "backend/scripts/heal_stock_code_contamination.py",
        "columns": ["stock_number", *CONTAMINATED_FIELDS, "mileage"],
        "rows": [
            {"id": p.car_id, "vin": p.vin, "token": p.token, "prior": p.prior}
            for p in plans
        ],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    return path


def create_backup_table(conn, plans: list[HealPlan], ts: str) -> str:
    """Timestamped in-DB backup of every affected row's prior values."""
    table = f"cars_backup_stock_contam_{ts}"
    if not re.fullmatch(r"[a-z0-9_]+", table):  # defensive; ts is digits/underscore only
        raise ValueError(f"unsafe backup table name: {table!r}")
    ids = [p.car_id for p in plans]
    ph = ", ".join(["%s"] * len(ids))
    cur = conn.cursor()
    cur.execute(
        f"""
        CREATE TABLE {table} AS
        SELECT id, vin, stock_number, drivetrain, transmission, exterior_color, interior_color,
               mileage
        FROM cars WHERE id IN ({ph})
        """,
        ids,
    )
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    row = cur.fetchone()
    n = row[0] if not isinstance(row, dict) else next(iter(row.values()))
    if int(n) != len(ids):
        raise RuntimeError(f"backup table {table} has {n} rows, expected {len(ids)}")
    conn.commit()
    return table


# --------------------------------------------------------------------------- heal phase

def apply_heal(conn, plans: list[HealPlan]) -> dict[str, int]:
    """Set stock_number from the token (when NULL) and NULL only the token-carrying fields."""
    counts: dict[str, int] = {"stock_number": 0, **{f: 0 for f in CONTAMINATED_FIELDS}, "mileage": 0}
    cur = conn.cursor()
    for p in plans:
        sets: list[str] = []
        params: list[Any] = []
        if p.set_stock_number:
            sets.append("stock_number = %s")
            params.append(p.token)
            counts["stock_number"] += 1
        for f in p.fields_to_null:
            sets.append(f"{f} = NULL")
            counts[f] += 1
        if p.null_mileage:
            sets.append("mileage = NULL")
            counts["mileage"] += 1
        if not sets:
            continue
        params.append(p.car_id)
        cur.execute(f"UPDATE cars SET {', '.join(sets)} WHERE id = %s", params)
    conn.commit()
    return counts


# --------------------------------------------------------------------------- vPIC phase

def _default_post_json(url: str, data: str, timeout_s: float = 60.0) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        data=urllib.parse.urlencode({"format": "json", "data": data}).encode("utf-8"),
        headers={
            "User-Agent": "SarrafiCollection-stock-code-heal/1.0",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def vpic_batch_decode(
    vins: list[str],
    *,
    post_json: Callable[[str, str], dict[str, Any]] | None = None,
    batch_size: int = VPIC_BATCH_MAX,
    sleep_s: float = 1.5,
) -> dict[str, dict[str, str]]:
    """Decode VINs via vPIC DecodeVINValuesBatch; return {VIN: flat_result_row}."""
    poster = post_json or _default_post_json
    batch_size = max(1, min(int(batch_size), VPIC_BATCH_MAX))
    out: dict[str, dict[str, str]] = {}
    valid = [v for v in vins if looks_like_decode_vin(v)]
    for i in range(0, len(valid), batch_size):
        chunk = valid[i : i + batch_size]
        try:
            body = poster(VPIC_BATCH_URL, ";".join(chunk))
        except (urllib.error.URLError, json.JSONDecodeError, TypeError, ValueError) as e:
            log.warning("vPIC batch %d-%d failed: %s", i, i + len(chunk), e)
            continue
        results = body.get("Results") if isinstance(body, dict) else None
        if isinstance(results, list):
            for row in results:
                if not isinstance(row, dict):
                    continue
                flat = {str(k): str(v) if v is not None else "" for k, v in row.items()}
                vin = (flat.get("VIN") or "").strip().upper()
                if vin:
                    out[vin] = flat
        if i + batch_size < len(valid) and sleep_s > 0:
            time.sleep(sleep_s)  # be polite to the free NHTSA API
    return out


def derive_vpic_patch(flat: dict[str, str]) -> dict[str, str]:
    """drivetrain/transmission only, and only when confidently derived from the VIN."""
    patch = flat_vpic_result_to_car_patch(flat)
    out: dict[str, str] = {}
    dt = patch.get("drivetrain")
    if dt in CONFIDENT_DRIVETRAINS:
        out["drivetrain"] = dt
    tx = (patch.get("transmission") or "").strip()
    if tx:
        out["transmission"] = tx
    return out


def apply_vpic_rederive(
    conn,
    plans: list[HealPlan],
    *,
    post_json: Callable[[str, str], dict[str, Any]] | None = None,
    batch_size: int = VPIC_BATCH_MAX,
    sleep_s: float = 1.5,
) -> dict[str, int]:
    """Fill drivetrain/transmission on healed rows from vPIC (only those two columns)."""
    counts = {"drivetrain": 0, "transmission": 0, "vins_decoded": 0}
    plans = [p for p in plans if {"drivetrain", "transmission"} & set(p.fields_to_null)]
    flat_by_vin = vpic_batch_decode(
        [p.vin for p in plans], post_json=post_json, batch_size=batch_size, sleep_s=sleep_s
    )
    counts["vins_decoded"] = len(flat_by_vin)
    cur = conn.cursor()
    for p in plans:
        flat = flat_by_vin.get(p.vin)
        if not flat:
            p.notes.append("vpic:no_result")
            continue
        patch = derive_vpic_patch(flat)
        if not patch:
            p.notes.append("vpic:nothing_confident")
            continue
        # Only fill columns this script just nulled (never clobber a genuine value).
        cols = [c for c in ("drivetrain", "transmission") if c in patch and c in p.fields_to_null]
        if not cols:
            continue
        sets = ", ".join(f"{c} = %s" for c in cols)
        guards = " AND ".join(f"{c} IS NULL" for c in cols)
        params = [patch[c] for c in cols] + [p.car_id]
        cur.execute(f"UPDATE cars SET {sets} WHERE id = %s AND {guards}", params)
        if getattr(cur, "rowcount", 1) == 0:
            p.notes.append("vpic:guard_skipped")
            continue
        for c in cols:
            counts[c] += 1
            setattr(p, f"vpic_{c}", patch[c])
    conn.commit()
    return counts


# --------------------------------------------------------------------------- main

def summarize_plans(plans: list[HealPlan]) -> dict[str, int]:
    c: dict[str, int] = {"rows": len(plans), "stock_number_to_set": 0}
    for f in CONTAMINATED_FIELDS:
        c[f"{f}_to_null"] = 0
    c["mileage_to_null"] = 0
    for p in plans:
        if p.set_stock_number:
            c["stock_number_to_set"] += 1
        for f in p.fields_to_null:
            c[f"{f}_to_null"] += 1
        if p.null_mileage:
            c["mileage_to_null"] += 1
    return c


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1].strip())
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", default=True,
                      help="Report what would change without writing (default).")
    mode.add_argument("--apply", action="store_true",
                      help="Actually write: backup, heal, vPIC re-derive, refresh scores.")
    ap.add_argument("--limit", type=int, default=None, metavar="N",
                    help="Process at most N contaminated rows.")
    ap.add_argument("--batch-size", type=int, default=VPIC_BATCH_MAX,
                    help=f"vPIC batch size (max {VPIC_BATCH_MAX}).")
    ap.add_argument("--sleep", type=float, default=1.5,
                    help="Seconds to sleep between vPIC batches.")
    ap.add_argument("--skip-vpic", action="store_true",
                    help="Apply the heal but skip the vPIC re-derivation phase.")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    conn = get_conn()
    try:
        plans = find_contaminated_rows(conn, limit=args.limit)
        summary = summarize_plans(plans)
        print(f"Contaminated rows found: {summary['rows']}")
        for k, v in summary.items():
            if k != "rows":
                print(f"  {k}: {v}")
        tokens = Counter(p.token for p in plans)
        print(f"Distinct tokens: {len(tokens)} (sample: {list(tokens)[:8]})")
        if plans:
            p0 = plans[0]
            print(f"Sample row id={p0.car_id} vin={p0.vin} token={p0.token} "
                  f"null_fields={p0.fields_to_null} set_stock_number={p0.set_stock_number} "
                  f"null_mileage={p0.null_mileage} (prior mileage={p0.prior.get('mileage')})")

        if not args.apply:
            n_vins = sum(
                1 for p in plans
                if looks_like_decode_vin(p.vin)
                and {"drivetrain", "transmission"} & set(p.fields_to_null)
            )
            n_batches = (n_vins + args.batch_size - 1) // max(1, args.batch_size) if n_vins else 0
            print(f"[dry-run] Would snapshot {len(plans)} rows to JSON + backup table, "
                  f"then UPDATE stock_number/{'/'.join(CONTAMINATED_FIELDS)}/mileage.")
            print(f"[dry-run] Would send {n_vins} VINs to vPIC in {n_batches} batch(es) "
                  f"to re-derive drivetrain/transmission.")
            print("[dry-run] No changes written. Re-run with --apply to execute.")
            return 0

        if not plans:
            print("Nothing to heal.")
            return 0

        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        snap_path = write_json_snapshot(plans, ts)
        print(f"JSON snapshot: {snap_path}")
        table = create_backup_table(conn, plans, ts)
        print(f"Backup table: {table} ({len(plans)} rows)")

        heal_counts = apply_heal(conn, plans)
        print("Heal phase per-column change counts:")
        for k, v in heal_counts.items():
            print(f"  {k}: {v}")

        if args.skip_vpic:
            print("vPIC phase skipped (--skip-vpic).")
            vpic_counts = {"drivetrain": 0, "transmission": 0, "vins_decoded": 0}
        else:
            vpic_counts = apply_vpic_rederive(
                conn, plans, batch_size=args.batch_size, sleep_s=args.sleep
            )
            print("vPIC re-derivation counts:")
            for k, v in vpic_counts.items():
                print(f"  {k}: {v}")

        refreshed = 0
        for p in plans:
            try:
                refresh_car_data_quality_score(p.car_id)
                refreshed += 1
            except Exception:
                log.exception("data_quality_score refresh failed for car id=%s", p.car_id)
        print(f"data_quality_score refreshed: {refreshed}/{len(plans)}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
