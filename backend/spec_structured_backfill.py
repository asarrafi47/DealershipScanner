"""
Structured spec backfill: inventory_repair (EPA/trim) first, then NHTSA vPIC for remaining gaps.

Provenance for every inferred column is merged into ``cars.spec_source_json`` (same mechanism as
``backend.spec_backfill``); no separate ``spec_inference_*`` columns — keeps one JSON audit trail.

Tier order (mandatory):
  1) ``collect_row_storage_repairs`` / ``merge_verified_specs`` — do not reimplement cylinder/trans
     logic here; it lives in ``backend.knowledge_engine`` / ``backend.utils.inventory_repair``.
  2) NHTSA ``DecodeVinValuesExtended`` — only for slots still NULL/placeholder after tier 1;
     only when ``looks_like_decode_vin(vin)``. Optional SQLite ``nhtsa_vpic_cache`` avoids repeat HTTP.

Environment:
  SPEC_STRUCTURED_VPIC_OVERWRITE_DEALER — when truthy, vPIC may replace non-placeholder dealer text
  (default: off — never overwrite real dealer values).
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable

from backend.db import inventory_db
from backend.db.inventory_db import get_car_by_id, refresh_car_data_quality_score, update_car_row_partial
from backend.nhtsa_vpic import (
    decode_vpic_http_response,
    fetch_decode_vin_values_extended,
    flat_vpic_result_to_car_patch,
    looks_like_decode_vin,
)
from backend.utils.field_clean import is_effectively_empty
from backend.utils.inventory_repair import collect_row_storage_repairs
from backend.utils.spec_provenance import merge_spec_source_json

log = logging.getLogger(__name__)

# Columns vPIC is allowed to propose (subset of cars schema).
_VPIC_TARGET_KEYS = frozenset(
    {
        "year",
        "make",
        "model",
        "trim",
        "fuel_type",
        "cylinders",
        "transmission",
        "drivetrain",
        "body_style",
        "engine_description",
    }
)


def _vpic_overwrite_dealer_enabled() -> bool:
    return (os.environ.get("SPEC_STRUCTURED_VPIC_OVERWRITE_DEALER") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _is_ev_fuel_hint(car: dict[str, Any]) -> bool:
    ft = str(car.get("fuel_type") or "").lower()
    return "electric" in ft and "plug" not in ft


def slot_fillable_for_vpic(car: dict[str, Any], key: str, *, allow_overwrite_dealer: bool) -> bool:
    """True when vPIC may write *key* for this row snapshot."""
    if key not in _VPIC_TARGET_KEYS:
        return False
    cur = car.get(key)
    if allow_overwrite_dealer and cur is not None and str(cur).strip() != "":
        return True
    if key == "year":
        if cur is None:
            return True
        try:
            yi = int(cur)
            return yi <= 0
        except (TypeError, ValueError):
            return True
    if key == "cylinders":
        if _is_ev_fuel_hint(car):
            return False
        if cur is None or str(cur).strip() == "":
            return True
        try:
            return int(cur) <= 0
        except (TypeError, ValueError):
            return True
    return is_effectively_empty(cur)


def row_candidate_for_structured_spec_backfill(car: dict[str, Any]) -> bool:
    """
    Rows we bother scanning: ``is_car_incomplete`` OR obvious spec placeholders
    (matches product intent: incomplete / bad spec text).
    """
    from backend.db.inventory_db import is_car_incomplete

    if is_car_incomplete(car):
        return True
    for k in (
        "transmission",
        "drivetrain",
        "fuel_type",
        "body_style",
        "engine_description",
        "trim",
        "make",
        "model",
    ):
        if is_effectively_empty(car.get(k)):
            return True
    cyl = car.get("cylinders")
    if cyl is None or str(cyl).strip() == "":
        return True
    y = car.get("year")
    if y is None or str(y).strip() == "":
        return True
    return False


def _vpic_cache_get(conn: Any, vin: str) -> dict[str, Any] | None:
    cur = conn.cursor()
    cur.execute(
        "SELECT response_json FROM nhtsa_vpic_cache WHERE vin = ?",
        (vin.upper(),),
    )
    row = cur.fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except (TypeError, json.JSONDecodeError):
        return None


def _vpic_cache_put(conn: Any, vin: str, body: dict[str, Any]) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO nhtsa_vpic_cache (vin, response_json, fetched_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(vin) DO UPDATE SET
            response_json = excluded.response_json,
            fetched_at = excluded.fetched_at
        """,
        (vin.upper(), json.dumps(body, ensure_ascii=False)),
    )


@dataclass
class StructuredSpecBackfillResult:
    car_id: int
    applied: bool
    skip_reason: str = ""
    tier1_fields: list[str] = field(default_factory=list)
    tier2_fields: list[str] = field(default_factory=list)
    vpic_error: str | None = None
    # True when tier1/tier2 produced a non-empty ``combined`` patch (including dry-run).
    has_pending_patch: bool = False


def apply_structured_spec_backfill_for_car(
    car_id: int,
    *,
    dry_run: bool = False,
    get_json: Callable[[str], dict[str, Any]] | None = None,
    use_vpic_cache: bool = True,
) -> StructuredSpecBackfillResult:
    """
    Tier-1 inventory_repair patch, then tier-2 vPIC for remaining fillable slots.
    Merges provenance into ``spec_source_json``. Idempotent when DB already matches.
    """
    raw = get_car_by_id(car_id)
    if not raw:
        return StructuredSpecBackfillResult(car_id=car_id, applied=False, skip_reason="not_found", has_pending_patch=False)

    if not row_candidate_for_structured_spec_backfill(raw):
        return StructuredSpecBackfillResult(car_id=car_id, applied=False, skip_reason="already_complete", has_pending_patch=False)

    tier1 = collect_row_storage_repairs(raw)
    merged: dict[str, Any] = dict(raw)
    for k, v in tier1.items():
        merged[k] = v

    tier1_keys = sorted(tier1.keys())
    allow_ow = _vpic_overwrite_dealer_enabled()

    tier2: dict[str, Any] = {}
    vpic_err: str | None = None
    vin = (merged.get("vin") or "").strip().upper()
    if looks_like_decode_vin(vin):
        flat: dict[str, str] | None = None
        body: dict[str, Any] | None = None
        if use_vpic_cache:
            conn = inventory_db.get_conn()
            try:
                inventory_db.ensure_nhtsa_vpic_cache_table(conn)
                body = _vpic_cache_get(conn, vin)
            finally:
                conn.close()
        if body is None:
            api_body, flat, vpic_err = fetch_decode_vin_values_extended(vin, get_json=get_json)
            body = api_body
            if api_body is not None and use_vpic_cache and not dry_run:
                conn = inventory_db.get_conn()
                try:
                    inventory_db.ensure_nhtsa_vpic_cache_table(conn)
                    _vpic_cache_put(conn, vin, api_body)
                    conn.commit()
                finally:
                    conn.close()
        else:
            flat = decode_vpic_http_response(body) if body else None
            vpic_err = None if flat else "cache_decode_failed"

        if flat:
            vpic_patch = flat_vpic_result_to_car_patch(flat)
            for k, v in vpic_patch.items():
                if k not in _VPIC_TARGET_KEYS:
                    continue
                if not slot_fillable_for_vpic(merged, k, allow_overwrite_dealer=allow_ow):
                    continue
                if v is None or (isinstance(v, str) and not str(v).strip()):
                    continue
                tier2[k] = v

    combined = {**tier1, **tier2}
    if not combined:
        return StructuredSpecBackfillResult(
            car_id=car_id,
            applied=False,
            skip_reason="no_fillable_fields",
            tier1_fields=tier1_keys,
            vpic_error=vpic_err,
            has_pending_patch=False,
        )

    prov: dict[str, dict[str, Any]] = {}
    for k in tier1_keys:
        prov[k] = {
            "source": "inventory_repair",
            "detail": "clean_car_row_dict + merge_verified_specs (spec_structured_backfill tier 1)",
        }
    for k in tier2:
        prov[k] = {
            "source": "nhtsa_vpic",
            "detail": "DecodeVinValuesExtended",
            "url": "https://vpic.nhtsa.dot.gov/api/vehicles/decodevinvaluesextended/",
        }

    existing_spec = raw.get("spec_source_json")
    if isinstance(existing_spec, dict):
        existing_str = json.dumps(existing_spec, ensure_ascii=False)
    elif isinstance(existing_spec, str):
        existing_str = existing_spec
    else:
        existing_str = None
    spec_json = merge_spec_source_json(existing_str, prov)

    combined["spec_source_json"] = spec_json

    if dry_run:
        return StructuredSpecBackfillResult(
            car_id=car_id,
            applied=False,
            skip_reason="dry_run",
            tier1_fields=tier1_keys,
            tier2_fields=sorted(tier2.keys()),
            vpic_error=vpic_err,
            has_pending_patch=True,
        )

    update_car_row_partial(car_id, combined)
    refresh_car_data_quality_score(car_id)
    return StructuredSpecBackfillResult(
        car_id=car_id,
        applied=True,
        tier1_fields=tier1_keys,
        tier2_fields=sorted(tier2.keys()),
        vpic_error=vpic_err,
        has_pending_patch=True,
    )


def iter_candidate_car_ids(
    *,
    limit: int | None = None,
    vin: str | None = None,
) -> list[int]:
    """Ids for rows that pass ``row_candidate_for_structured_spec_backfill`` (scan newest first)."""
    conn = inventory_db.get_conn()
    conn.row_factory = None
    cur = conn.cursor()
    if vin and str(vin).strip():
        cur.execute("SELECT id FROM cars WHERE UPPER(TRIM(vin)) = ? ORDER BY id DESC", (vin.strip().upper(),))
    else:
        cur.execute("SELECT id FROM cars ORDER BY id DESC")
    ids = [int(r[0]) for r in cur.fetchall()]
    conn.close()

    out: list[int] = []
    for cid in ids:
        c = get_car_by_id(cid)
        if c and row_candidate_for_structured_spec_backfill(c):
            out.append(cid)
        if limit is not None and len(out) >= limit:
            break
    return out
