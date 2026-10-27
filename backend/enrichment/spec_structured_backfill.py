"""
Structured spec backfill: inventory_repair (EPA/trim) first, then NHTSA vPIC for remaining gaps.

Provenance for every inferred column is merged into ``cars.spec_source_json`` (same mechanism as
``backend.enrichment.spec_backfill``); no separate ``spec_inference_*`` columns — keeps one JSON audit trail.

Evaluation order (mandatory, P8B.6a):
  1) NHTSA ``DecodeVinValuesExtended`` for the vPIC-covered fields (drivetrain, fuel_type,
     cylinders, transmission, engine_description, body_style) — only when
     ``looks_like_decode_vin(vin)``. Optional ``nhtsa_vpic_cache`` avoids repeat HTTP.
  2) Tier 1, ``collect_row_storage_proposals`` / ``merge_verified_specs`` — do not reimplement
     cylinder/trans logic here; it lives in ``backend.enrichment.knowledge_engine`` /
     ``backend.utils.inventory_repair``. Labels: ``epa_catalog``, ``dealer_normalized``,
     ``dealer_text_heuristic``.
  Both tiers are decided together by ``apply_fact_proposals``: a lower rank never replaces a
  higher one, and a vPIC two-wheel drive is never filled AWD/4WD by tier 1.

Environment:
  SPEC_STRUCTURED_VPIC_OVERWRITE_DEALER — when truthy, vPIC may propose a value for a slot that
  already holds dealer text, but only where precedence allows (the rank rules in
  ``backend.vehicle_facts.precedence`` still refuse it for engine text and transmission, where
  the dealer outranks the decode). Default: off — never overwrite real dealer values.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable

from backend.db import inventory_db
from backend.db.inventory_db import get_car_by_id, refresh_car_data_quality_score, update_car_row_partial
from backend.enrichment.nhtsa_vpic import (
    decode_vpic_http_response,
    fetch_decode_vin_values_extended,
    flat_vpic_result_to_car_patch,
    looks_like_decode_vin,
)
from backend.utils.field_clean import is_effectively_empty
from backend.utils.inventory_repair import collect_row_storage_proposals, storage_derived_columns
from backend.utils.spec_field_normalize import split_trim_components, trim_named_in_listing
from backend.utils.spec_provenance import merge_spec_source_json
from backend.vehicle_facts import precedence as prec
from backend.vehicle_facts.apply import apply_fact_proposals

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


def vpic_trim_rejection_reason(car: dict[str, Any], trim: Any) -> str | None:
    """
    Why a vPIC-decoded trim must NOT be written for this row (None = corroborated).

    vPIC trims are VIN-pattern guesses, not listing facts: the decoder returns
    every-possible-trim lists ('Light, Light Long Range, Wind'), cab/body styles
    ('Crew Cab'), and 'Base'. Audit A.1 #9: 8,610 cars carried a trim their own
    listing never names, 2,989 provably from nhtsa_vpic. Rules:
      - a multi-value list (>2 components after split) is refused outright,
        even if one component matches — it is a decoder enumeration, not a trim;
      - otherwise the trim is written only when the listing's own text names it
        (``trim_named_in_listing``, same normalization as the nightly invariant).
    """
    parts = split_trim_components(trim)
    if not parts:
        return "empty"
    if len(parts) > 2:
        return "multi_value_list"
    if not trim_named_in_listing(car, trim):
        return "not_named_in_listing"
    return None


def _is_ev_fuel_hint(car: dict[str, Any]) -> bool:
    """No engine to have cylinders: ``vehicle_facts.electrification`` BEV / FCEV."""
    from backend.vehicle_facts.electrification import NO_ENGINE, electrification

    return electrification(car) in NO_ENGINE


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


def car_needs_transmission_or_cylinders_backfill(car: dict[str, Any]) -> bool:
    """
    True when tier-1 EPA/trim or NHTSA vPIC should run for missing transmission and/or
    (non-EV) cylinder count.

    Used on hot paths (e.g. upsert, post-repair) so we do not call vPIC for every
    ``is_car_incomplete`` row (e.g. only missing paint).
    """
    if is_effectively_empty(car.get("transmission")):
        return True
    cyl = car.get("cylinders")
    if cyl is None or str(cyl).strip() == "":
        return True
    try:
        c_int = int(cyl)
        if c_int <= 0 and not _is_ev_fuel_hint(car):
            return True
    except (TypeError, ValueError):
        return True
    return False


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
    try:
        c_int = int(cyl)
        if c_int <= 0 and not _is_ev_fuel_hint(car):
            return True
    except (TypeError, ValueError):
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


# Columns judged on the stored row (before tier 1): the vPIC-covered ones.
_VPIC_FIRST_KEYS = frozenset(prec.FIELDS) | {"body_style"}
_VPIC_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/decodevinvaluesextended/"
_TIER1_DETAIL = "clean_car_row_dict + merge_verified_specs (spec_structured_backfill tier 1)"


def _vpic_ctx(flat: dict[str, str] | None) -> dict[str, Any]:
    """Precedence ctx from a decode row (electrification and two-wheel-drive guards)."""
    if not flat:
        return {}
    ctx: dict[str, Any] = {"vpic": flat}
    if (flat.get("DriveType") or "").strip():
        ctx["vpic_drive"] = flat["DriveType"].strip()
    return ctx


def apply_structured_spec_backfill_for_car(
    car_id: int,
    *,
    dry_run: bool = False,
    get_json: Callable[[str], dict[str, Any]] | None = None,
    use_vpic_cache: bool = True,
) -> StructuredSpecBackfillResult:
    """
    The VIN decode first for the vPIC-covered fields, then tier-1 inventory_repair values,
    all fact columns decided together by ``apply_fact_proposals`` (a lower rank never
    replaces a higher one, and an EPA value never fills a slot the decode owns). Columns
    and ``spec_source_json`` go out in one UPDATE. Idempotent when DB already matches.
    """
    raw = get_car_by_id(car_id)
    if not raw:
        return StructuredSpecBackfillResult(car_id=car_id, applied=False, skip_reason="not_found", has_pending_patch=False)

    if not row_candidate_for_structured_spec_backfill(raw):
        return StructuredSpecBackfillResult(car_id=car_id, applied=False, skip_reason="already_complete", has_pending_patch=False)

    tier1 = collect_row_storage_proposals(raw)
    # The row as tier 1 would leave it: the view for the slots the decode does not own first.
    merged_view: dict[str, Any] = {**raw, **tier1.plain}
    for k, v, _src in tier1.proposals:
        merged_view[k] = v
    tier1_keys = sorted({k for k, _v, _s in tier1.proposals} | set(tier1.plain))
    allow_ow = _vpic_overwrite_dealer_enabled()

    def _view(k: str) -> dict[str, Any]:
        return raw if k in _VPIC_FIRST_KEYS else merged_view

    vpic_props: list[tuple[str, Any, str]] = []   # fact columns, source nhtsa_vpic
    vpic_plain: dict[str, Any] = {}               # columns without a rank (body_style, trim, ...)
    trim_rejection: dict[str, Any] | None = None
    vpic_err: str | None = None
    flat: dict[str, str] | None = None
    vin = (merged_view.get("vin") or "").strip().upper()
    needs_vpic = looks_like_decode_vin(vin) and (
        bool(tier1.proposals)
        or any(slot_fillable_for_vpic(_view(k), k, allow_overwrite_dealer=allow_ow) for k in _VPIC_TARGET_KEYS)
    )
    if needs_vpic:
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
            for k, v in flat_vpic_result_to_car_patch(flat).items():
                if k not in _VPIC_TARGET_KEYS:
                    continue
                if not slot_fillable_for_vpic(_view(k), k, allow_overwrite_dealer=allow_ow):
                    continue
                if v is None or (isinstance(v, str) and not str(v).strip()):
                    continue
                if k == "trim":
                    # Corroboration guard — intentionally NOT bypassed by
                    # SPEC_STRUCTURED_VPIC_OVERWRITE_DEALER: an uncorroborated
                    # decoder trim never lands on cars.trim, only in provenance.
                    reason = vpic_trim_rejection_reason(merged_view, v)
                    if reason is not None:
                        trim_rejection = {
                            "source": "nhtsa_vpic",
                            "detail": "DecodeVinValuesExtended",
                            "value": str(v)[:160],
                            "reason": reason,
                        }
                        continue
                if k in prec.FIELDS:
                    vpic_props.append((k, v, prec.NHTSA_VPIC))
                else:
                    vpic_plain[k] = v

    # One ordered plan, the decode first (later proposals see earlier winners). dry_run: the
    # columns and provenance below go out in a single UPDATE.
    res = apply_fact_proposals(None, raw, vpic_props + list(tier1.proposals), dry_run=True, ctx=_vpic_ctx(flat))

    columns: dict[str, Any] = {}
    prov: dict[str, dict[str, Any]] = {}
    tier2_keys: set[str] = set()
    for k, v in res.applied.items():
        columns[k] = v
        entry = dict(res.provenance.get(k) or {})
        if entry.get("source") == prec.NHTSA_VPIC:
            entry.update(detail="DecodeVinValuesExtended", url=_VPIC_URL)
            tier2_keys.add(k)
        else:
            entry["detail"] = _TIER1_DETAIL
        prov[k] = entry
    for k, entry in res.provenance.items():
        if k not in res.applied:
            prov[k] = entry  # a refused signal, recorded as a discrepancy
    for k, v in vpic_plain.items():
        columns[k] = v
        prov[k] = {"source": "nhtsa_vpic", "detail": "DecodeVinValuesExtended", "url": _VPIC_URL}
        tier2_keys.add(k)
    for k, v in tier1.plain.items():
        if k in columns:
            continue  # the decode owns this slot
        columns[k] = v
        if k in tier1.sources:
            prov[k] = {"source": tier1.sources[k], "detail": _TIER1_DETAIL}
    if "interior_color_buckets" not in columns:
        columns.update(storage_derived_columns(raw, columns))
    if trim_rejection is not None:
        # Non-column provenance key: refused value recorded WITHOUT writing cars.trim.
        prov["trim_rejected"] = trim_rejection

    existing_spec = raw.get("spec_source_json")
    if isinstance(existing_spec, dict):
        existing_str = json.dumps(existing_spec, ensure_ascii=False)
    elif isinstance(existing_spec, str):
        existing_str = existing_spec
    else:
        existing_str = None
    tier2_fields = sorted(tier2_keys)
    tier1_fields = [k for k in tier1_keys if k not in tier2_keys]

    if not columns:
        # Nothing writable — but a refused vPIC trim (or a recorded discrepancy) must
        # still be observable in provenance, or the refusal is invisible to the audit trail.
        if prov and not dry_run:
            update_car_row_partial(car_id, {"spec_source_json": merge_spec_source_json(existing_str, prov)})
        return StructuredSpecBackfillResult(
            car_id=car_id,
            applied=False,
            skip_reason="no_fillable_fields",
            tier1_fields=tier1_keys,
            vpic_error=vpic_err,
            has_pending_patch=False,
        )

    columns["spec_source_json"] = merge_spec_source_json(existing_str, prov)

    if dry_run:
        return StructuredSpecBackfillResult(
            car_id=car_id,
            applied=False,
            skip_reason="dry_run",
            tier1_fields=tier1_fields,
            tier2_fields=tier2_fields,
            vpic_error=vpic_err,
            has_pending_patch=True,
        )

    update_car_row_partial(car_id, columns)
    refresh_car_data_quality_score(car_id)
    return StructuredSpecBackfillResult(
        car_id=car_id,
        applied=True,
        tier1_fields=tier1_fields,
        tier2_fields=tier2_fields,
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
