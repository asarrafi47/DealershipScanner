"""
The one DB-aware apply path for fact proposals (P8A.3).

Writers gather ``(field, value[, source])`` proposals and call
:func:`apply_fact_proposals`; the rank rules live in
:mod:`backend.vehicle_facts.precedence` (pure) and are not re-implemented by the
callers. The accepted column values and the provenance patch go out in ONE
``update_car_row_partial`` call (one UPDATE statement, so columns and
``spec_source_json`` commit together), which keeps that function's guards
(vocabulary coercion, mild-hybrid fuel, cylinder sentinels). When a guard
changes a value, the provenance ``value`` no longer normalizes to the column and
the stale-provenance rule treats the entry as ``unknown`` on the next read.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from backend.vehicle_facts import precedence as prec


@dataclass
class ApplyResult:
    applied: dict[str, Any] = field(default_factory=dict)      # column -> new value
    kept: list[dict[str, Any]] = field(default_factory=list)   # refused: field, source, value, reason
    discrepancies: dict[str, dict] = field(default_factory=dict)  # field -> losing signal
    provenance: dict[str, dict] = field(default_factory=dict)  # the patch that was (or would be) merged
    dry_run: bool = False
    written: bool = False


def _read_row(conn: Any, car_id: int) -> dict[str, Any]:
    if conn is not None:
        cur = conn.execute("SELECT * FROM cars WHERE id = ?", (car_id,))
        names = [d[0] for d in cur.description]
        r = cur.fetchone()
        return dict(zip(names, r)) if r is not None else {}
    from backend.db.repositories.cars_repo import get_car_by_id

    return dict(get_car_by_id(car_id) or {})


def _triples(proposals: Iterable[tuple], source: str | None) -> list[tuple[str, Any, str]]:
    out: list[tuple[str, Any, str]] = []
    for p in proposals:
        if len(p) == 3:
            f, v, s = p
        elif len(p) == 2 and source:
            (f, v), s = p, source
        else:
            raise ValueError("proposal needs (field, value, source) or a default source")
        out.append((f, v, s))
    return out


def apply_fact_proposals(conn: Any, row: Mapping[str, Any] | int, proposals: Iterable[tuple],
                         source: str | None = None, *, vpic: Mapping[str, Any] | None = None,
                         dry_run: bool = False, ctx: Mapping[str, Any] | None = None) -> ApplyResult:
    """Decide *proposals* against the stored row and persist what wins.

    *row* is the current ``cars`` row (a dict with ``id``) or a car id to read
    through *conn* (``None`` uses the repository connection). *source* is the
    default for 2-tuple proposals. *vpic* is the decode for the electrification
    and drive guards. A dry run decides and reports but writes nothing.
    """
    if isinstance(row, Mapping):
        current = dict(row)
    else:
        current = _read_row(conn, int(row))
    car_id = current.get("id") if current.get("id") is not None else (row if not isinstance(row, Mapping) else None)

    c = dict(ctx or {})
    if vpic is not None:
        c.setdefault("vpic", vpic)
        raw_drive = vpic.get("drive_type_raw") or vpic.get("drivetrain_raw") or vpic.get("drivetrain")
        if raw_drive:
            c.setdefault("vpic_drive", raw_drive)

    changes, patch, decisions = prec.plan_updates(current, _triples(proposals, source), c)
    res = ApplyResult(applied=dict(changes), provenance=dict(patch), dry_run=dry_run)
    for d in decisions:
        if d.write:
            continue
        res.kept.append({"field": d.field, "source": d.source, "reason": d.reason})
        if d.discrepancy:
            res.discrepancies[d.field] = dict(d.discrepancy)

    if dry_run or not patch or car_id is None:
        return res

    from backend.db.repositories.cars_repo import update_car_row_partial
    from backend.utils.spec_provenance import merge_spec_source_json

    existing = current.get("spec_source_json")
    if isinstance(existing, dict):
        existing = json.dumps(existing)
    merged = merge_spec_source_json(existing, patch)
    update_car_row_partial(int(car_id), {**changes, "spec_source_json": merged})
    res.written = True
    return res


def apply_labeled_patch(row: Mapping[str, Any], updates: Mapping[str, Any], source: str, *,
                        detail: str | None = None, meta: Mapping[str, Any] | None = None,
                        sources: Mapping[str, str] | None = None, vpic: Mapping[str, Any] | None = None, dry_run: bool = False) -> ApplyResult:
    """Repair/recovery scripts' write path (P8B.7): a ``{column: value}`` patch from one source.

    Columns under fact precedence are decided by :func:`apply_fact_proposals` (a lower rank
    never replaces a higher one). Every other column in *updates* (price, mileage, trim,
    photos, ...) has no rank: it goes out as given, and its provenance entry records
    *source*, so no script write is untraceable. *meta* columns (run bookkeeping) carry no
    provenance. Columns, ranked values and provenance leave in ONE ``update_car_row_partial``
    call. *sources* overrides *source* per column. ``ApplyResult.applied`` holds every column that was (or, dry run, would be) written.
    """
    from backend.utils.spec_provenance import merge_spec_source_json

    src = dict(sources or {})
    ranked = [(k, v, src.get(k, source)) for k, v in updates.items() if k in prec.FIELDS and v is not None]
    plain = {k: v for k, v in updates.items() if not (k in prec.FIELDS and v is not None)}
    res = apply_fact_proposals(None, row, ranked, vpic=vpic, dry_run=True)
    columns: dict[str, Any] = {**plain, **res.applied}
    prov: dict[str, dict] = dict(res.provenance)
    for k, v in plain.items():
        entry: dict[str, Any] = {"source": src.get(k, source), "value": None if v is None else str(v)[:160]}
        if detail:
            entry["detail"] = detail
        prov[k] = entry
    if detail:
        for k in res.applied:
            if k in prov:
                prov[k] = {**prov[k], "detail": detail}
    out = ApplyResult(applied=dict(columns), kept=res.kept, discrepancies=res.discrepancies,
                      provenance=prov, dry_run=dry_run)
    if dry_run or not columns or row.get("id") is None:
        return out

    from backend.db.repositories.cars_repo import update_car_row_partial

    existing = row.get("spec_source_json")
    if isinstance(existing, dict):
        existing = json.dumps(existing)
    write = {**columns, **(meta or {}), "spec_source_json": merge_spec_source_json(existing, prov)}
    update_car_row_partial(int(row["id"]), write)
    out.written = True
    return out
