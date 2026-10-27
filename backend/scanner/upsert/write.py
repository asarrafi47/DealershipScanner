"""The per-vehicle write loop of :func:`backend.scanner.database.upsert_vehicles`."""
from __future__ import annotations

from backend.parsers.base import is_preassignment_vin
from backend.scanner.upsert import ownership as _own
from backend.scanner.upsert.guard import GuardWindow, backstop_owner
from backend.scanner.upsert.rows import normalize_vehicle_row
from backend.scanner.upsert.serialize import price_history_json
from backend.scanner.upsert.sql import (
    UPSERT_CARS_SQL,
    fetch_prev_price_row,
    prepare_row,
    trace_vin_readback,
    upsert_params,
)


def write_rows(
    conn,
    cursor,
    vehicles: list[dict],
    *,
    now: str,
    existing_spec_src: dict[str, str],
    window: GuardWindow,
    conflicts: dict[str, tuple[str, str]],
    commit_every: int,
    stored_facts: dict[str, dict] | None = None,
) -> int:
    """Normalize and upsert each vehicle in the given (sorted-VIN) order.

    Commits every ``commit_every`` written rows (the caller commits the tail).
    A row the SQL backstop refuses (rowcount 0 with the guard on) is added to
    ``conflicts`` in place and not counted. ``stored_facts`` (from the prefetch) lets a
    higher-ranked stored fact survive the feed (P8B.1). Returns the number of rows written.
    """
    count = 0
    pools = _own.load_pools()
    for raw in vehicles:
        v = normalize_vehicle_row(raw)
        _vin_key = (v.get("vin") or "").strip()
        row = prepare_row(v, existing_spec_src.get(_vin_key), (stored_facts or {}).get(_vin_key))
        if row is None:
            continue
        vin = row.vin
        _claimant = str(v.get("dealer_id") or "").strip()
        _evidence = raw.get("_rooftop_evidence")
        # D-N17b: pre-assignment IDs (I/O/Q) are outside the VIN-owner guard, backstop too.
        _guard = window.on and not is_preassignment_vin(vin)
        _prev_row = fetch_prev_price_row(cursor, vin)
        history_json = price_history_json(
            _prev_row[1] if _prev_row else None,
            _prev_row[0] if _prev_row else None,
            row.price,
            now,
        )
        cursor.execute(
            UPSERT_CARS_SQL,
            upsert_params(
                row,
                now=now,
                price_history_json=history_json,
                guard_on=_guard,
                guard_cutoff_iso=window.cutoff_iso,
                rooftop_tier=_own.norm_tier(raw.get("_rooftop_evidence_tier")),
                rooftop_evidence=_evidence if isinstance(_evidence, str) and _evidence else None,
                pool_lists=_own.backstop_lists(pools, _claimant),
            ),
        )
        if _guard and getattr(cursor, "rowcount", 1) == 0:
            # Backstop fired: another writer owns this VIN (fresh, active).
            conflicts[vin] = (backstop_owner(cursor, vin), _claimant)
            continue
        count += 1
        if count % commit_every == 0:
            conn.commit()
        trace_vin_readback(cursor, vin, v)
    return count
