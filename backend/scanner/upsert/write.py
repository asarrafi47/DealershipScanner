"""The per-vehicle write loop of :func:`backend.scanner.database.upsert_vehicles`."""
from __future__ import annotations

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
) -> int:
    """Normalize and upsert each vehicle in the given (sorted-VIN) order.

    Commits every ``commit_every`` written rows (the caller commits the tail).
    A row the SQL backstop refuses (rowcount 0 with the guard on) is added to
    ``conflicts`` in place and not counted. Returns the number of rows written.
    """
    count = 0
    for raw in vehicles:
        v = normalize_vehicle_row(raw)
        row = prepare_row(v, existing_spec_src.get((v.get("vin") or "").strip()))
        if row is None:
            continue
        vin = row.vin
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
                guard_on=window.on,
                guard_cutoff_iso=window.cutoff_iso,
            ),
        )
        if window.on and getattr(cursor, "rowcount", 1) == 0:
            # Backstop fired: another writer owns this VIN (fresh, active).
            conflicts[vin] = (backstop_owner(cursor, vin), str(v.get("dealer_id") or "").strip())
            continue
        count += 1
        if count % commit_every == 0:
            conn.commit()
        trace_vin_readback(cursor, vin, v)
    return count
