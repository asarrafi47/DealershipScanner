"""Phase 2 of ``search_cars``: full rows in rank order, shaped and filtered."""
from __future__ import annotations

from typing import Any, Callable

from backend.db.repositories.base_repo import _placeholders
from backend.db.repositories.cars_repo import (
    _parse_car_gallery,
    _parse_car_history_highlights,
)


def fetch_rows_in_order(cursor: Any, ids: list[int]) -> list[dict]:
    """``SELECT *`` for ``ids``; rows come back in ``ids`` order, missing ids dropped."""
    cursor.execute(f"SELECT * FROM cars WHERE id IN ({_placeholders(ids)})", ids)
    by_id: dict[int, dict] = {}
    for row in cursor.fetchall():
        d = dict(row)
        by_id[int(d["id"])] = d
    return [by_id[i] for i in ids if i in by_id]


def shape_rows(rows: list[dict]) -> list[dict]:
    """Decode the JSON columns the result dicts carry parsed (in place)."""
    for c in rows:
        _parse_car_gallery(c)
        _parse_car_history_highlights(c)
    return rows


def hydrate_in_rank_order(
    conn: Any,
    ordered_ids: list[int],
    *,
    lim: int | None,
    chunk_size: int,
    keep: Callable[[list[dict]], list[dict]],
    distance_by_id: dict[int, float] | None,
) -> list[dict]:
    """Hydrate ``chunk_size`` ids at a time until ``lim`` rows survive ``keep``.

    ``distance_by_id`` (radius searches only) stamps ``distance_miles`` on each
    surviving row. The result may run past ``lim``; the caller caps it.
    """
    collected: list[dict] = []
    cursor = conn.cursor()
    for start in range(0, len(ordered_ids), chunk_size):
        chunk = ordered_ids[start : start + chunk_size]
        rows = keep(shape_rows(fetch_rows_in_order(cursor, chunk)))
        if distance_by_id is not None:
            for c in rows:
                c["distance_miles"] = distance_by_id.get(int(c["id"]), 0.0)
        collected.extend(rows)
        if lim is not None and len(collected) >= lim:
            break
    return collected
