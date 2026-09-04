"""
Per-dealer rollup of the rooftop-attribution verdict table, for the admin resolution
view (``/admin/attribution``).

Deliberately reuses the exact SQL and refiled/unconfirmed logic in
``cars_repo.car_attribution_states`` (the listings-grid caveat reader) rather than
re-deriving a parallel query: ``car_attribution`` is read-time evidence a script
writes and nothing else touches, and letting the admin rollup disagree with the
public-facing caveat about whether a car counts as "resolved" would be its own
data-quality defect layered on top of the one this exists to expose.

Written against ``classify_attribution.py`` / migrations/V011__car_attribution.sql:
``confirmed`` / ``conflicting`` / ``unverified`` are the only verdicts that script
writes; a car is "stuck" here when the site cannot currently stand behind its filed
location AND no logged ``apply_attribution_moves`` run has already fixed it -- see
``_attribution_location_unconfirmed`` for the exact rule (conflicting is always
stuck; unverified is stuck only at a group-fed dealer; a refiled car is never
stuck, because it has already been resolved onto the rooftop its photos name).
"""

from __future__ import annotations

import logging
from typing import Any

from backend.db.repositories.base_repo import _placeholders, db_conn
from backend.db.repositories.cars_repo import (
    _ATTRIBUTION_STATE_SQL,
    _ATTRIBUTION_STATE_SQL_NO_MOVE_LOG,
    _attribution_location_unconfirmed,
)

_log = logging.getLogger(__name__)


def dealer_attribution_resolution() -> list[dict[str, Any]]:
    """
    One row per group-fed dealer: confirmed / conflicting / unverified counts, a
    ``stuck`` count (cars this system currently cannot vouch for the location of),
    how many of those were already resolved by a logged re-attribution move, and a
    short ``stuck_why`` breakdown (which other rooftop(s) the stuck cars' own
    photographs name, and how many have no photographic evidence at all).

    Rooftop-scoped dealers are excluded: an occasional conflicting/unverified row
    there is noise on an otherwise-trusted feed, not the "which stores are
    unlocatable" question this view answers (mirrors classify_attribution.py's
    ``_SELF_SHARE_ROOFTOP`` gate, which is what decided ``scope='group'`` in the
    first place).

    Returns ``[]`` -- never raises -- when ``car_attribution`` / ``dealer_feed_scope``
    do not exist yet (a fresh SQLite dev DB, or the classifier has never run): the
    admin page must show "no data yet", not 500.
    """

    def _fetch(sql: str) -> list[tuple]:
        with db_conn() as conn:
            return conn.execute(sql, []).fetchall()

    try:
        rows = _fetch(_ATTRIBUTION_STATE_SQL)
    except Exception:
        try:
            # car_move_log only exists once apply_attribution_moves has run; see
            # the identical fallback in cars_repo.car_attribution_states.
            rows = _fetch(_ATTRIBUTION_STATE_SQL_NO_MOVE_LOG)
        except Exception:
            _log.warning(
                "dealer_attribution_resolution: car_attribution/dealer_feed_scope "
                "unavailable (fresh DB, or classify_attribution.py has never run)",
                exc_info=True,
            )
            return []

    per_dealer: dict[str, dict[str, Any]] = {}
    for row in rows:
        status = str(row[1] or "").strip().lower()
        rooftop = (str(row[2]).strip() or None) if row[2] is not None else None
        dealer_id = str(row[4] or "").strip()
        group_fed = str(row[5] or "").strip().lower() == "group"
        moved_to = str(row[6]).strip().lower() if row[6] is not None else ""
        if not dealer_id or not group_fed or status not in ("confirmed", "conflicting", "unverified"):
            continue  # this view is specifically the group-fed dealer question

        refiled = bool(moved_to and dealer_id.lower() == moved_to)
        stuck = _attribution_location_unconfirmed(status, group_fed=group_fed, refiled=refiled)

        d = per_dealer.setdefault(
            dealer_id,
            {"confirmed": 0, "conflicting": 0, "unverified": 0, "stuck": 0,
             "resolved_by_move": 0, "rooftop_counts": {}},
        )
        d[status] += 1
        if refiled:
            d["resolved_by_move"] += 1
        if stuck:
            d["stuck"] += 1
            if status == "conflicting" and rooftop:
                d["rooftop_counts"][rooftop] = d["rooftop_counts"].get(rooftop, 0) + 1

    dealer_ids = list(per_dealer)
    names: dict[str, str] = {}
    totals: dict[str, int] = {}
    if dealer_ids:
        with db_conn() as conn:
            placeholder_sql = _placeholders(dealer_ids)
            for did, name, total in conn.execute(
                f"SELECT dealer_id, MAX(dealer_name), COUNT(*) FROM cars "
                f"WHERE dealer_id IN ({placeholder_sql}) AND listing_removed_at IS NULL "
                f"GROUP BY dealer_id",
                dealer_ids,
            ).fetchall():
                names[str(did)] = name or ""
                totals[str(did)] = int(total or 0)

    out: list[dict[str, Any]] = []
    for did, d in per_dealer.items():
        why_bits = []
        top_rooftops = sorted(d["rooftop_counts"].items(), key=lambda kv: -kv[1])[:2]
        if top_rooftops:
            why_bits.append("names " + ", ".join(f"{name!r} ({n})" for name, n in top_rooftops))
        if d["unverified"]:
            why_bits.append(f"{d['unverified']} with no gallery evidence at all")
        out.append({
            "dealer_id": did,
            "dealer_name": names.get(did) or did,
            "active_cars": totals.get(did, 0),
            "confirmed": d["confirmed"],
            "conflicting": d["conflicting"],
            "unverified": d["unverified"],
            "stuck": d["stuck"],
            "resolved_by_move": d["resolved_by_move"],
            "stuck_why": "; ".join(why_bits) if why_bits else "—",
        })
    out.sort(key=lambda r: (-r["stuck"], -r["conflicting"], r["dealer_id"]))
    return out
