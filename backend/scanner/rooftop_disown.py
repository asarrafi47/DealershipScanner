"""Shared rooftop-attribution reconcile policy for every scan path.

Both scan paths reach the same fork after
:func:`backend.parsers.resolve_rooftop_attribution` has split a payload:

* the rows it KEPT are this store's and are written, and
* the rows it REFUSED are not written — but only some of those refusals are
  evidence about a *car*, and only those may un-list anything.

That distinction lives here, once, so the delta (HTTP replay) path and the full
(browser) path cannot drift apart on it.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("scanner")

# ── the one policy list ──────────────────────────────────────────────────────
#
# A refusal is evidence about a CAR only when the feed positively assigned that
# car to a rooftop that is not this store:
#   ``sibling_rooftop``                  — the group payload names this store AND
#                                          the other rooftop; the car is theirs.
#   ``single_rooftop_is_not_this_store`` — the payload names exactly one store
#                                          and it is not us.
#
# ``target_rooftop_unidentified`` (and ``unstamped_row_in_group_feed``) mean we
# could not work out WHICH rooftop is this store — a statement about our roster,
# not about any car. Acting on it un-lists a live dealer's whole inventory
# because a database field is blank: bmwofmurrieta-com has 1,941 active cars and
# a feed that resolves to two Murrieta rooftops with no roster street_address to
# separate them. Refuse to WRITE on those, never to un-list.
EVIDENCE_BACKED_REJECTS = frozenset({"sibling_rooftop", "single_rooftop_is_not_this_store"})


def split_refusals(refused: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """``(rows that may be un-listed, count of rows that may not)``."""
    evidenced = [v for v in refused if v.get("_rooftop_reject") in EVIDENCE_BACKED_REJECTS]
    return evidenced, len(refused) - len(evidenced)


def roster_place(dealer_url: str) -> dict[str, str]:
    """This store's postal address from the dealership registry, or empty dict.

    Group feeds whose rooftops are address blocks rather than store names can
    only be told apart by address, so without this the rooftop gate cannot
    identify the store being scanned and refuses the whole payload. Best-effort
    by design: a dealer missing from the registry just falls back to the name
    and host tiers, which is where it was before.
    """
    if not dealer_url:
        return {}
    try:
        from backend.db.inventory_db import db_conn
        from backend.listings.dealer_registry_match import resolve_car_dealership_registry_id

        reg_id = resolve_car_dealership_registry_id({"dealer_url": dealer_url})
        if not reg_id:
            return {}
        with db_conn() as conn:
            row = conn.execute(
                "SELECT street_address, city, state, zip_code FROM dealerships WHERE id = ?",
                (reg_id,),
            ).fetchone()
        if not row:
            return {}
        return {
            "dealer_address": row[0] or "",
            "dealer_city": row[1] or "",
            "dealer_state": row[2] or "",
            "dealer_zip": row[3] or "",
        }
    except Exception as exc:  # registry is an optimisation, never a scan blocker
        logger.debug("roster place lookup failed for %s: %s", dealer_url, exc)
        return {}


def disown_foreign_rooftop_vins(dealer_id: str, vins: set[str]) -> int:
    """Unlist cars stamped onto *dealer_id* that this run's feed assigns elsewhere.

    Refusing the rows at parse time stops NEW mis-attributions, but a group feed
    has been stamping siblings' cars onto this store on every previous scan and
    those rows stay ``listing_active = 1`` forever otherwise: the reconcile pass
    that would retire them needs near-full VIN coverage, which this dealer can
    no longer reach once the siblings' rows are (correctly) refused.

    This is not a guess about a missing car — the feed named a different
    storefront for these exact VINs in this very run. Scoped to *dealer_id*, so
    if the true rooftop is also scanned its own upsert re-lists the VIN there.

    Callers must pass ONLY VINs whose refusal reason is in
    :data:`EVIDENCE_BACKED_REJECTS`.
    """
    if not dealer_id or not vins:
        return 0
    from datetime import datetime, timezone

    from backend.db.inventory_db import db_conn

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    changed = 0
    ordered = sorted(vins)
    with db_conn() as conn:
        cur = conn.cursor()
        for i in range(0, len(ordered), 500):
            chunk = ordered[i : i + 500]
            placeholders = ",".join("?" * len(chunk))
            cur.execute(
                f"""
                UPDATE cars
                   SET listing_active = 0, listing_removed_at = ?
                 WHERE TRIM(IFNULL(dealer_id, '')) = ?
                   AND COALESCE(listing_active, 1) = 1
                   AND UPPER(TRIM(vin)) IN ({placeholders})
                """,
                tuple([now, dealer_id] + chunk),
            )
            changed += int(cur.rowcount or 0)
        conn.commit()
    return changed


__all__ = [
    "EVIDENCE_BACKED_REJECTS",
    "disown_foreign_rooftop_vins",
    "roster_place",
    "split_refusals",
]
