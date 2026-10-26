"""What a refusal is allowed to do: refuse the write, or also un-list the car.

Moved from ``backend/scanner/rooftop_disown.py`` (2026-10-01); that module
re-exports these names. Every scan path reaches the same fork after the gate
has split a payload:

* the rows it KEPT are this store's and are written, and
* the rows it REFUSED are not written — but only some of those refusals are
  evidence about a *car*, and only those may un-list anything.
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

# Refusals that leave a VIN OUT of the reconcile pass's protected set (P5A.5): the feed
# positively assigned the car elsewhere, so a row already stored here is struck under the
# two-strike policy instead of being held listed. ``third_rooftop`` (a class-(d) row stored
# under a sibling) is struck the same way. ``vdp_host_other_store`` is in neither set.
UNPROTECTED_REFUSALS = EVIDENCE_BACKED_REJECTS | {"third_rooftop"}


def protected_refusal_vins(*vehicle_lists: list[dict[str, Any]], dropped: dict[str, str] | None = None) -> set[str]:
    """Normalized VINs refused WITHOUT evidence about the car: reconcile must never retire them.

    *vehicle_lists* are refused rows (``_rooftop_reject`` set); *dropped* is the
    ``{vin: reason}`` map ``drop_unattributable_vehicles`` returns.
    """
    from backend.scanner.inventory_reconcile import normalize_scanner_vin

    out: set[str] = set()
    for rows in vehicle_lists:
        for v in rows or ():
            if v.get("_rooftop_reject") and v.get("_rooftop_reject") not in UNPROTECTED_REFUSALS:
                vin = normalize_scanner_vin(v.get("vin"))
                if vin:
                    out.add(vin)
    for vin, reason in (dropped or {}).items():
        if reason not in UNPROTECTED_REFUSALS and vin:
            out.add(vin)
    return out


def split_refusals(refused: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """``(rows that may be un-listed, count of rows that may not)``."""
    evidenced = [v for v in refused if v.get("_rooftop_reject") in EVIDENCE_BACKED_REJECTS]
    return evidenced, len(refused) - len(evidenced)


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

    ``SCANNER_NO_RETIRE`` (the one no-retire switch) makes this a no-op that returns 0.

    Callers must pass ONLY VINs whose refusal reason is in
    :data:`EVIDENCE_BACKED_REJECTS`.
    """
    if not dealer_id or not vins:
        return 0
    from datetime import datetime, timezone

    from backend.db.inventory_db import db_conn
    from backend.scanner.inventory_reconcile import no_retire_switch

    if no_retire_switch():
        logger.warning("disown %s: SCANNER_NO_RETIRE is set; leaving %d VIN(s) listed", dealer_id, len(vins))
        return 0

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    changed = 0
    retired_ids: list[int] = []
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
             RETURNING id
                """,
                tuple([now, dealer_id] + chunk),
            )
            ids = [int(r[0]) for r in cur.fetchall()]
            retired_ids.extend(ids)
            changed += len(ids)
        conn.commit()
    if retired_ids:  # the incomplete-listings index drops cars that left the active set (IL-05)
        try:
            from backend.db.incomplete_listings_db import delete_incomplete_records

            delete_incomplete_records(retired_ids)
        except Exception:  # noqa: BLE001 - the unlist is committed
            logger.warning("disown %s: could not delete incomplete-index rows", dealer_id, exc_info=True)
    return changed


__all__ = ["EVIDENCE_BACKED_REJECTS", "UNPROTECTED_REFUSALS", "protected_refusal_vins", "disown_foreign_rooftop_vins", "split_refusals"]
