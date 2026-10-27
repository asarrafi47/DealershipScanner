"""Step 3: NHTSA vPIC decode + heal for the scanned dealers. Moved verbatim from backend/scripts/dealer_pipeline.py (audit F11)."""
from __future__ import annotations

from typing import Any

from backend.scanner.pipeline.db import get_conn


# --------------------------------------------------------------------------
# 3. vpic
# --------------------------------------------------------------------------

def no_trim_dealers(cur, dealer_ids: list[str]) -> list[str]:
    """Dealers (of *dealer_ids*) whose active rows carry no trim at all: the feed has
    none, so a single-valued vPIC trim may fill it (D-N31). A dealer with even one
    trimmed row has a trim-bearing feed and is left alone."""
    if not dealer_ids:
        return []
    cur.execute(
        "SELECT c.dealer_id FROM cars c WHERE COALESCE(c.listing_active,1)=1 AND c.dealer_id IN ("
        + ",".join("?" * len(dealer_ids)) + ") GROUP BY c.dealer_id "
        "HAVING SUM(CASE WHEN COALESCE(c.trim, '') != '' THEN 1 ELSE 0 END) = 0",
        tuple(dealer_ids),
    )
    return [r[0] for r in cur.fetchall()]


def vpic_for_dealers(dealer_ids: list[str]) -> dict[str, Any]:
    from backend.enrichment.vpic_facts import decode_missing_vins, heal_rows
    from backend.parsers.base import is_preassignment_vin

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT vin FROM cars WHERE COALESCE(listing_active,1)=1 AND length(vin)=17 AND dealer_id IN ("
            + ",".join("?" * len(dealer_ids)) + ")",
            tuple(dealer_ids),
        )
        vins = [r[0] for r in cur.fetchall()]
        # D-N17b: I/O/Q pre-assignment IDs are never decoded (decode_missing_vins would
        # drop them too; filtering here keeps the count in the result).
        real = [v for v in vins if not is_preassignment_vin(v)]
        dec = decode_missing_vins(conn, real)
        if len(real) != len(vins):
            dec["preassignment_skipped"] = len(vins) - len(real)
        heal = heal_rows(conn, dealers=dealer_ids, trim_dealers=no_trim_dealers(cur, dealer_ids))
        heal.pop("examples", None)
        return {"decode": dec, "heal": heal}
    finally:
        conn.close()
