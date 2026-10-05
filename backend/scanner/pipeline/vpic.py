"""Step 3: NHTSA vPIC decode + heal for the scanned dealers. Moved verbatim from backend/scripts/dealer_pipeline.py (audit F11)."""
from __future__ import annotations

from typing import Any

from backend.scanner.pipeline.db import get_conn


# --------------------------------------------------------------------------
# 3. vpic
# --------------------------------------------------------------------------

def vpic_for_dealers(dealer_ids: list[str]) -> dict[str, Any]:
    from backend.enrichment.vpic_facts import decode_missing_vins, heal_rows

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT vin FROM cars WHERE COALESCE(listing_active,1)=1 AND length(vin)=17 AND dealer_id IN ("
            + ",".join("?" * len(dealer_ids)) + ")",
            tuple(dealer_ids),
        )
        vins = [r[0] for r in cur.fetchall()]
        dec = decode_missing_vins(conn, vins)
        heal = heal_rows(conn, dealers=dealer_ids)
        heal.pop("examples", None)
        return {"decode": dec, "heal": heal}
    finally:
        conn.close()
