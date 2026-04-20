#!/usr/bin/env python3
"""
Second-pass repair for incomplete inventory rows: inventory JSON → VDP → Mazda-safe inference.

Usage (from project root):
  python scripts/recover_incomplete_listings.py --limit 20
  python scripts/recover_incomplete_listings.py --vin JM3KFBBM8P0281095 --trace
  python scripts/recover_incomplete_listings.py --dry-run --skip-vdp

Requires: requests, playwright (chromium). Set INVENTORY_DB_PATH if not using ./inventory.db.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.chdir(ROOT)

from backend.db.inventory_db import (  # noqa: E402
    ensure_cars_table_columns,
    get_car_by_id,
    get_conn,
    is_car_incomplete,
    refresh_car_data_quality_score,
    update_car_row_partial,
    _parse_car_gallery,
    _parse_car_history_highlights,
)
from backend.utils.field_clean import clean_car_row_dict  # noqa: E402
from backend.utils.incomplete_recovery import (  # noqa: E402
    RECOVERY_VALUE_FIELDS,
    apply_patches_to_dict,
    compute_recovery_metrics,
    count_recovery_missing,
    finalize_recovery_status,
    merge_recovery_patch,
    mazda_deterministic_patch,
    preserve_placeholder_image_if_no_http,
    promotion_eligible,
    recover_vehicle_vdp_async,
    try_fetch_inventory_vehicle,
)
from backend.utils.incomplete_recovery import _utc_now_iso  # noqa: E402


def _json_safe(val: Any) -> Any:
    if isinstance(val, (dict, list, str, int, float, bool)) or val is None:
        return val
    return str(val)


def _diff_updates(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    keys = set(before.keys()) | set(after.keys())
    for k in keys:
        b, a = before.get(k), after.get(k)
        if k == "gallery":
            def norm_g(x):
                if isinstance(x, list):
                    return x
                if isinstance(x, str):
                    try:
                        return json.loads(x)
                    except (json.JSONDecodeError, TypeError):
                        return []
                return []

            if json.dumps(norm_g(b)) != json.dumps(norm_g(a)):
                out[k] = a
            continue
        if b != a:
            out[k] = a
    return out


def _select_candidates(limit: int | None, vin: str | None, min_score: float) -> list[dict[str, Any]]:
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM cars")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    for c in rows:
        _parse_car_gallery(c)
        _parse_car_history_highlights(c)

    inc = [c for c in rows if is_car_incomplete(c)]
    scored: list[tuple[float, dict[str, Any]]] = []
    for c in inc:
        m = compute_recovery_metrics(c)
        if m["recoverability_score"] < min_score:
            continue
        scored.append((m["recoverability_score"], c))
    scored.sort(key=lambda x: -x[0])
    out = [t[1] for t in scored]
    if vin:
        vu = vin.strip().upper()
        out = [c for c in out if (c.get("vin") or "").strip().upper() == vu]
    if limit is not None:
        out = out[:limit]
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Recover incomplete inventory rows (inventory → VDP → Mazda rules).")
    p.add_argument("--limit", type=int, default=30, help="Max vehicles (sorted by recoverability score desc).")
    p.add_argument("--vin", type=str, default="", help="Only this VIN (must be incomplete).")
    p.add_argument("--min-score", type=float, default=0.0, help="Minimum recoverability_score.")
    p.add_argument("--skip-vdp", action="store_true", help="Inventory + Mazda rules only (no Playwright).")
    p.add_argument("--dry-run", action="store_true", help="Do not write SQLite.")
    p.add_argument("--trace", action="store_true", help="Print JSON detail per vehicle.")
    args = p.parse_args()

    conn = get_conn()
    ensure_cars_table_columns(conn.cursor())
    conn.commit()
    conn.close()

    vin_f = args.vin.strip().upper() if args.vin else None
    candidates = _select_candidates(args.limit, vin_f, args.min_score)
    if not candidates:
        print("No incomplete candidates matched filters.")
        return 0

    print(f"Selected {len(candidates)} incomplete row(s).")
    if vin_f:
        print(f"VIN filter: {vin_f}")

    async def _run() -> dict[str, Any]:
        stats: dict[str, Any] = {
            "total": len(candidates),
            "recovered": 0,
            "partially_recovered": 0,
            "unrecoverable": 0,
            "field_hits": Counter(),
        }

        from playwright.async_api import async_playwright

        async def process_loop(page: Optional[Any]) -> None:
            for c in candidates:
                cid = int(c["id"])
                before_row = get_car_by_id(cid)
                if not before_row:
                    continue
                before_missing = count_recovery_missing(before_row)
                before_snap = dict(before_row)
                working = dict(before_snap)
                notes: list[str] = []
                sources: list[str] = []

                inv_match, inv_trace = try_fetch_inventory_vehicle(working)
                notes.extend(inv_trace)
                if inv_match:
                    patch = merge_recovery_patch(working, inv_match, source_tag="inventory", notes=notes)
                    apply_patches_to_dict(working, patch)
                    sources.append("inventory")

                if not args.skip_vdp and page is not None:
                    dn = (working.get("dealer_name") or "recovery")[:80]
                    vdp_stats = await recover_vehicle_vdp_async(page, working, dn)
                    if vdp_stats.get("enriched") or (vdp_stats.get("filled") or []):
                        sources.append("vdp")
                    notes.append(
                        "vdp:"
                        + json.dumps(
                            {
                                "enriched": vdp_stats.get("enriched"),
                                "filled": vdp_stats.get("filled"),
                                "visited": vdp_stats.get("visited"),
                            }
                        )
                    )

                maz_notes: list[str] = []
                maz = mazda_deterministic_patch(working, maz_notes)
                notes.extend(maz_notes)
                if maz:
                    mp = merge_recovery_patch(working, maz, source_tag="mazda_rules", notes=notes)
                    apply_patches_to_dict(working, mp)
                    sources.append("mazda_rules")

                clean = clean_car_row_dict(dict(working))
                preserve_placeholder_image_if_no_http(before_snap, clean)
                after_missing = count_recovery_missing(clean)
                metrics_after = compute_recovery_metrics(clean)

                vehicle_only = _diff_updates(before_snap, dict(clean))
                st, summary = finalize_recovery_status(
                    before_missing, after_missing, bool(vehicle_only)
                )
                meta_keys = {
                    "recovery_status": st,
                    "recovery_attempted_at": _utc_now_iso(),
                    "recovery_source": ",".join(dict.fromkeys(sources)) if sources else "none",
                    "recovery_notes": json.dumps(notes[:120]),
                    "missing_field_count": after_missing,
                    "recoverability_score": metrics_after["recoverability_score"],
                }
                raw_updates = dict(vehicle_only)
                raw_updates.update(meta_keys)

                if st == "recovered":
                    stats["recovered"] += 1
                elif st == "partially_recovered":
                    stats["partially_recovered"] += 1
                else:
                    stats["unrecoverable"] += 1

                for line in notes:
                    if ":" in line and not line.startswith("vdp:"):
                        stats["field_hits"][line.split(":", 1)[0]] += 1

                prom = promotion_eligible(clean) and not is_car_incomplete(clean)
                print(
                    f"[{clean.get('vin')}] {st} | missing {before_missing}->{after_missing} | "
                    f"sources={meta_keys['recovery_source']} | promote={prom} | {summary}"
                )

                if args.trace:
                    print(
                        json.dumps(
                            {
                                "metrics": metrics_after,
                                "before_subset": {
                                    k: _json_safe(before_snap.get(k))
                                    for k in RECOVERY_VALUE_FIELDS + ("price", "image_url", "title", "source_url")
                                },
                                "after_subset": {
                                    k: _json_safe(clean.get(k))
                                    for k in RECOVERY_VALUE_FIELDS + ("price", "image_url", "title", "source_url")
                                },
                            },
                            indent=2,
                            default=str,
                        )
                    )

                if not args.dry_run and raw_updates:
                    update_car_row_partial(cid, {k: v for k, v in raw_updates.items() if k != "id"})
                    refresh_car_data_quality_score(cid)

        if args.skip_vdp:
            await process_loop(None)
        else:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                try:
                    context = await browser.new_context(
                        viewport={"width": 1365, "height": 900},
                        user_agent=(
                            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                        ),
                    )
                    page = await context.new_page()
                    await process_loop(page)
                finally:
                    await browser.close()
        return stats

    stats = asyncio.run(_run())

    print("--- summary ---")
    print(f"total scanned: {stats['total']}")
    print(f"recovered: {stats['recovered']}")
    print(f"partially_recovered: {stats['partially_recovered']}")
    print(f"unrecoverable: {stats['unrecoverable']}")
    print("top recovery note prefixes:", stats["field_hits"].most_common(20))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
