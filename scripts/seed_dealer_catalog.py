#!/usr/bin/env python3
"""Seed ``dealer_catalog`` so the scanner scheduler has a fleet to rotate.

``schedule_due_refresh_jobs`` only enqueues dealers that already have a
``dealer_catalog`` row past its ``next_scan_at``. Until now the only writer was
``record_catalog_after_success``, which runs *after* a job succeeds -- so the
catalog could never grow beyond whatever was onboarded by hand. This backfills
it from the same roster ``scanner.py`` itself resolves ``--dealer-id`` against,
which guarantees every seeded id is actually scannable.

Roster sources (``--source``):
  active     dealers with live cars (~465 in prod). THE DEFAULT.
  scannable  active inventory UNION every stored recipe -- ~22,000 in prod,
             i.e. the nationwide discovery registry. Refused above --max-seed.
  manifest   the dealers.json file (or DEALERS_MANIFEST_PATH).

Existing rows are left alone by default (their cadence is already in flight);
pass --reschedule to re-stagger them too. Rescheduling never touches a dealer's
``scan_interval_hours`` and skips dealers the scheduler has parked for being
unreachable (``dealer_scan_status`` dns_fail / redirect_offsite inside the
backoff window) -- otherwise a reseed would resurrect exactly the dealers the
scheduler just decided to leave alone.

Usage (inside the Railway worker container, where the roster and DB resolve):
  cd /app && python3 scripts/seed_dealer_catalog.py --limit 25 --dry-run
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
_log = logging.getLogger("seed-catalog")

_ROSTER_FLAGS = ("DEALERS_FROM_SCANNABLE", "DEALERS_FROM_ACTIVE_INVENTORY", "DEALERS_FROM_DB")
_PARKED_REASONS = ("dns_fail", "redirect_offsite")


def _roster(source: str) -> list[dict]:
    """Load the dealer roster exactly the way the scanner will at scan time.

    ``load_manifest()`` checks the roster flags in a fixed priority order
    (SCANNABLE, then ACTIVE_INVENTORY, then DB), so the competing flags have to
    be cleared or an inherited SCANNABLE=1 would silently win over ``active``.
    """
    for k in _ROSTER_FLAGS:
        os.environ.pop(k, None)
    if source == "scannable":
        os.environ["DEALERS_FROM_SCANNABLE"] = "1"
    elif source == "active":
        os.environ["DEALERS_FROM_ACTIVE_INVENTORY"] = "1"
    # "manifest": no flag -> load_manifest() reads the file.
    from backend.scanner.manifest import load_manifest

    return load_manifest() or []


def _parked_dealer_ids(cur, qmarks_to_percent_s, now: datetime, backoff_days: int) -> set[str]:
    """Dealers the scheduler has deliberately pushed out past the unreachable backoff."""
    try:
        cur.execute(
            qmarks_to_percent_s(
                "SELECT dealer_key, reason, checked_at FROM dealer_scan_status "
                "WHERE reason IN (?, ?)"
            ),
            _PARKED_REASONS,
        )
        rows = cur.fetchall() or []
    except Exception:
        # dealer_scan_status may not exist (migration V008 not applied here).
        return set()
    parked: set[str] = set()
    window = timedelta(days=backoff_days)
    for dealer_key, _reason, checked_at in rows:
        try:
            checked = datetime.fromisoformat(str(checked_at))
            if checked.tzinfo is None:
                checked = checked.replace(tzinfo=timezone.utc)
        except (TypeError, ValueError):
            continue
        if now - checked < window:
            parked.add(str(dealer_key or "").strip())
    return parked


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--source",
        choices=("active", "scannable", "manifest"),
        default="active",
        help="Roster to seed from (default: active = dealers with live cars, ~465).",
    )
    ap.add_argument("--interval-hours", type=int, default=24, help="Cadence for NEWLY seeded dealers.")
    ap.add_argument(
        "--stagger-minutes",
        type=float,
        default=0.0,
        help="Spacing between consecutive next_scan_at values. 0 = spread evenly across one interval.",
    )
    ap.add_argument(
        "--limit", type=int, default=0,
        help="Seed at most N NEW dealers (0 = all). Applied after existing rows are excluded, "
             "so repeated runs keep adding new ones.",
    )
    ap.add_argument(
        "--max-seed", type=int, default=1000,
        help="Refuse to seed more than this many dealers in one run (guards against the "
             "22,000-row 'scannable' roster). Raise it explicitly if you really mean it.",
    )
    ap.add_argument(
        "--reschedule",
        action="store_true",
        help="Also re-stagger next_scan_at for dealers already in the catalog "
             "(their scan_interval_hours is left untouched; parked dealers are skipped).",
    )
    ap.add_argument("--dry-run", action="store_true", help="Report what would change, write nothing.")
    args = ap.parse_args()

    # Same bootstrap as scanner.py: .env first, then vault, so INVENTORY_DATABASE_URL
    # resolves on a correctly configured machine instead of always aborting.
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
    try:
        from backend.utils.kmac_vault import load_kmac_vault_secrets

        load_kmac_vault_secrets()
    except Exception as exc:  # vault is optional here; the DB URL is what matters
        _log.debug("vault preload skipped: %s", exc)

    from backend.db.inventory_pg import is_inventory_postgres, pg_connect, qmarks_to_percent_s

    if not is_inventory_postgres():
        _log.error("INVENTORY_DATABASE_URL must point at Postgres.")
        return 1

    dealers = _roster(args.source)
    if not dealers:
        _log.error("Roster '%s' is empty -- refusing to seed.", args.source)
        return 1

    seen: set[str] = set()
    roster_ids: list[str] = []
    for d in dealers:
        did = str(d.get("dealer_id") or "").strip()
        if did and did not in seen:
            seen.add(did)
            roster_ids.append(did)
    _log.info("Roster '%s': %d dealer(s)", args.source, len(roster_ids))

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    interval = max(1, int(args.interval_hours))

    conn = pg_connect()
    inserted = rescheduled = skipped_existing = skipped_parked = 0
    try:
        cur = conn.cursor()
        if args.dry_run:
            # Never run DDL under --dry-run. Verify the table exists instead.
            cur.execute("SELECT to_regclass('public.dealer_catalog')")
            row = cur.fetchone()
            if not row or not row[0]:
                _log.error(
                    "dealer_catalog does not exist. Run once without --dry-run (or start the "
                    "scanner worker, which creates it) before dry-running."
                )
                return 1
        else:
            from backend.scanner.job_queue import init_job_queue_schema

            init_job_queue_schema()

        cur.execute("SELECT dealer_id FROM dealer_catalog")
        existing = {str(r[0]).strip() for r in (cur.fetchall() or [])}

        new_ids = sorted(i for i in roster_ids if i not in existing)
        skipped_existing = len(roster_ids) - len(new_ids)
        if args.limit:
            new_ids = new_ids[: args.limit]
        if len(new_ids) > args.max_seed:
            _log.error(
                "Refusing to seed %d dealers (> --max-seed %d). Source '%s' is probably the "
                "nationwide registry; use --source active, --limit, or raise --max-seed on purpose.",
                len(new_ids), args.max_seed, args.source,
            )
            return 1

        resched_ids: list[str] = []
        if args.reschedule:
            parked = _parked_dealer_ids(cur, qmarks_to_percent_s, now, _backoff_days())
            for i in sorted(existing & set(roster_ids)):
                if i in parked:
                    skipped_parked += 1
                else:
                    resched_ids.append(i)

        # Stagger across ONE interval so the fleet does not all fire at once.
        # The step is computed from the rows actually being written this run.
        total = len(new_ids) + len(resched_ids)
        if args.stagger_minutes > 0:
            step = timedelta(minutes=args.stagger_minutes)
        else:
            step = timedelta(hours=interval) / max(1, total)

        slot = 0
        for did in new_ids:
            nxt = (now + step * slot).isoformat()
            slot += 1
            if not args.dry_run:
                cur.execute(
                    qmarks_to_percent_s(
                        "INSERT INTO dealer_catalog "
                        "(dealer_id, scan_interval_hours, next_scan_at, onboarded_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?) ON CONFLICT (dealer_id) DO NOTHING"
                    ),
                    (did, interval, nxt, now_iso, now_iso),
                )
            inserted += 1
        for did in resched_ids:
            nxt = (now + step * slot).isoformat()
            slot += 1
            if not args.dry_run:
                # Only the schedule moves; the dealer's own cadence is preserved.
                cur.execute(
                    qmarks_to_percent_s(
                        "UPDATE dealer_catalog SET next_scan_at = ?, updated_at = ? WHERE dealer_id = ?"
                    ),
                    (nxt, now_iso, did),
                )
            rescheduled += 1
        if not args.dry_run:
            conn.commit()
    finally:
        conn.close()

    verb = "would seed" if args.dry_run else "seeded"
    _log.info(
        "%s %d new dealer(s) from '%s' (rescheduled=%d, already-present=%d, parked-skipped=%d, "
        "cadence=%dh, spacing=%s)",
        verb, inserted, args.source, rescheduled, skipped_existing, skipped_parked, interval, step,
    )
    if inserted or rescheduled:
        last = (now + step * max(0, slot - 1)).isoformat(timespec="minutes")
        _log.info("Last scheduled first scan lands at %s", last)
    return 0


def _backoff_days() -> int:
    from backend.scanner.job_queue import _unreachable_backoff_days

    return _unreachable_backoff_days()


if __name__ == "__main__":
    raise SystemExit(main())
