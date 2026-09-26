"""
Quarantine vehicle-price rows that leaked into the ``package_values`` registry.

Window-sticker parsing used to feed a sticker's BASE VEHICLE PRICE, TOTAL VEHICLE
PRICE and destination-charge lines into the package-value registry as if they were
optional equipment (see ``_is_vehicle_price_row`` in
``backend/enrichment/window_sticker_service.py`` and the "NOT FIXED HERE" block
comment in ``backend/enrichment/package_registry.py``). Live damage: 390 rows over
$25,000 — ``name_display='Base'`` at $198,300 on a Mercedes S-Class, ``'i8Roadster'``
at $163,300, ``'WHICHEVER COMES FIRST ... TOTAL VEHICLE PRICE*'`` at $161,990.

The writer is now filtered; this script cleans the rows the unfiltered writer left
behind. It consumes two files:

* ``--suspects``: the suspect-rows dump, shaped like ``package_values`` rows carrying
  at least the key columns (year, make, model, trim, kind, match_key) plus a
  ``row_index``;
* ``--labels``: a labels file ``[{"row_index": int, "label": str, "confidence": str}]``
  classifying each suspect row.

For rows labelled ``base_vehicle_price`` / ``total_vehicle_price`` /
``destination_or_fee`` with confidence ``high`` or ``medium`` it:

1. snapshots the FULL current ``package_values`` row into
   ``package_values_msrp_quarantine`` (created if absent: same columns plus
   ``quarantined_at timestamptz``, ``label text``, ``reason text``), then
2. sets ``msrp = NULL, msrp_source = NULL, msrp_authority = 0`` on the
   ``package_values`` row.

Rows are never DELETED — the names may still be legitimate package names worth
keeping unpriced; only the poisoned price is withdrawn, and the snapshot preserves
the prior value (a verification pass must always store what it removed).

Dry-run is the DEFAULT. Writing requires an explicit ``--apply``, and ``--apply``
refuses to touch more than 450 rows as a sanity bound.

    .venv/bin/python -m backend.scripts.quarantine_package_value_msrps \
        --labels /path/to/labels.json            # dry-run (default)
    .venv/bin/python -m backend.scripts.quarantine_package_value_msrps \
        --labels /path/to/labels.json --apply    # write
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_log = logging.getLogger("quarantine_package_value_msrps")

# Output of the suspect-row census; repo-local and gitignored (workspace/).
_DEFAULT_SUSPECTS = str(
    Path(__file__).resolve().parents[2] / "workspace" / "data_quality" / "suspect_rows.json"
)

# Labels that mean "this price is the CAR's price (or a mandatory fee), not an
# option's" — the only ones this script acts on.
QUARANTINE_LABELS = ("base_vehicle_price", "total_vehicle_price", "destination_or_fee")
ACCEPTED_CONFIDENCE = ("high", "medium")

# Refuse --apply beyond this many rows. The audited damage was 390 rows; anything
# far past that means the labels file is wrong, not the registry.
MAX_APPLY_ROWS = 450

# package_values natural key (UNIQUE constraint), used to locate each live row.
_KEY_COLUMNS = ("year", "make", "model", "trim", "kind", "match_key")


def _dsn() -> str:
    import os

    dsn = (os.environ.get("INVENTORY_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if dsn:
        return dsn
    env = _REPO_ROOT / ".env"
    if env.exists():
        m = re.search(r"^INVENTORY_DATABASE_URL=(.+)$", env.read_text(), re.M)
        if m:
            return m.group(1).strip().strip("'\"")
    raise SystemExit("INVENTORY_DATABASE_URL is not set")


def _load_json_list(path: Path, what: str) -> list[dict[str, Any]]:
    try:
        data = json.loads(path.read_text())
    except OSError as e:
        raise SystemExit(f"cannot read {what} file {path}: {e}")
    except json.JSONDecodeError as e:
        raise SystemExit(f"{what} file {path} is not valid JSON: {e}")
    if not isinstance(data, list) or not all(isinstance(x, dict) for x in data):
        raise SystemExit(f"{what} file {path} must be a JSON array of objects")
    return data


def _select_targets(
    labels: list[dict[str, Any]], suspects: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Join labels to suspect rows on row_index; keep only quarantinable ones."""
    by_index: dict[int, dict[str, Any]] = {}
    for row in suspects:
        try:
            by_index[int(row["row_index"])] = row
        except (KeyError, TypeError, ValueError):
            raise SystemExit("every suspect row must carry an integer row_index")

    targets: list[dict[str, Any]] = []
    unmatched = 0
    for lab in labels:
        label = str(lab.get("label") or "").strip().lower()
        confidence = str(lab.get("confidence") or "").strip().lower()
        if label not in QUARANTINE_LABELS or confidence not in ACCEPTED_CONFIDENCE:
            continue
        try:
            idx = int(lab["row_index"])
        except (KeyError, TypeError, ValueError):
            raise SystemExit("every label must carry an integer row_index")
        row = by_index.get(idx)
        if row is None:
            unmatched += 1
            continue
        missing = [k for k in _KEY_COLUMNS if k not in row]
        if missing:
            raise SystemExit(
                f"suspect row_index={idx} lacks key column(s) {missing}; "
                f"need all of {_KEY_COLUMNS}"
            )
        targets.append({**row, "_label": label, "_confidence": confidence})
    if unmatched:
        _log.warning(
            "%d label(s) referenced row_index values absent from the suspects file", unmatched
        )
    return targets


def _find_live_row(cur, target: dict[str, Any]) -> dict[str, Any] | None:
    """The current package_values row for this target's natural key, or None."""
    cur.execute(
        """
        SELECT * FROM package_values
        WHERE year IS NOT DISTINCT FROM %s
          AND make = %s AND model = %s
          AND trim IS NOT DISTINCT FROM %s
          AND kind = %s AND match_key = %s
        """,
        (
            target.get("year"),
            target.get("make"),
            target.get("model"),
            target.get("trim") if target.get("trim") is not None else "",
            target.get("kind"),
            target.get("match_key"),
        ),
    )
    row = cur.fetchone()
    if row is None:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def _ensure_quarantine_table(cur) -> None:
    # Same columns as package_values (no constraints/indexes carried over — the
    # same row may be quarantined more than once) plus audit columns.
    cur.execute(
        "CREATE TABLE IF NOT EXISTS package_values_msrp_quarantine (LIKE package_values)"
    )
    cur.execute(
        """
        ALTER TABLE package_values_msrp_quarantine
            ADD COLUMN IF NOT EXISTS quarantined_at timestamptz NOT NULL DEFAULT now(),
            ADD COLUMN IF NOT EXISTS label text,
            ADD COLUMN IF NOT EXISTS reason text
        """
    )


def _quarantine_one(cur, live: dict[str, Any], label: str, reason: str) -> None:
    """Snapshot the full live row, then null its msrp fields. Never deletes."""
    cols = [c for c in live.keys()]
    col_list = ", ".join(cols) + ", label, reason"
    placeholders = ", ".join(["%s"] * (len(cols) + 2))
    cur.execute(
        f"INSERT INTO package_values_msrp_quarantine ({col_list}) VALUES ({placeholders})",
        [live[c] for c in cols] + [label, reason],
    )
    cur.execute(
        """
        UPDATE package_values
        SET msrp = NULL, msrp_source = NULL, msrp_authority = 0
        WHERE id = %s
        """,
        (live["id"],),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--labels", required=True,
        help='labels JSON: [{"row_index": int, "label": str, "confidence": str}, ...]',
    )
    ap.add_argument(
        "--suspects", default=_DEFAULT_SUSPECTS,
        help="suspect-rows JSON carrying package_values key columns + row_index",
    )
    ap.add_argument(
        "--apply", action="store_true",
        help="actually write (default is dry-run: report only, touch nothing)",
    )
    ap.add_argument(
        "--sample", type=int, default=10,
        help="how many affected rows to print in the report (default 10)",
    )
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    labels = _load_json_list(Path(args.labels), "labels")
    suspects = _load_json_list(Path(args.suspects), "suspects")
    targets = _select_targets(labels, suspects)
    _log.info(
        "%d label(s), %d suspect row(s), %d quarantinable target(s) "
        "(labels %s at confidence %s)",
        len(labels), len(suspects), len(targets),
        "/".join(QUARANTINE_LABELS), "/".join(ACCEPTED_CONFIDENCE),
    )
    if not targets:
        _log.info("nothing to do")
        return 0

    # Export the DSN before touching repo internals (see
    # feed_package_registry_from_vision.py for why .env is parsed directly).
    import os

    os.environ.setdefault("INVENTORY_DATABASE_URL", _dsn())

    import psycopg

    conn = psycopg.connect(_dsn(), connect_timeout=15)
    try:
        conn.autocommit = False
        cur = conn.cursor()

        matched: list[tuple[dict[str, Any], dict[str, Any]]] = []  # (target, live row)
        missing = 0
        already_null = 0
        for target in targets:
            live = _find_live_row(cur, target)
            if live is None:
                missing += 1
                _log.warning(
                    "no live package_values row for row_index=%s key=%s",
                    target.get("row_index"),
                    {k: target.get(k) for k in _KEY_COLUMNS},
                )
                continue
            if live.get("msrp") is None:
                already_null += 1
                continue
            matched.append((target, live))

        by_label: dict[str, int] = {}
        for target, _live in matched:
            by_label[target["_label"]] = by_label.get(target["_label"], 0) + 1

        _log.info(
            "would quarantine %d row(s) (%d target(s) had no live row, %d already have msrp NULL)",
            len(matched), missing, already_null,
        )
        for label in QUARANTINE_LABELS:
            _log.info("  %-20s %d", label, by_label.get(label, 0))
        for target, live in matched[: max(args.sample, 0)]:
            _log.info(
                "  sample: id=%s %s %s %s trim=%r %r msrp=%s source=%s label=%s/%s",
                live.get("id"), live.get("year"), live.get("make"), live.get("model"),
                live.get("trim"), live.get("name_display"), live.get("msrp"),
                live.get("msrp_source"), target["_label"], target["_confidence"],
            )

        if not args.apply:
            conn.rollback()
            _log.info("dry-run (default): nothing written; pass --apply to quarantine")
            return 0

        if len(matched) > MAX_APPLY_ROWS:
            conn.rollback()
            _log.error(
                "refusing --apply: %d rows exceeds the sanity bound of %d "
                "(the audited damage was ~390 rows; recheck the labels file)",
                len(matched), MAX_APPLY_ROWS,
            )
            return 2

        _ensure_quarantine_table(cur)
        for target, live in matched:
            reason = (
                f"vehicle-price line fed into registry by unfiltered oem_sticker writer; "
                f"labelled {target['_label']} ({target['_confidence']}) "
                f"row_index={target.get('row_index')}"
            )
            _quarantine_one(cur, live, target["_label"], reason)
        conn.commit()
        _log.info(
            "quarantined %d row(s): snapshot in package_values_msrp_quarantine, "
            "msrp/msrp_source nulled, msrp_authority reset to 0 (rows kept)",
            len(matched),
        )
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
