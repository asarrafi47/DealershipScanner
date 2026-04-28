#!/usr/bin/env python3
"""
Fix BMW trim fields by combining model designation with any drive/variant suffix.

BMW series cars (330i, 430i, 530i, 740i, M235i, M4, etc.) encode the trim
in the model name. When trim is empty or only has a suffix (xDrive, Gran Coupe),
build the full trim = "{model} {suffix}".

X-series, Z-series, and i-series already have correct trims — left untouched.

Usage:
  python fix_bmw_trims.py            # apply fixes
  python fix_bmw_trims.py --dry-run  # preview without writing
"""
import argparse
import logging
import os
import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv
    load_project_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("fix_bmw_trims")

DB_PATH = os.environ.get("INVENTORY_DB_PATH", "inventory.db")

# BMW series model codes: digits + letter suffix (i/e/d/s) or M + ...
# Matches: 330i, 430i, 430I, 530e, 540i, 740i, 840i, M235i, M340i, M4, M5, M8, etc.
# Does NOT match: X1, X3, X5, X7, Z4, i4, i7, iX (those have correct trims already)
_SERIES_MODEL_RE = re.compile(
    r'^(M\d{1,3}[ids]?|[2-9]\d{2}[eids]?)$',
    re.IGNORECASE
)

# Suffixes that should be appended to the model code to form the full trim
_DRIVE_SUFFIXES = re.compile(
    r'^(xDrive|sDrive|Gran Coupe|GC|Cabriolet|Touring|Competition|convertible)',
    re.IGNORECASE
)


def _normalize_model(model: str) -> str:
    """Normalize model case: 430I → 430i, M235I → M235i."""
    # BMW uses lowercase 'i' — uppercase I at end is a scanning artifact
    return re.sub(r'(?<=[0-9])I$', 'i', model.strip(), flags=re.IGNORECASE)


def _is_series_car(model: str) -> bool:
    """True for series number cars (330i, M4, etc.) — not X/Z/i prefix models."""
    m = model.strip()
    return bool(_SERIES_MODEL_RE.match(m))


def _build_trim(model_norm: str, existing_trim: str) -> str:
    """Compute corrected trim value."""
    t = (existing_trim or "").strip()
    if not t:
        return model_norm
    # Trim already starts with the model code → already correct
    if t.lower().startswith(model_norm.lower()):
        return t
    # Trim is just a drive/body suffix → prepend model
    return f"{model_norm} {t}"


def main():
    ap = argparse.ArgumentParser(description="Fix BMW trim fields")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    cars = conn.execute(
        "SELECT id, vin, year, model, trim, title FROM cars WHERE make='BMW'"
    ).fetchall()

    updates = []
    for row in cars:
        model_raw = (row["model"] or "").strip()
        if not _is_series_car(model_raw):
            continue  # X-series, i-series, Z-series — already correct

        model_norm = _normalize_model(model_raw)
        old_trim = row["trim"] or ""
        new_trim = _build_trim(model_norm, old_trim)

        # Also fix the model field if it had uppercase I (430I → 430i)
        new_model = model_norm if model_norm != model_raw else None

        changed = (new_trim != old_trim) or (new_model is not None)
        if not changed:
            continue

        updates.append({
            "id": row["id"],
            "vin": row["vin"],
            "year": row["year"],
            "old_model": model_raw,
            "new_model": new_model or model_raw,
            "old_trim": old_trim,
            "new_trim": new_trim,
        })

    if not updates:
        logger.info("No BMW trims to fix.")
        conn.close()
        return

    for u in updates:
        model_change = f" model: {u['old_model']}→{u['new_model']}" if u['old_model'] != u['new_model'] else ""
        trim_change = f" trim: '{u['old_trim']}'→'{u['new_trim']}'" if u['old_trim'] != u['new_trim'] else ""
        logger.info("[%s] %s BMW%s%s", u["vin"], u["year"], model_change, trim_change)

    if not args.dry_run:
        for u in updates:
            conn.execute(
                "UPDATE cars SET model=?, trim=? WHERE id=?",
                (u["new_model"], u["new_trim"], u["id"])
            )
        conn.commit()
        logger.info("Updated %d BMW records.", len(updates))
    else:
        logger.info("DRY-RUN: would update %d BMW records.", len(updates))

    conn.close()


if __name__ == "__main__":
    main()
