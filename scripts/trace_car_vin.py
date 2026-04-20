#!/usr/bin/env python3
"""Print DB row + serializer + detail-template display snapshot for a VIN (local dev)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from backend.db.inventory_db import get_car_by_vin  # noqa: E402
from backend.knowledge_engine import prepare_car_detail_context  # noqa: E402
from backend.utils.car_serialize import (  # noqa: E402
    build_detail_display_snapshot,
    serialize_car_for_api,
)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: trace_car_vin.py <VIN>", file=sys.stderr)
        sys.exit(2)
    vin = sys.argv[1].strip().upper()
    raw = get_car_by_vin(vin) or get_car_by_vin(sys.argv[1].strip())
    if not raw:
        print(json.dumps({"ok": False, "error": "not_found", "vin": vin}))
        sys.exit(1)
    ctx = prepare_car_detail_context(raw)
    vs = ctx.get("verified_specs") or {}
    detail = serialize_car_for_api(raw, include_verified=False, verified_specs=vs)
    listing = serialize_car_for_api(raw, include_verified=False)
    snap = build_detail_display_snapshot(vs, detail)
    out = {
        "ok": True,
        "vin": raw.get("vin"),
        "car_id": raw.get("id"),
        "raw_db_row": {k: raw[k] for k in sorted(raw.keys())},
        "verified_specs": vs,
        "serialized_car_detail": detail,
        "serialized_listing_style": listing,
        "display_values_car_detail_template": snap,
    }
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
