from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from oem_intake.paths import BMW_RAW_DIR, ensure_bmw_dirs


def append_bmw_batch_jsonl(payload: dict) -> Path:
    """Append one JSON line per ingest batch for external tooling / audits."""
    ensure_bmw_dirs()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = BMW_RAW_DIR / f"bmw_batch_{ts}.jsonl"
    with path.open("w", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return path
