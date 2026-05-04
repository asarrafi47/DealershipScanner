"""Debug artifact writers for BMW USA locator intake."""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_TITLE_RE = re.compile(r"<title[^>]*>([^<]+)</title>", re.I | re.DOTALL)


def extract_html_title(html: str) -> str:
    m = _TITLE_RE.search(html or "")
    return (m.group(1).strip() if m else "")[:500]


def debug_dir(project_root: Path) -> Path:
    d = project_root / "data" / "oem" / "bmw" / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_debug_artifact(project_root: Path, payload: dict[str, Any]) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    path = debug_dir(project_root) / f"bmw_ingest_debug_{ts}.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path
