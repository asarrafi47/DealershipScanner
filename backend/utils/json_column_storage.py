"""
Persist JSON array columns as NULL when empty (never literal ``[]`` in SQLite/Postgres TEXT).
"""
from __future__ import annotations

import json
from typing import Any


def nullable_json_array_text(value: Any) -> str | None:
    """Return JSON array text for storage, or NULL when the array is empty."""
    if value is None:
        return None
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False) if value else None
    if isinstance(value, str):
        s = value.strip()
        if not s or s in ("[]", "null"):
            return None
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return s if parsed else None
        except (json.JSONDecodeError, TypeError):
            pass
        return s
    return None
