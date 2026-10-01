"""Value coercion shared by the verified-spec steps."""

from __future__ import annotations

from typing import Any


def int_or_none(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
