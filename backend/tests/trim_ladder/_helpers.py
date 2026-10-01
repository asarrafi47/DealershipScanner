"""Helpers shared by more than one trim-ladder test module."""

from __future__ import annotations


def _all_bullets(result: dict) -> list[str]:
    return [b for s in (result or {}).get("steps") or [] for b in (s.get("adds") or [])]
