"""B-024: BMW i-series model names are not inline-N cylinder counts."""
from __future__ import annotations

from backend.utils.query_parser import _extract_cylinders


def test_bmw_i4_not_cylinders() -> None:
    for q in ("bmw i4", "bmw i3 2022", "i7 xdrive"):
        assert _extract_cylinders(q) is None


def test_explicit_inline_still_parsed() -> None:
    assert _extract_cylinders("inline-6 sedan") == 6
    assert _extract_cylinders("v8 truck") == 8
