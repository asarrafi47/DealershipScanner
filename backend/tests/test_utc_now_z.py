"""P11A.6: ``utc_now_z`` keeps the stored ``scraped_at`` format byte-identical
to the former ``datetime.utcnow().isoformat() + "Z"`` (TEXT compared lexically)."""
from __future__ import annotations

import subprocess
import warnings
from datetime import datetime, timezone

import pytest

from backend.scanner.upsert import serialize


def _freeze(monkeypatch, instant: datetime):
    class _Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return instant.astimezone(tz) if tz else instant.replace(tzinfo=None)

    monkeypatch.setattr(serialize, "datetime", _Frozen)


@pytest.mark.parametrize(
    "instant,expected",
    [
        (datetime(2026, 10, 27, 12, 0, 0, 123456, tzinfo=timezone.utc), "2026-10-27T12:00:00.123456Z"),
        (datetime(2026, 10, 27, 12, 0, 0, 0, tzinfo=timezone.utc), "2026-10-27T12:00:00Z"),
    ],
)
def test_golden_format_matches_old_utcnow_string(monkeypatch, instant, expected):
    _freeze(monkeypatch, instant)
    old = instant.replace(tzinfo=None).isoformat() + "Z"
    assert serialize.utc_now_z() == old == expected


def test_live_value_shape_and_lexical_order():
    a = serialize.utc_now_z()
    b = serialize.utc_now_z()
    assert a.endswith("Z") and "+" not in a
    datetime.fromisoformat(a[:-1])
    assert a <= b


def test_no_deprecation_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        serialize.utc_now_z()


def test_no_utcnow_calls_outside_tests():
    out = subprocess.run(
        ["git", "grep", "-n", "utcnow()", "--", "backend/*.py", ":!backend/tests"],
        capture_output=True, text=True,
    ).stdout
    hits = [l for l in out.splitlines() if "_utcnow" not in l]
    assert hits == []
