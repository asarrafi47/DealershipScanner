"""Tests for scan price provenance tracking."""
from __future__ import annotations

import json

from backend.utils.price_provenance import (
    latest_price_drop,
    merge_price_provenance_for_upsert,
    price_dropped_on_scan,
    price_history_events_for_vdp,
)


def test_price_dropped_on_scan() -> None:
    assert price_dropped_on_scan(46000, 45000) is True
    assert price_dropped_on_scan(45000, 46000) is False
    assert price_dropped_on_scan(45000, 45000) is False
    assert price_dropped_on_scan(None, 45000) is False


def test_merge_new_row_seeds_listed_event() -> None:
    raw = merge_price_provenance_for_upsert(
        existing_provenance_json=None,
        existing_price=None,
        existing_first_seen_at=None,
        incoming_price=45999,
        recorded_at="2026-06-01T12:00:00Z",
        is_new_row=True,
    )
    assert raw is not None
    blob = json.loads(raw)
    assert len(blob["history"]) == 1
    assert blob["history"][0]["price"] == 45999
    assert blob["history"][0]["event"] == "listed"


def test_merge_existing_row_appends_drop() -> None:
    seed = merge_price_provenance_for_upsert(
        existing_provenance_json=None,
        existing_price=None,
        existing_first_seen_at=None,
        incoming_price=46000,
        recorded_at="2026-06-01T12:00:00Z",
        is_new_row=True,
    )
    updated = merge_price_provenance_for_upsert(
        existing_provenance_json=seed,
        existing_price=46000,
        existing_first_seen_at="2026-06-01T12:00:00Z",
        incoming_price=45000,
        recorded_at="2026-06-15T12:00:00Z",
        is_new_row=False,
    )
    assert updated is not None
    blob = json.loads(updated)
    assert len(blob["history"]) == 2
    assert blob["latest_change"]["direction"] == "down"
    assert blob["latest_change"]["delta"] == -1000
    assert latest_price_drop(updated)["new_price"] == 45000


def test_merge_existing_row_seeds_when_same_price() -> None:
    raw = merge_price_provenance_for_upsert(
        existing_provenance_json=None,
        existing_price=41000,
        existing_first_seen_at="2026-06-01T12:00:00Z",
        incoming_price=41000,
        recorded_at="2026-06-20T12:00:00Z",
        is_new_row=False,
    )
    assert raw is not None
    blob = json.loads(raw)
    assert len(blob["history"]) == 1
    assert blob["history"][0]["price"] == 41000
    assert blob["history"][0]["event"] == "listed"


def test_price_history_events_for_vdp() -> None:
    raw = merge_price_provenance_for_upsert(
        existing_provenance_json=None,
        existing_price=None,
        existing_first_seen_at=None,
        incoming_price=42000,
        recorded_at="2026-06-01T12:00:00Z",
        is_new_row=True,
    )
    events = price_history_events_for_vdp(raw)
    assert len(events) == 1
    assert events[0]["price"] == 42000.0
