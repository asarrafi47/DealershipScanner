"""Dealer scan profile and recovery chain prioritization."""

from __future__ import annotations

import sqlite3

from backend.scanner.dealer_profile import (
    get_cached_winning_strategy,
    manifest_recovery_strategies,
    manifest_skip_recovery,
    prioritize_recovery_chain,
    record_winning_strategy,
)
from backend.scanner.inventory_recovery import recovery_strategy_names


def test_prioritize_recovery_chain_moves_cached_first():
    chain = ["dealer_venom_typesense", "dealer_inspire_algolia", "html_next_data"]
    out = prioritize_recovery_chain(chain, cached_strategy="dealer_inspire_algolia")
    assert out[0] == "dealer_inspire_algolia"
    assert len(out) == len(chain)


def test_manifest_recovery_strategies_filters_invalid():
    dealer = {"recovery_strategies": ["dealer_inspire_algolia", "not_a_strategy", "html_next_data"]}
    assert manifest_recovery_strategies(dealer) == ["dealer_inspire_algolia", "html_next_data"]


def test_manifest_skip_recovery():
    assert manifest_skip_recovery({"skip_recovery": True}) is True
    assert manifest_skip_recovery({"skip_recovery": "yes"}) is True
    assert manifest_skip_recovery({}) is False


def test_recovery_strategy_names_manifest_override():
    chain = recovery_strategy_names(
        set(),
        manifest_strategies=["dealer_on_cosmos", "html_next_data"],
        cached_strategy=None,
    )
    assert chain == ["dealer_on_cosmos", "html_next_data"]


def test_record_and_read_winning_strategy(tmp_path, monkeypatch):
    db_path = tmp_path / "inventory.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    from backend.db import inventory_db as inv

    inv._INVENTORY_DB_PATH = None
    with inv.db_conn() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cars (id INTEGER PRIMARY KEY, vin TEXT)"
        )
        conn.commit()

    record_winning_strategy("test-dealer", "dealer_inspire_algolia", platform_hints={"algolia"})
    assert get_cached_winning_strategy("test-dealer") == "dealer_inspire_algolia"
    assert get_cached_winning_strategy("missing") is None
