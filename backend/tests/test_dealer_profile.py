"""Dealer scan profile and recovery chain prioritization."""

from __future__ import annotations

import sqlite3

from backend.scanner.dealer.profile import (
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

    # inv.DB_PATH is resolved once at import time, so the env var above has no effect
    # on it post-import — patch the module attribute directly.
    monkeypatch.setattr(inv, "DB_PATH", str(db_path))
    with inv.db_conn() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cars (id INTEGER PRIMARY KEY, vin TEXT)"
        )
        conn.commit()

    record_winning_strategy("test-dealer", "dealer_inspire_algolia", platform_hints={"algolia"})
    assert get_cached_winning_strategy("test-dealer") == "dealer_inspire_algolia"
    assert get_cached_winning_strategy("missing") is None


def test_strategy_names_have_one_source():
    import backend.scanner.dealer.profile as prof
    import backend.scanner.inventory_recovery as ir

    assert ir.RECOVERY_STRATEGY_ORDER is prof.RECOVERY_STRATEGY_ORDER
    assert ir.HTTP_SAFE_STRATEGIES is prof.HTTP_SAFE_STRATEGIES
    assert prof._VALID_STRATEGIES == frozenset(prof.RECOVERY_STRATEGY_ORDER)


def test_cached_shopperexpress_api_moves_first_and_nothing_else_reorders():
    from backend.scanner.dealer.profile import RECOVERY_STRATEGY_ORDER

    base = list(RECOVERY_STRATEGY_ORDER)
    chain = ["dealer_inspire_algolia", "html_next_data", "shopperexpress_api", "dealer_on_cosmos"]
    out = prioritize_recovery_chain(chain, cached_strategy="shopperexpress_api")
    assert out == ["shopperexpress_api", "dealer_inspire_algolia", "html_next_data", "dealer_on_cosmos"]
    assert recovery_strategy_names(set(), cached_strategy="shopperexpress_api")[0] == "shopperexpress_api"
    assert recovery_strategy_names(set(), cached_strategy="shopperexpress_api")[1:] == base[1:]
    assert recovery_strategy_names(set()) == base


def test_shopperexpress_api_winner_is_cached(monkeypatch):
    import backend.scanner.dealer.profile as prof

    writes = []

    class _Cur:
        def execute(self, sql, params=()):
            if "INSERT" in sql:
                writes.append(params)

    class _Conn:
        def cursor(self):
            return _Cur()

        def commit(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(prof, "db_conn", lambda: _Conn())
    prof.record_winning_strategy("d1", "shopperexpress_api", platform_hints={"shopperexpress"})
    assert writes and writes[0][1] == "shopperexpress_api"
