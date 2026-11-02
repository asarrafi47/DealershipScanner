"""adapt_sql_for_postgres_execute is a pure, memoized str -> str|None function."""
from backend.db import inventory_pg


def test_cache_hit_returns_equal_output():
    sql = "INSERT INTO cars (vin, price) VALUES (?, ?) ON CONFLICT (vin) DO UPDATE SET price = IFNULL(excluded.price, 0) WHERE note LIKE 'a%'"
    inventory_pg.adapt_sql_for_postgres_execute.cache_clear()
    first = inventory_pg.adapt_sql_for_postgres_execute(sql)
    second = inventory_pg.adapt_sql_for_postgres_execute(sql)
    assert first == second
    assert "%s" in first and "COALESCE(" in first and "'a%%'" in first
    info = inventory_pg.adapt_sql_for_postgres_execute.cache_info()
    assert info.hits == 1 and info.misses == 1
    assert info.maxsize == 512


def test_pragma_noop_cached_as_none():
    assert inventory_pg.adapt_sql_for_postgres_execute("PRAGMA journal_mode=WAL") is None
    assert inventory_pg.adapt_sql_for_postgres_execute("PRAGMA journal_mode=WAL") is None
