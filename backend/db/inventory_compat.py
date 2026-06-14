"""PostgreSQL inventory connections (uniform cursor API; SQLite only for pytest)."""

from __future__ import annotations

from typing import Any, Iterator, Literal

from backend.db.inventory_pg import (
    _INVENTORY_POSTGRES_REQUIRED_MSG,
    adapt_sql_for_postgres_execute,
    assert_inventory_backend_configured,
    inventory_sqlite_tests_allowed,
    is_inventory_postgres,
)


class InventoryCursor:
    def __init__(self, raw: Any, *, backend: Literal["sqlite", "postgres"]):
        self._c = raw
        self._backend = backend
        self._noop = False

    @property
    def connection(self) -> Any:
        return self._c.connection

    def execute(self, sql: str, params: tuple | list | None = None) -> InventoryCursor:
        params = tuple(params) if params is not None else ()
        if self._backend == "postgres":
            adapted = adapt_sql_for_postgres_execute(sql)
            if adapted is None:
                self._noop = True
                return self
            self._noop = False
            self._c.execute(adapted, params)
            return self
        self._noop = False
        self._c.execute(sql, params)
        return self

    def executemany(self, sql: str, seq_of_params: Iterator[tuple]) -> InventoryCursor:
        if self._backend == "postgres":
            adapted = adapt_sql_for_postgres_execute(sql)
            if adapted is None:
                self._noop = True
                return self
            self._noop = False
            self._c.executemany(adapted, seq_of_params)
            return self
        self._noop = False
        self._c.executemany(sql, seq_of_params)
        return self

    def fetchone(self) -> Any:
        if self._noop:
            return None
        return self._c.fetchone()

    def fetchall(self) -> list:
        if self._noop:
            return []
        return self._c.fetchall()

    def __iter__(self) -> Iterator[Any]:
        return iter(self._c)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._c, name)


class InventoryConnection:
    def __init__(self, raw: Any, *, backend: Literal["sqlite", "postgres"]):
        self._raw = raw
        self._backend = backend
        self.row_factory: Any = None

    def cursor(self, row_factory: Any = None) -> InventoryCursor:
        if self._backend == "postgres":
            from psycopg.rows import dict_row

            rf = row_factory if row_factory is not None else self.row_factory
            if rf is not None:
                return InventoryCursor(self._raw.cursor(row_factory=dict_row), backend="postgres")
            return InventoryCursor(self._raw.cursor(), backend="postgres")
        c = self._raw.cursor()
        rf = row_factory if row_factory is not None else self.row_factory
        if rf is not None:
            c.row_factory = rf
        return InventoryCursor(c, backend="sqlite")

    def execute(self, sql: str, params: tuple | list | None = None) -> InventoryCursor:
        cur = self.cursor()
        cur.execute(sql, params or ())
        return cur

    def commit(self) -> None:
        self._raw.commit()

    def rollback(self) -> None:
        self._raw.rollback()

    def close(self) -> None:
        self._raw.close()


def open_inventory_connection() -> InventoryConnection:
    if is_inventory_postgres():
        from backend.db.inventory_pg import pg_connect

        return InventoryConnection(pg_connect(), backend="postgres")
    assert_inventory_backend_configured()
    if inventory_sqlite_tests_allowed():
        from backend.db import inventory_db as invdb

        return InventoryConnection(invdb._sqlite_connect_raw(), backend="sqlite")
    raise RuntimeError(_INVENTORY_POSTGRES_REQUIRED_MSG)
