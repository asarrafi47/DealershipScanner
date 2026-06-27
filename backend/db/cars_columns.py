"""Runtime introspection of ``cars`` table columns (Postgres + SQLite)."""

from __future__ import annotations

from typing import Any

_CARS_COLUMNS: set[str] | None = None


def reset_cars_columns_cache() -> None:
    global _CARS_COLUMNS
    _CARS_COLUMNS = None


def cars_table_columns(conn: Any | None = None) -> set[str]:
    global _CARS_COLUMNS
    if _CARS_COLUMNS is not None and conn is None:
        return _CARS_COLUMNS

    from backend.db.inventory_pg import is_inventory_postgres

    if conn is None:
        from backend.db.inventory_db import db_conn

        with db_conn() as c:
            cols = cars_table_columns(c)
        return cols

    cur = conn.cursor()
    if is_inventory_postgres():
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'cars'
            """
        )
        cols = {str(r[0]) for r in cur.fetchall()}
    else:
        cur.execute("PRAGMA table_info(cars)")
        cols = {str(r[1]) for r in cur.fetchall()}
    if conn is not None:
        _CARS_COLUMNS = cols
    return cols


def cars_has_column(name: str) -> bool:
    return name in cars_table_columns()


def _epa_forced_induction_subquery_sql(*, alias: str = "cars") -> str:
    """Best-match ``epa_master.forced_induction`` for a listing row."""
    return f"""(
        SELECT e.forced_induction
        FROM epa_master e
        WHERE e.year = {alias}.year
          AND lower(trim(e.make)) = lower(trim({alias}.make))
          AND lower(trim(e.model)) = lower(trim({alias}.model))
        ORDER BY
          CASE
            WHEN lower(trim(coalesce(e.trim, ''))) = lower(trim(coalesce({alias}.trim, '')))
            THEN 0 ELSE 1
          END,
          e.id
        LIMIT 1
    )"""


def cars_forced_induction_sql_expr(*, alias: str = "cars") -> str:
    """
    SQL expression for forced induction on a listing row.

    Uses ``cars.forced_induction`` when populated; otherwise best-match ``epa_master`` row.
    """
    epa = _epa_forced_induction_subquery_sql(alias=alias)
    if cars_has_column("forced_induction"):
        return (
            f"COALESCE(NULLIF(TRIM({alias}.forced_induction), ''), {epa})"
        )
    return epa
