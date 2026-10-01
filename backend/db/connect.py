"""One way for scripts to find and open the inventory Postgres.

Audit 2026-10-01 (datascripts.md F8) found 25 private ``_dsn()`` helpers, ~8
``_connect()`` helpers and 29 hand-rolled ``re.search(r"^INVENTORY_DATABASE_URL=...")``
parsers of ``.env`` across ``backend/scripts``. They disagreed: some ignored
``DATABASE_URL``, some stripped quotes and some did not, one read ``.env`` from the
current working directory, and they differed on autocommit and connect timeout.
This module is the single source:

* :func:`inventory_dsn` -- one precedence, everywhere:

  1. ``INVENTORY_DATABASE_URL`` from the process environment,
  2. ``DATABASE_URL`` from the process environment,
  3. only when both are blank: ``<repo>/.env`` is loaded through
     :func:`backend.utils.project_env.load_project_dotenv` (shell values win, and it
     is a no-op while ``PROJECT_DOTENV_DISABLE`` is on, i.e. under pytest), then
     1 and 2 are read again.

  ``.env`` is never parsed with a regex. Values are stripped of whitespace and one
  pair of surrounding quotes.

* :func:`connect` -- ``psycopg.connect`` with the caller's autocommit / read-only
  mode. ``read_only=True`` is enforced by the server
  (``SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY``), not by convention.

* :func:`connection` -- context-manager form of :func:`connect`: commits on a clean
  exit (unless autocommit), rolls back on an exception, always closes.

Mac-mini caution (memory: mini .env points at its own local Postgres): the ``.env``
fallback is only reached when the shell exports neither URL. Export
``INVENTORY_DATABASE_URL`` explicitly on hosts whose ``.env`` is not the target.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator

DSN_ENV_KEYS: tuple[str, ...] = ("INVENTORY_DATABASE_URL", "DATABASE_URL")
DEFAULT_CONNECT_TIMEOUT = 15

_MISSING_MSG = (
    "INVENTORY_DATABASE_URL is not set (checked INVENTORY_DATABASE_URL, DATABASE_URL, "
    "then the repo .env)"
)


class InventoryDsnMissing(SystemExit, RuntimeError):
    """No inventory DSN anywhere.

    A ``SystemExit`` so an uncaught one ends a CLI with the message (what every
    script copy did), and a ``RuntimeError`` so library-style callers that catch
    ``Exception`` / ``RuntimeError`` (data_quality_invariants) still catch it.
    """


def _clean(raw: str | None) -> str:
    val = (raw or "").strip()
    if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
        val = val[1:-1].strip()
    return val


def _from_environ() -> str:
    for key in DSN_ENV_KEYS:
        val = _clean(os.environ.get(key))
        if val:
            return val
    return ""


def _load_project_dotenv() -> None:
    # Indirection so tests can point the fallback at a temp .env.
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()


def inventory_dsn(*, require: bool = True, export: bool = False) -> str | None:
    """The inventory Postgres URL, by the one precedence documented above.

    ``require=True`` raises :class:`InventoryDsnMissing` when nothing is found;
    ``require=False`` returns ``None`` instead.

    ``export=True`` also puts the result in ``os.environ["INVENTORY_DATABASE_URL"]``
    when that is blank, for scripts that call library code (package_registry,
    resolve_trim_ladder, backend.db.*) which reads the environment itself.
    """
    dsn = _from_environ()
    if not dsn:
        # A key exported blank (``INVENTORY_DATABASE_URL=``) counts as unset, as it
        # did in every script copy -- but python-dotenv never fills a key that is
        # present, so lift blank keys out for the load and put them back if .env
        # did not fill them (pytest pins them blank and switches .env off).
        blank = [k for k in DSN_ENV_KEYS if k in os.environ and not _clean(os.environ[k])]
        for key in blank:
            del os.environ[key]
        try:
            _load_project_dotenv()
        finally:
            for key in blank:
                os.environ.setdefault(key, "")
        dsn = _from_environ()
    if not dsn:
        if require:
            raise InventoryDsnMissing(_MISSING_MSG)
        return None
    if export and not _clean(os.environ.get("INVENTORY_DATABASE_URL")):
        os.environ["INVENTORY_DATABASE_URL"] = dsn
    return dsn


def connect(
    autocommit: bool = False,
    read_only: bool = False,
    *,
    timeout: int | None = DEFAULT_CONNECT_TIMEOUT,
    dsn: str | None = None,
    export: bool = False,
) -> Any:
    """Open a psycopg connection to the inventory database.

    ``timeout=None`` passes no ``connect_timeout`` (libpq default: wait forever).
    ``export`` is forwarded to :func:`inventory_dsn`.
    """
    import psycopg

    url = dsn or inventory_dsn(export=export)
    kwargs: dict[str, Any] = {}
    if autocommit:
        kwargs["autocommit"] = True
    if timeout is not None:
        kwargs["connect_timeout"] = timeout
    conn = psycopg.connect(url, **kwargs)
    if read_only:
        with conn.cursor() as cur:
            cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        if not autocommit:
            conn.commit()
    return conn


@contextmanager
def connection(
    autocommit: bool = False,
    read_only: bool = False,
    *,
    timeout: int | None = DEFAULT_CONNECT_TIMEOUT,
    dsn: str | None = None,
    export: bool = False,
) -> Iterator[Any]:
    """``with connection() as conn:`` -- commit on success, rollback on error, close."""
    conn = connect(autocommit, read_only, timeout=timeout, dsn=dsn, export=export)
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except BaseException:
        if not autocommit:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        conn.close()


__all__ = [
    "DEFAULT_CONNECT_TIMEOUT",
    "DSN_ENV_KEYS",
    "InventoryDsnMissing",
    "connect",
    "connection",
    "inventory_dsn",
]
