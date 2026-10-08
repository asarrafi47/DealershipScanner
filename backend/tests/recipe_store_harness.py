"""Two hosts, one recipe store: a per-test harness for recipe sync tests.

``TwoHostRecipeStore`` stands in for the scanners that share one ``dealer_recipes``
store but keep their own recipe file cache (the MBP, the mini, Railway):

- the store is a SQLite file under the test's ``tmp_path``, wired through
  ``recipe_store._conn``, with ``recipe_store._table_ready`` reset so the table is
  created in that file;
- each host gets its own ``recipes.RECIPES_DIR`` under ``tmp_path``; ``use(host)`` or
  ``with store.on(host):`` switches the process to that host's cache.

It replaces the shared hermetic session DB that the store otherwise reaches through
``backend.db.inventory_db.get_conn`` (conftest's session copy of the inventory DB):
``dealer_recipes`` rows written there leak from test to test within a session, which
is why test_recipes.py's alias tests switch the store off (``no_recipes_db``). Every
connection the harness opens asserts that its path is under ``tmp_path`` and is not
that session DB.

``freeze_clock(now)`` replaces the ``time`` module that ``backend.scanner.recipes``
sees with one whose ``time()`` returns ``now`` and counts its calls, so tests can
order saves without sleeping and count the clock readings a save takes.

Not a test module (no ``test_`` prefix); test modules build it in a fixture::

    @pytest.fixture
    def store(tmp_path, monkeypatch):
        return TwoHostRecipeStore(tmp_path, monkeypatch)
"""
from __future__ import annotations

import json
import sqlite3
import time as _time
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any

import pytest

import backend.scanner.recipes as rec
from backend.scanner import recipe_store

DEFAULT_HOSTS = ("mbp", "mini")


def _is_under(path: Path, root: Path) -> bool:
    try:
        Path(path).resolve().relative_to(Path(root).resolve())
    except ValueError:
        return False
    return True


def session_inventory_db_path() -> Path | None:
    """The DB ``recipe_store`` reaches without the harness (``inventory_db.DB_PATH``,
    which conftest points at the session copy), or None when it cannot be read."""
    try:
        from backend.db import inventory_db

        raw = getattr(inventory_db, "DB_PATH", None)
    except Exception:  # noqa: BLE001 — the check is advisory when the module is absent
        return None
    return Path(raw) if raw else None


class FrozenClock:
    """Stands in for the ``time`` module inside ``backend.scanner.recipes``.

    ``time()`` returns ``now`` (set it between steps) and counts its calls; every
    other attribute is the real ``time`` module's.
    """

    def __init__(self, now: float) -> None:
        self.now = float(now)
        self.calls = 0

    def time(self) -> float:
        self.calls += 1
        return self.now

    def __getattr__(self, name: str) -> Any:
        return getattr(_time, name)


class TwoHostRecipeStore:
    """One ``dealer_recipes`` store under ``tmp_path`` shared by several recipe caches."""

    def __init__(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        hosts: tuple[str, ...] = DEFAULT_HOSTS,
    ) -> None:
        if len(set(hosts)) < 2:
            raise ValueError("a two-host store needs at least two distinct host names")
        self.tmp_path = Path(tmp_path)
        self.db_path = self.tmp_path / "recipe_store" / "dealer_recipes.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.assert_private_db()
        self.hosts = tuple(hosts)
        self.dirs: dict[str, Path] = {h: self.tmp_path / "hosts" / h / "recipes" for h in self.hosts}
        for d in self.dirs.values():
            assert _is_under(d, self.tmp_path), f"host cache {d} is not under {self.tmp_path}"
        self.connections = 0
        self.current = self.hosts[0]
        self._mp = monkeypatch
        monkeypatch.setenv("RECIPES_DB_DISABLED", "")
        monkeypatch.setattr(recipe_store, "_conn", self._connect)
        monkeypatch.setattr(recipe_store, "_table_ready", False)
        self.use(self.hosts[0])

    # ── wiring ────────────────────────────────────────────────────────────────

    def assert_private_db(self) -> None:
        """The store file is under ``tmp_path`` and is not the session inventory DB."""
        assert _is_under(self.db_path, self.tmp_path), (
            f"recipe store {self.db_path} is not under the test's tmp_path {self.tmp_path}"
        )
        session_db = session_inventory_db_path()
        if session_db is not None:
            assert self.db_path.resolve() != session_db.resolve(), (
                f"recipe store {self.db_path} is the shared session DB"
            )

    def _connect(self) -> sqlite3.Connection:
        self.assert_private_db()
        self.connections += 1
        return sqlite3.connect(self.db_path)

    def use(self, host: str) -> Path:
        """Make ``host``'s cache the process's ``RECIPES_DIR`` (until the next switch)."""
        d = self.dirs[host]
        self._mp.setattr(rec, "RECIPES_DIR", d)
        self.current = host
        return d

    @contextmanager
    def on(self, host: str) -> Iterator[Path]:
        """Run the block as ``host``; the previous host is restored afterwards."""
        previous = self.current
        self.use(host)
        try:
            yield self.dirs[host]
        finally:
            self.use(previous)

    def freeze_clock(self, now: float) -> FrozenClock:
        clock = FrozenClock(now)
        self._mp.setattr(rec, "time", clock)
        return clock

    # ── cache files ───────────────────────────────────────────────────────────

    def file_path(self, host: str, dealer_id: str) -> Path:
        return self.dirs[host] / f"{rec._recipe_slug(dealer_id)}.json"

    def file_rows(self, host: str, dealer_id: str) -> list[dict[str, Any]] | None:
        """The rows in ``host``'s cache file for the dealer (None when there is no file)."""
        path = self.file_path(host, dealer_id)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def write_file(self, host: str, dealer_id: str, rows: list[dict[str, Any]]) -> Path:
        """Seed ``host``'s cache file verbatim, as code that predates the stamp left it."""
        path = self.file_path(host, dealer_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(rows, indent=1), encoding="utf-8")
        return path

    # ── the shared store ──────────────────────────────────────────────────────

    def write_db(self, dealer_id: str, rows: list[dict[str, Any]]) -> None:
        """Seed the store row verbatim through ``db_save_recipes`` (no stamp)."""
        assert recipe_store.db_save_recipes(rec._recipe_slug(dealer_id), rows), "seed write-through failed"

    def db_row(self, dealer_id: str) -> dict[str, Any] | None:
        """The ``dealer_recipes`` row (recipes parsed into ``rows``), or None."""
        self.assert_private_db()
        if not self.db_path.exists():
            return None
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            try:
                row = conn.execute(
                    "SELECT * FROM dealer_recipes WHERE dealer_id = ?", (rec._recipe_slug(dealer_id),)
                ).fetchone()
            except sqlite3.OperationalError:  # table not created yet
                return None
        if row is None:
            return None
        out = dict(row)
        out["rows"] = json.loads(out.get("recipes_json") or "[]")
        return out
