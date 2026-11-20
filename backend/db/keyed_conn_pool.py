"""
Reuse of already-keyed SQLCipher connections (2026-11-20 perf).

Why: every ``get_users_conn()`` opened a new SQLCipher connection, and ``PRAGMA key`` with a
passphrase runs PBKDF2 key derivation on the first read (~130 ms on a dev VM, more on
Railway's shared vCPU). A signed-in page opened 2-5 of them, so 250-650 ms of every
logged-in request was key derivation. Encryption is unchanged: the same file, the same
passphrase key format, the same PRAGMAs; only an already-keyed handle is handed out again.

Contract of a pooled connection (:class:`PooledConnection`):

* One borrower at a time. ``close()`` hands the handle back: it is rolled back (no open
  transaction survives), ``row_factory`` / ``text_factory`` / ``isolation_level`` are reset
  to the values it had when it was opened, and any use of the wrapper or of a cursor taken
  from it after ``close()`` raises ``ProgrammingError`` exactly like a closed connection.
* Any exception raised through the wrapper or its cursors marks the handle broken; a broken
  handle is closed on ``close()``, never pooled.
* Pool entries are keyed by (db path, hash of the key). A different path (tests that
  monkeypatch ``USERS_DB_PATH``) or a different key never gets another entry's handle.
* The file identity (``st_dev``, ``st_ino``) is recorded at open and checked at every
  checkout and return: a file replaced by rename (restore, migration copy) drops every
  handle on the old inode. The pooled handle keeps the old inode allocated, so the number
  cannot be reused by a new file while a handle on it is still pooled.
* Handles idle longer than ``max_idle_s`` are closed instead of handed out; at most
  ``size`` idle handles are kept (bounded memory: about one SQLite page cache each).
* Fork safety (gunicorn ``--preload``): the pool remembers the pid that filled it. In a
  child the inherited handles are parked in ``_orphans`` and never used or closed (SQLite
  forbids carrying a connection across ``fork()``; closing it in the child is also unsafe),
  and a wrapper borrowed before the fork is never returned to the child's pool.
"""

from __future__ import annotations

import hashlib
import os
import sys
import threading
import time
from typing import Any, Callable

_RESET_ATTRS = ("row_factory", "text_factory", "isolation_level")


def _file_ident(path: str) -> tuple[int, int] | None:
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_dev, st.st_ino)


def _close_quietly(conn: Any) -> None:
    try:
        conn.close()
    except Exception:
        pass


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


class _Entry:
    __slots__ = ("conn", "ident", "idle_since", "defaults", "cursor_type", "closed_exc")

    def __init__(self, conn: Any, ident: tuple[int, int] | None, defaults: dict) -> None:
        self.conn = conn
        self.ident = ident
        self.idle_since = time.monotonic()
        self.defaults = defaults
        # The driver's own ProgrammingError (sqlcipher3.dbapi2 / sqlite3) for use after close.
        drv = sys.modules.get(type(conn).__module__)
        self.closed_exc: type[BaseException] = getattr(drv, "ProgrammingError", RuntimeError)
        try:
            cur = conn.cursor()
            self.cursor_type: type | None = type(cur)
            cur.close()
        except Exception:
            self.cursor_type = None


class KeyedConnPool:
    """A small LIFO pool of keyed connections for one database family."""

    def __init__(self, name: str, *, size_env: str, idle_env: str, default_size: int = 8,
                 default_max_idle_s: int = 300) -> None:
        self.name = name
        self._size_env = size_env
        self._idle_env = idle_env
        self._default_size = default_size
        self._default_max_idle_s = default_max_idle_s
        self._lock = threading.Lock()
        self._idle: dict[tuple[str, str], list[_Entry]] = {}
        self._pid = os.getpid()
        self._orphans: list[Any] = []
        self.opens = 0  # handles opened through this pool (for tests / diagnostics)
        _POOLS.append(self)

    # -- configuration ---------------------------------------------------------------
    def size(self) -> int:
        return max(0, _env_int(self._size_env, self._default_size))

    def max_idle_s(self) -> float:
        return float(max(0, _env_int(self._idle_env, self._default_max_idle_s)))

    # -- fork handling -----------------------------------------------------------------
    def _after_fork_in_child(self) -> None:
        # The parent may have held the lock at fork time; a fresh one cannot deadlock.
        self._lock = threading.Lock()
        self._adopt_pid_locked()

    def _adopt_pid_locked(self) -> None:
        pid = os.getpid()
        if pid == self._pid:
            return
        for entries in self._idle.values():
            self._orphans.extend(e.conn for e in entries)
        self._idle.clear()
        self._pid = pid

    # -- pool operations ---------------------------------------------------------------
    @staticmethod
    def pool_key(path: str, secret: str) -> tuple[str, str]:
        return (path, hashlib.sha256(secret.encode("utf-8")).hexdigest()[:16])

    def checkout(self, path: str, secret: str, opener: Callable[[], Any]) -> Any:
        """Return a :class:`PooledConnection` for *path*; *opener* makes a new keyed handle."""
        size = self.size()
        if size <= 0:
            return opener()
        key = self.pool_key(path, secret)
        ident = _file_ident(path)
        now = time.monotonic()
        max_idle = self.max_idle_s()
        stale: list[Any] = []
        entry: _Entry | None = None
        with self._lock:
            self._adopt_pid_locked()
            # One database per family at a time in practice: entries for another path or
            # key (a test that switched USERS_DB_PATH, a rotated key) are dropped here.
            for other in [k for k in self._idle if k != key]:
                stale.extend(e.conn for e in self._idle.pop(other))
            entries = self._idle.get(key) or []
            keep: list[_Entry] = []
            for e in entries:
                if ident is None or e.ident != ident or now - e.idle_since > max_idle:
                    stale.append(e.conn)
                else:
                    keep.append(e)
            if keep:
                entry = keep.pop()  # LIFO: the warmest page cache
            if keep:
                self._idle[key] = keep
            else:
                self._idle.pop(key, None)
            pid = self._pid
        for c in stale:
            _close_quietly(c)
        if entry is None:
            conn = opener()
            self.opens += 1
            defaults = {}
            for a in _RESET_ATTRS:
                try:
                    defaults[a] = getattr(conn, a)
                except Exception:
                    pass
            entry = _Entry(conn, _file_ident(path), defaults)
        return PooledConnection(self, entry, key, path, pid)

    def _checkin(self, entry: _Entry, key: tuple[str, str], path: str, pid: int,
                 broken: bool) -> None:
        conn = entry.conn
        if os.getpid() != pid:
            # Borrowed before a fork: the handle belongs to the parent process.
            with self._lock:
                self._orphans.append(conn)
            return
        if broken or entry.ident is None or self.size() <= 0:
            _close_quietly(conn)
            return
        try:
            conn.rollback()
            for a, v in entry.defaults.items():
                if getattr(conn, a) != v:
                    setattr(conn, a, v)
            if getattr(conn, "in_transaction", False):
                raise RuntimeError("transaction still open after rollback")
        except Exception:
            _close_quietly(conn)
            return
        if _file_ident(path) != entry.ident:
            _close_quietly(conn)
            return
        entry.idle_since = time.monotonic()
        with self._lock:
            self._adopt_pid_locked()
            if self._pid != pid:
                self._orphans.append(conn)
                return
            entries = self._idle.setdefault(key, [])
            if len(entries) >= self.size():
                full = True
            else:
                entries.append(entry)
                full = False
        if full:
            _close_quietly(conn)

    def reset(self) -> None:
        """Close every idle handle (test hook; also usable after an ops restore)."""
        with self._lock:
            self._adopt_pid_locked()
            stale = [e.conn for entries in self._idle.values() for e in entries]
            self._idle.clear()
        for c in stale:
            _close_quietly(c)

    def idle_count(self) -> int:
        with self._lock:
            return sum(len(v) for v in self._idle.values())


_POOLS: list[KeyedConnPool] = []


def _after_fork_in_child() -> None:
    for p in _POOLS:
        p._after_fork_in_child()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_in_child)


def _noop(*_a: Any, **_k: Any) -> None:
    return None


class _Guarded:
    """Shared plumbing: refuse use after close, mark the handle broken on any error."""

    def _wrap_call(self, owner: "PooledConnection", fn: Callable, raw_result_of: Any) -> Callable:
        def call(*args: Any, **kwargs: Any) -> Any:
            owner._live()
            try:
                out = fn(*args, **kwargs)
            except BaseException:
                owner._broken = True
                raise
            if out is raw_result_of:
                return self
            if _is_raw_cursor(owner, out):
                return PooledCursor(owner, out)
            return out

        return call


def _is_raw_cursor(owner: "PooledConnection", obj: Any) -> bool:
    ct = owner._cursor_type
    return ct is not None and isinstance(obj, ct)


class PooledConnection(_Guarded):
    """A borrowed keyed connection; ``close()`` gives it back to the pool."""

    _OWN = frozenset({"_pool", "_entry", "_key", "_path", "_pid", "_closed_exc", "_conn",
                      "_broken", "_cursor_type"})

    def __init__(self, pool: KeyedConnPool, entry: _Entry, key: tuple[str, str], path: str,
                 pid: int) -> None:
        object.__setattr__(self, "_pool", pool)
        object.__setattr__(self, "_entry", entry)
        object.__setattr__(self, "_key", key)
        object.__setattr__(self, "_path", path)
        object.__setattr__(self, "_pid", pid)
        object.__setattr__(self, "_closed_exc", entry.closed_exc)
        object.__setattr__(self, "_conn", entry.conn)
        object.__setattr__(self, "_broken", False)
        object.__setattr__(self, "_cursor_type", entry.cursor_type)

    def _live(self) -> Any:
        conn = self._conn
        if conn is None:
            raise self._closed_exc("Cannot operate on a closed database.")
        return conn

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._live(), name)
        if callable(attr) and not isinstance(attr, type):
            return self._wrap_call(self, attr, self._conn)
        return attr

    def __setattr__(self, name: str, value: Any) -> None:
        if name in self._OWN:
            object.__setattr__(self, name, value)
        else:
            setattr(self._live(), name, value)

    def __enter__(self) -> "PooledConnection":
        self._live().__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        if exc_type is not None:
            self._broken = True
        try:
            return self._live().__exit__(exc_type, exc, tb)
        except BaseException:
            self._broken = True
            raise

    def close(self) -> None:
        conn = self._conn
        if conn is None:
            return
        self._conn = None
        self._pool._checkin(self._entry, self._key, self._path, self._pid, self._broken)

    @property
    def raw_connection(self) -> Any:
        """The underlying keyed handle (tests / diagnostics only)."""
        return self._live()


class PooledCursor(_Guarded):
    """A cursor that stops working once its connection was handed back."""

    def __init__(self, owner: PooledConnection, cur: Any) -> None:
        object.__setattr__(self, "_owner", owner)
        object.__setattr__(self, "_cur", cur)

    def __getattr__(self, name: str) -> Any:
        owner = self._owner
        if name == "close" and owner._conn is None:
            return _noop  # closing a cursor after its connection is harmless
        owner._live()
        attr = getattr(self._cur, name)
        if callable(attr) and not isinstance(attr, type):
            return self._wrap_call(owner, attr, self._cur)
        return attr

    def __setattr__(self, name: str, value: Any) -> None:
        self._owner._live()
        setattr(self._cur, name, value)

    def __iter__(self) -> "PooledCursor":
        return self

    def __next__(self) -> Any:
        self._owner._live()
        try:
            return next(self._cur)
        except StopIteration:
            raise
        except BaseException:
            self._owner._broken = True
            raise
