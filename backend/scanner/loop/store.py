"""The scan loop's file store: a virtual file tree in the Railway database (table ``loop_files``, V032).

Railway publishes dealer logs, run bundles, the recipe mirror, statuses and ``fix_cycles/`` into it. Cloud sessions
read it and submit requests, proposals and their own dealer notes through ``/api/loop`` (``backend/routes/loop_api.py``).
Nothing in git, nothing on a public host.

Two implementations share one interface: ``MemStore`` (tests, offline tools) and ``PgStore`` (Postgres).

Rules enforced here, not in the callers:
  * ``path_ok``: only the roots in ``ROOTS``, safe components, no ``..``, at most 200 characters.
  * ``MAX_FILE_BYTES`` per file by root, ``MAX_TOTAL_BYTES`` for the whole table (a put that would pass it is refused).
  * ``writer`` is ``railway`` or ``cloud``; ``cloud`` writes go through ``submit_check`` first.
"""

from __future__ import annotations

import hashlib
import io
import re
import tarfile
from dataclasses import dataclass

from backend.scanner.loop import requests as req

ROOTS = ("workspace/dealer_logs/", "runs/", "mirror/", "requests/", "proposals/", "fix_cycles/", "config/",
         "findings/", "evidence/")
MAX_PATH = 200
MAX_FILE_BYTES = {"runs/": 2 * 1024 * 1024, "mirror/": 1024 * 1024, "evidence/": 1024 * 1024,
                  "workspace/dealer_logs/": 512 * 1024, "findings/": 64 * 1024, "proposals/": req.MAX_PROPOSAL_BYTES,
                  "requests/": 16 * 1024, "fix_cycles/": 64 * 1024, "config/": 4 * 1024}
MAX_TOTAL_BYTES = 256 * 1024 * 1024
MAX_EXPORT_BYTES = 48 * 1024 * 1024
RETAIN_FLEET_RUNS = 30
SESSION_LOG_FILES = ("summary.md", "location.md")
_COMPONENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")


class StoreError(Exception):
    """A refused or failed store operation; the message never carries file content."""


@dataclass(frozen=True)
class Entry:
    path: str
    sha256: str
    size: int


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def path_ok(path) -> bool:
    if not isinstance(path, str) or not path or len(path) > MAX_PATH or path.endswith("/"):
        return False
    if not any(path.startswith(root) for root in ROOTS):
        return False
    return all(_COMPONENT_RE.fullmatch(part) is not None for part in path.split("/"))


def file_limit(path: str) -> int:
    for root, limit in MAX_FILE_BYTES.items():
        if path.startswith(root):
            return limit
    return 0


def prefix_ok(prefix) -> bool:
    return (isinstance(prefix, str) and len(prefix) <= MAX_PATH and any(prefix.startswith(r) for r in ROOTS)
            and ".." not in prefix and "//" not in prefix and "\\" not in prefix)


def check_put(path, data) -> None:
    if not path_ok(path):
        raise StoreError("path not allowed")
    if not isinstance(data, (bytes, bytearray)):
        raise StoreError("body must be bytes")
    if len(data) > file_limit(path):
        raise StoreError("file too large for its folder")


def submit_check(path: str, data: bytes, exists: bool) -> None:
    """What a cloud session may write: new request and proposal files (strictly parsed, never overwritten), its own
    summary and location notes, and findings. Everything else is Railway's. Raises ``StoreError``."""
    check_put(path, data)
    parts = path.split("/")
    try:
        if path.startswith("requests/"):
            name = parts[1][:-5] if parts[1].endswith(".json") else ""
            if len(parts) != 2 or not req.request_name_ok(name):
                raise StoreError("request name not allowed")
            if exists:
                raise StoreError("exists")
            req.parse_request(data)
        elif path.startswith("proposals/"):
            if len(parts) != 3 or not req.proposal_path_ok(path, parts[1]):
                raise StoreError("proposal path not allowed")
            if exists:
                raise StoreError("exists")
            req.parse_proposal(data, parts[1])
        elif path.startswith("workspace/dealer_logs/"):
            if len(parts) != 4 or parts[3] not in SESSION_LOG_FILES:
                raise StoreError("only summary.md and location.md are writable")
        elif path.startswith("findings/"):
            if len(parts) != 2 or not parts[1].endswith(".md"):
                raise StoreError("findings are .md files")
        else:
            raise StoreError("folder is not writable by a session")
    except req.ParseError as exc:
        raise StoreError("schema: " + str(exc)) from None


class MemStore:
    """In-memory store with the same rules as ``PgStore``."""

    def __init__(self, max_total: int = MAX_TOTAL_BYTES):
        self.files: dict = {}
        self.max_total = max_total

    def get(self, path):
        row = self.files.get(path)
        return row[0] if row else None

    def exists(self, path) -> bool:
        return path in self.files

    def list(self, prefix=""):
        return [Entry(p, sha(b), len(b)) for p, (b, _w) in sorted(self.files.items()) if p.startswith(prefix)]

    def total_bytes(self) -> int:
        return sum(len(b) for b, _w in self.files.values())

    def put(self, path, data, writer="railway") -> bool:
        """Store ``data``; ``False`` when identical content is already there."""
        check_put(path, data)
        data = bytes(data)
        old = self.files.get(path)
        if old and old[0] == data:
            return False
        if self.total_bytes() - (len(old[0]) if old else 0) + len(data) > self.max_total:
            raise StoreError("store is full")
        self.files[path] = (data, writer)
        return True

    def delete_prefix(self, prefix) -> int:
        if not prefix_ok(prefix):
            raise StoreError("prefix not allowed")
        gone = [p for p in self.files if p.startswith(prefix)]
        for p in gone:
            del self.files[p]
        return len(gone)


class PgStore:
    """Postgres store over ``loop_files``. ``connect`` returns a new psycopg connection (autocommit not required)."""

    def __init__(self, connect=None, max_total: int = MAX_TOTAL_BYTES):
        if connect is None:
            from backend.db.connect import connect as _connect

            connect = _connect
        self._connect = connect
        self.max_total = max_total

    def _run(self, sql, args=(), fetch="none"):
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, args)
                out = cur.fetchall() if fetch == "all" else (cur.fetchone() if fetch == "one" else None)
                rowcount = cur.rowcount
            conn.commit()
            return out, rowcount
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get(self, path):
        row, _ = self._run("SELECT body FROM public.loop_files WHERE path = %s", (path,), "one")
        return bytes(row[0]) if row else None

    def exists(self, path) -> bool:
        row, _ = self._run("SELECT 1 FROM public.loop_files WHERE path = %s", (path,), "one")
        return row is not None

    def list(self, prefix=""):
        like = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        rows, _ = self._run("SELECT path, sha256, size FROM public.loop_files WHERE path LIKE %s ORDER BY path",
                            (like,), "all")
        return [Entry(r[0], r[1], r[2]) for r in rows]

    def total_bytes(self) -> int:
        row, _ = self._run("SELECT COALESCE(SUM(size), 0) FROM public.loop_files", (), "one")
        return int(row[0])

    def put(self, path, data, writer="railway") -> bool:
        check_put(path, data)
        data = bytes(data)
        digest = sha(data)
        row, _ = self._run("SELECT sha256, size FROM public.loop_files WHERE path = %s", (path,), "one")
        if row and row[0] == digest:
            return False
        if self.total_bytes() - (row[1] if row else 0) + len(data) > self.max_total:
            raise StoreError("store is full")
        self._run(
            "INSERT INTO public.loop_files (path, body, sha256, size, writer) VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (path) DO UPDATE SET body = EXCLUDED.body, sha256 = EXCLUDED.sha256, size = EXCLUDED.size, "
            "writer = EXCLUDED.writer, updated_at = now()", (path, data, digest, len(data), writer))
        return True

    def delete_prefix(self, prefix) -> int:
        if not prefix_ok(prefix):
            raise StoreError("prefix not allowed")
        like = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        _, count = self._run("DELETE FROM public.loop_files WHERE path LIKE %s", (like,))
        return count


def prune_runs(store, keep: int = RETAIN_FLEET_RUNS) -> int:
    """Delete the oldest ``runs/fleet_*`` folders beyond ``keep``; returns how many were removed."""
    names = sorted({e.path.split("/")[1] for e in store.list("runs/fleet_") if e.path.count("/") >= 2})
    removed = 0
    for name in names[:-keep] if len(names) > keep else []:
        removed += store.delete_prefix("runs/" + name + "/")
    return removed


def export_tar(store, prefixes) -> bytes:
    """A gzip tar of every file under the given prefixes, at most ``MAX_EXPORT_BYTES`` of content."""
    buffer = io.BytesIO()
    total = 0
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for prefix in prefixes:
            for entry in store.list(prefix):
                data = store.get(entry.path)
                if data is None:
                    continue
                total += len(data)
                if total > MAX_EXPORT_BYTES:
                    raise StoreError("export too large; ask for a narrower prefix")
                info = tarfile.TarInfo(entry.path)
                info.size = len(data)
                info.mtime = 0
                tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue()
