#!/usr/bin/env python3
"""Optional scrub of dead TOTP seeds and MFA phones in users.db and dev_users.db (P16.3).

TOTP and SMS MFA were removed from the app (P16.2); any seed or phone still stored is dead
data. Dry run by default: opens both DBs through the app's own openers
(``get_users_conn`` / ``get_dev_users_conn``, so SQLCipher keys apply) and prints the
counts of non-empty ``totp_secret``, non-empty ``mfa_phone`` and ``totp_enabled = 1``.
If every count is 0 there is nothing to do.

``--apply`` (run it inside the web container via ``railway ssh``, the only access to the
volume, in a stated maintenance window):
  1. copies each DB file to ``<--backup-dir>/<UTC ts>/`` and checks the copy opens with the
     same key (a backup that does not open is not a backup; the run stops);
  2. one transaction per DB sets the dead columns to NULL / 0, with
     ``PRAGMA secure_delete = ON`` (not VACUUM against a live DB);
  3. re-counts; every count must be 0.
Then verify admin and user logins by hand. The backup still holds the seeds: delete it after
verification with ``--purge-backup <dir>`` on the dated step the owner writes down.

Columns already dropped by V031 / P16.2 are skipped (reported "column absent").
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

# (label, table, column, "non-empty text" or "flag set")
CHECKS = (
    ("users", "totp_secret", "text"),
    ("users", "mfa_phone", "text"),
    ("users", "totp_enabled", "flag"),
    ("admin_users", "totp_secret", "text"),
    ("admin_users", "totp_enabled", "flag"),
)


def _columns(conn: Any, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _where(col: str, kind: str) -> str:
    if kind == "flag":
        return f"{col} = 1"
    return f"{col} IS NOT NULL AND {col} <> ''"


def count_dead(conn: Any, tables: tuple[str, ...]) -> dict[str, int | str]:
    """Counts per ``table.column``; a missing table or column reports 'absent'."""
    out: dict[str, int | str] = {}
    for table, col, kind in CHECKS:
        if table not in tables:
            continue
        key = f"{table}.{col}"
        cols = _columns(conn, table)
        if col not in cols:
            out[key] = "absent"
            continue
        out[key] = int(
            conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {_where(col, kind)}").fetchone()[0]
        )
    return out


def total(counts: dict[str, int | str]) -> int:
    return sum(v for v in counts.values() if isinstance(v, int))


def scrub(conn: Any, tables: tuple[str, ...]) -> None:
    """One transaction: null the seeds and phones, clear the flag. Caller commits."""
    conn.execute("PRAGMA secure_delete = ON")
    conn.execute("BEGIN")
    try:
        for table, col, kind in CHECKS:
            if table not in tables or col not in _columns(conn, table):
                continue
            new = "0" if kind == "flag" else "NULL"
            conn.execute(f"UPDATE {table} SET {col} = {new} WHERE {_where(col, kind)}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _db_file(conn: Any) -> str:
    # PRAGMA database_list works on sqlite3 and sqlcipher3 alike.
    for _, name, path in conn.execute("PRAGMA database_list").fetchall():
        if name == "main":
            return path
    raise RuntimeError("cannot resolve the database file")


def backup_db(conn: Any, dest_dir: Path, opener_for_path: Callable[[str], Any]) -> Path:
    """Checkpoint, copy the file, and prove the copy opens with the key."""
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception:
        pass
    src = Path(_db_file(conn))
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    shutil.copy2(src, dest)
    os.chmod(dest, 0o600)
    check = opener_for_path(str(dest))
    try:
        check.execute("SELECT count(*) FROM sqlite_master").fetchone()
    finally:
        check.close()
    return dest


def _opener_for_path(env_key_name: str) -> Callable[[str], Any]:
    """Open an arbitrary file with the DB's SQLCipher key when one is configured."""

    def _open(path: str) -> Any:
        key = (os.environ.get(env_key_name) or "").strip()
        from backend.utils.credential_db_encryption import sqlcipher_available

        if key and sqlcipher_available():
            import sqlcipher3 as sc  # type: ignore[import-not-found]

            conn = sc.connect(path)
            conn.execute("PRAGMA key = '%s'" % key.replace("'", "''"))
            return conn
        import sqlite3

        return sqlite3.connect(path)

    return _open


def targets() -> list[tuple[str, Callable[[], Any], tuple[str, ...], Callable[[str], Any]]]:
    from backend.db.dev_users_sqlite import get_dev_users_conn
    from backend.db.users_sqlite import get_users_conn

    return [
        ("users.db", get_users_conn, ("users",), _opener_for_path("USERS_DB_ENCRYPTION_KEY")),
        (
            "dev_users.db",
            get_dev_users_conn,
            ("admin_users",),
            _opener_for_path("DEV_USERS_DB_ENCRYPTION_KEY"),
        ),
    ]


def run(apply: bool, backup_dir: Path, tgts=None) -> int:
    tgts = tgts if tgts is not None else targets()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    pending = False
    for name, opener, tables, path_opener in tgts:
        conn = opener()
        try:
            before = count_dead(conn, tables)
            print(f"{name}: {before}")
            if total(before) == 0:
                print(f"{name}: nothing to scrub")
                continue
            pending = True
            if not apply:
                continue
            dest = backup_db(conn, backup_dir / ts, path_opener)
            print(f"{name}: backup {dest} opens with the key")
            scrub(conn, tables)
            after = count_dead(conn, tables)
            print(f"{name}: after {after}")
            if total(after) != 0:
                print(f"{name}: counts not 0 after apply", file=sys.stderr)
                return 1
        finally:
            conn.close()
    if pending and not apply:
        print("dry run: nothing written. Re-run with --apply in the maintenance window.")
    elif pending and apply:
        print(
            f"applied. Verify admin and user logins, then delete the backup: "
            f"--purge-backup {backup_dir / ts}"
        )
    return 0


def purge(path: Path) -> int:
    if not path.is_dir():
        print(f"not a directory: {path}", file=sys.stderr)
        return 2
    shutil.rmtree(path)
    print(f"deleted {path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="back up, then scrub (default: dry run)")
    ap.add_argument("--backup-dir", default="/data/backups", help="parent of the <ts>/ backup dir")
    ap.add_argument("--purge-backup", metavar="DIR", help="delete a backup dir after verification")
    args = ap.parse_args(argv)
    if args.purge_backup:
        return purge(Path(args.purge_backup))
    return run(args.apply, Path(args.backup_dir))


if __name__ == "__main__":
    sys.exit(main())
