"""Admin users for /dev dashboard — stored in dev_users.db (not public users.db)."""

from __future__ import annotations

import hmac
import logging
import os
import sqlite3
from pathlib import Path

from backend.db.dev_users_sqlite import DB_PATH as DEV_USERS_DB_PATH
from backend.db.dev_users_sqlite import get_dev_users_conn
from backend.db.password_hash import hash_password, password_needs_rehash, verify_or_legacy
from backend.db import users_sqlite as legacy_users_sqlite
from backend.utils.bootstrap_policy import bootstrap_force_admin_password
from backend.utils.runtime_env import is_production_env

logger = logging.getLogger(__name__)


class AdminUserExists(Exception):
    """The username or email is already an operator account (both backends)."""


def dev_users_backend() -> str:
    """``sqlite`` (SQLCipher dev_users.db, the default) or ``pg`` (public.admin_users, V035).

    N37.9: the flag stays off until the dev_users cutover (plan section 5); anything
    other than ``pg`` keeps the SQLite path untouched.
    """
    v = (os.environ.get("DEV_USERS_DB_BACKEND") or "").strip().lower()
    return "pg" if v == "pg" else "sqlite"


def _use_pg() -> bool:
    return dev_users_backend() == "pg"


def _pg_conn():
    from backend.db.connect import connect

    return connect()


def _env_admin() -> tuple[str, str, str]:
    user = (os.environ.get("ADMIN_USERNAME") or "admin").strip() or "admin"
    email = (os.environ.get("ADMIN_EMAIL") or "admin@localhost").strip() or "admin@localhost"
    return user, email, (os.environ.get("ADMIN_PASSWORD") or "").strip()


def _pg_seed_env_admin(cur, *, force_password: bool) -> None:
    """Insert the env admin (ADMIN_*) when absent; same rules as the SQLite bootstrap."""
    user, email, plain = _env_admin()
    if not plain:
        if is_production_env():
            raise RuntimeError(
                "FLASK_ENV=production requires ADMIN_PASSWORD to be set for /dev admin bootstrap."
            )
        plain = "changeme"
    cur.execute("SELECT 1 FROM admin_users WHERE lower(username) = lower(%s)", (user,))
    existing = cur.fetchone()
    cur.execute(
        "INSERT INTO admin_users (username, email, password) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
        (user, email, hash_password(plain)),
    )
    if existing and os.environ.get("ADMIN_PASSWORD") and force_password:
        cur.execute(
            "UPDATE admin_users SET password = %s WHERE lower(username) = lower(%s)",
            (hash_password(plain), user),
        )


def _pg_init_admin_db() -> None:
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            _pg_seed_env_admin(cur, force_password=bootstrap_force_admin_password())
        conn.commit()
    finally:
        conn.close()


def _pg_break_glass(cur, login: str, password: str) -> bool:
    """Env-seeded break-glass admin: ADMIN_USERNAME/ADMIN_PASSWORD log in even when the
    table has no row for that user (empty table, lost row). The row is created on the
    spot so the session has a real id for TOTP. Only the env admin, only the exact
    ADMIN_PASSWORD (constant-time compare); a row that already exists is never touched
    here, so a changed password in the table still wins."""
    user, email, plain = _env_admin()
    if not plain or login.lower() not in (user.lower(), email.lower()):
        return False
    if not hmac.compare_digest(password.encode(), plain.encode()):
        return False
    cur.execute("SELECT 1 FROM admin_users WHERE lower(username) = lower(%s)", (user,))
    if cur.fetchone():
        return False
    cur.execute(
        "INSERT INTO admin_users (username, email, password) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
        (user, email, hash_password(plain)),
    )
    return True


def _pg_authenticate(li: str, password: str) -> tuple[int, str] | None:
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            def lookup():
                cur.execute(
                    "SELECT id, username, password FROM admin_users "
                    "WHERE lower(username) = lower(%s) OR lower(email) = lower(%s) ORDER BY id LIMIT 1",
                    (li, li),
                )
                return cur.fetchone()

            row = lookup()
            if not row:
                if _pg_break_glass(cur, li, password):
                    conn.commit()
                    logger.warning("break-glass env admin seeded on login for %r", li)
                    row = lookup()
                if not row:
                    conn.rollback()
                    return None
            uid, uname, stored = row
            ok = verify_or_legacy(password, stored)
            if ok and password_needs_rehash(stored):
                cur.execute(
                    "UPDATE admin_users SET password = %s WHERE id = %s AND password = %s",
                    (hash_password(password), uid, stored),
                )
            conn.commit()
    finally:
        conn.close()
    return (int(uid), str(uname)) if ok else None


def dev_public_registration_allowed() -> bool:
    """
    Open /dev/register for creating new dev operator accounts.
    Off by default in all environments; only on when ALLOW_DEV_PUBLIC_REGISTER=1.
    """
    o = (os.environ.get("ALLOW_DEV_PUBLIC_REGISTER") or "").strip().lower()
    return o in ("1", "true", "yes", "on")


def _migrate_legacy_admin_users_if_empty(conn: sqlite3.Connection) -> None:
    """One-time copy from users.db.admin_users when dev_users.db is new (plain SQLite legacy only)."""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM admin_users")
    if cur.fetchone()[0] > 0:
        return
    legacy_path = Path(legacy_users_sqlite.DB_PATH).expanduser()
    if not legacy_path.is_file():
        return
    leg: sqlite3.Connection | None = None
    try:
        leg = sqlite3.connect(str(legacy_path), timeout=5.0)
        leg_cur = leg.cursor()
        leg_cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='admin_users'"
        )
        if not leg_cur.fetchone():
            return
        rows = leg_cur.execute(
            "SELECT username, email, password FROM admin_users"
        ).fetchall()
        for username, email, pw_hash in rows:
            try:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO admin_users (username, email, password)
                    VALUES (?, ?, ?)
                    """,
                    (username, email, pw_hash),
                )
            except sqlite3.Error:
                logger.exception("admin_users migration row failed for %r", username)
        conn.commit()
        if rows:
            logger.info("Migrated %d admin_users row(s) from %s to dev_users.db", len(rows), legacy_path)
    except sqlite3.Error as e:
        logger.info("Skipping admin_users migration (legacy DB unreadable or not plain SQLite): %s", e)
    finally:
        if leg is not None:
            leg.close()


def init_admin_db() -> None:
    if _use_pg():
        _pg_init_admin_db()
        return
    conn = get_dev_users_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            totp_enabled INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.commit()
    try:
        cursor.execute("PRAGMA table_info(admin_users)")
        cols = {r[1] for r in cursor.fetchall()}
        for col, ddl in (
            ("totp_enabled", "ALTER TABLE admin_users ADD COLUMN totp_enabled INTEGER NOT NULL DEFAULT 0"),
        ):
            if col not in cols:
                cursor.execute(ddl)
        conn.commit()
    except sqlite3.Error:
        pass
    _migrate_legacy_admin_users_if_empty(conn)

    default_user = (os.environ.get("ADMIN_USERNAME") or "admin").strip() or "admin"
    default_email = (os.environ.get("ADMIN_EMAIL") or "admin@localhost").strip() or "admin@localhost"
    if is_production_env():
        default_pw_plain = (os.environ.get("ADMIN_PASSWORD") or "").strip()
        if not default_pw_plain:
            conn.close()
            raise RuntimeError(
                "FLASK_ENV=production requires ADMIN_PASSWORD to be set for /dev admin bootstrap."
            )
    else:
        default_pw_plain = (os.environ.get("ADMIN_PASSWORD") or "changeme").strip() or "changeme"
    default_pw = hash_password(default_pw_plain)
    cursor.execute(
        "SELECT id FROM admin_users WHERE username = ?",
        (default_user,),
    )
    existing = cursor.fetchone()
    cursor.execute(
        """
        INSERT OR IGNORE INTO admin_users (username, email, password)
        VALUES (?, ?, ?)
        """,
        (default_user, default_email, default_pw),
    )
    if existing and os.environ.get("ADMIN_PASSWORD") and bootstrap_force_admin_password():
        cursor.execute(
            "UPDATE admin_users SET password = ? WHERE username = ?",
            (hash_password(default_pw_plain), default_user),
        )
    conn.commit()
    conn.close()


def save_dev_admin_user(username: str, email: str, password: str) -> None:
    """Raises AdminUserExists when the username or email is taken."""
    if _use_pg():
        import psycopg

        conn = _pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO admin_users (username, email, password) VALUES (%s, %s, %s)",
                    (username.strip(), email.strip(), hash_password(password)),
                )
            conn.commit()
        except psycopg.errors.UniqueViolation as e:
            conn.rollback()
            raise AdminUserExists(str(e)) from e
        finally:
            conn.close()
        return
    conn = get_dev_users_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO admin_users (username, email, password) VALUES (?, ?, ?)",
            (username.strip(), email.strip(), hash_password(password)),
        )
        conn.commit()
    except Exception as e:
        # By name, not sqlite3.IntegrityError: SQLCipher raises its own class, and the
        # no-SQLite allowlist only shrinks.
        if type(e).__name__ != "IntegrityError":
            raise
        raise AdminUserExists(str(e)) from e
    finally:
        conn.close()


def check_admin(login_input: str, password: str) -> bool:
    return authenticate_admin(login_input, password) is not None


def authenticate_admin(login_input: str, password: str) -> tuple[int, str] | None:
    li = (login_input or "").strip()
    if not li:
        return None
    if _use_pg():
        return _pg_authenticate(li, password)
    conn = get_dev_users_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, username, password FROM admin_users
        WHERE lower(username) = lower(?) OR lower(email) = lower(?)
        """,
        (li, li),
    )
    row = cursor.fetchone()
    if not row:
        conn.close()
        return None
    uid, uname, stored = row
    ok = verify_or_legacy(password, stored)
    if ok and password_needs_rehash(stored):
        cursor.execute(
            "UPDATE admin_users SET password = ? WHERE id = ? AND password = ?",
            (hash_password(password), uid, stored),
        )
        conn.commit()
    conn.close()
    if not ok:
        return None
    return int(uid), str(uname)




def dev_users_db_path() -> str:
    return str(Path(DEV_USERS_DB_PATH).expanduser().resolve())
