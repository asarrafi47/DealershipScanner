"""Admin users for /dev dashboard — stored in dev_users.db (not public users.db)."""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path

from backend.db.dev_users_sqlite import DB_PATH as DEV_USERS_DB_PATH
from backend.db.dev_users_sqlite import get_dev_users_conn
from backend.db.password_hash import hash_password, verify_or_legacy
from backend.db import users_sqlite as legacy_users_sqlite
from backend.utils.runtime_env import is_production_env

logger = logging.getLogger(__name__)


def dev_public_registration_allowed() -> bool:
    """
    Open /dev/register for creating new dev operator accounts.
    Production: off unless ALLOW_DEV_PUBLIC_REGISTER is truthy.
    Non-production: on unless DEV_DISABLE_PUBLIC_REGISTER is truthy.
    """
    o = (os.environ.get("ALLOW_DEV_PUBLIC_REGISTER") or "").strip().lower()
    d = (os.environ.get("DEV_DISABLE_PUBLIC_REGISTER") or "").strip().lower()
    if is_production_env():
        return o in ("1", "true", "yes", "on")
    return d not in ("1", "true", "yes", "on")


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
    conn = get_dev_users_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            totp_secret TEXT,
            totp_enabled INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.commit()
    try:
        cursor.execute("PRAGMA table_info(admin_users)")
        cols = {r[1] for r in cursor.fetchall()}
        for col, ddl in (
            ("totp_secret", "ALTER TABLE admin_users ADD COLUMN totp_secret TEXT"),
            ("totp_enabled", "ALTER TABLE admin_users ADD COLUMN totp_enabled INTEGER NOT NULL DEFAULT 0"),
            ("mfa_method", "ALTER TABLE admin_users ADD COLUMN mfa_method TEXT"),
            ("mfa_phone", "ALTER TABLE admin_users ADD COLUMN mfa_phone TEXT"),
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
        """
        INSERT OR IGNORE INTO admin_users (username, email, password)
        VALUES (?, ?, ?)
        """,
        (default_user, default_email, default_pw),
    )
    if os.environ.get("ADMIN_PASSWORD"):
        cursor.execute(
            "UPDATE admin_users SET password = ? WHERE username = ?",
            (hash_password(default_pw_plain), default_user),
        )
    conn.commit()
    conn.close()


def save_dev_admin_user(username: str, email: str, password: str) -> None:
    conn = get_dev_users_conn()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO admin_users (username, email, password) VALUES (?, ?, ?)",
        (username.strip(), email.strip(), hash_password(password)),
    )
    conn.commit()
    conn.close()


def check_admin(login_input: str, password: str) -> bool:
    return authenticate_admin(login_input, password) is not None


def authenticate_admin(login_input: str, password: str) -> tuple[int, str] | None:
    conn = get_dev_users_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, username, password FROM admin_users
        WHERE username = ? OR email = ?
        """,
        (login_input, login_input),
    )
    row = cursor.fetchone()
    if not row:
        conn.close()
        return None
    uid, uname, stored = row
    ok = verify_or_legacy(password, stored)
    if ok and not stored.startswith("$2"):
        cursor.execute(
            "UPDATE admin_users SET password = ? WHERE id = ?",
            (hash_password(password), uid),
        )
        conn.commit()
    conn.close()
    if not ok:
        return None
    return int(uid), str(uname)


def get_admin_totp(user_id: int) -> dict | None:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return None
    if uid <= 0:
        return None
    conn = get_dev_users_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(admin_users)")
    cols = {r[1] for r in cursor.fetchall()}
    if "totp_secret" not in cols:
        conn.close()
        return {"enabled": False, "secret": ""}
    cursor.execute("SELECT totp_secret, totp_enabled FROM admin_users WHERE id = ?", (uid,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    sec, en = row
    return {"enabled": bool(en), "secret": sec or ""}


def set_admin_totp(user_id: int, *, secret: str, enabled: bool) -> bool:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid <= 0:
        return False
    conn = get_dev_users_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(admin_users)")
    cols = {r[1] for r in cursor.fetchall()}
    if "totp_secret" not in cols:
        conn.close()
        return False
    sec = (secret or "").strip().upper().replace(" ", "")
    cursor.execute(
        "UPDATE admin_users SET totp_secret = ?, totp_enabled = ? WHERE id = ?",
        (sec or None, 1 if enabled else 0, uid),
    )
    ok = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return bool(ok)


def dev_users_db_path() -> str:
    return str(Path(DEV_USERS_DB_PATH).expanduser().resolve())
