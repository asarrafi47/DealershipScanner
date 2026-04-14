"""Admin users for /dev dashboard — separate from public app users (users table)."""

from __future__ import annotations

import os

from backend.db.password_hash import hash_password, verify_or_legacy
from backend.db.users_db import get_conn


def init_admin_db() -> None:
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        )
        """
    )
    default_user = (os.environ.get("ADMIN_USERNAME") or "admin").strip() or "admin"
    default_email = (os.environ.get("ADMIN_EMAIL") or "admin@localhost").strip() or "admin@localhost"
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


def check_admin(login_input: str, password: str) -> bool:
    return authenticate_admin(login_input, password) is not None


def authenticate_admin(login_input: str, password: str) -> tuple[int, str] | None:
    conn = get_conn()
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
