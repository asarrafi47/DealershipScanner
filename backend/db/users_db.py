import os
import sqlite3

from backend.db.password_hash import hash_password, verify_or_legacy
from backend.db.users_sqlite import DB_PATH, get_users_conn
from backend.utils.runtime_env import is_production_env


def get_conn():
    return get_users_conn()


def init_users_db():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        )
        """
    )
    allow_default = (os.environ.get("ALLOW_DEFAULT_APP_USER") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    if (not is_production_env()) or allow_default:
        default_pw = hash_password("password")
        cursor.execute(
            """
            INSERT OR IGNORE INTO users (username, email, password)
            VALUES ('admin', 'admin@admin.com', ?)
            """,
            (default_pw,),
        )
    conn.commit()
    conn.close()


def check_user(login_input, password):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, username, email, password FROM users WHERE username = ? OR email = ?",
        (login_input, login_input),
    )
    row = cursor.fetchone()
    if not row:
        conn.close()
        return False
    uid, _u, _e, stored = row
    ok = verify_or_legacy(password, stored)
    if ok and not stored.startswith("$2"):
        cursor.execute(
            "UPDATE users SET password = ? WHERE id = ?",
            (hash_password(password), uid),
        )
        conn.commit()
    conn.close()
    return ok


def save_user(username, email, password):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO users (username, email, password) VALUES (?, ?, ?)",
        (username, email, hash_password(password)),
    )
    conn.commit()
    conn.close()
