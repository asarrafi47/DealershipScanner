import os
import secrets
import sqlite3
from pathlib import Path

from werkzeug.security import check_password_hash, generate_password_hash

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_USERS_DB = _REPO_ROOT / "users.db"
DB_PATH = os.environ.get("USERS_DB_PATH", str(_DEFAULT_USERS_DB))


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _looks_like_password_hash(value: str) -> bool:
    return value.startswith("pbkdf2:") or value.startswith("scrypt:")


def init_users_db():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def get_user_by_id(user_id: int):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    row = cursor.fetchone()
    conn.close()
    return row


def get_user_by_login(login_input: str):
    li = (login_input or "").strip()
    if not li:
        return None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM users WHERE lower(username) = lower(?) OR email = ?",
        (li, li.lower()),
    )
    row = cursor.fetchone()
    conn.close()
    return row


def _upgrade_plaintext_password(user_id: int, plain: str) -> None:
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE users SET password = ? WHERE id = ?",
        (generate_password_hash(plain), user_id),
    )
    conn.commit()
    conn.close()


def verify_login(login_input: str, password: str):
    """Return user row if credentials are valid; upgrade legacy plaintext hashes in place."""
    if not (login_input or "").strip():
        return None
    if password is None or password == "":
        return None
    row = get_user_by_login(login_input)
    if row is None:
        return None
    stored = row["password"] or ""
    if _looks_like_password_hash(stored):
        if check_password_hash(stored, password):
            return row
        return None
    if not stored:
        return None
    try:
        ok = secrets.compare_digest(stored, password)
    except (TypeError, ValueError):
        return None
    if ok:
        _upgrade_plaintext_password(row["id"], password)
        return get_user_by_id(row["id"])
    return None


def create_user(username: str, email: str, password: str):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO users (username, email, password) VALUES (?, ?, ?)",
        (username.strip(), email.strip().lower(), generate_password_hash(password)),
    )
    conn.commit()
    conn.close()
