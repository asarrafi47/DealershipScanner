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
    cursor.execute("PRAGMA table_info(users)")
    ucols = {row[1] for row in cursor.fetchall()}
    for col, ddl in (
        ("role", "ALTER TABLE users ADD COLUMN role TEXT"),
        ("dealer_id", "ALTER TABLE users ADD COLUMN dealer_id TEXT"),
        ("dealership_registry_id", "ALTER TABLE users ADD COLUMN dealership_registry_id INTEGER"),
    ):
        if col not in ucols:
            cursor.execute(ddl)
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
    try:
        cursor.execute("UPDATE users SET role = 'admin' WHERE lower(username) = 'admin'")
        cursor.execute(
            "UPDATE users SET role = 'dealer_staff' WHERE (role IS NULL OR trim(role) = '') "
            "AND lower(username) != 'admin'"
        )
    except sqlite3.Error:
        pass
    conn.commit()
    conn.close()


def get_user_by_login(login_input: str) -> dict | None:
    """Return user row for a username or email, or None (includes ``role``, ``dealer_id`` when present)."""
    li = (login_input or "").strip()
    if not li:
        return None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = [r[1] for r in cursor.fetchall()]
    want = ["id", "username", "email"]
    for extra in ("role", "dealer_id", "dealership_registry_id"):
        if extra in cols:
            want.append(extra)
    cursor.execute(
        f"SELECT {', '.join(want)} FROM users WHERE username = ? OR email = ?",
        (li, li),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    out: dict = {}
    for i, k in enumerate(want):
        out[k] = row[i]
    out["id"] = int(out["id"])
    if "dealership_registry_id" in out and out["dealership_registry_id"] is not None:
        try:
            out["dealership_registry_id"] = int(out["dealership_registry_id"])
        except (TypeError, ValueError):
            out["dealership_registry_id"] = None
    if "role" not in out or out["role"] is None:
        out["role"] = "dealer_staff"
    return out


def get_user_profile(user_id: int) -> dict | None:
    """Load ``users`` row by id (for admin authorization)."""
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return None
    if uid <= 0:
        return None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = [r[1] for r in cursor.fetchall()]
    want = ["id", "username", "email"]
    for extra in ("role", "dealer_id", "dealership_registry_id"):
        if extra in cols:
            want.append(extra)
    cursor.execute(f"SELECT {', '.join(want)} FROM users WHERE id = ?", (uid,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    out: dict = {}
    for i, k in enumerate(want):
        out[k] = row[i]
    out["id"] = int(out["id"])
    if "dealership_registry_id" in out and out["dealership_registry_id"] is not None:
        try:
            out["dealership_registry_id"] = int(out["dealership_registry_id"])
        except (TypeError, ValueError):
            out["dealership_registry_id"] = None
    if "role" not in out or out["role"] is None:
        out["role"] = "dealer_staff"
    return out


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


def save_user(username, email, password) -> int:
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    has_role = any(r[1] == "role" for r in cursor.fetchall())
    if has_role:
        cursor.execute(
            "INSERT INTO users (username, email, password, role) VALUES (?, ?, ?, 'dealer_staff')",
            (username, email, hash_password(password)),
        )
    else:
        cursor.execute(
            "INSERT INTO users (username, email, password) VALUES (?, ?, ?)",
            (username, email, hash_password(password)),
        )
    uid = int(cursor.lastrowid)
    conn.commit()
    conn.close()
    return uid
