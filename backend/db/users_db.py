import os
import secrets
import sqlite3
import threading
import time
from functools import lru_cache

from backend.db.password_hash import hash_password, password_needs_rehash, verify_or_legacy
from backend.db.users_sqlite import DB_PATH, get_users_conn
from backend.utils.roles import ROLE_ADMIN
from backend.utils.runtime_env import is_production_env


def get_conn():
    return get_users_conn()


@lru_cache(maxsize=1)
def _users_select_columns() -> tuple[str, ...]:
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = [r[1] for r in cursor.fetchall()]
    conn.close()
    want = ["id", "username", "email"]
    for extra in (
        "role",
        "dealer_id",
        "dealership_registry_id",
        "org_id",
        "totp_enabled",
        "mfa_phone",
        "is_premium",
        "google_sub",
        "apple_sub",
        "is_active",
    ):
        if extra in cols:
            want.append(extra)
    return tuple(want)


def _user_dict_from_row(row: tuple, want: tuple[str, ...]) -> dict:
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
    if "totp_enabled" in out:
        out["totp_enabled"] = bool(out["totp_enabled"])
    if "totp_secret" in out and out["totp_secret"] is None:
        out["totp_secret"] = ""
    if "mfa_phone" in out and out["mfa_phone"] is None:
        out["mfa_phone"] = ""
    if "is_premium" in out:
        out["is_premium"] = bool(out["is_premium"])
    if "is_active" in out:
        out["is_active"] = bool(out["is_active"])
    return out


def _user_row_is_active(user: dict) -> bool:
    if "is_active" not in user:
        return True
    return bool(user.get("is_active"))


def _schedule_password_rehash(user_id: int, password: str, stored_hash: str) -> None:
    """Upgrade weak hashes without blocking the login response."""

    expected = (stored_hash or "").strip()
    if not expected:
        return

    def _run() -> None:
        h = hash_password(password)
        for attempt in range(6):
            try:
                conn = get_conn()
                cur = conn.cursor()
                cur.execute(
                    "UPDATE users SET password = ? WHERE id = ? AND password = ?",
                    (h, user_id, expected),
                )
                conn.commit()
                conn.close()
                return
            except sqlite3.OperationalError as ex:
                if "locked" not in str(ex).lower() or attempt >= 5:
                    return
                time.sleep(0.08 * (attempt + 1))

    threading.Thread(target=_run, name=f"pw-rehash-{user_id}", daemon=True).start()


def _env_admin_set_clause(cursor: sqlite3.Cursor) -> str:
    """Return ``SET role=…, totp off, …`` fragment for env-listed admin accounts."""
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    parts = ["role = ?", "totp_enabled = 0"]
    if "totp_secret" in cols:
        parts.append("totp_secret = NULL")
    if "mfa_method" in cols:
        parts.append("mfa_method = NULL")
    return ", ".join(parts)


def _apply_env_admin_privileges(cursor: sqlite3.Cursor) -> None:
    """Promote (or create in dev) ``APP_ADMIN_EMAILS`` / ``APP_ADMIN_USERNAMES`` accounts to admin and disable MFA.
    Creation only happens when ALLOW_DEFAULT_APP_USER=1 (non-production).
    """
    from backend.utils.roles import admin_emails, admin_usernames
    from backend.utils.runtime_env import is_production_env

    emails = admin_emails()
    usernames_list = admin_usernames()
    if not emails and not usernames_list:
        return

    allow_default = (os.environ.get("ALLOW_DEFAULT_APP_USER") or "").strip().lower() in (
        "1", "true", "yes", "on"
    )
    set_sql = _env_admin_set_clause(cursor)
    default_pw_plain = (os.environ.get("ADMIN_PASSWORD") or "ChangeMe2026!").strip()
    default_pw = hash_password(default_pw_plain)

    for em in emails:
        cursor.execute(
            f"UPDATE users SET {set_sql} WHERE lower(email) = lower(?)",
            (ROLE_ADMIN, em),
        )
        if cursor.rowcount == 0 and allow_default and not is_production_env():
            uname = em.split("@")[0]
            cursor.execute(
                """
                INSERT INTO users (username, email, password, role, totp_enabled)
                VALUES (?, ?, ?, 'admin', 0)
                """,
                (uname, em, default_pw),
            )

    for un in usernames_list:
        cursor.execute(
            f"UPDATE users SET {set_sql} WHERE lower(username) = lower(?)",
            (ROLE_ADMIN, un),
        )
        if cursor.rowcount == 0 and allow_default and not is_production_env():
            email_guess = f"{un}@localhost"
            cursor.execute(
                """
                INSERT INTO users (username, email, password, role, totp_enabled)
                VALUES (?, ?, ?, 'admin', 0)
                """,
                (un, email_guess, default_pw),
            )


def sync_env_admin_user_row(user_id: int) -> None:
    """After login or when env changed, ensure env-listed users have admin + MFA off in SQLite."""
    from backend.utils.roles import account_has_env_admin_privilege

    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return
    if uid <= 0:
        return
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT email, username FROM users WHERE id = ?", (uid,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return
    email, username = row[0], row[1]
    if not account_has_env_admin_privilege(str(email or ""), str(username or "")):
        conn.close()
        return
    set_sql = _env_admin_set_clause(cursor)
    cursor.execute(
        f"UPDATE users SET {set_sql} WHERE id = ?",
        (ROLE_ADMIN, uid),
    )
    conn.commit()
    conn.close()


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
        ("org_id", "ALTER TABLE users ADD COLUMN org_id INTEGER"),
        ("totp_secret", "ALTER TABLE users ADD COLUMN totp_secret TEXT"),
        ("totp_enabled", "ALTER TABLE users ADD COLUMN totp_enabled INTEGER NOT NULL DEFAULT 0"),
        ("mfa_method", "ALTER TABLE users ADD COLUMN mfa_method TEXT"),
        ("mfa_phone", "ALTER TABLE users ADD COLUMN mfa_phone TEXT"),
        ("is_premium", "ALTER TABLE users ADD COLUMN is_premium INTEGER NOT NULL DEFAULT 0"),
        ("premium_stripe_customer_id", "ALTER TABLE users ADD COLUMN premium_stripe_customer_id TEXT"),
        ("premium_stripe_session_id", "ALTER TABLE users ADD COLUMN premium_stripe_session_id TEXT"),
        (
            "premium_stripe_subscription_id",
            "ALTER TABLE users ADD COLUMN premium_stripe_subscription_id TEXT",
        ),
        ("google_sub", "ALTER TABLE users ADD COLUMN google_sub TEXT"),
        ("apple_sub", "ALTER TABLE users ADD COLUMN apple_sub TEXT"),
        (
            "subscription_plan_id",
            "ALTER TABLE users ADD COLUMN subscription_plan_id TEXT",
        ),
        (
            "email_verified_at",
            "ALTER TABLE users ADD COLUMN email_verified_at TEXT",
        ),
        (
            "email_verify_token_hash",
            "ALTER TABLE users ADD COLUMN email_verify_token_hash TEXT",
        ),
        (
            "password_reset_token_hash",
            "ALTER TABLE users ADD COLUMN password_reset_token_hash TEXT",
        ),
        (
            "password_reset_expires_at",
            "ALTER TABLE users ADD COLUMN password_reset_expires_at INTEGER",
        ),
        (
            "is_active",
            "ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1",
        ),
    ):
        if col not in ucols:
            cursor.execute(ddl)
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_google_sub "
        "ON users(google_sub) WHERE google_sub IS NOT NULL AND google_sub != ''"
    )
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_apple_sub "
        "ON users(apple_sub) WHERE apple_sub IS NOT NULL AND apple_sub != ''"
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS orgs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            stripe_customer_id TEXT,
            stripe_subscription_id TEXT,
            stripe_subscription_status TEXT,
            stripe_current_period_end TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS org_invites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id INTEGER NOT NULL,
            token_hash TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            used_at TEXT,
            used_by_user_id INTEGER,
            FOREIGN KEY (org_id) REFERENCES orgs(id),
            FOREIGN KEY (used_by_user_id) REFERENCES users(id)
        )
        """
    )
    allow_default = (os.environ.get("ALLOW_DEFAULT_APP_USER") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    # Never silently seed weak default credentials. If a legacy dev DB already has
    # admin/password, remove it unless explicitly opted-in via ALLOW_DEFAULT_APP_USER.
    try:
        cursor.execute(
            "SELECT id, password FROM users WHERE lower(username) = 'admin' AND lower(email) = 'admin@admin.com' LIMIT 1"
        )
        row = cursor.fetchone()
        if row:
            uid, stored = row
            try:
                is_legacy_default = bool(verify_or_legacy("password", stored))
            except Exception:
                is_legacy_default = False
            if is_legacy_default and not allow_default:
                cursor.execute("DELETE FROM users WHERE id = ?", (int(uid),))
    except sqlite3.Error:
        pass

    if allow_default and (not is_production_env()):
        default_pw = hash_password("password")
        cursor.execute(
            """
            INSERT OR IGNORE INTO users (username, email, password, role)
            VALUES ('admin', 'admin@admin.com', ?, 'admin')
            """,
            (default_pw,),
        )

    # Always seed APP_ADMIN_USERNAMES in development (convenience for the main developer)
    if not is_production_env():
        try:
            from backend.utils.roles import admin_usernames, admin_emails
            _admin_email_map = {e.split("@")[0].lower(): e for e in admin_emails()}
            for uname in admin_usernames():
                email_guess = _admin_email_map.get(uname.lower()) or f"{uname}@localhost"
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO users (username, email, password, role, totp_enabled)
                    VALUES (?, ?, ?, 'admin', 0)
                    """,
                    (uname, email_guess, hash_password("ChangeMe2026!")),
                )
            _apply_env_admin_privileges(cursor)
        except Exception as e:
            print(f"Warning: Could not seed APP_ADMIN_USERNAMES: {e}")

    try:
        cursor.execute(
            "UPDATE users SET role = 'dealer_staff' WHERE (role IS NULL OR trim(role) = '')"
        )
    except sqlite3.Error:
        pass

    conn.commit()
    conn.close()
    _users_select_columns.cache_clear()

    try:
        from backend.db.user_history_db import ensure_car_history_table
        ensure_car_history_table()
    except Exception:
        pass


def get_user_totp(user_id: int) -> dict | None:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return None
    if uid <= 0:
        return None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    if "totp_secret" not in cols:
        conn.close()
        return {"enabled": False, "secret": ""}
    if "totp_enabled" in cols:
        cursor.execute("SELECT totp_secret, totp_enabled FROM users WHERE id = ?", (uid,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        sec, en = row
        return {"enabled": bool(en), "secret": sec or ""}
    cursor.execute("SELECT totp_secret FROM users WHERE id = ?", (uid,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    sec = row[0]
    return {"enabled": bool(sec), "secret": sec or ""}


def set_user_totp(user_id: int, *, secret: str, enabled: bool) -> bool:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid <= 0:
        return False
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    if "totp_secret" not in cols:
        conn.close()
        return False
    sec = (secret or "").strip().upper().replace(" ", "")
    if "totp_enabled" in cols:
        cursor.execute(
            "UPDATE users SET totp_secret = ?, totp_enabled = ? WHERE id = ?",
            (sec or None, 1 if enabled else 0, uid),
        )
    else:
        cursor.execute(
            "UPDATE users SET totp_secret = ? WHERE id = ?",
            (sec or None, uid),
        )
    ok = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return bool(ok)


def get_user_by_login(login_input: str) -> dict | None:
    """Return user row for a username or email, or None (includes ``role``, ``dealer_id`` when present)."""
    li = (login_input or "").strip()
    if not li:
        return None
    want = _users_select_columns()
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT {', '.join(want)} FROM users WHERE lower(username) = lower(?) OR lower(email) = lower(?)",
        (li, li),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    return _user_dict_from_row(row, want)


def update_user_profile(user_id: int, username: str, email: str) -> str | None:
    """Update username and email for a signed-in user. Returns an error message or None."""
    from backend.utils.registration_validation import (
        MAX_EMAIL_LEN,
        MAX_USERNAME_LEN,
        normalize_registration_email,
        normalize_registration_username,
    )

    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return "Invalid account."
    if uid <= 0:
        return "Invalid account."

    uname = normalize_registration_username(username)
    em = normalize_registration_email(email)
    if len(uname) < 2:
        return "Username must be at least 2 characters."
    if len(uname) > MAX_USERNAME_LEN:
        return f"Username must be at most {MAX_USERNAME_LEN} characters."
    if len(em) < 3 or "@" not in em:
        return "Enter a valid email address."
    if len(em) > MAX_EMAIL_LEN:
        return f"Email must be at most {MAX_EMAIL_LEN} characters."

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id FROM users WHERE lower(username) = lower(?) AND id != ?",
        (uname, uid),
    )
    if cursor.fetchone():
        conn.close()
        return "That username is already taken."
    cursor.execute(
        "SELECT id FROM users WHERE lower(email) = lower(?) AND id != ?",
        (em, uid),
    )
    if cursor.fetchone():
        conn.close()
        return "That email is already registered."
    cursor.execute(
        "UPDATE users SET username = ?, email = ? WHERE id = ?",
        (uname, em, uid),
    )
    if cursor.rowcount == 0:
        conn.close()
        return "Account not found."
    conn.commit()
    conn.close()
    return None


def change_user_password(user_id: int, current_password: str, new_password: str) -> str | None:
    """Verify current password and set a new hash. Returns an error message or None."""
    from backend.utils.registration_validation import MAX_PASSWORD_LEN

    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return "Invalid account."
    if uid <= 0:
        return "Invalid account."
    cur_pw = (current_password or "").strip()
    new_pw = (new_password or "").strip()
    if not cur_pw or not new_pw:
        return "Enter your current password and a new password."
    if len(new_pw) > MAX_PASSWORD_LEN:
        return f"Password must be at most {MAX_PASSWORD_LEN} characters."

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT password FROM users WHERE id = ?", (uid,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return "Account not found."
    stored = row[0]
    if not verify_or_legacy(cur_pw, stored):
        conn.close()
        return "Current password is incorrect."
    cursor.execute(
        "UPDATE users SET password = ? WHERE id = ?",
        (hash_password(new_pw), uid),
    )
    conn.commit()
    conn.close()
    return None


def reset_user_password(user_id: int, new_password: str) -> str | None:
    """Set a new password hash without verifying the current password (reset flow)."""
    from backend.utils.registration_validation import MAX_PASSWORD_LEN

    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return "Invalid account."
    if uid <= 0:
        return "Invalid account."
    new_pw = (new_password or "").strip()
    if not new_pw:
        return "Enter a new password."
    if len(new_pw) > MAX_PASSWORD_LEN:
        return f"Password must be at most {MAX_PASSWORD_LEN} characters."

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users WHERE id = ?", (uid,))
    if not cursor.fetchone():
        conn.close()
        return "Account not found."
    cursor.execute(
        "UPDATE users SET password = ? WHERE id = ?",
        (hash_password(new_pw), uid),
    )
    conn.commit()
    conn.close()
    return None


def authenticate_app_user(login_input: str, password: str) -> dict | None:
    """Verify credentials and return the user row, or None."""
    li = (login_input or "").strip()
    if not li or not password:
        return None
    want = _users_select_columns()
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT {', '.join(want)}, password FROM users WHERE lower(username) = lower(?) OR lower(email) = lower(?)",
        (li, li),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    stored = row[-1]
    if not verify_or_legacy(password, stored):
        return None
    user = _user_dict_from_row(row[:-1], want)
    if not _user_row_is_active(user):
        return None
    if password_needs_rehash(stored):
        _schedule_password_rehash(int(user["id"]), password, stored)
    return user


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
    for extra in ("role", "dealer_id", "dealership_registry_id", "org_id", "mfa_phone", "is_premium", "subscription_plan_id", "is_active"):
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
    if "mfa_phone" in out and out["mfa_phone"] is None:
        out["mfa_phone"] = ""
    return out


def get_user_email_verification_state(user_id: int) -> dict | None:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return None
    if uid <= 0:
        return None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    want = ["id", "email"]
    for extra in (
        "email_verified_at",
        "email_verify_token_hash",
        "google_sub",
        "apple_sub",
    ):
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
    return out


def set_user_email_verify_token(user_id: int, token_hash: str) -> bool:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid <= 0:
        return False
    th = (token_hash or "").strip()
    if not th:
        return False
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    if "email_verify_token_hash" not in cols:
        conn.close()
        return False
    cursor.execute(
        "UPDATE users SET email_verify_token_hash = ?, email_verified_at = NULL WHERE id = ?",
        (th, uid),
    )
    ok = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return bool(ok)


def clear_user_email_verify_token(user_id: int) -> bool:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid <= 0:
        return False
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    if "email_verify_token_hash" not in cols:
        conn.close()
        return False
    cursor.execute(
        "UPDATE users SET email_verify_token_hash = NULL WHERE id = ?",
        (uid,),
    )
    ok = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return bool(ok)


def mark_user_email_verified(user_id: int) -> bool:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid <= 0:
        return False
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    if "email_verified_at" not in cols:
        conn.close()
        return False
    cursor.execute(
        "UPDATE users SET email_verified_at = datetime('now') WHERE id = ?",
        (uid,),
    )
    ok = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return bool(ok)


def get_user_id_by_email_verify_token_hash(token_hash: str) -> int | None:
    th = (token_hash or "").strip()
    if not th:
        return None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    if "email_verify_token_hash" not in cols:
        conn.close()
        return None
    cursor.execute(
        "SELECT id FROM users WHERE email_verify_token_hash = ? LIMIT 1",
        (th,),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return None


def set_user_password_reset_token(user_id: int, token_hash: str, expires_at: int) -> bool:
    try:
        uid = int(user_id)
        exp = int(expires_at)
    except (TypeError, ValueError):
        return False
    if uid <= 0 or exp <= 0:
        return False
    th = (token_hash or "").strip()
    if not th:
        return False
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    if "password_reset_token_hash" not in cols or "password_reset_expires_at" not in cols:
        conn.close()
        return False
    cursor.execute(
        "UPDATE users SET password_reset_token_hash = ?, password_reset_expires_at = ? WHERE id = ?",
        (th, exp, uid),
    )
    ok = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return bool(ok)


def clear_user_password_reset_token(user_id: int) -> bool:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid <= 0:
        return False
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    if "password_reset_token_hash" not in cols:
        conn.close()
        return False
    cursor.execute(
        "UPDATE users SET password_reset_token_hash = NULL, password_reset_expires_at = NULL WHERE id = ?",
        (uid,),
    )
    ok = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return bool(ok)


def get_user_id_by_password_reset_token_hash(token_hash: str) -> int | None:
    import time

    th = (token_hash or "").strip()
    if not th:
        return None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    if "password_reset_token_hash" not in cols or "password_reset_expires_at" not in cols:
        conn.close()
        return None
    cursor.execute(
        "SELECT id, password_reset_expires_at FROM users WHERE password_reset_token_hash = ? LIMIT 1",
        (th,),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    try:
        uid = int(row[0])
        exp = int(row[1] or 0)
    except (TypeError, ValueError):
        return None
    if exp <= int(time.time()):
        return None
    return uid


def set_user_mfa_phone(user_id: int, phone: str | None) -> bool:
    """Optional column; legacy (SMS MFA removed)."""
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid <= 0:
        return False
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    if "mfa_phone" not in cols:
        conn.close()
        return False
    p = (phone or "").strip() or None
    cursor.execute("UPDATE users SET mfa_phone = ? WHERE id = ?", (p, uid))
    ok = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return bool(ok)


def check_user(login_input, password):
    li = (login_input or "").strip()
    if not li:
        return False
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    has_is_active = any(r[1] == "is_active" for r in cursor.fetchall())
    active_col = ", is_active" if has_is_active else ""
    cursor.execute(
        f"""
        SELECT id, username, email, password{active_col} FROM users
        WHERE lower(username) = lower(?) OR lower(email) = lower(?)
        """,
        (li, li),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return False
    uid, _u, _e, stored = row[0], row[1], row[2], row[3]
    if has_is_active and not bool(row[4]):
        return False
    ok = verify_or_legacy(password, stored)
    if ok and password_needs_rehash(stored):
        _schedule_password_rehash(int(uid), password, stored)
    return ok


def create_org(name: str) -> int:
    nm = (name or "").strip()
    if not nm:
        raise ValueError("Organization name is required.")
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO orgs (name) VALUES (?)", (nm,))
    oid = int(cursor.lastrowid)
    conn.commit()
    conn.close()
    return oid


def get_org(org_id: int) -> dict | None:
    try:
        oid = int(org_id)
    except (TypeError, ValueError):
        return None
    if oid <= 0:
        return None
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, name, stripe_customer_id, stripe_subscription_id,
               stripe_subscription_status, stripe_current_period_end
        FROM orgs WHERE id = ?
        """,
        (oid,),
    )
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def update_org_stripe_subscription(
    org_id: int,
    *,
    customer_id: str | None = None,
    subscription_id: str | None = None,
    status: str | None = None,
    current_period_end_iso: str | None = None,
) -> None:
    try:
        oid = int(org_id)
    except (TypeError, ValueError):
        return
    if oid <= 0:
        return
    sets = []
    params: list = []
    if customer_id is not None:
        sets.append("stripe_customer_id = ?")
        params.append((customer_id or "").strip() or None)
    if subscription_id is not None:
        sets.append("stripe_subscription_id = ?")
        params.append((subscription_id or "").strip() or None)
    if status is not None:
        sets.append("stripe_subscription_status = ?")
        params.append((status or "").strip().lower() or None)
    if current_period_end_iso is not None:
        sets.append("stripe_current_period_end = ?")
        params.append((current_period_end_iso or "").strip() or None)
    if not sets:
        return
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(f"UPDATE orgs SET {', '.join(sets)} WHERE id = ?", (*params, oid))
    conn.commit()
    conn.close()


def user_exists_by_email(email: str) -> bool:
    e = (email or "").strip().lower()
    if not e or "@" not in e:
        return False
    conn = get_conn()
    row = conn.execute(
        "SELECT 1 FROM users WHERE lower(email) = ? LIMIT 1",
        (e,),
    ).fetchone()
    conn.close()
    return row is not None


def get_user_by_google_sub(google_sub: str) -> dict | None:
    sub = (google_sub or "").strip()
    if not sub:
        return None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = [r[1] for r in cursor.fetchall()]
    if "google_sub" not in cols:
        conn.close()
        return None
    want = ["id", "username", "email", "google_sub"]
    for extra in ("role", "org_id", "is_premium"):
        if extra in cols:
            want.append(extra)
    cursor.execute(
        f"SELECT {', '.join(want)} FROM users WHERE google_sub = ? LIMIT 1",
        (sub,),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    out: dict = {}
    for i, k in enumerate(want):
        out[k] = row[i]
    out["id"] = int(out["id"])
    if "role" not in out or out["role"] is None:
        out["role"] = "dealer_staff"
    if "is_premium" in out:
        out["is_premium"] = bool(out["is_premium"])
    return out


def link_user_google_sub(user_id: int, google_sub: str) -> bool:
    sub = (google_sub or "").strip()
    if not sub:
        return False
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid <= 0:
        return False
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    if "google_sub" not in cols:
        conn.close()
        return False
    cursor.execute("SELECT google_sub FROM users WHERE id = ?", (uid,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return False
    existing = (row[0] or "").strip()
    if existing and existing != sub:
        conn.close()
        return False
    if existing == sub:
        conn.close()
        return True
    cursor.execute(
        "UPDATE users SET google_sub = ? WHERE id = ? AND (google_sub IS NULL OR google_sub = '')",
        (sub, uid),
    )
    ok = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return bool(ok)


def get_user_by_apple_sub(apple_sub: str) -> dict | None:
    sub = (apple_sub or "").strip()
    if not sub:
        return None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = [r[1] for r in cursor.fetchall()]
    if "apple_sub" not in cols:
        conn.close()
        return None
    want = ["id", "username", "email", "apple_sub"]
    for extra in ("role", "org_id", "is_premium"):
        if extra in cols:
            want.append(extra)
    cursor.execute(
        f"SELECT {', '.join(want)} FROM users WHERE apple_sub = ? LIMIT 1",
        (sub,),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    out: dict = {}
    for i, k in enumerate(want):
        out[k] = row[i]
    out["id"] = int(out["id"])
    if "role" not in out or out["role"] is None:
        out["role"] = "dealer_staff"
    if "is_premium" in out:
        out["is_premium"] = bool(out["is_premium"])
    return out


def link_user_apple_sub(user_id: int, apple_sub: str) -> bool:
    sub = (apple_sub or "").strip()
    if not sub:
        return False
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid <= 0:
        return False
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    if "apple_sub" not in cols:
        conn.close()
        return False
    cursor.execute("SELECT apple_sub FROM users WHERE id = ?", (uid,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return False
    existing = (row[0] or "").strip()
    if existing and existing != sub:
        conn.close()
        return False
    if existing == sub:
        conn.close()
        return True
    cursor.execute(
        "UPDATE users SET apple_sub = ? WHERE id = ? AND (apple_sub IS NULL OR apple_sub = '')",
        (sub, uid),
    )
    ok = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return bool(ok)


def user_exists_by_username(username: str) -> bool:
    u = (username or "").strip()
    if not u:
        return False
    conn = get_conn()
    row = conn.execute(
        "SELECT 1 FROM users WHERE lower(username) = lower(?) LIMIT 1",
        (u,),
    ).fetchone()
    conn.close()
    return row is not None


def save_oauth_user(
    username: str,
    email: str,
    google_sub: str,
    *,
    role: str = "dealer_staff",
    org_id: int | None = None,
) -> int:
    """Create an app user authenticated via Google (random password hash, not usable for login)."""
    password_h = hash_password(secrets.token_urlsafe(48))
    username = (username or "").strip()
    email = (email or "").strip().lower()
    sub = (google_sub or "").strip()
    if not sub:
        raise ValueError("google_sub required")
    role_val = (role or "dealer_staff").strip().lower()
    last_ex: Exception | None = None
    for attempt in range(6):
        conn: sqlite3.Connection | None = None
        try:
            conn = get_conn()
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(users)")
            cols = {r[1] for r in cursor.fetchall()}
            if "google_sub" not in cols:
                raise sqlite3.OperationalError("google_sub column missing")
            fields = ["username", "email", "password", "google_sub"]
            values: list[object] = [username, email, password_h, sub]
            if "role" in cols:
                fields.append("role")
                values.append(role_val)
            if "org_id" in cols and org_id is not None:
                fields.append("org_id")
                values.append(int(org_id))
            placeholders = ", ".join("?" for _ in fields)
            cursor.execute(
                f"INSERT INTO users ({', '.join(fields)}) VALUES ({placeholders})",
                tuple(values),
            )
            uid = int(cursor.lastrowid)
            conn.commit()
            conn.close()
            return uid
        except sqlite3.OperationalError as ex:
            last_ex = ex
            if "locked" not in str(ex).lower():
                if conn:
                    try:
                        conn.close()
                    except sqlite3.Error:
                        pass
                raise
            if conn:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
            if attempt >= 5:
                break
            time.sleep(0.1 * (attempt + 1))
    if last_ex:
        raise last_ex
    raise RuntimeError("save_oauth_user failed")


def save_apple_oauth_user(
    username: str,
    email: str,
    apple_sub: str,
    *,
    role: str = "dealer_staff",
    org_id: int | None = None,
) -> int:
    """Create an app user authenticated via Apple (random password hash)."""
    password_h = hash_password(secrets.token_urlsafe(48))
    username = (username or "").strip()
    email = (email or "").strip().lower()
    sub = (apple_sub or "").strip()
    if not sub:
        raise ValueError("apple_sub required")
    role_val = (role or "dealer_staff").strip().lower()
    last_ex: Exception | None = None
    for attempt in range(6):
        conn: sqlite3.Connection | None = None
        try:
            conn = get_conn()
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(users)")
            cols = {r[1] for r in cursor.fetchall()}
            if "apple_sub" not in cols:
                raise sqlite3.OperationalError("apple_sub column missing")
            fields = ["username", "email", "password", "apple_sub"]
            values: list[object] = [username, email, password_h, sub]
            if "role" in cols:
                fields.append("role")
                values.append(role_val)
            if "org_id" in cols and org_id is not None:
                fields.append("org_id")
                values.append(int(org_id))
            placeholders = ", ".join("?" for _ in fields)
            cursor.execute(
                f"INSERT INTO users ({', '.join(fields)}) VALUES ({placeholders})",
                tuple(values),
            )
            uid = int(cursor.lastrowid)
            conn.commit()
            conn.close()
            return uid
        except sqlite3.OperationalError as ex:
            last_ex = ex
            if "locked" not in str(ex).lower():
                if conn:
                    try:
                        conn.close()
                    except sqlite3.Error:
                        pass
                raise
            if conn:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
            if attempt >= 5:
                break
            time.sleep(0.1 * (attempt + 1))
    if last_ex:
        raise last_ex
    raise RuntimeError("save_apple_oauth_user failed")


def save_user(username, email, password, *, role: str = "dealer_staff", org_id: int | None = None) -> int:
    # Hash before opening the DB to avoid holding a SQLite connection during bcrypt.
    password_h = hash_password(password)
    username = (username or "").strip()
    email = (email or "").strip().lower()
    role_val = (role or "dealer_staff").strip().lower()
    last_ex: Exception | None = None
    for attempt in range(6):
        conn: sqlite3.Connection | None = None
        try:
            conn = get_conn()
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(users)")
            has_role = any(r[1] == "role" for r in cursor.fetchall())
            cursor.execute("PRAGMA table_info(users)")
            cols = {r[1] for r in cursor.fetchall()}
            if has_role and "org_id" in cols:
                cursor.execute(
                    "INSERT INTO users (username, email, password, role, org_id) VALUES (?, ?, ?, ?, ?)",
                    (username, email, password_h, role_val, int(org_id) if org_id else None),
                )
            elif has_role:
                cursor.execute(
                    "INSERT INTO users (username, email, password, role) VALUES (?, ?, ?, ?)",
                    (username, email, password_h, role_val),
                )
            else:
                cursor.execute(
                    "INSERT INTO users (username, email, password) VALUES (?, ?, ?)",
                    (username, email, password_h),
                )
            uid = int(cursor.lastrowid)
            conn.commit()
            conn.close()
            return uid
        except sqlite3.OperationalError as ex:
            last_ex = ex
            if "locked" not in str(ex).lower():
                if conn:
                    try:
                        conn.close()
                    except sqlite3.Error:
                        pass
                raise
            if conn:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
            if attempt >= 5:
                break
            time.sleep(0.1 * (attempt + 1))
    if last_ex:
        raise last_ex
    raise RuntimeError("save_user failed after retries")


def delete_user_by_email(email: str) -> bool:
    """
    Remove an app user by email (lowercased match) and dependent rows.
    Also deletes their dealer vehicle rows; removes empty orgs left behind.
    """
    e = (email or "").strip().lower()
    if not e or "@" not in e:
        return False
    from backend.db import dealer_portal_db as ddb

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    ucols = {r[1] for r in cursor.fetchall()}
    if "org_id" in ucols:
        cursor.execute("SELECT id, org_id FROM users WHERE lower(email) = ? LIMIT 1", (e,))
    else:
        cursor.execute("SELECT id FROM users WHERE lower(email) = ? LIMIT 1", (e,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return False
    uid = int(row[0])
    org_id = int(row[1]) if len(row) > 1 and row[1] is not None else None
    ddb.delete_vehicles_for_user(uid)
    try:
        cursor.execute("DELETE FROM org_invites WHERE used_by_user_id = ?", (uid,))
    except sqlite3.Error:
        pass
    for attempt in range(8):
        try:
            try:
                conn.execute("PRAGMA busy_timeout=10000")
            except sqlite3.Error:
                pass
            cursor.execute("DELETE FROM users WHERE id = ?", (uid,))
            if cursor.rowcount < 1:
                conn.close()
                return False
            conn.commit()
            conn.close()
            break
        except sqlite3.OperationalError as ex:
            if "locked" not in str(ex).lower() or attempt >= 7:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
                return False
            time.sleep(0.15 * (attempt + 1))
    else:
        try:
            conn.close()
        except sqlite3.Error:
            pass
        return False
    if org_id and org_id > 0:
        conn2 = get_conn()
        c2 = conn2.cursor()
        c2.execute("SELECT COUNT(*) FROM users WHERE org_id = ?", (org_id,))
        n = int(c2.fetchone()[0] or 0)
        if n == 0:
            try:
                c2.execute("DELETE FROM orgs WHERE id = ?", (org_id,))
            except sqlite3.Error:
                pass
        conn2.commit()
        conn2.close()
    return True


def grant_user_premium(
    user_id: int,
    *,
    customer_id: str | None = None,
    session_id: str | None = None,
    subscription_id: str | None = None,
    plan_id: str | None = None,
) -> None:
    """Set is_premium=1 and store Stripe identifiers for a user."""
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return
    pid = (plan_id or "").strip().lower() or None
    conn = get_conn()
    c = conn.cursor()
    c.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in c.fetchall()}
    if pid and "subscription_plan_id" in cols:
        c.execute(
            """
            UPDATE users
            SET is_premium = 1,
                subscription_plan_id = ?,
                premium_stripe_customer_id = COALESCE(?, premium_stripe_customer_id),
                premium_stripe_session_id = COALESCE(?, premium_stripe_session_id),
                premium_stripe_subscription_id = COALESCE(?, premium_stripe_subscription_id)
            WHERE id = ?
            """,
            (pid, customer_id or None, session_id or None, subscription_id or None, uid),
        )
    else:
        c.execute(
            """
            UPDATE users
            SET is_premium = 1,
                premium_stripe_customer_id = COALESCE(?, premium_stripe_customer_id),
                premium_stripe_session_id = COALESCE(?, premium_stripe_session_id),
                premium_stripe_subscription_id = COALESCE(?, premium_stripe_subscription_id)
            WHERE id = ?
            """,
            (customer_id or None, session_id or None, subscription_id or None, uid),
        )
    conn.commit()
    conn.close()


def revoke_user_premium(user_id: int) -> None:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return
    conn = get_conn()
    c = conn.cursor()
    c.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in c.fetchall()}
    if "subscription_plan_id" in cols:
        c.execute(
            "UPDATE users SET is_premium = 0, subscription_plan_id = NULL WHERE id = ?",
            (uid,),
        )
    else:
        c.execute("UPDATE users SET is_premium = 0 WHERE id = ?", (uid,))
    conn.commit()
    conn.close()


def get_user_premium_status(user_id: int) -> bool:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    conn = get_conn()
    row = conn.execute("SELECT is_premium FROM users WHERE id = ?", (uid,)).fetchone()
    conn.close()
    return bool(row and row[0])


def get_user_billing_snapshot(user_id: int) -> dict | None:
    """Billing fields for account UI (no secrets)."""
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return None
    if uid <= 0:
        return None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    want = ["id", "is_premium"]
    for extra in (
        "subscription_plan_id",
        "premium_stripe_customer_id",
        "premium_stripe_subscription_id",
    ):
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
    out["is_premium"] = bool(out.get("is_premium"))
    return out


def _saved_car_counts_for_users(user_ids: list[int]) -> dict[int, int]:
    if not user_ids:
        return {}
    try:
        from backend.db import inventory_db

        placeholders = ",".join("?" * len(user_ids))
        with inventory_db.db_conn() as conn:
            rows = conn.execute(
                f"SELECT user_id, COUNT(*) AS n FROM saved_cars "
                f"WHERE user_id IN ({placeholders}) GROUP BY user_id",
                tuple(int(i) for i in user_ids),
            ).fetchall()
        return {int(r[0]): int(r[1]) for r in rows}
    except Exception:
        return {}


def _org_names_for_users(org_ids: list[int]) -> dict[int, str]:
    ids = sorted({int(i) for i in org_ids if i})
    if not ids:
        return {}
    conn = get_conn()
    cursor = conn.cursor()
    placeholders = ",".join("?" * len(ids))
    cursor.execute(
        f"SELECT id, name FROM orgs WHERE id IN ({placeholders})",
        tuple(ids),
    )
    rows = cursor.fetchall()
    conn.close()
    return {int(r[0]): str(r[1] or "") for r in rows}


def _admin_login_methods(row: dict) -> str:
    methods: list[str] = []
    if (row.get("google_sub") or "").strip():
        methods.append("Google")
    if (row.get("apple_sub") or "").strip():
        methods.append("Apple")
    if not methods:
        methods.append("Email")
    return " · ".join(methods)


def _admin_scope_label(row: dict, org_names: dict[int, str]) -> str:
    dealer_id = (row.get("dealer_id") or "").strip()
    if dealer_id:
        return f"Dealer {dealer_id}"
    reg = row.get("dealership_registry_id")
    if reg is not None and str(reg).strip():
        return f"Registry #{int(reg)}"
    org_id = row.get("org_id")
    if org_id is not None:
        try:
            oid = int(org_id)
        except (TypeError, ValueError):
            oid = 0
        if oid > 0:
            name = (org_names.get(oid) or "").strip()
            return name or f"Org #{oid}"
    return "—"


def list_users_for_admin(*, search: str = "", limit: int = 100) -> list[dict]:
    """Site-admin user list (no password hashes)."""
    lim = max(1, min(int(limit or 100), 500))
    q = (search or "").strip().lower()
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    colset = {r[1] for r in cursor.fetchall()}
    want = ["id", "username", "email", "role"]
    for extra in (
        "is_premium",
        "subscription_plan_id",
        "email_verified_at",
        "is_active",
        "dealer_id",
        "dealership_registry_id",
        "org_id",
        "google_sub",
        "apple_sub",
        "totp_enabled",
        "mfa_method",
        "premium_stripe_customer_id",
        "premium_stripe_subscription_id",
        "created_at",
    ):
        if extra in colset:
            want.append(extra)
    sql = f"SELECT {', '.join(want)} FROM users"
    params: list[object] = []
    if q:
        sql += " WHERE lower(username) LIKE ? OR lower(email) LIKE ?"
        like = f"%{q}%"
        params.extend([like, like])
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(lim)
    cursor.execute(sql, tuple(params))
    rows = cursor.fetchall()
    conn.close()
    out: list[dict] = []
    for row in rows:
        item: dict = {}
        for i, k in enumerate(want):
            item[k] = row[i]
        item["id"] = int(item["id"])
        if "is_premium" in item:
            item["is_premium"] = bool(item["is_premium"])
        if "is_active" in item:
            item["is_active"] = bool(item["is_active"])
        else:
            item["is_active"] = True
        if "totp_enabled" in item:
            item["totp_enabled"] = bool(item["totp_enabled"])
        out.append(item)

    user_ids = [int(u["id"]) for u in out]
    saved_counts = _saved_car_counts_for_users(user_ids)
    org_ids: list[int] = []
    for u in out:
        oid = u.get("org_id")
        if oid is not None:
            try:
                org_ids.append(int(oid))
            except (TypeError, ValueError):
                pass
    org_names = _org_names_for_users(org_ids)

    for item in out:
        uid = int(item["id"])
        plan_id = (item.get("subscription_plan_id") or "").strip().lower()
        if not plan_id:
            plan_id = "complete" if item.get("is_premium") else "free"
        item["plan_label"] = plan_id
        item["login_methods"] = _admin_login_methods(item)
        item["email_verified"] = bool((item.get("email_verified_at") or "").strip())
        item["stripe_linked"] = bool((item.get("premium_stripe_customer_id") or "").strip())
        item["stripe_subscribed"] = bool(
            (item.get("premium_stripe_subscription_id") or "").strip()
        )
        item["saved_count"] = int(saved_counts.get(uid, 0))
        item["scope_label"] = _admin_scope_label(item, org_names)
        mfa_method = (item.get("mfa_method") or "").strip().lower()
        if item.get("totp_enabled"):
            item["mfa_label"] = (mfa_method or "totp").upper()
        elif mfa_method:
            item["mfa_label"] = mfa_method.upper()
        else:
            item["mfa_label"] = "—"
        # Drop OAuth subject ids from API surface (login_methods is enough).
        item.pop("google_sub", None)
        item.pop("apple_sub", None)
        item.pop("premium_stripe_customer_id", None)
        item.pop("premium_stripe_subscription_id", None)

    return out


ADMIN_ASSIGNABLE_ROLES: tuple[str, ...] = (
    "admin",
    "general_user",
    "dealership_owner",
    "dealership_admin",
    "dealership_member",
    "dealer_staff",
)


def admin_role_is_assignable(role: str | None) -> bool:
    return (role or "").strip().lower() in ADMIN_ASSIGNABLE_ROLES


def get_user_admin_record(user_id: int) -> dict | None:
    """Load a user row for site-admin edit forms (no secrets)."""
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return None
    if uid <= 0:
        return None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    colset = {r[1] for r in cursor.fetchall()}
    want = ["id", "username", "email", "role"]
    for extra in (
        "is_active",
        "dealer_id",
        "dealership_registry_id",
        "org_id",
        "google_sub",
        "apple_sub",
        "email_verified_at",
    ):
        if extra in colset:
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
    if "is_active" in out:
        out["is_active"] = bool(out["is_active"])
    else:
        out["is_active"] = True
    out["has_oauth"] = bool((out.get("google_sub") or "").strip() or (out.get("apple_sub") or "").strip())
    out.pop("google_sub", None)
    out.pop("apple_sub", None)
    return out


def _user_has_env_admin_privilege_by_id(user_id: int) -> bool:
    from backend.utils.roles import account_has_env_admin_privilege

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT email, username FROM users WHERE id = ?", (int(user_id),))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return False
    return account_has_env_admin_privilege(str(row[0] or ""), str(row[1] or ""))


def admin_create_user(
    *,
    username: str,
    email: str,
    password: str,
    role: str,
    dealer_id: str | None = None,
    dealership_registry_id: int | None = None,
    min_password_len: int = 8,
) -> tuple[int | None, str | None]:
    from backend.utils.registration_validation import registration_form_error
    from backend.utils.roles import REGISTRATION_ADMIN_BLOCKED_MSG, registration_blocked_by_env_admin

    err = registration_form_error(username, email, password, min_password_len=min_password_len)
    if err:
        return None, err
    if registration_blocked_by_env_admin(email, username):
        return None, REGISTRATION_ADMIN_BLOCKED_MSG
    role_val = (role or "general_user").strip().lower()
    if not admin_role_is_assignable(role_val):
        return None, "Invalid role."
    try:
        uid = save_user(username, email, password, role=role_val)
    except Exception:
        return None, "Could not create user (username or email may already exist)."
    scope_err = admin_update_user_scope(
        uid,
        dealer_id=dealer_id,
        dealership_registry_id=dealership_registry_id,
    )
    if scope_err:
        delete_user_by_id(uid, allow_env_admin=True)
        return None, scope_err
    return uid, None


def admin_update_user_scope(
    user_id: int,
    *,
    dealer_id: str | None = None,
    dealership_registry_id: int | None = None,
) -> str | None:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return "Invalid user."
    if uid <= 0:
        return "Invalid user."
    dealer_val = (dealer_id or "").strip() or None
    reg_val: int | None = None
    if dealership_registry_id is not None and str(dealership_registry_id).strip() != "":
        try:
            reg_val = int(dealership_registry_id)
        except (TypeError, ValueError):
            return "Dealership registry id must be a number."
        if reg_val <= 0:
            reg_val = None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    sets: list[str] = []
    params: list[object] = []
    if "dealer_id" in cols:
        sets.append("dealer_id = ?")
        params.append(dealer_val)
    if "dealership_registry_id" in cols:
        sets.append("dealership_registry_id = ?")
        params.append(reg_val)
    if not sets:
        conn.close()
        return None
    params.append(uid)
    cursor.execute(f"UPDATE users SET {', '.join(sets)} WHERE id = ?", tuple(params))
    if cursor.rowcount < 1:
        conn.close()
        return "User not found."
    conn.commit()
    conn.close()
    return None


def admin_update_user(
    user_id: int,
    *,
    username: str,
    email: str,
    role: str,
    dealer_id: str | None = None,
    dealership_registry_id: int | None = None,
    actor_user_id: int,
) -> str | None:
    from backend.utils.registration_validation import (
        MAX_EMAIL_LEN,
        MAX_USERNAME_LEN,
        normalize_registration_email,
        normalize_registration_username,
    )
    from backend.utils.roles import ROLE_ADMIN, registration_blocked_by_env_admin

    try:
        uid = int(user_id)
        actor = int(actor_user_id)
    except (TypeError, ValueError):
        return "Invalid user."
    if uid <= 0:
        return "Invalid user."

    uname = normalize_registration_username(username)
    em = normalize_registration_email(email)
    if len(uname) < 2:
        return "Username must be at least 2 characters."
    if len(uname) > MAX_USERNAME_LEN:
        return f"Username must be at most {MAX_USERNAME_LEN} characters."
    if len(em) < 3 or "@" not in em:
        return "Enter a valid email address."
    if len(em) > MAX_EMAIL_LEN:
        return f"Email must be at most {MAX_EMAIL_LEN} characters."

    role_val = (role or "general_user").strip().lower()
    if not admin_role_is_assignable(role_val):
        return "Invalid role."

    env_priv = _user_has_env_admin_privilege_by_id(uid)
    if env_priv and role_val != ROLE_ADMIN:
        return "Env-configured site admins must keep the admin role."
    if registration_blocked_by_env_admin(em, uname) and role_val == ROLE_ADMIN and not env_priv:
        return "This email or username is reserved for env-configured admins only."

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id FROM users WHERE lower(username) = lower(?) AND id != ?",
        (uname, uid),
    )
    if cursor.fetchone():
        conn.close()
        return "That username is already taken."
    cursor.execute(
        "SELECT id FROM users WHERE lower(email) = lower(?) AND id != ?",
        (em, uid),
    )
    if cursor.fetchone():
        conn.close()
        return "That email is already registered."

    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    sets = ["username = ?", "email = ?"]
    params: list[object] = [uname, em]
    if "role" in cols:
        sets.append("role = ?")
        params.append(role_val)
    params.append(uid)
    cursor.execute(f"UPDATE users SET {', '.join(sets)} WHERE id = ?", tuple(params))
    if cursor.rowcount < 1:
        conn.close()
        return "User not found."
    conn.commit()
    conn.close()

    scope_err = admin_update_user_scope(
        uid,
        dealer_id=dealer_id,
        dealership_registry_id=dealership_registry_id,
    )
    if scope_err:
        return scope_err
    if env_priv:
        sync_env_admin_user_row(uid)
    return None


def admin_reset_user_password(
    user_id: int,
    new_password: str,
    *,
    min_password_len: int = 8,
) -> str | None:
    from backend.utils.registration_validation import MAX_PASSWORD_LEN

    new_pw = (new_password or "").strip()
    if len(new_pw) < min_password_len:
        return f"Password must be at least {min_password_len} characters."
    if len(new_pw) > MAX_PASSWORD_LEN:
        return f"Password must be at most {MAX_PASSWORD_LEN} characters."
    err = reset_user_password(int(user_id), new_pw)
    if err:
        return err
    clear_user_password_reset_token(int(user_id))
    return None


def delete_user_by_id(user_id: int, *, allow_env_admin: bool = False) -> tuple[bool, str | None]:
    """Delete user by id. Returns (ok, error_message)."""
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False, "Invalid user."
    if uid <= 0:
        return False, "Invalid user."
    if not allow_env_admin and _user_has_env_admin_privilege_by_id(uid):
        return False, "Cannot delete env-configured site admin accounts."
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT email FROM users WHERE id = ?", (uid,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return False, "User not found."
    email = str(row[0] or "").strip()
    if not email:
        return False, "User not found."
    if delete_user_by_email(email):
        return True, None
    return False, "Could not delete user."


def set_user_is_active(user_id: int, *, active: bool) -> bool:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid <= 0:
        return False
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cursor.fetchall()}
    if "is_active" not in cols:
        conn.close()
        return False
    cursor.execute(
        "UPDATE users SET is_active = ? WHERE id = ?",
        (1 if active else 0, uid),
    )
    ok = cursor.rowcount > 0
    conn.commit()
    conn.close()
    if ok:
        _users_select_columns.cache_clear()
    return bool(ok)
