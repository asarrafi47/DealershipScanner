import os
import secrets
import sqlite3
import time

from backend.db.password_hash import hash_password, verify_or_legacy
from backend.db.users_sqlite import DB_PATH, get_users_conn
from backend.utils.roles import ROLE_ADMIN
from backend.utils.runtime_env import is_production_env


def get_conn():
    return get_users_conn()


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
    ):
        if col not in ucols:
            cursor.execute(ddl)
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_google_sub "
        "ON users(google_sub) WHERE google_sub IS NOT NULL AND google_sub != ''"
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
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users)")
    cols = [r[1] for r in cursor.fetchall()]
    want = ["id", "username", "email"]
    for extra in (
        "role",
        "dealer_id",
        "dealership_registry_id",
        "org_id",
        "totp_enabled",
        "totp_secret",
        "mfa_phone",
        "is_premium",
        "google_sub",
    ):
        if extra in cols:
            want.append(extra)
    cursor.execute(
        f"SELECT {', '.join(want)} FROM users WHERE lower(username) = lower(?) OR lower(email) = lower(?)",
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
    if "totp_enabled" in out:
        out["totp_enabled"] = bool(out["totp_enabled"])
    if "totp_secret" in out and out["totp_secret"] is None:
        out["totp_secret"] = ""
    if "mfa_phone" in out and out["mfa_phone"] is None:
        out["mfa_phone"] = ""
    if "is_premium" in out:
        out["is_premium"] = bool(out["is_premium"])
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
    for extra in ("role", "dealer_id", "dealership_registry_id", "org_id", "mfa_phone", "is_premium"):
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
    cursor.execute(
        """
        SELECT id, username, email, password FROM users
        WHERE lower(username) = lower(?) OR lower(email) = lower(?)
        """,
        (li, li),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return False
    uid, _u, _e, stored = row
    ok = verify_or_legacy(password, stored)
    if ok and not stored.startswith("$2"):
        h = hash_password(password)
        for attempt in range(6):
            try:
                conn2 = get_conn()
                cur2 = conn2.cursor()
                cur2.execute(
                    "UPDATE users SET password = ? WHERE id = ?",
                    (h, uid),
                )
                conn2.commit()
                conn2.close()
                break
            except sqlite3.OperationalError as ex:
                if "locked" not in str(ex).lower() or attempt >= 5:
                    raise
                time.sleep(0.08 * (attempt + 1))
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
) -> None:
    """Set is_premium=1 and store Stripe identifiers for a user."""
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return
    conn = get_conn()
    c = conn.cursor()
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
    conn.execute("UPDATE users SET is_premium = 0 WHERE id = ?", (uid,))
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
