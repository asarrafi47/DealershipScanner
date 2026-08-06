import os
import sqlite3

from backend.db.password_hash import hash_password, verify_password
from backend.utils.roles import ROLE_ADMIN
from backend.utils.runtime_env import is_production_env

from ._common import _users_select_columns, get_conn


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
    # The column-level UNIQUE on email/username is case-SENSITIVE while login
    # matches case-insensitively, so Foo@x.com and foo@x.com could coexist as
    # separate accounts. Enforce uniqueness on the folded value too. If a legacy
    # DB already holds such duplicates the index cannot be built — log which
    # rows collide instead of deleting accounts.
    for idx_name, expr in (
        ("idx_users_email_ci", "lower(email)"),
        ("idx_users_username_ci", "lower(username)"),
    ):
        try:
            cursor.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {idx_name} ON users({expr})"
            )
        except sqlite3.IntegrityError:
            cursor.execute(
                f"SELECT {expr}, COUNT(*) FROM users GROUP BY {expr} HAVING COUNT(*) > 1"
            )
            dupes = cursor.fetchall()
            print(
                f"WARNING: cannot create {idx_name}: duplicate accounts differ only "
                f"by case and must be merged manually: {dupes}"
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
                # Detect the weak default directly: verify_or_legacy no longer
                # accepts plaintext without an explicit opt-in, but this row
                # must be purged regardless of that gate.
                is_legacy_default = stored == "password" or bool(
                    verify_password("password", stored)
                )
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

    # Promote env-listed admin accounts; in development, creation of missing
    # ones happens inside _apply_env_admin_privileges and only with
    # ALLOW_DEFAULT_APP_USER=1 (honoring ADMIN_PASSWORD). No ungated seeding of
    # a fixed default password.
    if not is_production_env():
        try:
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
