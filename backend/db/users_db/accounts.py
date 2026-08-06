import secrets
import sqlite3
import time

from backend.db.password_hash import hash_password

from ._common import get_conn


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
