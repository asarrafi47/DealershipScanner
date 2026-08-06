from ._common import _users_select_columns, get_conn
from .accounts import delete_user_by_email, save_user
from .auth import clear_user_password_reset_token, reset_user_password
from .schema import sync_env_admin_user_row


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
        int(actor_user_id)
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
