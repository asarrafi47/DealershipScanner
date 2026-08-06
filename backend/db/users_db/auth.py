from backend.db.password_hash import hash_password, password_needs_rehash, verify_or_legacy

from ._common import (
    _schedule_password_rehash,
    _user_dict_from_row,
    _user_row_is_active,
    _users_select_columns,
    get_conn,
)


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
