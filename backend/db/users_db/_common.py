import sqlite3
import threading
import time
from functools import lru_cache

from backend.db.password_hash import hash_password
from backend.db.users_sqlite import DB_PATH, get_users_conn

__all__ = [
    "DB_PATH",
    "get_users_conn",
    "get_conn",
    "_users_select_columns",
    "_user_dict_from_row",
    "_user_row_is_active",
    "_schedule_password_rehash",
]


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
