#!/usr/bin/env python3
"""Local bootstrap: ensure site admin user, promote APP_ADMIN_* accounts, sync password."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.db.password_hash import hash_password
from backend.db.users_db import get_conn, init_users_db
from backend.utils.roles import ROLE_ADMIN, admin_emails, admin_usernames
from backend.utils.runtime_env import is_production_env


def _primary_admin_identity() -> tuple[str, str]:
    unames = sorted(admin_usernames())
    emails = sorted(admin_emails())
    username = unames[0] if unames else "asarrafi"
    email = emails[0] if emails else f"{username}@sarraficars.com"
    return username, email


def _ensure_admin_user(username: str, email: str, pw_plain: str | None) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, username FROM users WHERE lower(username) = lower(?)",
        (username,),
    )
    row = cur.fetchone()
    if row:
        cur.execute(
            "UPDATE users SET role = ?, email = ?, totp_enabled = 0 WHERE id = ?",
            (ROLE_ADMIN, email, int(row[0])),
        )
        conn.commit()
        conn.close()
        return

    cur.execute(
        "SELECT id FROM users WHERE lower(username) = lower(?)",
        ("ksarrafi",),
    )
    legacy = cur.fetchone()
    if legacy:
        cur.execute(
            """
            UPDATE users
            SET username = ?, email = ?, role = ?, totp_enabled = 0
            WHERE id = ?
            """,
            (username, email, ROLE_ADMIN, int(legacy[0])),
        )
        conn.commit()
        conn.close()
        print(f"bootstrap_site_admin: renamed ksarrafi -> {username}")
        return

    if not pw_plain:
        conn.close()
        return

    cur.execute(
        """
        INSERT INTO users (username, email, password, role, totp_enabled)
        VALUES (?, ?, ?, ?, 0)
        """,
        (username, email, hash_password(pw_plain), ROLE_ADMIN),
    )
    conn.commit()
    conn.close()
    print(f"bootstrap_site_admin: created admin user {username}")


def _reset_passwords(pw_plain: str) -> list[str]:
    targets: set[str] = set()
    for u in admin_usernames():
        targets.add(u.lower())
    for e in admin_emails():
        local = e.split("@", 1)[0].strip().lower()
        if local:
            targets.add(local)

    if not targets:
        return []

    pw_hash = hash_password(pw_plain)
    conn = get_conn()
    cur = conn.cursor()
    updated: list[str] = []
    for uname in sorted(targets):
        cur.execute(
            "UPDATE users SET password = ? WHERE lower(username) = lower(?)",
            (pw_hash, uname),
        )
        if cur.rowcount:
            updated.append(uname)
    conn.commit()
    conn.close()
    return updated


def main() -> int:
    if not is_production_env():
        os.environ.setdefault("ALLOW_UNENCRYPTED_USER_DB", "1")
    init_users_db()

    username, email = _primary_admin_identity()
    pw = (os.environ.get("ADMIN_PASSWORD") or "").strip()
    _ensure_admin_user(username, email, pw or None)
    if username.lower() == "asarrafi":
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET role = 'general_user' WHERE lower(username) = 'ksarrafi'"
        )
        conn.commit()
        conn.close()

    if not pw:
        print("bootstrap_site_admin: no ADMIN_PASSWORD — promoted env admins only")
        return 0

    updated = _reset_passwords(pw)
    if updated:
        print("bootstrap_site_admin: password synced for", ", ".join(updated))
    else:
        print("bootstrap_site_admin: admin user ready; password unchanged")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
