#!/usr/bin/env python3
"""
Print summary of app login accounts (users.db) — no passwords.

Use when debugging "cannot log in": confirm the DB path, that your username exists,
and whether APP_ADMIN_EMAILS / APP_ADMIN_USERNAMES match (operators set those in .env only).

Run from repo root:

  python -m backend.scripts.app_users_status
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path


def main() -> int:
    repo = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo))
    os.chdir(repo)

    try:
        from backend.utils.project_env import load_project_dotenv

        load_project_dotenv()
    except ImportError:
        pass

    from backend.utils.roles import admin_emails, admin_usernames

    raw_path = os.environ.get("USERS_DB_PATH", "users.db")
    db_path = Path(raw_path).expanduser().resolve()
    print(f"USERS_DB_PATH → {db_path} (exists={db_path.is_file()})")
    print(f"APP_ADMIN_EMAILS (parsed): {sorted(admin_emails()) or '(none)'}")
    print(f"APP_ADMIN_USERNAMES (parsed): {sorted(admin_usernames()) or '(none)'}")
    print(f"ALLOW_DEFAULT_APP_USER: {os.environ.get('ALLOW_DEFAULT_APP_USER', 'not set')}")
    print()

    if not db_path.is_file():
        print("No database file — register at /register first, or fix USERS_DB_PATH.")
        return 1

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info(users)")
        cols = {r[1] for r in cur.fetchall()}
        want = ["id", "username", "email"]
        for c in ("role", "totp_enabled"):
            if c in cols:
                want.append(c)
        cur.execute(f"SELECT {', '.join(want)} FROM users ORDER BY id")
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        print("users table is empty — create an account at /register (app login), not only /dev/login.")
        return 0

    print(f"{'id':>4}  {'username':<24}  {'email':<36}  {'role':<18}  totp_on")
    print("-" * 110)
    for r in rows:
        d = dict(r)
        rid = d.get("id", "")
        un = (d.get("username") or "")[:24]
        em = (d.get("email") or "")[:36]
        role = (d.get("role") or "")[:18]
        te = d.get("totp_enabled")
        totp = "?" if te is None else ("yes" if int(te) else "no")
        print(f"{rid!s:>4}  {un:<24}  {em:<36}  {role:<18}  {totp}")
    print()
    print(
        "Tip: Main app login uses users.db. /dev/login uses dev_users.db — different accounts.\n"
        "Admin + no MFA: add your email to APP_ADMIN_EMAILS or username to APP_ADMIN_USERNAMES in .env, restart run.py."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
