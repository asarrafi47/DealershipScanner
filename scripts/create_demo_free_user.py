#!/usr/bin/env python3
"""Create or reset the shared free-tier demo account (general_user, not premium).

Run while the app is stopped, or if users.db is not locked by a running server:

  python scripts/create_demo_free_user.py

Login at /login with the credentials printed below.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

USERNAME = "demo_free"
EMAIL = "demo_free@example.com"
PASSWORD = "Demo-Free-Test-9!"


def main() -> int:
    from backend.db.password_hash import hash_password
    from backend.db.users_db import get_user_by_login, init_users_db, save_user
    from backend.utils.roles import ROLE_GENERAL

    init_users_db()
    existing = get_user_by_login(EMAIL) or get_user_by_login(USERNAME)
    if existing:
        import sqlite3
        from backend.db.users_sqlite import users_db_path

        pw = hash_password(PASSWORD)
        conn = sqlite3.connect(users_db_path(), timeout=30)
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("UPDATE users SET password = ?, role = ?, is_premium = 0 WHERE id = ?", (
            pw,
            ROLE_GENERAL,
            int(existing["id"]),
        ))
        conn.commit()
        conn.close()
        print(f"Reset password for existing account id={existing['id']}")
    else:
        uid = save_user(USERNAME, EMAIL, PASSWORD, role=ROLE_GENERAL, org_id=None)
        print(f"Created account id={uid}")

    print()
    print("Free (regular) demo account:")
    print(f"  Username: {USERNAME}")
    print(f"  Email:    {EMAIL}")
    print(f"  Password: {PASSWORD}")
    print(f"  Plan:     Free (not premium)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
