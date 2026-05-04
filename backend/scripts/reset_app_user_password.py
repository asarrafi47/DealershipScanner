#!/usr/bin/env python3
"""
Reset password for a user in users.db (recovery only).

Requires ALLOW_LOCAL_PASSWORD_RESET=1 in the environment (not production-friendly).

Usage (repo root):

  ALLOW_LOCAL_PASSWORD_RESET=1 python -m backend.scripts.reset_app_user_password --login asarrafi

Password is read from stdin (hidden) unless --password-env VAR reads from that env var.
"""

from __future__ import annotations

import argparse
import getpass
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

    if (os.environ.get("ALLOW_LOCAL_PASSWORD_RESET") or "").strip().lower() not in (
        "1",
        "true",
        "yes",
        "on",
    ):
        print(
            "Refusing to run: set ALLOW_LOCAL_PASSWORD_RESET=1 for this one-shot recovery.",
            file=sys.stderr,
        )
        return 2

    from backend.db.password_hash import hash_password
    from backend.utils.runtime_env import is_production_env

    if is_production_env():
        print("Refusing to run when FLASK_ENV=production.", file=sys.stderr)
        return 2

    ap = argparse.ArgumentParser(description="Reset users.db password for app login.")
    ap.add_argument("--login", required=True, help="Username or email stored in users.db")
    ap.add_argument(
        "--password-env",
        metavar="VAR",
        default=None,
        help="Read new password from this environment variable (avoid shell history)",
    )
    args = ap.parse_args()

    raw_path = os.environ.get("USERS_DB_PATH", "users.db")
    db_path = Path(raw_path).expanduser().resolve()
    if not db_path.is_file():
        print(f"No database at {db_path}", file=sys.stderr)
        return 1

    if args.password_env:
        pw = (os.environ.get(args.password_env) or "").strip()
        if not pw:
            print(f"Env {args.password_env!r} is empty.", file=sys.stderr)
            return 1
    else:
        pw = getpass.getpass("New password: ")
        pw2 = getpass.getpass("Again: ")
        if pw != pw2:
            print("Passwords do not match.", file=sys.stderr)
            return 1

    li = (args.login or "").strip()
    if len(pw) < 8:
        print("Password must be at least 8 characters.", file=sys.stderr)
        return 1

    h = hash_password(pw)
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE users SET password = ?
        WHERE lower(username) = lower(?) OR lower(email) = lower(?)
        """,
        (h, li, li),
    )
    n = cur.rowcount
    conn.commit()
    conn.close()
    if n < 1:
        print(f"No user matched login {li!r}. Run: python -m backend.scripts.app_users_status", file=sys.stderr)
        return 1
    print(f"Updated password for matching row(s): {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
