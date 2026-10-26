#!/usr/bin/env python3
"""Read-only pre-flight for migrations/V031 (drop dead TOTP/phone columns).

Prints the users row count and how many rows still hold a non-empty totp_secret,
mfa_phone or mfa_method, for the SQLite users DB and, when INVENTORY_DATABASE_URL /
--pg-url is given, the Postgres ``users`` table. Never writes. Exit 0 always; the owner
reads the counts and decides whether to scrub first (P16.3).
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

DEAD = ("totp_secret", "mfa_phone", "mfa_method")


def count_rows(cur, table: str, cols: set[str], ph: str) -> dict:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    out = {"rows": int(cur.fetchone()[0])}
    for c in DEAD:
        if c in cols:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {c} IS NOT NULL AND {c} <> ''")
            out[c] = int(cur.fetchone()[0])
        else:
            out[c] = "column absent"
    return out


def check_sqlite(path: str) -> dict:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(users)")
        cols = {r[1] for r in cur.fetchall()}
        return count_rows(cur, "users", cols, "?")
    finally:
        conn.close()


def check_pg(url: str) -> dict:
    import psycopg

    with psycopg.connect(url) as conn:
        conn.read_only = True
        cur = conn.cursor()
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'users'"
        )
        cols = {r[0] for r in cur.fetchall()}
        return count_rows(cur, "public.users", cols, "%s")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sqlite", help="path to a plain (unencrypted) users SQLite file")
    ap.add_argument("--pg-url", default=os.environ.get("INVENTORY_DATABASE_URL", ""))
    args = ap.parse_args(argv)
    if not args.sqlite and not args.pg_url:
        print("give --sqlite PATH and/or --pg-url URL (or set INVENTORY_DATABASE_URL)")
        return 2
    if args.sqlite:
        print("sqlite:", check_sqlite(args.sqlite))
    if args.pg_url:
        print("postgres:", check_pg(args.pg_url))
    return 0


if __name__ == "__main__":
    sys.exit(main())
