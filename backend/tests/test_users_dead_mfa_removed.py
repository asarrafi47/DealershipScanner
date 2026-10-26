"""P16.2 / P16.2m: dead TOTP and phone storage is retired (D-SEC2 a)."""
from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    "mod,names",
    [
        ("backend.db.users_db", ("get_user_totp", "set_user_totp", "set_user_mfa_phone")),
        ("backend.db.users_db.auth", ("get_user_totp", "set_user_totp", "set_user_mfa_phone")),
        ("backend.db.admin_users_db", ("get_admin_totp", "set_admin_totp")),
    ],
)
def test_helpers_gone(mod, names):
    import importlib

    m = importlib.import_module(mod)
    for n in names:
        assert not hasattr(m, n), n
    assert not any(n in getattr(m, "__all__", ()) for n in names)


def test_fresh_users_db_has_no_dead_columns(app_factory):
    app_factory()
    from backend.db.users_db import get_user_by_login, save_user
    from backend.db.users_db._common import get_conn
    from backend.utils.roles import ROLE_GENERAL

    conn = get_conn()
    cols = {r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()}
    conn.close()
    assert not cols & {"totp_secret", "mfa_phone", "mfa_method"}
    assert "totp_enabled" in cols
    save_user("nomfa", "nomfa@example.com", "long-enough-password", role=ROLE_GENERAL)
    u = get_user_by_login("nomfa")
    assert u["totp_enabled"] is False
    assert "mfa_phone" not in u and "totp_secret" not in u


def test_env_admin_clause_touches_no_dead_columns(app_factory):
    app_factory()
    from backend.db.users_db._common import get_conn
    from backend.db.users_db.schema import _env_admin_set_clause

    conn = get_conn()
    clause = _env_admin_set_clause(conn.cursor())
    conn.close()
    assert clause == "role = ?, totp_enabled = 0"


def test_legacy_columns_in_old_db_are_ignored(app_factory):
    """A pre-P16.2 users.db still has the columns; reads must not surface them."""
    app_factory()
    from backend.db.users_db import get_user_by_login, list_users_for_admin, save_user
    from backend.db.users_db._common import _users_select_columns, get_conn
    from backend.utils.roles import ROLE_GENERAL

    conn = get_conn()
    for c in ("totp_secret", "mfa_phone", "mfa_method"):
        conn.execute(f"ALTER TABLE users ADD COLUMN {c} TEXT")
    conn.commit()
    conn.close()
    _users_select_columns.cache_clear()
    save_user("legacy", "legacy@example.com", "long-enough-password", role=ROLE_GENERAL)
    conn = get_conn()
    conn.execute("UPDATE users SET totp_secret='JBSWY3DP', mfa_phone='555', mfa_method='totp'")
    conn.commit()
    conn.close()
    u = get_user_by_login("legacy")
    assert "totp_secret" not in u and "mfa_phone" not in u
    rows = list_users_for_admin() if callable(list_users_for_admin) else []
    for r in rows:
        assert "mfa_method" not in r and r["mfa_label"] == "—"


def test_v036_drops_exactly_the_dead_columns():
    sql = (_ROOT / "migrations" / "V031__users_drop_dead_mfa_columns.sql").read_text()
    body = "\n".join(l for l in sql.splitlines() if not l.lstrip().startswith("--"))
    drops = re.findall(r"DROP COLUMN IF EXISTS (\w+)", body)
    assert drops == ["totp_secret", "mfa_phone", "mfa_method"]
    assert "totp_enabled" not in body
    assert "RAISE" not in body


def test_preflight_counts_sqlite(tmp_path):
    import sqlite3

    from backend.scripts.preflight_drop_dead_mfa import check_sqlite

    p = tmp_path / "u.db"
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE users (id INTEGER, totp_secret TEXT, mfa_phone TEXT)")
    c.executemany("INSERT INTO users VALUES (?,?,?)", [(1, "ABC", None), (2, "", "555"), (3, None, None)])
    c.commit()
    c.close()
    assert check_sqlite(str(p)) == {
        "rows": 3, "totp_secret": 1, "mfa_phone": 1, "mfa_method": "column absent"
    }
