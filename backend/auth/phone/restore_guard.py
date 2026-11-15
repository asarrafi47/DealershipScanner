"""Restore guard for phone sign-in (card S5 review (f)).

Phone identities live in Postgres and point at account ids in the SQLite users database. If the
users database is ever restored from an older copy while Postgres is not, a new account could be
given an id an identity already points at, and that stranger would be signed in to someone
else's number. So in production, with phone sign-in on, the app refuses to start when the
highest account id any identity names is above the highest id ``users`` ever issued. It also refuses when
either side cannot be read: an unchecked start is the one this guard exists to prevent.

Plain values only; nothing here logs a number, an id or a hash.
"""

from __future__ import annotations

from collections.abc import Callable

from backend.auth.phone import config
from backend.utils.runtime_env import is_production_env


# ``users.id`` is AUTOINCREMENT, so SQLite never hands out an id twice and keeps the highest id it
# ever issued in its sequence table. Deleting the newest account lowers MAX(id) but not that mark,
# so a deleted account whose identity row is still waiting for its purge does not stop the boot.
# A restored older copy carries its own, lower mark: that is the case this guard catches.
_USERS_HIGH_WATER = (
    "SELECT MAX(COALESCE((SELECT MAX(id) FROM users), 0), "
    "COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'users'), 0))"
)


def max_users_id() -> int:
    """Highest account id the users database ever issued (0 when it never issued one)."""
    from backend.db.users_db._common import get_conn

    conn = get_conn()
    try:
        row = conn.execute(_USERS_HIGH_WATER).fetchone()
    finally:
        conn.close()
    return int(row[0] or 0)


def max_identity_user_id() -> int:
    from backend.db.repositories.auth_phone_repo import max_identity_user_id as read

    return read()


def restore_guard_problem(
    *,
    identity_max: Callable[[], int] | None = None,
    users_max: Callable[[], int] | None = None,
) -> str | None:
    """A short reason (no values) when the identities point past the users table; else None."""
    identity_max = identity_max or max_identity_user_id
    users_max = users_max or max_users_id
    try:
        ident = int(identity_max())
    except Exception as exc:  # table missing (V031 not applied) or database down
        return "the phone identity table could not be read (%s)" % type(exc).__name__
    try:
        users = int(users_max())
    except Exception as exc:
        return "the users table could not be read (%s)" % type(exc).__name__
    if ident > users:
        return "a phone identity names an account id above the highest id users ever issued (was users restored?)"
    return None


def assert_identity_ids_within_users(**readers: Callable[[], int]) -> None:
    """Refuse a production start with ``AUTH_PHONE_LOGIN_ENABLED`` on when the guard fails."""
    if not is_production_env() or not config.enabled():
        return
    problem = restore_guard_problem(**readers)
    if problem:
        raise RuntimeError(
            "AUTH_PHONE_LOGIN_ENABLED is on but the restore guard failed: "
            + problem
            + ". Restore both databases from the same point, or turn the flag off."
        )
