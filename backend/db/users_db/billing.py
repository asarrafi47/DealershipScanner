from ._common import get_conn


def grant_user_premium(
    user_id: int,
    *,
    customer_id: str | None = None,
    session_id: str | None = None,
    subscription_id: str | None = None,
    plan_id: str | None = None,
) -> None:
    """Set is_premium=1 and store Stripe identifiers for a user."""
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return
    pid = (plan_id or "").strip().lower() or None
    conn = get_conn()
    c = conn.cursor()
    c.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in c.fetchall()}
    if pid and "subscription_plan_id" in cols:
        c.execute(
            """
            UPDATE users
            SET is_premium = 1,
                subscription_plan_id = ?,
                premium_stripe_customer_id = COALESCE(?, premium_stripe_customer_id),
                premium_stripe_session_id = COALESCE(?, premium_stripe_session_id),
                premium_stripe_subscription_id = COALESCE(?, premium_stripe_subscription_id)
            WHERE id = ?
            """,
            (pid, customer_id or None, session_id or None, subscription_id or None, uid),
        )
    else:
        c.execute(
            """
            UPDATE users
            SET is_premium = 1,
                premium_stripe_customer_id = COALESCE(?, premium_stripe_customer_id),
                premium_stripe_session_id = COALESCE(?, premium_stripe_session_id),
                premium_stripe_subscription_id = COALESCE(?, premium_stripe_subscription_id)
            WHERE id = ?
            """,
            (customer_id or None, session_id or None, subscription_id or None, uid),
        )
    conn.commit()
    conn.close()


def revoke_user_premium(user_id: int) -> None:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return
    conn = get_conn()
    c = conn.cursor()
    c.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in c.fetchall()}
    if "subscription_plan_id" in cols:
        c.execute(
            "UPDATE users SET is_premium = 0, subscription_plan_id = NULL WHERE id = ?",
            (uid,),
        )
    else:
        c.execute("UPDATE users SET is_premium = 0 WHERE id = ?", (uid,))
    conn.commit()
    conn.close()


def get_user_premium_status(user_id: int) -> bool:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    conn = get_conn()
    row = conn.execute("SELECT is_premium FROM users WHERE id = ?", (uid,)).fetchone()
    conn.close()
    return bool(row and row[0])


def get_user_billing_snapshot(user_id: int) -> dict | None:
    """Billing fields for account UI (no secrets)."""
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
    want = ["id", "is_premium"]
    for extra in (
        "subscription_plan_id",
        "premium_stripe_customer_id",
        "premium_stripe_subscription_id",
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
    out["is_premium"] = bool(out.get("is_premium"))
    return out
