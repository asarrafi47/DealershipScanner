"""User-submitted dealership review routes (additive).

Two POST surfaces under the existing dealership research page:

* ``POST /dealership/<dealer_key>/reviews`` — a logged-in user posts (or edits)
  their single review for a dealer.
* ``POST /dealership/<dealer_key>/reviews/<review_id>/report`` — anyone flags a
  review; three reports auto-hide it (``status='flagged'``).

Dealer-key resolution reuses ``dealership_page._resolve_dealer`` so the hostname
-> ``dealer_id`` logic is defined in exactly one place.
"""
from __future__ import annotations

from flask import jsonify, redirect, request, session, url_for

from backend.routes.dealership_page import _resolve_dealer


def _client_ip_hash() -> str | None:
    from backend.reviews.guards import hash_ip

    try:
        from backend.utils.client_ip import client_ip

        return hash_ip(client_ip(request))
    except Exception:
        return hash_ip(request.remote_addr)


def _user_display_name(user_id: int) -> str | None:
    """Resolve the poster's display name (``users.username``) at post time."""
    try:
        from backend.db.users_db import get_user_profile

        prof = get_user_profile(user_id)
        if prof:
            return prof.get("username") or None
    except Exception:
        pass
    return None


def _back_to_reviews(dealer_key: str, error: str | None = None):
    params = {"dealer_key": dealer_key}
    if error:
        params["review_error"] = error
    return redirect(url_for("dealership_research_page", **params) + "#reviews")


def submit_review(dealer_key: str):
    user_id = session.get("user_id")
    if not user_id:
        # Not signed in: send to login, then back to the dealer page.
        nxt = url_for("dealership_research_page", dealer_key=dealer_key) + "#reviews"
        return redirect(url_for("login_page", next=nxt))

    _dealership, dealer_id = _resolve_dealer(dealer_key)
    if not dealer_id:
        return _back_to_reviews(dealer_key, "We couldn't find that dealership.")

    from backend.reviews.guards import _truthy, check_rate_limit, validate_review

    is_anonymous = _truthy(request.form.get("is_anonymous"))
    addon_fee_reported = _truthy(request.form.get("addon_fee_reported"))
    payload = {
        "rating": request.form.get("rating"),
        "body": request.form.get("body"),
        "addon_fee_reported": addon_fee_reported,
        "addon_fee_amount": request.form.get("addon_fee_amount"),
        "addon_fee_desc": request.form.get("addon_fee_desc"),
    }

    ok, err = validate_review(payload)
    if not ok:
        return _back_to_reviews(dealer_key, err)

    from backend.db.inventory_db import get_conn
    from backend.reviews.store import upsert_review

    conn = get_conn()
    try:
        ok_rl, err_rl = check_rate_limit(conn, int(user_id))
        if not ok_rl:
            return _back_to_reviews(dealer_key, err_rl)

        amount = None
        if addon_fee_reported:
            raw = (request.form.get("addon_fee_amount") or "").strip()
            if raw:
                try:
                    amount = float(raw)
                except ValueError:
                    amount = None

        upsert_review(
            conn,
            dealer_id,
            int(user_id),
            rating=int(payload["rating"]),
            body=(payload["body"] or "").strip(),
            display_name=_user_display_name(int(user_id)),
            is_anonymous=is_anonymous,
            addon_fee_reported=addon_fee_reported,
            addon_fee_amount=amount,
            addon_fee_desc=(request.form.get("addon_fee_desc") or "").strip() or None,
            ip_hash=_client_ip_hash(),
        )
    except Exception:
        return _back_to_reviews(dealer_key, "Something went wrong saving your review. Please try again.")
    finally:
        conn.close()

    return _back_to_reviews(dealer_key)


def report_review(dealer_key: str, review_id: int):
    """Increment a review's report count (light IP-hash rate limit, no login)."""
    from backend.db.inventory_db import get_conn
    from backend.reviews.store import increment_report

    wants_json = request.accept_mimetypes.best_match(
        ["application/json", "text/html"]
    ) == "application/json"

    conn = get_conn()
    try:
        result = increment_report(conn, int(review_id))
    except Exception:
        result = None
    finally:
        conn.close()

    if wants_json:
        if result is None:
            return jsonify({"ok": False, "error": "not_found"}), 404
        return jsonify({"ok": True, "report_count": result["report_count"], "status": result["status"]})
    return _back_to_reviews(dealer_key)


def register(app) -> None:
    """Attach the review POST routes (additive; bare endpoint names)."""
    app.add_url_rule(
        "/dealership/<dealer_key>/reviews",
        endpoint="dealership_submit_review",
        view_func=submit_review,
        methods=["POST"],
    )
    app.add_url_rule(
        "/dealership/<dealer_key>/reviews/<int:review_id>/report",
        endpoint="dealership_report_review",
        view_func=report_review,
        methods=["POST"],
    )
