"""Site-admin review moderation at ``/admin/reviews``.

Reviews auto-flip to ``status='flagged'`` at ``REPORT_FLAG_THRESHOLD`` reports
(``backend/reviews/store.py: increment_report``) with no surface to see or
reverse that — this is that surface: list flagged reviews, unflag (restore to
``published``) or remove (permanently hide) one.
"""
from __future__ import annotations

from flask import abort, flash, redirect, render_template, request, url_for

from backend.dealer.admin.routes import _session_profile, store_admin_bp
from backend.utils.roles import is_admin_role


def _require_site_admin():
    p = _session_profile()
    if not p or not is_admin_role(p.get("role")):
        return None
    return p


@store_admin_bp.route("/reviews")
def admin_reviews_hub():
    """List flagged dealer reviews awaiting moderation (site admin only)."""
    if not _require_site_admin():
        flash("Review moderation requires a site admin account.", "error")
        return redirect(url_for("store_admin.admin_site_hub"))

    from backend.db.inventory_db import get_conn
    from backend.reviews.store import list_flagged_reviews

    conn = get_conn()
    try:
        reviews = list_flagged_reviews(conn, limit=200)
    finally:
        conn.close()
    return render_template("admin/reviews.html", reviews=reviews)


@store_admin_bp.route("/reviews/<int:review_id>/unflag", methods=["POST"])
def admin_review_unflag(review_id: int):
    """Restore a flagged review to published and reset its report count."""
    if not _require_site_admin():
        abort(403)

    from backend.db.inventory_db import get_conn
    from backend.reviews.store import set_review_status

    conn = get_conn()
    try:
        ok = set_review_status(conn, review_id, "published")
    finally:
        conn.close()
    flash("Review restored." if ok else "Review not found.", "success" if ok else "error")
    return redirect(url_for("store_admin.admin_reviews_hub"))


@store_admin_bp.route("/reviews/<int:review_id>/remove", methods=["POST"])
def admin_review_remove(review_id: int):
    """Permanently hide a flagged review."""
    if not _require_site_admin():
        abort(403)

    from backend.db.inventory_db import get_conn
    from backend.reviews.store import set_review_status

    conn = get_conn()
    try:
        ok = set_review_status(conn, review_id, "removed")
    finally:
        conn.close()
    flash("Review removed." if ok else "Review not found.", "success" if ok else "error")
    return redirect(url_for("store_admin.admin_reviews_hub"))
