"""Site-admin data quality hub: incomplete listings and dealership issue stats."""

from __future__ import annotations

from flask import flash, redirect, render_template, url_for

from backend.db.dealerships_db import list_recent_dealerships
from backend.db.inventory_db import get_dealership_issue_stats
from backend.dealer.admin.incomplete_listings_api import incomplete_listings_count
from backend.dealer.admin.routes import _session_profile, store_admin_bp
from backend.utils.roles import is_admin_role


def _require_site_admin():
    p = _session_profile()
    if not p or not is_admin_role(p.get("role")):
        return None
    return p


@store_admin_bp.route("/data-quality")
def admin_data_quality_hub():
    if not _require_site_admin():
        flash("Data quality tools require a site admin account.", "error")
        return redirect(url_for("store_admin.admin_site_hub"))

    return render_template(
        "admin/data_quality.html",
        incomplete_count=incomplete_listings_count(),
        dealership_stats=get_dealership_issue_stats(limit=25),
    )
