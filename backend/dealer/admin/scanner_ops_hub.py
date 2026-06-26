"""Site-admin scanner ops hub (smart import, registry maintenance)."""

from __future__ import annotations

from flask import flash, redirect, render_template, url_for

from backend.db.dealerships_db import list_recent_dealerships
from backend.dealer.admin.routes import _session_profile, store_admin_bp
from backend.dev.routes import _dev_status_shell
from backend.utils.roles import is_admin_role


def _require_site_admin():
    p = _session_profile()
    if not p or not is_admin_role(p.get("role")):
        return None
    return p


@store_admin_bp.route("/scanner-ops")
def admin_scanner_ops_hub():
    if not _require_site_admin():
        flash("Scanner tools require a site admin account.", "error")
        return redirect(url_for("store_admin.admin_site_hub"))

    return render_template(
        "admin/scanner_ops.html",
        dealerships=list_recent_dealerships(10),
        status=_dev_status_shell(),
    )
