"""
Site-admin rooftop-attribution resolution view: ``/admin/attribution``.

Surfaces, per group-fed dealer, what ``classify_attribution.py`` decided --
confirmed / conflicting / unverified counts and a "stuck" count with a short why
-- which until now existed only as INFO-level log lines from a script someone had
to remember to run and read. See backend/db/repositories/attribution_repo.py for
the query and backend/scripts/classify_attribution.py for what writes the table.
"""

from __future__ import annotations

from flask import flash, redirect, render_template, request, url_for

from backend.db.inventory_db import dealer_attribution_resolution
from backend.dealer.admin.routes import _session_profile, store_admin_bp
from backend.utils.roles import is_admin_role

#: Columns the table may be sorted by, and the (reversed-by-default) direction that
#: makes browsing sense for each -- stuck/conflicting/unverified/confirmed/active_cars
#: default to worst-or-biggest first, dealer_name reads better ascending.
_SORTABLE_COLUMNS = {
    "stuck": True, "conflicting": True, "unverified": True, "confirmed": True,
    "resolved_by_move": True, "active_cars": True, "dealer_name": False,
}
_DEFAULT_SORT = "stuck"


def _require_site_admin():
    p = _session_profile()
    if not p or not is_admin_role(p.get("role")):
        return None
    return p


@store_admin_bp.route("/attribution")
def admin_attribution_hub():
    if not _require_site_admin():
        flash("The attribution resolution view requires a site admin account.", "error")
        return redirect(url_for("store_admin.admin_site_hub"))

    sort = request.args.get("sort") or _DEFAULT_SORT
    if sort not in _SORTABLE_COLUMNS:
        sort = _DEFAULT_SORT
    default_desc = _SORTABLE_COLUMNS[sort]
    dir_param = request.args.get("dir")
    descending = (dir_param == "desc") if dir_param in ("asc", "desc") else default_desc

    rows = dealer_attribution_resolution()
    rows.sort(key=lambda r: r.get(sort) if r.get(sort) is not None else 0, reverse=descending)

    totals = {
        key: sum(r.get(key, 0) for r in rows)
        for key in ("confirmed", "conflicting", "unverified", "stuck", "resolved_by_move")
    }

    def sort_url(column: str) -> str:
        next_dir = "asc" if (column == sort and descending) else "desc"
        return url_for("store_admin.admin_attribution_hub", sort=column, dir=next_dir)

    return render_template(
        "admin/attribution.html",
        rows=rows,
        totals=totals,
        sort=sort,
        descending=descending,
        sort_url=sort_url,
    )
