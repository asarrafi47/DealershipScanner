"""Site-admin dealer onboarding hub at ``/admin/dealers`` (Postgres job queue)."""

from __future__ import annotations

from flask import Response, flash, redirect, render_template, request, url_for

from backend.dealer.admin.routes import _session_profile, store_admin_bp
from backend.db import inventory_pg
from backend.scanner.job_queue import list_dealer_catalog, list_recent_jobs
from backend.scanner.scrape_confidence import job_display_status
from backend.utils.csrf import validate_csrf_form
from backend.utils.roles import is_admin_role


# N3.10 stopgap: no worker has consumed ``dealer_jobs`` since the worker and
# scheduler services were deleted on 2026-10-08 (SCANNING_OPS_LOG.md), so an
# onboard job queued here would wait forever. P15A.3 (D-IF3 (a)) retires the
# queue path and hands onboarding to dealer-discovery.
ONBOARDING_PAUSED_MESSAGE = "Onboarding is paused; use dealer-discovery (see P15A.3)"


def _require_site_admin():
    p = _session_profile()
    if not p or not is_admin_role(p.get("role")):
        return None
    return p


@store_admin_bp.route("/dealers", methods=["GET", "POST"])
def admin_dealers_hub():
    """View the dealer catalog and job history (site admin only).

    Onboarding is paused (N3.10): a POST answers 409 with
    ``ONBOARDING_PAUSED_MESSAGE`` and enqueues nothing.
    """
    if not _require_site_admin():
        flash("Dealer onboarding hub requires a site admin account.", "error")
        return redirect(url_for("store_admin.admin_site_hub"))

    if request.method == "POST":
        validate_csrf_form()
        return Response(ONBOARDING_PAUSED_MESSAGE + "\n", status=409, mimetype="text/plain")

    postgres_ok = inventory_pg.is_inventory_postgres()
    catalog = list_dealer_catalog(limit=50) if postgres_ok else []
    jobs = []
    if postgres_ok:
        import json as _json

        for row in list_recent_jobs(limit=30):
            result: dict = {}
            raw = row.get("result_json")
            if raw:
                try:
                    result = _json.loads(raw) if isinstance(raw, str) else (raw or {})
                except (_json.JSONDecodeError, TypeError, ValueError):
                    result = {}
            row = dict(row)
            row["result"] = result
            if isinstance(result.get("scrape_confidence"), dict):
                row["scrape_confidence"] = result["scrape_confidence"]
            row["display_status"] = job_display_status(
                str(row.get("status") or ""),
                str(row.get("job_type") or ""),
                result,
            )
            jobs.append(row)
    return render_template(
        "admin/dealers.html",
        postgres_ok=postgres_ok,
        catalog=catalog,
        jobs=jobs,
        find_dealers_url=url_for("find_dealers_page"),
        onboarding_paused_message=ONBOARDING_PAUSED_MESSAGE,
    )
