"""Site-admin dealer onboarding hub at ``/admin/dealers`` (Postgres job queue)."""

from __future__ import annotations

from flask import flash, redirect, render_template, request, url_for

from backend.dealer.admin.routes import _session_profile, store_admin_bp
from backend.db.inventory_pg import is_inventory_postgres
from backend.scanner.job_queue import enqueue_job, list_dealer_catalog, list_recent_jobs
from backend.scanner.scrape_confidence import job_display_status
from backend.utils.csrf import validate_csrf_form
from backend.utils.roles import is_admin_role


def _require_site_admin():
    p = _session_profile()
    if not p or not is_admin_role(p.get("role")):
        return None
    return p


@store_admin_bp.route("/dealers", methods=["GET", "POST"])
def admin_dealers_hub():
    """Discover dealers, view catalog/jobs, enqueue onboard scans (site admin only)."""
    if not _require_site_admin():
        flash("Dealer onboarding hub requires a site admin account.", "error")
        return redirect(url_for("store_admin.admin_site_hub"))

    postgres_ok = is_inventory_postgres()
    if request.method == "POST":
        validate_csrf_form()
        if not postgres_ok:
            flash("Postgres inventory (INVENTORY_DATABASE_URL) is required for job queue.", "error")
        else:
            dealer_id = (request.form.get("dealer_id") or "").strip()
            url = (request.form.get("url") or "").strip()
            if not dealer_id or not url:
                flash("dealer_id and url are required.", "error")
            else:
                job_id = enqueue_job(
                    dealer_id=dealer_id,
                    job_type="onboard",
                    payload={"url": url},
                )
                if job_id:
                    flash(f"Queued onboard job #{job_id} for {dealer_id}.", "success")
                else:
                    flash("Failed to enqueue job.", "error")
        return redirect(url_for("store_admin.admin_dealers_hub"))

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
    )
