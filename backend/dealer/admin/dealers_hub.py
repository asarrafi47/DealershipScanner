"""Site-admin dealer onboarding hub at ``/admin/dealers`` (Postgres job queue)."""

from __future__ import annotations

from flask import flash, redirect, render_template, request, url_for

from backend.dealer.admin.onboard_api import request_dealer_onboard
from backend.dealer.admin.routes import _session_profile, store_admin_bp
from backend.db.inventory_pg import is_inventory_postgres
from backend.scanner.job_queue import list_dealer_scan_registry, list_recent_jobs
from backend.scanner.scrape_confidence import job_display_status
from backend.utils.client_ip import client_ip
from backend.utils.csrf import validate_csrf_form
from backend.utils.ip_rate_limit import allow_request
from backend.utils.roles import is_admin_role

_ONBOARD_FLASH_MESSAGES = {
    "invalid_url": "Enter a valid http or https inventory URL.",
    "invalid_dealer_id": "Dealer ID must be lowercase letters, numbers, and hyphens only.",
    "postgres_required": "Postgres inventory (INVENTORY_DATABASE_URL) is required for job queue.",
    "enqueue_failed": "Failed to enqueue onboard job. Try again.",
    "rate_limited": "Too many onboard requests from your network. Try again in an hour.",
}


def _require_site_admin():
    p = _session_profile()
    if not p or not is_admin_role(p.get("role")):
        return None
    return p


@store_admin_bp.route("/dealers", methods=["GET", "POST"])
def admin_dealers_hub():
    """Discover dealers, view scan registry/jobs, enqueue onboard scans (site admin only)."""
    if not _require_site_admin():
        flash("Dealer onboarding hub requires a site admin account.", "error")
        return redirect(url_for("store_admin.admin_site_hub"))

    postgres_ok = is_inventory_postgres()
    if request.method == "POST":
        validate_csrf_form()
        ip = client_ip(request)
        if not allow_request(f"dealer-onboard:{ip}", max_events=30, window_seconds=3600.0):
            flash(_ONBOARD_FLASH_MESSAGES["rate_limited"], "error")
        else:
            url = (request.form.get("url") or "").strip()
            dealer_id = (request.form.get("dealer_id") or "").strip() or None
            name = (request.form.get("name") or "").strip() or None
            if not url:
                flash("URL is required.", "error")
            else:
                ok, err, data = request_dealer_onboard(
                    url=url,
                    dealer_id=dealer_id,
                    name=name,
                )
                if ok:
                    flash(
                        f"Queued onboard job #{data['job_id']} for {data['dealer_id']}.",
                        "success",
                    )
                else:
                    flash(
                        _ONBOARD_FLASH_MESSAGES.get(err, "Failed to queue onboard job."),
                        "error",
                    )
        return redirect(url_for("store_admin.admin_dealers_hub"))

    scan_registry = list_dealer_scan_registry(limit=50) if postgres_ok else []
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
        scan_registry=scan_registry,
        jobs=jobs,
        find_dealers_url=url_for("find_dealers_page"),
    )
