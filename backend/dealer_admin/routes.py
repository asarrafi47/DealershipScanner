"""
Store admin UI: ``/admin`` (app ``users`` session — not ``/dev``).

Mutations require CSRF (``validate_csrf_form`` in ``main`` ``before_request``).
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from flask import (
    Blueprint,
    abort,
    flash,
    redirect,
    render_template,
    request,
    Response,
    session,
    url_for,
)

from backend.db.inventory_db import get_car_by_id, list_scan_runs, update_car_row_partial
from backend.db.users_db import get_user_profile
from backend.dealer_admin import inventory_queries as invq
from backend.dealer_admin.merchandising import (
    customer_ready_codes,
    days_in_inventory,
    derive_price_provenance,
    https_gallery_count,
    issue_label,
    kpi_counts_for_rows,
    merchandising_issue_codes,
)
from backend.utils.csrf import ensure_csrf_token

_log = logging.getLogger(__name__)

store_admin_bp = Blueprint("store_admin", __name__, url_prefix="/admin")

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _session_profile() -> dict[str, Any] | None:
    uid = session.get("user_id")
    if uid is None:
        return None
    try:
        uid_i = int(uid)
    except (TypeError, ValueError):
        return None
    return get_user_profile(uid_i)


def _require_store_profile():
    p = _session_profile()
    if not p:
        return None
    role = (p.get("role") or "dealer_staff").strip().lower()
    if role == "admin":
        return p
    if (p.get("dealer_id") or "").strip() or p.get("dealership_registry_id") is not None:
        return p
    return False


@store_admin_bp.before_request
def _require_access():
    if request.endpoint == "store_admin.static":
        return None
    if session.get("user_id") and not session.get("mfa_ok"):
        return redirect(url_for("mfa_verify"))
    gate = _require_store_profile()
    if gate is None:
        return redirect(url_for("login_page"))
    if gate is False:
        flash(
            "Store admin is not enabled for this account. Ask an administrator to assign "
            "a dealer_id or dealership_registry_id to your user.",
            "error",
        )
        return redirect(url_for("dashboard"))
    return None


@store_admin_bp.context_processor
def _ctx():
    p = _session_profile()
    return {
        "admin_profile": p,
        "is_store_admin": (p or {}).get("role") == "admin",
        "csrf_token": ensure_csrf_token(),
    }


@store_admin_bp.route("/")
def admin_home():
    p = _session_profile()
    assert p
    dash = invq.dashboard_summary(p)
    role_l = (p.get("role") or "").strip().lower()
    scans = list_scan_runs(
        dealer_id=(p.get("dealer_id") or "").strip() if role_l != "admin" else None,
        limit=8,
    )
    return render_template("admin/dashboard.html", dash=dash, scans=scans)


@store_admin_bp.route("/inventory")
def admin_inventory():
    p = _session_profile()
    assert p
    page = int(request.args.get("page") or 1)
    per_page = int(request.args.get("per_page") or 40)
    sort = (request.args.get("sort") or "scraped_at").strip()
    direction = (request.args.get("dir") or "desc").strip()
    q = (request.args.get("q") or "").strip()
    active_only = (request.args.get("active") or "1").strip() != "0"
    stale_only = request.args.get("stale") == "1"
    rows, total = invq.list_inventory_rows(
        p,
        page=page,
        per_page=per_page,
        sort=sort,
        direction=direction,
        q=q,
        active_only=active_only,
        stale_only=stale_only,
    )
    enriched: list[dict[str, Any]] = []
    for r in rows:
        issues = merchandising_issue_codes(r)
        enriched.append(
            {
                **r,
                "_issues": issues,
                "_issue_labels": [issue_label(c) for c in issues],
                "_days": days_in_inventory(r),
                "_photo_n": https_gallery_count(r),
            }
        )
    total_pages = max(1, (total + per_page - 1) // per_page)
    kpis = kpi_counts_for_rows(rows)
    return render_template(
        "admin/inventory.html",
        rows=enriched,
        total=total,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        sort=sort,
        direction=direction,
        q=q,
        active_only=active_only,
        stale_only=stale_only,
        kpis=kpis,
    )


@store_admin_bp.route("/inventory/<int:car_id>")
def admin_inventory_detail(car_id: int):
    p = _session_profile()
    assert p
    row = get_car_by_id(car_id, include_inactive=True)
    if not row:
        abort(404)
    if not invq.car_visible_to_profile(p, car_id):
        abort(404)

    issues = merchandising_issue_codes(row)
    checklist = customer_ready_codes(row)
    prov = derive_price_provenance(row)
    pkg_pretty = None
    if row.get("packages"):
        try:
            pj = json.loads(row["packages"]) if isinstance(row["packages"], str) else row["packages"]
            pkg_pretty = json.dumps(pj, indent=2, ensure_ascii=False) if pj is not None else None
        except (json.JSONDecodeError, TypeError, ValueError):
            pkg_pretty = str(row["packages"])[:12000]
    spec_pretty = None
    if row.get("spec_source_json"):
        try:
            sj = json.loads(row["spec_source_json"]) if isinstance(row["spec_source_json"], str) else row["spec_source_json"]
            spec_pretty = json.dumps(sj, indent=2, ensure_ascii=False) if sj is not None else None
        except (json.JSONDecodeError, TypeError, ValueError):
            spec_pretty = str(row["spec_source_json"])[:12000]

    return render_template(
        "admin/inventory_detail.html",
        car=row,
        issues=issues,
        issue_labels=[issue_label(c) for c in issues],
        checklist=checklist,
        price_rows=prov,
        packages_pretty=pkg_pretty,
        spec_pretty=spec_pretty,
        days_in_inv=days_in_inventory(row),
    )


@store_admin_bp.route("/inventory/<int:car_id>/notes", methods=["POST"])
def admin_inventory_notes(car_id: int):
    p = _session_profile()
    assert p
    row = get_car_by_id(car_id, include_inactive=True)
    if not row or not invq.car_visible_to_profile(p, car_id):
        abort(404)
    notes = (request.form.get("internal_notes") or "").strip()[:16000]
    update_car_row_partial(car_id, {"internal_notes": notes or None})
    flash("Internal notes saved.", "success")
    return redirect(url_for("store_admin.admin_inventory_detail", car_id=car_id))


@store_admin_bp.route("/inventory/<int:car_id>/review", methods=["POST"])
def admin_inventory_review(car_id: int):
    p = _session_profile()
    assert p
    row = get_car_by_id(car_id, include_inactive=True)
    if not row or not invq.car_visible_to_profile(p, car_id):
        abort(404)
    cur = 1 if (request.form.get("marked_for_review") == "1") else 0
    update_car_row_partial(car_id, {"marked_for_review": cur})
    flash("Review flag updated.", "success")
    return redirect(url_for("store_admin.admin_inventory_detail", car_id=car_id))


@store_admin_bp.route("/inventory/export.csv")
def admin_inventory_export():
    p = _session_profile()
    assert p
    rows = invq.export_inventory_rows(p, limit=int(request.args.get("limit") or 5000))
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "id",
            "vin",
            "stock",
            "year",
            "make",
            "model",
            "trim",
            "condition",
            "mileage",
            "price",
            "msrp",
            "dealer_id",
            "dealer_name",
            "scraped_at",
            "first_seen_at",
            "last_price_change_at",
            "listing_active",
            "data_quality_score",
            "merchandising_issues",
        ]
    )
    for r in rows:
        iss = ";".join(merchandising_issue_codes(r))
        w.writerow(
            [
                r.get("id"),
                r.get("vin"),
                r.get("stock_number"),
                r.get("year"),
                r.get("make"),
                r.get("model"),
                r.get("trim"),
                r.get("condition"),
                r.get("mileage"),
                r.get("price"),
                r.get("msrp"),
                r.get("dealer_id"),
                r.get("dealer_name"),
                r.get("scraped_at"),
                r.get("first_seen_at"),
                r.get("last_price_change_at"),
                r.get("listing_active"),
                r.get("data_quality_score"),
                iss,
            ]
        )
    data = buf.getvalue()
    return Response(
        data,
        mimetype="text/csv",
        headers={"Content-Disposition": 'attachment; filename="inventory_export.csv"'},
    )


@store_admin_bp.route("/scans")
def admin_scans():
    p = _session_profile()
    assert p
    role_l = (p.get("role") or "").strip().lower()
    if role_l == "admin":
        did = None
    else:
        did = (p.get("dealer_id") or "").strip() or None
    if role_l != "admin" and not did:
        scans = []
        scan_scope_note = (
            "Scan history is keyed by dealer_id. Ask an admin to set your users.dealer_id "
            "to match manifest dealer_id, or rely on manual scanner logs."
        )
    else:
        scan_scope_note = None
        scans = list_scan_runs(dealer_id=did, limit=80)
    for s in scans:
        try:
            s["_summary"] = json.loads(s.get("summary_json") or "{}")
        except (json.JSONDecodeError, TypeError, ValueError):
            s["_summary"] = {}
    return render_template("admin/scans.html", scans=scans, scan_scope_note=scan_scope_note)


def _spawn_scanner(dealer_id: str) -> None:
    script = PROJECT_ROOT / "scanner.py"
    try:
        subprocess.Popen(
            [sys.executable, str(script), "--dealer-id", dealer_id],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError:
        _log.exception("Failed to spawn scanner for dealer_id=%s", dealer_id)


@store_admin_bp.route("/dealers/<path:dealer_id>/rescan", methods=["POST"])
def admin_dealer_rescan(dealer_id: str):
    p = _session_profile()
    assert p
    if (p.get("role") or "").strip().lower() != "admin":
        abort(403)
    if (os.environ.get("ALLOW_STORE_ADMIN_RESCAN") or "").strip().lower() not in ("1", "true", "yes", "on"):
        flash("Re-scan from the web UI is disabled. Set ALLOW_STORE_ADMIN_RESCAN=1 or run scanner.py manually.", "error")
        return redirect(url_for("store_admin.admin_scans"))
    d = dealer_id.strip()
    if not d or not re.fullmatch(r"[A-Za-z0-9._-]{1,120}", d):
        flash("Invalid dealer id.", "error")
        return redirect(url_for("store_admin.admin_scans"))
    threading.Thread(target=_spawn_scanner, args=(d,), name=f"rescan-{d}", daemon=True).start()
    flash(f"Started background scan for dealer_id={d}. Check /admin/scans and server logs for progress.", "success")
    return redirect(url_for("store_admin.admin_scans"))
