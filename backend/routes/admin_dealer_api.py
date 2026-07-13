"""Site-admin dealer onboarding + dealer-jobs JSON API.

Endpoint names (``api_admin_dealer_*``) are preserved exactly — the CSRF
before_request hook in ``backend.main`` matches them by name.
"""

from __future__ import annotations

from flask import jsonify, request, session

from backend.routes._shared import _client_ip
from backend.utils.ip_rate_limit import allow_request
from backend.utils.roles import is_admin_role


def api_admin_dealer_onboard():
    """Site admin: queue Smart Import onboard from Find dealers (locator → scrape bridge)."""
    if not is_admin_role(session.get("user_role")):
        return jsonify({"ok": False, "error": "forbidden"}), 403

    ip = _client_ip()
    if not allow_request(f"dealer-onboard:{ip}", max_events=30, window_seconds=3600.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    body = request.get_json(silent=True) or {}
    from backend.dealer.admin.onboard_api import request_dealer_onboard

    ok, err, data = request_dealer_onboard(
        url=(body.get("url") or "").strip(),
        dealer_id=(body.get("dealer_id") or "").strip() or None,
        name=(body.get("name") or "").strip() or None,
    )
    if not ok:
        status = 503 if err == "postgres_required" else 400
        if err == "forbidden":
            status = 403
        return jsonify({"ok": False, "error": err}), status
    return jsonify({"ok": True, **data})


def _serialize_dealer_job_row(row: dict) -> dict:
    import json as _json

    out = dict(row)
    for key in ("payload_json", "result_json"):
        raw = out.pop(key, None)
        parsed: dict = {}
        if raw:
            try:
                parsed = _json.loads(raw) if isinstance(raw, str) else (raw or {})
            except (_json.JSONDecodeError, TypeError, ValueError):
                parsed = {}
        out["payload" if key == "payload_json" else "result"] = parsed
    result = out.get("result") or {}
    if isinstance(result, dict) and isinstance(result.get("ai_diagnosis"), dict):
        out["ai_diagnosis"] = result["ai_diagnosis"]
    if isinstance(result, dict) and isinstance(result.get("scrape_confidence"), dict):
        out["scrape_confidence"] = result["scrape_confidence"]
    from backend.scanner.scrape_confidence import job_display_status

    out["display_status"] = job_display_status(
        str(out.get("status") or ""),
        str(out.get("job_type") or ""),
        result if isinstance(result, dict) else {},
    )
    return out


def api_admin_dealer_jobs():
    """Site admin: poll dealer_jobs for live onboarding progress."""
    if not is_admin_role(session.get("user_role")):
        return jsonify({"ok": False, "error": "forbidden"}), 403

    from backend.db.inventory_pg import is_inventory_postgres
    from backend.scanner.job_queue import list_dealer_catalog, list_recent_jobs

    if not is_inventory_postgres():
        return jsonify({"ok": False, "error": "postgres_required", "jobs": [], "catalog": []}), 503

    try:
        limit = min(max(int(request.args.get("limit") or 30), 1), 100)
    except (TypeError, ValueError):
        limit = 30

    jobs = [_serialize_dealer_job_row(j) for j in list_recent_jobs(limit=limit)]
    catalog = list_dealer_catalog(limit=50)
    active = sum(1 for j in jobs if (j.get("status") or "") in ("queued", "running"))
    return jsonify(
        {
            "ok": True,
            "jobs": jobs,
            "catalog": catalog,
            "active_count": active,
        }
    )


def api_admin_dealer_job_detail(job_id: int):
    """Site admin: full job detail for sidebar (payload, result log, error)."""
    if not is_admin_role(session.get("user_role")):
        return jsonify({"ok": False, "error": "forbidden"}), 403

    from backend.db.inventory_pg import is_inventory_postgres
    from backend.scanner.job_queue import get_job

    if not is_inventory_postgres():
        return jsonify({"ok": False, "error": "postgres_required"}), 503

    row = get_job(job_id)
    if not row:
        return jsonify({"ok": False, "error": "not_found"}), 404
    return jsonify({"ok": True, "job": _serialize_dealer_job_row(row)})


def api_admin_dealer_job_retry(job_id: int):
    """Site admin: re-queue a failed dealer job."""
    if not is_admin_role(session.get("user_role")):
        return jsonify({"ok": False, "error": "forbidden"}), 403

    ip = _client_ip()
    if not allow_request(f"dealer-job-retry:{ip}", max_events=60, window_seconds=3600.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    from backend.scanner.job_queue import retry_failed_job

    ok, err, data = retry_failed_job(job_id)
    if not ok:
        status = 503 if err == "postgres_required" else 400
        if err == "not_found":
            status = 404
        return jsonify({"ok": False, "error": err, **data}), status
    return jsonify({"ok": True, **data})


def api_admin_dealer_job_diagnose(job_id: int):
    """Site admin: AI/rule diagnosis for a failed dealer job."""
    if not is_admin_role(session.get("user_role")):
        return jsonify({"ok": False, "error": "forbidden"}), 403

    from backend.db.inventory_pg import is_inventory_postgres
    from backend.scanner.job_queue import diagnose_job_row, get_job, _merge_result_diagnosis

    if not is_inventory_postgres():
        return jsonify({"ok": False, "error": "postgres_required"}), 503

    row = get_job(job_id)
    if not row:
        return jsonify({"ok": False, "error": "not_found"}), 404
    from backend.scanner.job_queue import _job_retry_eligible

    if not _job_retry_eligible(row)[0]:
        return jsonify({"ok": False, "error": "not_failed", "status": row.get("status")}), 400

    body = request.get_json(silent=True) or {}
    use_llm = body.get("use_llm", True) is not False
    diagnosis = diagnose_job_row(row, use_llm=use_llm)
    _merge_result_diagnosis(job_id, diagnosis)
    return jsonify({"ok": True, "job_id": job_id, "diagnosis": diagnosis})


def api_admin_dealer_job_smart_retry(job_id: int):
    """Site admin: diagnose then re-queue a failed job with guided retry hints."""
    if not is_admin_role(session.get("user_role")):
        return jsonify({"ok": False, "error": "forbidden"}), 403

    ip = _client_ip()
    if not allow_request(f"dealer-job-smart-retry:{ip}", max_events=30, window_seconds=3600.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    from backend.scanner.job_queue import smart_retry_failed_job

    body = request.get_json(silent=True) or {}
    use_llm = body.get("use_llm", True) is not False
    ok, err, data = smart_retry_failed_job(job_id, use_llm=use_llm)
    if not ok:
        status = 503 if err == "postgres_required" else 400
        if err == "not_found":
            status = 404
        return jsonify({"ok": False, "error": err, **data}), status
    return jsonify({"ok": True, **data})


def register(app) -> None:
    """Attach routes to ``app`` keeping the original bare endpoint names."""
    app.add_url_rule(
        "/api/admin/dealer-onboard", view_func=api_admin_dealer_onboard, methods=["POST"]
    )
    app.add_url_rule("/api/admin/dealer-jobs", view_func=api_admin_dealer_jobs)
    app.add_url_rule(
        "/api/admin/dealer-jobs/<int:job_id>", view_func=api_admin_dealer_job_detail
    )
    app.add_url_rule(
        "/api/admin/dealer-jobs/<int:job_id>/retry",
        view_func=api_admin_dealer_job_retry,
        methods=["POST"],
    )
    app.add_url_rule(
        "/api/admin/dealer-jobs/<int:job_id>/diagnose",
        view_func=api_admin_dealer_job_diagnose,
        methods=["POST"],
    )
    app.add_url_rule(
        "/api/admin/dealer-jobs/<int:job_id>/smart-retry",
        view_func=api_admin_dealer_job_smart_retry,
        methods=["POST"],
    )
