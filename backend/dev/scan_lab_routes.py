"""Register dev scan-lab routes on the existing ``dev`` blueprint."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from flask import Blueprint, abort, jsonify, make_response, render_template, request, session, url_for

from backend.dev import scan_lab as sl
from backend.db.inventory_db import _UPDATABLE_CAR_COLUMNS, get_car_by_id


def _scan_lab_nav_context() -> dict[str, Any]:
    return {"admin_username": session.get("admin_username") or ""}


def register_scan_lab_routes(dev_bp: Blueprint) -> None:
    @dev_bp.route("/scan-lab")
    def scan_lab_home():
        cfg = sl.manifest_lab_config()
        summary = sl.inventory_summary_for_manifest()
        active = sl.active_scan_lab_job()
        return render_template(
            "dev_scan_lab.html",
            lab=cfg,
            summary=summary,
            active_job=active,
            db_counts=sl.db_table_counts(),
            **_scan_lab_nav_context(),
        )

    @dev_bp.route("/scan-lab/listings")
    def scan_lab_listings():
        cfg = sl.manifest_lab_config()
        dealer_id = (request.args.get("dealer_id") or "").strip()
        return render_template(
            "dev_scan_lab_listings.html",
            lab=cfg,
            filter_dealer_id=dealer_id,
            **_scan_lab_nav_context(),
        )

    @dev_bp.route("/scan-lab/db")
    def scan_lab_db():
        cfg = sl.manifest_lab_config()
        q = (request.args.get("q") or "").strip()
        dealer_id = (request.args.get("dealer_id") or "").strip()
        page = sl.list_scan_lab_cars(
            dealer_id=dealer_id or None,
            q=q or None,
            limit=50,
            offset=0,
        )
        return render_template(
            "dev_scan_lab_db.html",
            lab=cfg,
            cars=page["cars"],
            total=page["total"],
            q=q,
            filter_dealer_id=dealer_id,
            updatable_columns=sorted(_UPDATABLE_CAR_COLUMNS),
            **_scan_lab_nav_context(),
        )

    @dev_bp.route("/scan-lab/car/<int:car_id>")
    def scan_lab_car_detail(car_id: int):
        """Full VDP under dev auth — same template as public ``/car/<id>``."""
        car_raw = get_car_by_id(car_id, include_inactive=False)
        if not car_raw or not sl.car_in_manifest_scope(car_raw):
            abort(404)
        from backend.main import _build_car_detail_view_context

        ctx = _build_car_detail_view_context(car_id, car_raw)
        ctx["logged_in"] = True
        ctx["scan_lab_mode"] = True
        ctx["scan_lab_back_url"] = url_for("dev.scan_lab_listings")
        admin_name = session.get("admin_username") or "dev"
        resp = make_response(
            render_template(
                "car.html",
                **ctx,
                logged_in_user=admin_name,
                admin_username=admin_name,
                has_paid_access=True,
                is_admin=True,
                billing_stripe_enabled=False,
            )
        )
        resp.headers["Cache-Control"] = "private, no-cache"
        return resp

    @dev_bp.route("/scan-lab/api/summary")
    def scan_lab_api_summary():
        force = (request.args.get("refresh") or "").strip().lower() in ("1", "true", "yes")
        return jsonify(
            {"ok": True, "summary": sl.inventory_summary_for_manifest(force_refresh=force)}
        )

    @dev_bp.route("/scan-lab/api/cars")
    def scan_lab_api_cars():
        dealer_id = (request.args.get("dealer_id") or "").strip() or None
        q = (request.args.get("q") or "").strip() or None
        try:
            limit = int(request.args.get("limit") or 50)
        except (TypeError, ValueError):
            limit = 50
        try:
            offset = int(request.args.get("offset") or 0)
        except (TypeError, ValueError):
            offset = 0
        page = sl.list_scan_lab_cars(
            dealer_id=dealer_id,
            q=q,
            limit=limit,
            offset=offset,
        )
        return jsonify({"ok": True, **page})

    @dev_bp.route("/scan-lab/api/scan-runs")
    def scan_lab_api_scan_runs():
        runs = sl.list_scan_runs_for_manifest(limit=80)
        return jsonify({"ok": True, "runs": runs, "count": len(runs)})

    @dev_bp.route("/scan-lab/api/scan/start", methods=["POST"])
    def scan_lab_api_scan_start():
        data = request.get_json(silent=True) or {}
        manifest_raw = (data.get("manifest_path") or "").strip()
        mp = Path(manifest_raw) if manifest_raw else None
        if mp is not None and not mp.is_absolute():
            mp = sl.PROJECT_ROOT / mp
        job_id, err = sl.start_manifest_scan(manifest_path=mp)
        if err:
            code = 409 if err == "scan_already_running" else 400
            return jsonify({"ok": False, "error": err}), code
        sl.invalidate_scan_lab_summary_cache()
        return jsonify({"ok": True, "job_id": job_id})

    @dev_bp.route("/scan-lab/api/scan/active")
    def scan_lab_api_scan_active():
        job = sl.active_scan_lab_job()
        return jsonify({"ok": True, "job": job})

    @dev_bp.route("/scan-lab/api/scan/<job_id>")
    def scan_lab_api_scan_job(job_id: str):
        job = sl.get_scan_lab_job(job_id)
        if not job:
            return jsonify(
                {
                    "ok": False,
                    "error": "job_expired",
                    "message": "Server restarted — refresh and start again.",
                }
            )
        return jsonify({"ok": True, **job})

    @dev_bp.route("/scan-lab/api/db/car/<int:car_id>")
    def scan_lab_api_db_car_get(car_id: int):
        row = sl.get_scan_lab_car_row(car_id)
        if not row:
            return jsonify({"ok": False, "error": "not_found"}), 404
        return jsonify({"ok": True, "car": row})

    @dev_bp.route("/scan-lab/api/db/car/<int:car_id>", methods=["PATCH"])
    def scan_lab_api_db_car_patch(car_id: int):
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"ok": False, "error": "json_object"}), 400
        fields: dict[str, Any] = {}
        for k, v in data.items():
            if k in _UPDATABLE_CAR_COLUMNS:
                fields[k] = v
        if not fields:
            return jsonify({"ok": False, "error": "no_updatable_fields"}), 400
        if not sl.patch_scan_lab_car(car_id, fields):
            return jsonify({"ok": False, "error": "not_found"}), 404
        return jsonify({"ok": True, "car_id": car_id, "updated_fields": sorted(fields.keys())})
