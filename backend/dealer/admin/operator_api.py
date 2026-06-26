"""Site-admin JSON APIs for operator tools (migrated from legacy ``/dev/api``)."""

from __future__ import annotations

from typing import Any

from flask import Flask, jsonify, request, session

from backend.dealer.admin.incomplete_listings_api import (
    delete_incomplete_car,
    export_incomplete_issue,
    incomplete_cars_response,
)
from backend.utils.roles import is_admin_role


def _require_site_admin_api() -> tuple[Any, int] | None:
    if not session.get("user_id") or not is_admin_role(session.get("user_role")):
        return jsonify({"ok": False, "error": "forbidden"}), 403
    return None


def register_admin_operator_api(app: Flask) -> None:
    """Register ``/api/admin/operator/*`` routes on the app (app session + site admin role)."""

    @app.route("/api/admin/operator/incomplete-cars")
    def api_admin_operator_incomplete_cars():
        gate = _require_site_admin_api()
        if gate:
            return gate
        return incomplete_cars_response()

    @app.route("/api/admin/operator/incomplete-export", methods=["POST"])
    @app.route("/api/admin/operator/incomplete-cars/log-issue", methods=["POST"])
    def api_admin_operator_incomplete_export():
        gate = _require_site_admin_api()
        if gate:
            return gate
        payload = request.get_json(silent=True) or {}
        issue = str(payload.get("issue") or "").strip()
        return export_incomplete_issue(issue)

    @app.route("/api/admin/operator/incomplete-cars/<int:car_id>", methods=["DELETE"])
    def api_admin_operator_delete_incomplete_car(car_id: int):
        gate = _require_site_admin_api()
        if gate:
            return gate
        return delete_incomplete_car(car_id)

    _delegate_dev_operator_routes(app)


def _delegate_dev_operator_routes(app: Flask) -> None:
    """Proxy scanner/registry maintenance handlers from ``backend.dev.routes``."""
    from backend.dev import routes as dev_routes

    def _wrap(handler):
        def inner(*args, **kwargs):
            gate = _require_site_admin_api()
            if gate:
                return gate
            return handler(*args, **kwargs)

        inner.__name__ = getattr(handler, "__name__", "operator_proxy")
        return inner

    routes: list[tuple[str, str, Any, list[str]]] = [
        ("GET", "/api/admin/operator/status", dev_routes.api_dev_status, []),
        ("GET", "/api/admin/operator/dealers", dev_routes.api_dev_dealers, []),
        ("GET", "/api/admin/operator/dealership-stats", dev_routes.api_dev_dealership_stats, []),
        ("POST", "/api/admin/operator/geocode-missing", dev_routes.api_geocode_missing, []),
        ("POST", "/api/admin/operator/deduplicate", dev_routes.api_deduplicate, []),
        ("POST", "/api/admin/operator/smart-import", dev_routes.api_smart_import, []),
        ("POST", "/api/admin/operator/smart-import-bulk", dev_routes.api_smart_import_bulk, []),
        ("GET", "/api/admin/operator/scanner-job/<job_id>", dev_routes.api_scanner_job, ["job_id"]),
        ("GET", "/api/admin/operator/import-queue/<queue_id>", dev_routes.api_import_queue, ["queue_id"]),
        ("DELETE", "/api/admin/operator/dealer/<int:dealer_id>", dev_routes.api_delete_dealer, ["dealer_id"]),
    ]

    for method, path, handler, extra_args in routes:
        view = _wrap(handler)
        app.add_url_rule(path, endpoint=f"api_admin_operator_{handler.__name__}", view_func=view, methods=[method])
