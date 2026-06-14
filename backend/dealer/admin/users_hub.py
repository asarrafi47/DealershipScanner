"""Site-admin user management at ``/admin/users``."""

from __future__ import annotations

import os

from flask import flash, redirect, render_template, request, session, url_for

from backend.dealer.admin.routes import _session_profile, store_admin_bp
from backend.db.users_db import (
    ADMIN_ASSIGNABLE_ROLES,
    admin_create_user,
    admin_reset_user_password,
    admin_update_user,
    delete_user_by_id,
    get_user_admin_record,
    list_users_for_admin,
    set_user_is_active,
)
from backend.utils.csrf import validate_csrf_form
from backend.utils.roles import is_admin_role

_MIN_PASSWORD_LEN = max(8, int(os.environ.get("MIN_PASSWORD_LENGTH", "8")))

_ROLE_LABELS = {
    "admin": "Site administrator",
    "general_user": "Member",
    "dealership_owner": "Dealership owner",
    "dealership_admin": "Dealership admin",
    "dealership_member": "Dealership member",
    "dealer_staff": "Store staff (legacy)",
}


def _require_site_admin():
    p = _session_profile()
    if not p or not is_admin_role(p.get("role")):
        return None
    return p


def _self_user_id() -> int:
    try:
        return int(session.get("user_id") or 0)
    except (TypeError, ValueError):
        return 0


def _redirect_users(q: str = "") -> str:
    if q.strip():
        return url_for("store_admin.admin_users_hub", q=q.strip())
    return url_for("store_admin.admin_users_hub")


@store_admin_bp.route("/users", methods=["GET", "POST"])
def admin_users_hub():
    """List app users; POST handles suspend/activate from list."""
    if not _require_site_admin():
        flash("User management requires a site admin account.", "error")
        return redirect(url_for("store_admin.admin_site_hub"))

    if request.method == "POST":
        validate_csrf_form()
        action = (request.form.get("action") or "").strip().lower()
        try:
            target_id = int(request.form.get("user_id") or 0)
        except (TypeError, ValueError):
            target_id = 0
        self_id = _self_user_id()
        q = (request.form.get("q") or "").strip()

        if target_id <= 0:
            flash("Invalid user.", "error")
        elif action == "suspend":
            if target_id == self_id:
                flash("You cannot suspend your own account.", "error")
            elif set_user_is_active(target_id, active=False):
                flash(f"Suspended user #{target_id}.", "success")
            else:
                flash("Could not suspend user.", "error")
        elif action == "activate":
            if set_user_is_active(target_id, active=True):
                flash(f"Reactivated user #{target_id}.", "success")
            else:
                flash("Could not reactivate user.", "error")
        elif action == "delete":
            if target_id == self_id:
                flash("You cannot delete your own account.", "error")
            else:
                ok, err = delete_user_by_id(target_id)
                if ok:
                    flash(f"Deleted user #{target_id}.", "success")
                else:
                    flash(err or "Could not delete user.", "error")
        else:
            flash("Unknown action.", "error")
        return redirect(_redirect_users(q))

    self_id = _self_user_id()
    search = (request.args.get("q") or "").strip()
    users = list_users_for_admin(search=search, limit=200)
    stats = {
        "total": len(users),
        "premium": sum(
            1 for u in users if u.get("is_premium") or (u.get("plan_label") or "free") != "free"
        ),
        "verified": sum(1 for u in users if u.get("email_verified")),
        "stripe": sum(1 for u in users if u.get("stripe_linked")),
        "suspended": sum(1 for u in users if not u.get("is_active")),
    }
    return render_template(
        "admin/users.html",
        users=users,
        search=search,
        user_stats=stats,
        self_user_id=self_id,
        role_labels=_ROLE_LABELS,
    )


@store_admin_bp.route("/users/new", methods=["GET", "POST"])
def admin_users_new():
    if not _require_site_admin():
        flash("User management requires a site admin account.", "error")
        return redirect(url_for("store_admin.admin_site_hub"))

    if request.method == "POST":
        validate_csrf_form()
        uid, err = admin_create_user(
            username=(request.form.get("username") or "").strip(),
            email=(request.form.get("email") or "").strip(),
            password=request.form.get("password") or "",
            role=(request.form.get("role") or "general_user").strip(),
            dealer_id=(request.form.get("dealer_id") or "").strip() or None,
            dealership_registry_id=_parse_registry_id(request.form.get("dealership_registry_id")),
            min_password_len=_MIN_PASSWORD_LEN,
        )
        if err:
            flash(err, "error")
            return render_template(
                "admin/user_form.html",
                form_mode="create",
                user=None,
                form_data=request.form,
                role_choices=ADMIN_ASSIGNABLE_ROLES,
                role_labels=_ROLE_LABELS,
                min_password_len=_MIN_PASSWORD_LEN,
            )
        flash(f"Created user #{uid}.", "success")
        return redirect(url_for("store_admin.admin_users_edit", user_id=uid))

    return render_template(
        "admin/user_form.html",
        form_mode="create",
        user=None,
        form_data=None,
        role_choices=ADMIN_ASSIGNABLE_ROLES,
        role_labels=_ROLE_LABELS,
        min_password_len=_MIN_PASSWORD_LEN,
    )


@store_admin_bp.route("/users/<int:user_id>/edit", methods=["GET", "POST"])
def admin_users_edit(user_id: int):
    if not _require_site_admin():
        flash("User management requires a site admin account.", "error")
        return redirect(url_for("store_admin.admin_site_hub"))

    user = get_user_admin_record(user_id)
    if not user:
        flash("User not found.", "error")
        return redirect(url_for("store_admin.admin_users_hub"))

    self_id = _self_user_id()

    if request.method == "POST":
        validate_csrf_form()
        action = (request.form.get("action") or "save").strip().lower()

        if action == "reset_password":
            new_pw = request.form.get("new_password") or ""
            confirm = request.form.get("new_password_confirm") or ""
            if new_pw != confirm:
                flash("Passwords do not match.", "error")
            else:
                err = admin_reset_user_password(
                    user_id,
                    new_pw,
                    min_password_len=_MIN_PASSWORD_LEN,
                )
                if err:
                    flash(err, "error")
                else:
                    flash(f"Password updated for user #{user_id}.", "success")
            return redirect(url_for("store_admin.admin_users_edit", user_id=user_id))

        if action == "delete":
            if user_id == self_id:
                flash("You cannot delete your own account.", "error")
            else:
                ok, err = delete_user_by_id(user_id)
                if ok:
                    flash(f"Deleted user #{user_id}.", "success")
                    return redirect(url_for("store_admin.admin_users_hub"))
                flash(err or "Could not delete user.", "error")
            return redirect(url_for("store_admin.admin_users_edit", user_id=user_id))

        err = admin_update_user(
            user_id,
            username=(request.form.get("username") or "").strip(),
            email=(request.form.get("email") or "").strip(),
            role=(request.form.get("role") or "general_user").strip(),
            dealer_id=(request.form.get("dealer_id") or "").strip() or None,
            dealership_registry_id=_parse_registry_id(request.form.get("dealership_registry_id")),
            actor_user_id=self_id,
        )
        if err:
            flash(err, "error")
            return render_template(
                "admin/user_form.html",
                form_mode="edit",
                user=user,
                form_data=request.form,
                role_choices=ADMIN_ASSIGNABLE_ROLES,
                role_labels=_ROLE_LABELS,
                min_password_len=_MIN_PASSWORD_LEN,
                self_user_id=self_id,
            )
        flash(f"Saved user #{user_id}.", "success")
        return redirect(url_for("store_admin.admin_users_edit", user_id=user_id))

    return render_template(
        "admin/user_form.html",
        form_mode="edit",
        user=user,
        form_data=None,
        role_choices=ADMIN_ASSIGNABLE_ROLES,
        role_labels=_ROLE_LABELS,
        min_password_len=_MIN_PASSWORD_LEN,
        self_user_id=self_id,
    )


def _parse_registry_id(raw: str | None) -> int | None:
    if raw is None or str(raw).strip() == "":
        return None
    try:
        val = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return val if val > 0 else None
