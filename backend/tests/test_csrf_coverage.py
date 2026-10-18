"""Every state-changing URL rule is CSRF-protected or explicitly classified (P3.4).

The app-wide CSRF hook (``backend/web/security.py``) matches bare endpoint
names, so a new POST/PUT/PATCH/DELETE route is unprotected until someone adds
it to a list. This test walks ``app.url_map`` and, for every mutating
(rule, method) pair, decides how it is protected:

* ``hook``: running the real CSRF hooks for that request (the app-wide
  ``_csrf_mutating_requests`` plus the blueprint's own hook, ``dev`` and
  ``dev_console``) reaches a CSRF validator. The validators are swapped for one
  that raises a sentinel, so no view runs and nothing is written. This covers
  the form and header lists, the ``dealer_portal.`` / ``store_admin.`` /
  ``api_admin_operator_`` prefixes, and both blueprint hooks.
* ``in_view``: the view calls ``validate_csrf_header()`` itself (community
  comments; checked from source below).
* ``exempt``: Stripe webhooks, verified by ``Stripe-Signature`` instead.
* ``mfa_stub``: 2FA was removed; these endpoints only redirect.

Anything else fails ``test_every_mutating_rule_is_csrf_protected_or_classified``.
Fix it by protecting the route (preferred) or, with a reason, classifying it here.

Note: ``store_admin.admin_users_hub`` / ``admin_dealers_hub`` also call
``validate_csrf_form()`` in-view, but the ``store_admin.`` prefix rule covers them
first, so they classify as ``hook``.
"""

from __future__ import annotations

import inspect
import sys

import pytest
from flask import request, session
from werkzeug.exceptions import HTTPException

MUTATING = ("POST", "PUT", "PATCH", "DELETE")

# Before-request hooks that run a CSRF validator, by blueprint (None = app-wide).
CSRF_HOOKS = {
    None: "_csrf_mutating_requests",  # backend/web/security.py
    "dev": "_dev_require_admin",  # backend/dev/routes.py (all mutating methods)
    "dev_console": "_dev_console_csrf",  # backend/dev/console.py (all mutating methods)
}

# (endpoint, method) -> helper in the view's module that must call validate_csrf_header().
IN_VIEW = {
    ("api_car_comments", "POST"): "_create_comment",
    ("api_dealer_comments", "POST"): "_create_comment",
    ("api_car_comment_delete", "DELETE"): "_delete_comment",
    ("api_dealer_comment_delete", "DELETE"): "_delete_comment",
    ("api_car_comment_flag", "POST"): "_flag_comment",
    ("api_dealer_comment_flag", "POST"): "_flag_comment",
}

# Server-to-server, verified by the Stripe-Signature header (billing/routes.py).
EXEMPT = {
    ("billing.stripe_webhook", "POST"),
    ("billing.premium_webhook", "POST"),
}

# 2FA removed: redirect-only handlers (auth/pages.py:280-292, dev/routes.py dev_mfa_gone;
# the /dev hook skips CSRF for dev.dev_mfa_gone on purpose).
MFA_STUB_PREFIXES = ("mfa_legacy_",)
MFA_STUB_ENDPOINTS = {"mfa_qr_complete_gone", "mfa_qr_confirm_gone", "dev.dev_mfa_gone"}
MFA_STUB_VIEWS = {"_app_mfa_gone", "mfa_qr_complete_gone", "mfa_qr_confirm_gone", "dev_mfa_gone"}

_VALIDATORS = ("validate_csrf_form", "validate_csrf_header")


class _CsrfChecked(Exception):
    """Raised by the stand-in validators: the hook reached a CSRF check."""


def _checked(*_args, **_kwargs):
    raise _CsrfChecked


def _csrf_hooks(app) -> dict:
    found = {}
    for bp, funcs in app.before_request_funcs.items():
        for fn in funcs:
            if getattr(fn, "__name__", None) == CSRF_HOOKS.get(bp):
                found[bp] = fn
    return found


def _mutating_pairs(app):
    for rule in app.url_map.iter_rules():
        for method in MUTATING:
            if method in (rule.methods or ()):
                yield rule, method


def _hooks_check_csrf(app, hooks, rule, method) -> bool:
    """Run the CSRF hooks for a logged-in request without a token; True if a validator ran."""
    url = app.url_map.bind("localhost").build(rule.endpoint, {arg: "1" for arg in rule.arguments}, method=method)
    with app.test_request_context(url, method=method):
        assert request.endpoint == rule.endpoint, (url, request.endpoint, rule.endpoint)
        session["user_id"] = 1
        chain = [hooks[None]] + [hooks[bp] for bp in request.blueprints if bp in hooks]
        for fn in chain:
            try:
                fn()
            except _CsrfChecked:
                return True
            except HTTPException:
                return False
    return False


def _is_mfa_stub(ep: str) -> bool:
    return ep in MFA_STUB_ENDPOINTS or ep.startswith(MFA_STUB_PREFIXES)


def classify(app, monkeypatch) -> tuple[dict[str, list[tuple[str, str, str]]], list[tuple[str, str, str]]]:
    """Return ({category: [(endpoint, method, rule)]}, unclassified)."""
    hooks = _csrf_hooks(app)
    assert set(hooks) == set(CSRF_HOOKS), f"CSRF hooks missing: {set(CSRF_HOOKS) - set(hooks)}"
    import backend.utils.csrf as csrf_mod

    seen: dict[str, list[tuple[str, str, str]]] = {"hook": [], "in_view": [], "exempt": [], "mfa_stub": []}
    unclassified: list[tuple[str, str, str]] = []
    with monkeypatch.context() as mp:
        # The /dev hook imports the validators at call time; security.py at import time.
        for name in _VALIDATORS:
            mp.setattr(csrf_mod, name, _checked)
            for fn in hooks.values():
                if name in fn.__globals__:
                    mp.setitem(fn.__globals__, name, _checked)
        for rule, method in _mutating_pairs(app):
            ep = rule.endpoint
            entry = (ep, method, rule.rule)
            if _hooks_check_csrf(app, hooks, rule, method):
                seen["hook"].append(entry)
            elif (ep, method) in IN_VIEW:
                seen["in_view"].append(entry)
            elif (ep, method) in EXEMPT:
                seen["exempt"].append(entry)
            elif _is_mfa_stub(ep):
                seen["mfa_stub"].append(entry)
            else:
                unclassified.append(entry)
    return seen, unclassified


@pytest.fixture
def app(app_factory):
    return app_factory(DEV_IP_ALLOWLIST="").app


def test_every_mutating_rule_is_csrf_protected_or_classified(app, monkeypatch):
    seen, unclassified = classify(app, monkeypatch)
    assert unclassified == [], (
        "state-changing routes with no CSRF check and no classification; add them to a list in "
        f"backend/web/security.py (or classify them in this file with a reason): {unclassified}"
    )
    # The operator DELETE routes are covered by the app-wide hook (P3.4).
    hooked = {(ep, m) for ep, m, _ in seen["hook"]}
    assert ("api_admin_operator_delete_incomplete_car", "DELETE") in hooked
    assert ("api_admin_operator_api_delete_dealer", "DELETE") in hooked
    assert seen["hook"], "the hook classifier found nothing; it is not exercising the hooks"


def test_classification_entries_match_real_rules(app):
    """No stale entries: each classified pair exists, and each claim holds for its view."""
    pairs = {(rule.endpoint, method) for rule, method in _mutating_pairs(app)}
    for key in list(IN_VIEW) + sorted(EXEMPT):
        assert key in pairs, f"{key} is classified but no such mutating rule exists"
    for ep in MFA_STUB_ENDPOINTS:
        assert any(e == ep for e, _ in pairs), f"{ep} is classified but not a mutating rule"

    for (ep, _method), helper_name in IN_VIEW.items():
        view = app.view_functions[ep]
        helper = getattr(sys.modules[view.__module__], helper_name)
        assert f"{helper_name}(" in inspect.getsource(view), (ep, helper_name)
        assert "validate_csrf_header()" in inspect.getsource(helper), helper_name

    for ep, _method in EXEMPT:
        src = inspect.getsource(app.view_functions[ep])
        assert "Stripe-Signature" in src and "invalid_signature" in src, ep

    for ep, _method in pairs:
        if _is_mfa_stub(ep):
            view = app.view_functions[ep]
            assert view.__name__ in MFA_STUB_VIEWS, (ep, view.__name__)
            src = inspect.getsource(view)
            assert "redirect(" in src or "return _app_mfa_gone()" in src, ep


def test_an_unclassified_dummy_post_fails_the_coverage_check(app_factory, monkeypatch):
    main = app_factory(DEV_IP_ALLOWLIST="")
    app = main.app
    try:
        app.add_url_rule("/__csrf_probe/post", endpoint="csrf_probe_post", view_func=lambda: "", methods=["POST"])
        app.add_url_rule("/__csrf_probe/delete", endpoint="csrf_probe_delete", view_func=lambda: "", methods=["DELETE"])
        # The operator prefix covers POST and DELETE only: a PUT under it is not protected.
        app.add_url_rule(
            "/__csrf_probe/operator-put",
            endpoint="api_admin_operator_csrf_probe_put",
            view_func=lambda: "",
            methods=["PUT"],
        )
        # Positive control: a POST under the operator prefix is protected by the hook.
        app.add_url_rule(
            "/__csrf_probe/operator-post",
            endpoint="api_admin_operator_csrf_probe_post",
            view_func=lambda: "",
            methods=["POST"],
        )
        seen, unclassified = classify(app, monkeypatch)
        assert sorted((ep, m) for ep, m, _ in unclassified) == [
            ("api_admin_operator_csrf_probe_put", "PUT"),
            ("csrf_probe_delete", "DELETE"),
            ("csrf_probe_post", "POST"),
        ]
        assert ("api_admin_operator_csrf_probe_post", "POST") in {(ep, m) for ep, m, _ in seen["hook"]}
    finally:
        # backend.main.app is shared by tests that import it directly (e.g. the
        # app-surface golden): rebuild it without the probe routes.
        app_factory(DEV_IP_ALLOWLIST="")


# --- the operator DELETE routes, end to end ---------------------------------


def _admin_client(app_factory, monkeypatch, tmp_path):
    from backend.tests.test_admin_operator_api import _login_admin, _make_app

    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        _login_admin(client, tmp_path)
    with client.session_transaction() as sess:
        assert sess.get("user_id"), "admin login failed"
        token = sess.get("_csrf_token")
    assert token
    return client, token


def test_operator_incomplete_car_delete_without_token_is_403(app_factory, monkeypatch, tmp_path):
    calls: list[int] = []

    def fake_delete(car_id):
        calls.append(car_id)
        return {"ok": True, "deleted": car_id}, 200

    monkeypatch.setattr("backend.dealer.admin.operator_api.delete_incomplete_car", fake_delete)
    client, token = _admin_client(app_factory, monkeypatch, tmp_path)

    rv = client.delete("/api/admin/operator/incomplete-cars/1")
    assert rv.status_code == 403
    rv = client.delete("/api/admin/operator/incomplete-cars/1", headers={"X-CSRF-Token": "wrong-" + token})
    assert rv.status_code == 403
    assert calls == []

    rv = client.delete("/api/admin/operator/incomplete-cars/1", headers={"X-CSRF-Token": token})
    assert rv.status_code == 200
    assert calls == [1]


def test_operator_dealer_delete_without_token_is_403(app_factory, monkeypatch, tmp_path):
    client, _token = _admin_client(app_factory, monkeypatch, tmp_path)
    rv = client.delete("/api/admin/operator/dealer/1")
    assert rv.status_code == 403


def test_anonymous_operator_delete_still_gets_the_views_403(app_factory, monkeypatch, tmp_path):
    from backend.tests.test_admin_operator_api import _make_app

    app = _make_app(app_factory, monkeypatch, tmp_path)
    rv = app.test_client().delete("/api/admin/operator/incomplete-cars/1")
    assert rv.status_code == 403
    assert (rv.get_json() or {}).get("error") == "forbidden"
