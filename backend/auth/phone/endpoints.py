"""The endpoint names that exist only while phone sign-in is on (card S8, minimal S9).

A leaf module with no imports, so ``backend/web/security.py`` (which runs the CSRF hook) and
``backend/auth/phone/routes.py`` can both read it without a cycle.

While ``AUTH_PHONE_LOGIN_ENABLED`` is off every one of these answers 404, a POST without a CSRF token
included: ``security._csrf_mutating_requests`` checks the flag before it checks the token, so a
caller cannot learn from a redirect or a 403 that the route is there.
"""

from __future__ import annotations

PHONE_FLAG_ENDPOINTS: frozenset[str] = frozenset(
    {
        # HTML pages and forms (endpoint names keep "login" so a bad CSRF token redirects to /login).
        "login_phone_page",  # GET  /login/phone
        "login_phone_start",  # POST /login/phone
        "login_phone_code_page",  # GET  /login/phone/code
        "login_phone_code_verify",  # POST /login/phone/code
        "login_phone_resend",  # POST /login/phone/resend
        "login_phone_change",  # POST /login/phone/change (use a different number)
        # The old email and password form, moved here when /login becomes the phone page.
        "staff_login_page",  # GET, POST /staff/login
        # JSON twins for the iOS app (header CSRF token).
        "api_auth_phone_start",  # POST /api/auth/phone/start
        "api_auth_phone_verify",  # POST /api/auth/phone/verify
        "api_auth_phone_resend",  # POST /api/auth/phone/resend
    }
)

# The POST subset that carries an HTML form with the csrf_token field, and the JSON subset that
# sends X-CSRF-Token. ``security.py`` adds them to its two lists.
PHONE_FORM_POST_ENDPOINTS: tuple[str, ...] = (
    "login_phone_start",
    "login_phone_code_verify",
    "login_phone_resend",
    "login_phone_change",
    "staff_login_page",
)
PHONE_HEADER_POST_ENDPOINTS: tuple[str, ...] = (
    "api_auth_phone_start",
    "api_auth_phone_verify",
    "api_auth_phone_resend",
)
