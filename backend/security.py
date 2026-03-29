import logging
import os
import secrets

from flask import Flask, jsonify, redirect, request, url_for
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_login import LoginManager
from flask_wtf.csrf import CSRFProtect

logger = logging.getLogger(__name__)

csrf = CSRFProtect()
login_manager = LoginManager()
limiter = Limiter(key_func=get_remote_address, storage_uri="memory://")

login_manager.login_view = "login_page"
login_manager.session_protection = "strong"


def _configure_secret_key(app: Flask) -> None:
    secret = os.environ.get("SECRET_KEY")
    if not secret:
        secret = secrets.token_hex(32)
        logger.warning(
            "SECRET_KEY is not set; using an ephemeral key (sessions reset on restart). "
            "Set SECRET_KEY in the environment for production."
        )
    app.config["SECRET_KEY"] = secret


def _configure_session_cookies(app: Flask) -> None:
    secure = os.environ.get("SESSION_COOKIE_SECURE", "").lower() in ("1", "true", "yes")
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=secure,
    )


def init_security(app: Flask) -> None:
    _configure_secret_key(app)
    _configure_session_cookies(app)
    app.config.setdefault("WTF_CSRF_TIME_LIMIT", None)

    csrf.init_app(app)
    login_manager.init_app(app)
    limiter.init_app(app)

    @login_manager.unauthorized_handler
    def _unauthorized():
        if request.path.startswith("/api/"):
            return jsonify({"ok": False, "error": "Authentication required"}), 401
        return redirect(url_for("login_page", next=request.url))
