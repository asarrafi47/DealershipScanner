"""DealershipScanner Flask application."""

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

import os
import sqlite3

from flask import Flask, jsonify, redirect, render_template, request, send_from_directory

from backend.ai_agent import run_car_page_chat
from backend.dev_console import register_dev_console
from backend.dev_routes import dev_bp
from backend.db.admin_users_db import init_admin_db
from backend.db.inventory_db import (
    get_car_by_id,
    get_filter_options,
    init_inventory_db,
    search_cars,
)
from backend.db.users_db import check_user, init_users_db, save_user
from backend.hybrid_inventory_search import (
    flask_request_to_search_cars_kwargs,
    hybrid_search_with_kwargs,
)
from backend.knowledge_engine import prepare_car_detail_context
from backend.listings import listings_page
from backend.utils.car_serialize import format_display_value, serialize_car_for_api
from backend.utils.oem_links import mopar_vin_lookup_url
from backend.utils.client_ip import client_ip as _client_ip_from_request
from backend.utils.csrf import ensure_csrf_token, validate_csrf_form, validate_csrf_header
from backend.utils.ip_rate_limit import allow_request
from backend.utils.query_parser import parse_natural_query
from backend.utils.registration_validation import registration_form_error
from backend.utils.runtime_env import is_production_env, session_cookie_secure_default

_MIN_PASSWORD_LEN = max(8, int(os.environ.get("MIN_PASSWORD_LENGTH", "8")))
_CHAT_MAX_MESSAGE = int(os.environ.get("CHAT_MAX_MESSAGE_CHARS", "4000"))
_CHAT_MAX_BODY = int(os.environ.get("CHAT_MAX_BODY_BYTES", "65536"))
_SMART_SEARCH_RPM = int(os.environ.get("RATE_LIMIT_SMART_SEARCH_PER_MIN", "90"))
_CHAT_RPM = int(os.environ.get("RATE_LIMIT_CAR_CHAT_PER_MIN", "40"))
_LOGIN_RPM = int(os.environ.get("RATE_LIMIT_LOGIN_PER_MIN", "30"))
_REGISTER_RPM = int(os.environ.get("RATE_LIMIT_REGISTER_PER_MIN", "10"))


def _client_ip() -> str:
    return _client_ip_from_request(request)


app = Flask(
    __name__,
    template_folder="../frontend/templates",
    static_folder="../frontend/static",
)

_raw_secret = (os.environ.get("SECRET_KEY") or os.environ.get("FLASK_SECRET_KEY") or "").strip()
if is_production_env():
    if not _raw_secret:
        raise RuntimeError(
            "SECRET_KEY or FLASK_SECRET_KEY must be set when FLASK_ENV=production (SEC-001)."
        )
    app.secret_key = _raw_secret
else:
    app.secret_key = _raw_secret or "dealership-scanner-dev-insecure"

app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = session_cookie_secure_default()

init_users_db()
init_admin_db()
init_inventory_db()
app.register_blueprint(dev_bp, url_prefix="/dev")
register_dev_console(app)


@app.context_processor
def inject_csrf_and_flags():
    return {
        "csrf_token": ensure_csrf_token(),
        "is_production": is_production_env(),
    }


@app.before_request
def _csrf_mutating_requests():
    if request.method != "POST":
        return
    ep = request.endpoint or ""
    if ep in ("login_page", "register_page"):
        validate_csrf_form()
    elif ep in ("api_search_smart", "api_car_chat"):
        validate_csrf_header()


# Report-only CSP (SEC-032): opt-in via CSP_REPORT_ONLY=1 after public pages avoid inline script/style.
_CSP_REPORT_ONLY = (
    "default-src 'self'; "
    "base-uri 'self'; "
    "form-action 'self'; "
    "frame-ancestors 'none'; "
    "object-src 'none'; "
    "img-src 'self' data: https: http: blob:; "
    "font-src 'self' https://fonts.gstatic.com data:; "
    "style-src 'self' https://fonts.googleapis.com; "
    "script-src 'self' https://esm.sh; "
    "connect-src 'self' https://esm.sh https://fonts.googleapis.com; "
    "worker-src 'self'; "
)


@app.after_request
def _csp_report_only_header(response):
    if (os.environ.get("CSP_REPORT_ONLY") or "").strip().lower() not in ("1", "true", "yes", "on"):
        return response
    if response.headers.get("Content-Security-Policy-Report-Only"):
        return response
    response.headers["Content-Security-Policy-Report-Only"] = _CSP_REPORT_ONLY
    return response


@app.template_filter("fmt_spec")
def _jinja_fmt_spec(value):
    return format_display_value(value)


@app.route("/favicon.ico")
def favicon():
    return send_from_directory(app.static_folder, "favicon.svg", mimetype="image/svg+xml")


@app.route("/")
def home():
    return redirect("/login")


@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
        ip = _client_ip()
        if not allow_request(f"login:{ip}", max_events=_LOGIN_RPM, window_seconds=60.0):
            return render_template("login.html", error="Too many login attempts. Try again in a minute."), 429
        login_input = request.form["login"]
        password = request.form["password"]
        if check_user(login_input, password):
            return redirect("/dashboard")
        return render_template("login.html", error="Invalid username/email or password.")
    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
def register_page():
    if request.method == "POST":
        ip = _client_ip()
        if not allow_request(f"register:{ip}", max_events=_REGISTER_RPM, window_seconds=60.0):
            return render_template("register.html", error="Too many registration attempts. Try again later."), 429
        username = request.form.get("username", "")
        email = request.form.get("email", "")
        password = request.form.get("password", "")
        err = registration_form_error(
            username, email, password, min_password_len=_MIN_PASSWORD_LEN
        )
        if err:
            return render_template("register.html", error=err)
        try:
            save_user(username.strip(), email.strip(), password)
        except sqlite3.IntegrityError:
            return render_template(
                "register.html",
                error="That username or email is already registered.",
            )
        return redirect("/dashboard")
    return render_template("register.html")


@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html", saved_cars=[])


@app.route("/search")
def search():
    g = request.args.getlist

    def scalar(key):
        vals = [v.strip() for v in request.args.getlist(key) if v.strip()]
        return vals[-1] if vals else ""

    zip_code = scalar("zip_code")
    radius = scalar("radius")
    max_price = scalar("max_price")
    max_mileage = scalar("max_mileage")
    reg_id_raw = scalar("dealership_registry_id")

    dealership_registry_id = None
    if reg_id_raw:
        try:
            dealership_registry_id = int(reg_id_raw)
        except ValueError:
            dealership_registry_id = None

    q_text = scalar("q") or scalar("search")
    sql_kwargs = flask_request_to_search_cars_kwargs(request)
    if q_text:
        results, _ = hybrid_search_with_kwargs(q_text, sql_kwargs, vector_top_k=100)
    else:
        results = search_cars(**sql_kwargs)

    initial_grid_cars = []
    if q_text:
        initial_grid_cars = [
            serialize_car_for_api(c, include_verified=False) for c in results
        ]

    active = {
        "make": g("make"),
        "model": g("model"),
        "trim": g("trim"),
        "fuel_type": g("fuel_type"),
        "cylinders": g("cylinders"),
        "transmission": g("transmission"),
        "drivetrain": g("drivetrain"),
        "body_style": g("body_style"),
        "exterior_color": g("exterior_color"),
        "interior_color": g("interior_color"),
        "country": g("country"),
        "max_price": max_price,
        "max_mileage": max_mileage,
        "zip_code": zip_code,
        "radius": radius,
        "dealership_registry_id": reg_id_raw,
        "q": q_text,
    }

    return render_template(
        "listings.html",
        active=active,
        options=get_filter_options(),
        initial_grid_cars=initial_grid_cars,
    )


@app.route("/car/<int:car_id>")
def car_detail(car_id):
    car_raw = get_car_by_id(car_id)
    if not car_raw:
        return redirect("/dashboard")
    ctx = prepare_car_detail_context(car_raw)
    car = serialize_car_for_api(
        car_raw,
        include_verified=False,
        verified_specs=ctx.get("verified_specs") or {},
    )
    return render_template(
        "car.html",
        car=car,
        gallery_images=ctx.get("gallery_images") or [],
        verified_specs=ctx.get("verified_specs") or {},
        mopar_vin_lookup_url=mopar_vin_lookup_url(make=car.get("make"), vin=car.get("vin")),
    )


@app.route("/listings")
def listings():
    return listings_page()


def _highlight_params_from_filters(filters: dict) -> list[str]:
    """UI filter control keys for styling (matches data-param / form names)."""
    keys = []
    for k in filters:
        if k == "exterior_color":
            keys.append("exterior_color")
        elif k == "drivetrain":
            keys.append("drivetrain")
        elif k == "body_style":
            keys.append("body_style")
        elif k == "max_price":
            keys.append("max_price")
        elif k == "max_mileage":
            keys.append("max_mileage")
        elif k in ("min_year", "max_year"):
            if "year" not in keys:
                keys.append("year")
        elif k in ("make", "model"):
            keys.append(k)
        elif k == "interior_color":
            keys.append("interior_color")
    return keys


@app.route("/api/search/smart", methods=["POST"])
def api_search_smart():
    ip = _client_ip()
    if not allow_request(f"smart:{ip}", max_events=_SMART_SEARCH_RPM, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    data = request.get_json() or {}
    q = (data.get("query") or data.get("q") or "").strip()
    filters = parse_natural_query(q)
    from backend.hybrid_inventory_search import hybrid_smart_search

    results, search_meta = hybrid_smart_search(q, filters, vector_top_k=100)
    safe_results = [serialize_car_for_api(c, include_verified=False) for c in results]
    return jsonify(
        {
            "filters": filters,
            "results": safe_results,
            "highlight": _highlight_params_from_filters(filters),
            "search_meta": search_meta,
        }
    )


@app.route("/api/car/<int:car_id>/chat", methods=["POST"])
def api_car_chat(car_id: int):
    ip = _client_ip()
    if not allow_request(f"chat:{ip}:{car_id}", max_events=_CHAT_RPM, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    if request.content_length is not None and request.content_length > _CHAT_MAX_BODY:
        return jsonify({"ok": False, "error": "payload_too_large"}), 413

    car_raw = get_car_by_id(car_id)
    if not car_raw:
        return jsonify({"ok": False, "error": "not_found"}), 404
    body = request.get_json() or {}
    message = (body.get("message") or body.get("q") or "").strip()
    if not message:
        return jsonify({"ok": False, "error": "message_required"}), 400
    if len(message) > _CHAT_MAX_MESSAGE:
        return jsonify({"ok": False, "error": "message_too_long"}), 400

    out = run_car_page_chat(car_raw, message)
    err = out.get("error")
    return jsonify(
        {
            "ok": err is None,
            "reply": out.get("reply") or "",
            "error": err,
        }
    )
