import sqlite3

from flask import Flask, jsonify, redirect, render_template, request, url_for
from flask_login import current_user, login_required, login_user, logout_user

from backend import auth  # noqa: F401 — registers user_loader
from backend.auth import User
from backend.db.inventory_db import init_inventory_db, search_cars, get_car_by_id, get_filter_options
from backend.db.users_db import create_user, init_users_db, verify_login
from backend.knowledge_engine import prepare_car_detail_context
from backend.listings import listings_page
from backend.security import csrf, init_security, limiter
from backend.utils.query_parser import parse_natural_query
from backend.utils.discovery import get_dealers_from_map, write_discovery_manifest

app = Flask(
    __name__,
    template_folder="../frontend/templates",
    static_folder="../frontend/static",
)

init_security(app)
init_users_db()
init_inventory_db()

app.config["LOGIN_DISABLED"] = False


def _safe_next_url(url: str | None) -> str | None:
    if url and url.startswith("/") and not url.startswith("//"):
        return url
    return None


def _validate_registration(username: str, email: str, password: str):
    username = (username or "").strip()
    email = (email or "").strip().lower()
    password = password or ""
    if len(username) < 2 or len(username) > 64:
        return None, "Username must be between 2 and 64 characters."
    if "@" not in email or len(email) > 255:
        return None, "Please enter a valid email address."
    if len(password) < 8:
        return None, "Password must be at least 8 characters."
    return (username, email, password), None


@app.route("/")
def home():
    return redirect("/login")


@app.route("/login", methods=["GET", "POST"])
@limiter.limit("10 per minute", methods=["POST"])
def login_page():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        login_input = (request.form.get("login") or "").strip()
        password = request.form.get("password") or ""
        if not login_input or not password:
            return render_template(
                "login.html",
                error="Enter both username/email and password.",
            )
        row = verify_login(login_input, password)
        if row:
            login_user(
                User(row["id"], row["username"], row["email"]),
                remember=True,
            )
            nxt = (
                _safe_next_url(request.args.get("next"))
                or _safe_next_url(request.form.get("next"))
                or "/dashboard"
            )
            return redirect(nxt)
        return render_template(
            "login.html",
            error="Invalid username/email or password.",
        )
    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
@limiter.limit("5 per minute", methods=["POST"])
def register_page():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        vals, err = _validate_registration(
            request.form.get("username"),
            request.form.get("email"),
            request.form.get("password"),
        )
        if err:
            return render_template("register.html", error=err)
        username, email, password = vals
        try:
            create_user(username, email, password)
        except sqlite3.IntegrityError:
            return render_template(
                "register.html",
                error="That username or email is already registered.",
            )
        row = verify_login(username, password)
        if row:
            login_user(
                User(row["id"], row["username"], row["email"]),
                remember=True,
            )
        return redirect("/dashboard")
    return render_template("register.html")


@app.route("/logout", methods=["POST"])
@login_required
def logout():
    logout_user()
    return redirect(url_for("login_page"))


@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html", saved_cars=[])


@app.route("/search")
@login_required
def search():
    g = request.args.getlist

    def scalar(key):
        vals = [v.strip() for v in request.args.getlist(key) if v.strip()]
        return vals[-1] if vals else ""

    zip_code = scalar("zip_code")
    radius = scalar("radius")
    max_price = scalar("max_price")
    max_mileage = scalar("max_mileage")

    results = search_cars(
        makes=g("make") or None,
        models=g("model") or None,
        trims=g("trim") or None,
        fuel_types=g("fuel_type") or None,
        cylinders=g("cylinders") or None,
        transmissions=g("transmission") or None,
        drivetrains=g("drivetrain") or None,
        exterior_colors=g("exterior_color") or None,
        interior_colors=g("interior_color") or None,
        countries=g("country") or None,
        max_price=float(max_price) if max_price else None,
        max_mileage=int(max_mileage) if max_mileage else None,
        zip_code=zip_code or None,
        radius_miles=float(radius) if radius else None,
    )

    active = {
        "make": g("make"),
        "model": g("model"),
        "trim": g("trim"),
        "fuel_type": g("fuel_type"),
        "cylinders": g("cylinders"),
        "transmission": g("transmission"),
        "drivetrain": g("drivetrain"),
        "exterior_color": g("exterior_color"),
        "interior_color": g("interior_color"),
        "country": g("country"),
        "max_price": max_price,
        "max_mileage": max_mileage,
        "zip_code": zip_code,
        "radius": radius,
    }

    return render_template(
        "listings.html",
        results=results,
        active=active,
        options=get_filter_options(),
    )


@app.route("/car/<int:car_id>")
@login_required
def car_detail(car_id):
    car = get_car_by_id(car_id)
    if not car:
        return redirect("/dashboard")
    ctx = prepare_car_detail_context(car)
    return render_template(
        "car.html",
        car=car,
        gallery_images=ctx.get("gallery_images") or [],
        verified_specs=ctx.get("verified_specs") or {},
    )


@app.route("/listings")
@login_required
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
        elif k == "max_price":
            keys.append("max_price")
        elif k == "max_mileage":
            keys.append("max_mileage")
        elif k in ("min_year", "max_year"):
            if "year" not in keys:
                keys.append("year")
        elif k in ("make", "model"):
            keys.append(k)
    return keys


@app.route("/api/search/smart", methods=["POST"])
@csrf.exempt  # JSON session API; CSRF mitigated by SameSite cookies + login_required
@login_required
def api_search_smart():
    data = request.get_json() or {}
    q = (data.get("query") or data.get("q") or "").strip()
    filters = parse_natural_query(q)
    results = []
    if filters:
        results = search_cars(
            makes=[filters["make"]] if filters.get("make") else None,
            models=[filters["model"]] if filters.get("model") else None,
            drivetrains=filters.get("drivetrain"),
            exterior_colors=filters.get("exterior_color"),
            min_year=filters.get("min_year"),
            max_year=filters.get("max_year"),
            max_price=filters.get("max_price"),
            max_mileage=filters.get("max_mileage"),
        )
    else:
        results = search_cars()
    return jsonify(
        {
            "filters": filters,
            "results": results,
            "highlight": _highlight_params_from_filters(filters),
        }
    )


def _parse_discovery_nearby():
    """Shared query/body parsing for map discovery."""
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        zip_code = (data.get("zip") or data.get("zip_code") or "").strip() or None
        radius_miles = float(data.get("radius_miles") or data.get("radius") or 25)
        lat_raw, lon_raw = data.get("lat"), data.get("lon")
        if lat_raw is not None and lon_raw is not None:
            lat, lon = float(lat_raw), float(lon_raw)
        else:
            lat, lon = None, None
        cd = data.get("check_dealer_com", True)
        if isinstance(cd, str):
            check_dealer_com = cd.lower() in ("1", "true", "yes")
        else:
            check_dealer_com = bool(cd)
    else:
        zip_code = (request.args.get("zip") or request.args.get("zip_code") or "").strip() or None
        radius_miles = float(request.args.get("radius_miles") or request.args.get("radius") or 25)
        lat = request.args.get("lat", type=float)
        lon = request.args.get("lon", type=float)
        check_dealer_com = request.args.get("check_dealer_com", default="true").lower() in (
            "1",
            "true",
            "yes",
        )
    return zip_code, radius_miles, lat, lon, check_dealer_com


@app.route("/api/discovery/nearby", methods=["GET", "POST"])
@csrf.exempt  # JSON session API; CSRF mitigated by SameSite cookies + login_required
@login_required
def api_discovery_nearby():
    zip_code, radius_miles, lat, lon, check_dealer_com = _parse_discovery_nearby()
    out = get_dealers_from_map(
        zip_code=zip_code,
        radius_miles=radius_miles,
        lat=lat,
        lon=lon,
        check_dealer_com=check_dealer_com,
    )
    return jsonify(out)


@app.route("/api/discovery/scan", methods=["POST"])
@csrf.exempt  # JSON session API; CSRF mitigated by SameSite cookies + login_required
@login_required
def api_discovery_scan():
    """Write dealers.discovery.json for a local scanner run (local_inventory.db)."""
    data = request.get_json() or {}
    dealers = data.get("dealers")
    if not isinstance(dealers, list):
        return jsonify({"ok": False, "error": "dealers array required"}), 400
    path = write_discovery_manifest(dealers)
    vetted = [d for d in dealers if d.get("dealer_com") and d.get("dealer_id")]
    return jsonify(
        {
            "ok": True,
            "manifest": str(path),
            "dealer_com_count": len(vetted),
            "hint": (
                "PowerShell: $env:INVENTORY_DB_PATH='local_inventory.db'; "
                "$env:DEALERS_MANIFEST='dealers.discovery.json'; node scanner.js"
            ),
        }
    )
