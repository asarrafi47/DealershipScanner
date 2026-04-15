import os

from flask import Flask, request, render_template, redirect, jsonify, send_from_directory
from backend.dev_routes import dev_bp
from backend.db.admin_users_db import init_admin_db
from backend.db.users_db import init_users_db, check_user, save_user
from backend.db.inventory_db import (
    init_inventory_db,
    search_cars,
    get_car_by_id,
    get_cars_by_ids,
    get_filter_options,
)
from backend.dev_console import register_dev_console
from backend.knowledge_engine import prepare_car_detail_context
from backend.ai_agent import run_car_page_chat
from backend.listings import listings_page
from backend.utils.query_parser import parse_natural_query

app = Flask(
    __name__,
    template_folder="../frontend/templates",
    static_folder="../frontend/static"
)

app.secret_key = (
    os.environ.get("SECRET_KEY")
    or os.environ.get("FLASK_SECRET_KEY")
    or "dealership-scanner-dev-insecure"
)

init_users_db()
init_admin_db()
init_inventory_db()
app.register_blueprint(dev_bp, url_prefix="/dev")
register_dev_console(app)


@app.route("/favicon.ico")
def favicon():
    return send_from_directory(app.static_folder, "favicon.svg", mimetype="image/svg+xml")


@app.route("/")
def home():
    return redirect("/login")


@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
        login_input = request.form["login"]
        password = request.form["password"]
        if check_user(login_input, password):
            return redirect("/dashboard")
        return render_template("login.html", error="Invalid username/email or password.")
    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
def register_page():
    if request.method == "POST":
        save_user(request.form["username"], request.form["email"], request.form["password"])
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

    zip_code    = scalar("zip_code")
    radius      = scalar("radius")
    max_price   = scalar("max_price")
    max_mileage = scalar("max_mileage")
    reg_id_raw  = scalar("dealership_registry_id")

    dealership_registry_id = None
    if reg_id_raw:
        try:
            dealership_registry_id = int(reg_id_raw)
        except ValueError:
            dealership_registry_id = None

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
        dealership_registry_id=dealership_registry_id,
    )

    active = {
        "make": g("make"), "model": g("model"), "trim": g("trim"),
        "fuel_type": g("fuel_type"),
        "cylinders": g("cylinders"), "transmission": g("transmission"),
        "drivetrain": g("drivetrain"), "exterior_color": g("exterior_color"),
        "interior_color": g("interior_color"),
        "country": g("country"),
        "max_price": max_price, "max_mileage": max_mileage,
        "zip_code": zip_code, "radius": radius,
        "dealership_registry_id": reg_id_raw,
    }

    return render_template("listings.html",
                           results=results,
                           active=active,
                           options=get_filter_options())


@app.route("/car/<int:car_id>")
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
def api_search_smart():
    data = request.get_json() or {}
    q = (data.get("query") or data.get("q") or "").strip()
    filters = parse_natural_query(q)
    results: list = []
    if q:
        try:
            from backend.vector.chroma_service import query_cars

            chroma_ids = query_cars(q, n_results=20)
            if chroma_ids:
                results = get_cars_by_ids(chroma_ids)
        except Exception:
            results = []
    if not results:
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
    return jsonify({
        "filters": filters,
        "results": results,
        "highlight": _highlight_params_from_filters(filters),
    })


@app.route("/api/car/<int:car_id>/chat", methods=["POST"])
def api_car_chat(car_id: int):
    car = get_car_by_id(car_id)
    if not car:
        return jsonify({"ok": False, "error": "not_found"}), 404
    body = request.get_json() or {}
    message = (body.get("message") or body.get("q") or "").strip()
    if not message:
        return jsonify({"ok": False, "error": "message_required"}), 400
    out = run_car_page_chat(car, message)
    err = out.get("error")
    return jsonify({
        "ok": err is None,
        "reply": out.get("reply") or "",
        "error": err,
    })
