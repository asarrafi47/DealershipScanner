from flask import Flask, request, render_template, redirect
from backend.db.users_db import init_users_db, check_user, save_user
from backend.db.inventory_db import init_inventory_db, search_cars, get_car_by_id, get_filter_options
from backend.listings import listings_page

app = Flask(
    __name__,
    template_folder="../frontend/templates",
    static_folder="../frontend/static"
)

init_users_db()
init_inventory_db()


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
    return render_template("dashboard.html")


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
        max_price=float(max_price) if max_price else None,
        max_mileage=int(max_mileage) if max_mileage else None,
        zip_code=zip_code or None,
        radius_miles=float(radius) if radius else None,
    )

    active = {
        "make": g("make"), "model": g("model"), "trim": g("trim"),
        "fuel_type": g("fuel_type"),
        "cylinders": g("cylinders"), "transmission": g("transmission"),
        "drivetrain": g("drivetrain"), "exterior_color": g("exterior_color"),
        "interior_color": g("interior_color"),
        "max_price": max_price, "max_mileage": max_mileage,
        "zip_code": zip_code, "radius": radius,
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
    return render_template("car.html", car=car)


@app.route("/listings")
def listings():
    return listings_page()
