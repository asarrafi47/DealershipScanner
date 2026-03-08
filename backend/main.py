from flask import Flask, request, render_template, redirect
from backend.db.users_db import init_users_db, check_user, save_user
from backend.db.inventory_db import init_inventory_db, search_cars, get_car_by_id
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
    make      = request.args.get("make", "").strip()
    model     = request.args.get("model", "").strip()
    zip_code  = request.args.get("zip_code", "").strip()
    radius    = request.args.get("radius", "").strip()
    max_price = request.args.get("max_price", "").strip()

    results = search_cars(
        make=make or None,
        model=model or None,
        max_price=float(max_price) if max_price else None,
        zip_code=zip_code or None,
        radius_miles=float(radius) if radius else None,
    )
    return render_template("dashboard.html", results=results, query={
        "make": make, "model": model, "zip_code": zip_code,
        "radius": radius, "max_price": max_price,
    })


@app.route("/car/<int:car_id>")
def car_detail(car_id):
    car = get_car_by_id(car_id)
    if not car:
        return redirect("/dashboard")
    return render_template("car.html", car=car)


@app.route("/listings")
def listings():
    return listings_page()
