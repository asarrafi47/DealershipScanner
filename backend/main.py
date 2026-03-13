from flask import Flask, request, render_template, redirect, jsonify
import requests
from backend.db.users_db import init_users_db, check_user, save_user
from backend.db.inventory_db import init_inventory_db, search_cars, get_car_by_id, get_car_by_vin, get_filter_options
from backend.listings import listings_page

try:
    from duckduckgo_search import DDGS
except ImportError:
    DDGS = None

app = Flask(
    __name__,
    template_folder="../frontend/templates",
    static_folder="../frontend/static"
)

init_users_db()
init_inventory_db()


def get_car_context(vin):
    """Fetch all details for the given VIN and format into a string for the AI."""
    car = get_car_by_vin(vin)
    if not car:
        return ""
    parts = [
        f"This is a {car.get('year', '')} {car.get('make', '')} {car.get('model', '')}",
        car.get("trim") and f", Trim: {car['trim']}" or "",
        f", Price: ${car.get('price', 0):,.0f}",
        f", Mileage: {car.get('mileage', 0):,}",
    ]
    if car.get("fuel_type"):
        parts.append(f", Fuel: {car['fuel_type']}")
    if car.get("cylinders") is not None:
        parts.append(", Cylinders: " + ("Electric" if car["cylinders"] == 0 else f"{car['cylinders']}-cyl"))
    if car.get("transmission"):
        parts.append(f", Transmission: {car['transmission']}")
    if car.get("drivetrain"):
        parts.append(f", Drivetrain: {car['drivetrain']}")
    if car.get("exterior_color"):
        parts.append(f", Exterior: {car['exterior_color']}")
    if car.get("interior_color"):
        parts.append(f", Interior: {car['interior_color']}")
    if car.get("dealer_name"):
        parts.append(f", Dealer: {car['dealer_name']}")
    if car.get("dealer_url"):
        parts.append(f", Dealer URL: {car['dealer_url']}")
    if car.get("zip_code"):
        parts.append(f", Zip: {car['zip_code']}")
    return "".join(parts).replace(" ,", ",").strip()


def search_web_for_car(car: dict, user_message: str, max_results: int = 6) -> str:
    """Use DuckDuckGo to search for car specs/details. Builds a short, focused query so DDG returns useful results (e.g. '2022 Audi Q5 Premium Plus horsepower')."""
    if DDGS is None or not car:
        return ""
    year = car.get("year") or ""
    make = (car.get("make") or "").strip()
    model = (car.get("model") or "").strip()
    trim = (car.get("trim") or "").strip()
    car_bits = [str(year), make, model, trim]
    car_query = " ".join(b for b in car_bits if b).strip()
    if not car_query:
        return ""
    msg = (user_message or "").strip()
    # Focused query for DDG: "2022 Audi Q5 Premium Plus horsepower" so spec pages rank well
    query = f"{car_query} {msg}".strip()
    if not query:
        return ""
    # If the user asked something short (e.g. "horsepower?"), add "specs" to get spec pages
    if len(msg) < 25 and msg.rstrip("?."):
        query = f"{car_query} {msg} specs".strip()
    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
        if not results:
            return ""
        parts = []
        for r in results:
            body = (r.get("body") or "").strip()
            title = (r.get("title") or "").strip()
            if body:
                parts.append(f"- {title}: {body[:450]}" + ("..." if len(body) > 450 else ""))
        return "\n".join(parts) if parts else ""
    except Exception:
        return ""


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


OLLAMA_URL = "http://localhost:11434/api/generate"
CHAT_MODEL = "llama3.2"


@app.route("/api/chat", methods=["POST"])
def api_chat():
    data = request.get_json() or {}
    message = (data.get("message") or "").strip()
    vin = (data.get("vin") or "").strip()
    if not message or not vin:
        return jsonify({"error": "message and vin are required"}), 400
    car = get_car_by_vin(vin)
    if not car:
        return jsonify({"error": "Car not found for this VIN"}), 404
    car_context = get_car_context(vin)
    web_snippets = search_web_for_car(car, message)
    system_parts = [
        "You are a helpful car salesman. Use the following listing data to answer the user: ",
        f"[{car_context}].",
    ]
    if web_snippets:
        system_parts.append(
            " Below are DuckDuckGo web search results for this car and the user's question. Use them to answer specs (horsepower, mpg, dimensions, etc.) and other details not in the listing. Prefer answers from these results when they apply.\n\n"
            f"{web_snippets}\n\n"
        )
    system_parts.append(
        " Only suggest contacting the dealer if the listing and search results above do not contain the answer."
    )
    system_prompt = "".join(system_parts)
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": CHAT_MODEL,
                "prompt": message,
                "system": system_prompt,
                "stream": False,
            },
            timeout=120,
        )
        resp.raise_for_status()
        body = resp.json()
        reply = body.get("response", "").strip()
        return jsonify({"reply": reply})
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e), "reply": ""}), 502
