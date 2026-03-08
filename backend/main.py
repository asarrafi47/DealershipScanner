import sqlite3
from flask import Flask, request, render_template, redirect
from backend.listings import listings_page
from backend.login import check_user
from backend.register import save_users

app = Flask(
    __name__,
    template_folder="../frontend/templates",
    static_folder="../frontend/static"
)

def init_db():
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        )
    """)
    cursor.execute("""
        INSERT OR IGNORE INTO users (username, email, password)
        VALUES ('admin', 'admin@admin.com', 'password')
    """)
    conn.commit()
    conn.close()

init_db()

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
        else:
            return render_template("login.html", error="Invalid username/email or password.")

    return render_template("login.html")

@app.route("/register", methods=["GET", "POST"])
def register_page():
    if request.method == "POST":
        username = request.form["username"]
        email = request.form["email"]
        password = request.form["password"]
        save_users(username, email, password)
        return redirect("/dashboard")
    return render_template("register.html")

@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html")

@app.route("/listings")
def listings():
    return listings_page()

def user():
    username = request.form["username"]
    email = request.form["email"]
    password = request.form["password"]

    return username, email, password