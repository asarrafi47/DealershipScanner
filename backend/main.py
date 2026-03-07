from flask import Flask, request, render_template, redirect

app = Flask(__name__)


@app.route("/")
def home():
    return redirect("/login")


@app.route("/login", methods=["GET"])
def login_page():
    return render_template("login.html")


@app.route("/register", methods=["GET"])
def register_page():
    return render_template("register.html")


@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html")


def user():
    username = request.form["username"]
    emailAddress = request.form["emailAddress"]
    password = request.form["password"]

    return username, emailAddress, password


if __name__ == "__main__":
    app.run(debug=True)