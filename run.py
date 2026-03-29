import os

from backend.main import app

if __name__ == "__main__":
    print("Open in browser: http://127.0.0.1:5000")
    if not os.environ.get("SECRET_KEY"):
        print("Tip: set SECRET_KEY for stable sessions across restarts (required in production).")
    debug = os.environ.get("FLASK_DEBUG", "1") == "1"
    app.run(debug=debug, host="127.0.0.1", port=5000)