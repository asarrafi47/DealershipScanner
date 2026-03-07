# import the Flask app from main.py
from backend.main import app

# start the server
if __name__ == "__main__":
    app.run(debug=True)