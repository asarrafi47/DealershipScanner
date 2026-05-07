from backend.main import app, socketio

if __name__ == "__main__":
    print("Open in browser: http://localhost:5001")
    # Socket.IO (QR MFA) uses threading; disable reloader in dev to avoid double-process quirks.
    socketio.run(
        app,
        debug=True,
        host="localhost",
        port=5001,
        use_reloader=False,
        allow_unsafe_werkzeug=True,
    )