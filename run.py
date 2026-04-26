from backend.main import app, socketio

if __name__ == "__main__":
    print("Open in browser: http://127.0.0.1:5000")
    # Socket.IO (QR MFA) uses threading; disable reloader in dev to avoid double-process quirks.
    socketio.run(
        app,
        debug=True,
        host="127.0.0.1",
        port=5000,
        use_reloader=False,
        allow_unsafe_werkzeug=True,
    )