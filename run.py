import os
from backend.main import app, socketio

if __name__ == "__main__":
    import sys

    try:
        from backend.scanner.window_sticker import pdf_text_extraction_available

        if not pdf_text_extraction_available():
            print(
                f"WARNING: pypdf not installed for {sys.executable}\n"
                f"  Window sticker options/specs need PDF text extraction.\n"
                f"  Run: {sys.executable} -m pip install pypdf",
                file=sys.stderr,
            )
    except Exception:
        pass

    public = os.environ.get("PUBLIC", "0") not in ("0", "false", "no")
    host = "0.0.0.0" if public else "localhost"
    port = int(os.environ.get("PORT", 5001))
    print(f"Open in browser: http://localhost:{port}")
    socketio.run(
        app,
        debug=not public,
        host=host,
        port=port,
        use_reloader=False,
        allow_unsafe_werkzeug=not public,
    )