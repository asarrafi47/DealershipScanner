import logging
import os

from backend.utils.project_env import ensure_backend_on_sys_path

ensure_backend_on_sys_path()

from backend.main import app, socketio
from backend.utils.runtime_env import is_production_env

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

    # Dev: the webapp owns the local LLM lifecycle (assistant-search fallback)
    # — started here, stopped on exit via atexit. Opt out: LOCAL_LLM_AUTOSTART=0.
    if not is_production_env():
        try:
            from backend.utils.local_llm import ensure_server_running

            if ensure_server_running():
                print("Local LLM (Ollama) ready for assistant search")
            else:
                print(
                    "WARNING: local LLM unavailable — assistant search runs parser-only",
                    file=sys.stderr,
                )
        except Exception as exc:
            print(f"WARNING: local LLM autostart failed: {exc}", file=sys.stderr)

    public = os.environ.get("PUBLIC", "0") not in ("0", "false", "no")
    host = "0.0.0.0" if public else "localhost"
    port = int(os.environ.get("PORT", 5001))
    # Avoid duplicate request lines (Werkzeug dev server + werkzeug logger).
    werkzeug_log = logging.getLogger("werkzeug")
    werkzeug_log.handlers.clear()
    werkzeug_log.propagate = False
    print(f"Open in browser: http://localhost:{port}")
    socketio.run(
        app,
        debug=not is_production_env(),
        host=host,
        port=port,
        use_reloader=False,
        log_output=False,
        # run.py is local / Cloudflare-tunnel dev only (see start.sh); not gunicorn prod.
        allow_unsafe_werkzeug=True,
    )