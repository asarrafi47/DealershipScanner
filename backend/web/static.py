"""Static assets: cache policy, cache-buster and precompressed sibling serving.

Every static URL the templates emit carries ?v={{ static_cache_ver }}, so the files
can be cached for a year and marked immutable; a changed asset gets a new query
string and therefore a new cache entry. The version used to be the mtime of the
dead style.css (nothing loads it, last touched 2026-08-03), which never moved when
a real partial or script changed -- harmless under Flask's default
``Cache-Control: no-cache``, a stale-asset bug the moment a long max-age is set.
It is now the newest mtime under frontend/static (precompressed siblings excluded,
they are rebuilt from the sources and would only echo the same change).

Moved out of ``backend/main.py`` (monolith audit 2026-10-01, W1).
"""

from __future__ import annotations

import mimetypes
import os
from pathlib import Path

from flask import Flask, current_app, request, send_from_directory

_STATIC_MAX_AGE = 31536000
_STATIC_UNSTAMPED_MAX_AGE = 3600
_STATIC_COMPRESSED_SUFFIXES = (".gz", ".br")


def compute_static_cache_ver(static_root) -> str:
    """Newest mtime (whole seconds) of any source file under ``static_root``.

    Pure so it can be unit-tested against a temp tree. ``.gz``/``.br`` siblings
    are skipped: they are derived from the sources by
    scripts/build_static_compressed.py and never change on their own."""
    newest = 0
    for dirpath, _dirs, files in os.walk(str(static_root)):
        for name in files:
            if name.endswith(_STATIC_COMPRESSED_SUFFIXES):
                continue
            try:
                mt = os.stat(os.path.join(dirpath, name)).st_mtime
            except OSError:
                continue
            if mt > newest:
                newest = mt
    return str(int(newest)) if newest else "1"


_static_cache_ver_memo: str | None = None


def static_cache_ver() -> str:
    """The ?v= stamp for this process.

    Computed once per process (gunicorn preloads the app, so the walk runs once);
    the Werkzeug dev server runs with debug=True and no reloader, where a static
    edit must show up on the next request, so debug recomputes every call (a
    few dozen stats)."""
    global _static_cache_ver_memo
    app = current_app
    if app.debug or _static_cache_ver_memo is None:
        _static_cache_ver_memo = compute_static_cache_ver(Path(app.static_folder).resolve())
    return _static_cache_ver_memo


def _precompressed_static_sibling(filename: str):
    """(encoding, sibling filename) for a ``.br``/``.gz`` sibling the client
    accepts, or None. A sibling older than its source is ignored, so an edited
    source is never shadowed by a stale build (scripts/build_static_compressed.py
    stamps each sibling with its source's mtime)."""
    from werkzeug.security import safe_join

    source = safe_join(current_app.static_folder, filename)
    if not source:
        return None
    try:
        src_mtime = os.stat(source).st_mtime
    except OSError:
        return None
    accepted = request.accept_encodings
    for encoding, suffix in (("br", ".br"), ("gzip", ".gz")):
        if accepted.quality(encoding) <= 0:
            continue
        try:
            st = os.stat(source + suffix)
        except OSError:
            continue
        if st.st_size > 0 and st.st_mtime >= src_mtime:
            return encoding, filename + suffix
    return None


def _static_view(filename: str):
    """Flask's static view, plus precompressed siblings.

    Nothing between the app and the browser compresses static files (gunicorn
    does not, Railway's edge does not), so a cold visit downloaded ~550 KB of
    CSS+JS that gzips to ~110 KB. The siblings are built by
    scripts/build_static_compressed.py (Dockerfile.web runs it)."""
    app = current_app
    pick = _precompressed_static_sibling(filename)
    if pick is None:
        resp = app.send_static_file(filename)
    else:
        encoding, sibling = pick
        mimetype = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        resp = send_from_directory(
            app.static_folder,
            sibling,
            mimetype=mimetype,
            max_age=app.get_send_file_max_age(filename),
            conditional=True,
        )
        resp.headers["Content-Encoding"] = encoding
    if filename.endswith((".js", ".css")):
        resp.vary.add("Accept-Encoding")
    return resp


def _static_cache_headers(resp):
    """``immutable`` for stamped static URLs; a short max-age for unstamped ones.

    An unstamped /static URL (favicon, placeholder.svg, brand art, Leaflet's
    marker PNGs referenced from its own CSS) has no way to bust the cache, so a
    year there would pin the old bytes until the browser evicts them."""
    if request.endpoint != "static" or resp.status_code not in (200, 304):
        return resp
    filename = (request.view_args or {}).get("filename") or ""
    if request.args.get("v") or filename.startswith("fonts/"):
        # Font files are named with their upstream version (inter-latin-v20), so
        # the name is the stamp; the @font-face url cannot carry ?v=.
        resp.cache_control.immutable = True
    elif resp.cache_control.max_age == _STATIC_MAX_AGE:
        resp.cache_control.max_age = _STATIC_UNSTAMPED_MAX_AGE
        resp.expires = None
    return resp


def register_static(app: Flask) -> None:
    """Year-long static max-age, the precompressed static view, the cache-header hook."""
    global _static_cache_ver_memo
    # A fresh app (e.g. ``importlib.reload(backend.main)``) recomputes the stamp.
    _static_cache_ver_memo = None
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = _STATIC_MAX_AGE
    app.view_functions["static"] = _static_view
    app.after_request(_static_cache_headers)
