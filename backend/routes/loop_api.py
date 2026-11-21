"""Scan loop API (docs/scan_loop/README.md): how cloud sessions reach the loop's file store in the Railway database.

  GET  /api/loop/list?prefix=P           JSON list of {path, sha256, size}          read token
  GET  /api/loop/file?path=P             the file's bytes                           read token
  GET  /api/loop/export?prefix=P&...     gzip tar of everything under the prefixes   read token
  POST /api/loop/submit                  {"path": P, "text": T}: one file            submit token

Two bearer tokens, both set only as variables of the web service (``LOOP_READ_TOKEN``, ``LOOP_SUBMIT_TOKEN``).
Without them every route answers 404, so the API does not exist until the owner turns it on. The submit token can
write only what ``store.submit_check`` allows: new request and proposal files (strictly parsed, never overwritten), a
session's own ``summary.md`` and ``location.md``, and findings, scrubbed and size-capped. It can never write a
recipe, a mirror, a bundle, a status or the config; applying anything is Railway's tick, not this API.
Sessions are callers of a server API, not browsers: no cookies, no CSRF token (the bearer token proves the caller).
"""

from __future__ import annotations

import hmac
import os

from flask import Blueprint, Response, jsonify, request

from backend.scanner.loop import requests as loop_requests
from backend.scanner.loop import scrub, store as loop_store

bp = Blueprint("loop_api", __name__, url_prefix="/api/loop")

MAX_BODY = 600 * 1024
_store_override = None


def get_store():
    """The store the routes use; tests set ``backend.routes.loop_api._store_override``."""
    if _store_override is not None:
        return _store_override
    return loop_store.PgStore()


def _token_ok(env_name: str) -> bool:
    expected = (os.environ.get(env_name) or "").strip()
    if len(expected) < 24:
        return False
    header = request.headers.get("Authorization", "")
    supplied = header[7:].strip() if header.lower().startswith("bearer ") else ""
    return hmac.compare_digest(supplied.encode(), expected.encode())


def _enabled() -> bool:
    return len((os.environ.get("LOOP_READ_TOKEN") or "").strip()) >= 24 or \
        len((os.environ.get("LOOP_SUBMIT_TOKEN") or "").strip()) >= 24


def _deny(code: int, error: str):
    resp = jsonify({"error": error})
    resp.status_code = code
    resp.headers["Cache-Control"] = "no-store"
    return resp


def _guard(env_name: str):
    if not _enabled():
        return _deny(404, "not found")
    if not _token_ok(env_name):
        return _deny(401, "unauthorized")
    return None


@bp.get("/list")
def list_files():
    denied = _guard("LOOP_READ_TOKEN")
    if denied:
        return denied
    prefix = request.args.get("prefix", "")
    if prefix and not loop_store.prefix_ok(prefix):
        return _deny(400, "bad prefix")
    entries = get_store().list(prefix)[:5000]
    resp = jsonify([{"path": e.path, "sha256": e.sha256, "size": e.size} for e in entries])
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp.get("/file")
def get_file():
    denied = _guard("LOOP_READ_TOKEN")
    if denied:
        return denied
    path = request.args.get("path", "")
    if not loop_store.path_ok(path):
        return _deny(400, "bad path")
    data = get_store().get(path)
    if data is None:
        return _deny(404, "not found")
    return Response(data, mimetype="application/octet-stream", headers={"Cache-Control": "no-store"})


@bp.get("/export")
def export():
    denied = _guard("LOOP_READ_TOKEN")
    if denied:
        return denied
    prefixes = request.args.getlist("prefix")[:12]
    if not prefixes or not all(loop_store.prefix_ok(p) for p in prefixes):
        return _deny(400, "bad prefix")
    try:
        data = loop_store.export_tar(get_store(), prefixes)
    except loop_store.StoreError as exc:
        return _deny(413, str(exc))
    return Response(data, mimetype="application/gzip", headers={"Cache-Control": "no-store"})


@bp.post("/submit")
def submit():
    denied = _guard("LOOP_SUBMIT_TOKEN")
    if denied:
        return denied
    if request.content_length is None or request.content_length > MAX_BODY:
        return _deny(413, "body too large")
    body = request.get_json(silent=True)
    if not isinstance(body, dict) or set(body) != {"path", "text"} or not all(isinstance(v, str) for v in body.values()):
        return _deny(400, "expected {path, text}")
    path, text = body["path"], body["text"]
    if not loop_store.path_ok(path):
        return _deny(400, "bad path")
    store = get_store()
    exists = store.exists(path)
    if path.startswith("requests/") and not exists:
        pending = [e for e in store.list("requests/") if e.path.endswith(".json") and not e.path.endswith(".status.json")
                   and not store.exists(e.path[:-5] + ".status.json")]
        if len(pending) >= loop_requests.MAX_UNPROCESSED:
            return _deny(429, "too many unprocessed requests")
    if path.startswith(("workspace/", "findings/")):
        text, _counts = scrub.scrub_text(text, os.environ)
    data = text.encode("utf-8")
    try:
        loop_store.submit_check(path, data, exists)
        scrub.assert_clean({path: data}, os.environ)
        changed = store.put(path, data, "cloud")
    except loop_store.StoreError as exc:
        message = str(exc)
        return _deny(409 if message == "exists" else 400, message)
    except ValueError:
        return _deny(400, "content failed the secret check")
    resp = jsonify({"path": path, "stored": changed})
    resp.headers["Cache-Control"] = "no-store"
    return resp
