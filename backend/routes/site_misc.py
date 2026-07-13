"""Infra routes: health check, favicon, locally-stored car images."""

from __future__ import annotations

import os
from pathlib import Path

from flask import abort, current_app, jsonify, send_from_directory

# Serve locally-downloaded car images (written by image_downloader.py).
# Stored under <project_root>/car_images/<dealer_id>/<vin>/<file>.
_CAR_IMAGES_DIR = Path(__file__).resolve().parent.parent.parent / "car_images"


def _app_version() -> str:
    version_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "VERSION"
    )
    try:
        with open(version_path, encoding="utf-8") as fh:
            return (fh.read() or "").strip() or "dev"
    except OSError:
        return "dev"


def health():
    return jsonify({"status": "ok", "version": _app_version()}), 200


def favicon():
    return send_from_directory(current_app.static_folder, "favicon.svg", mimetype="image/svg+xml")


def serve_car_image(filename: str):
    # Block path traversal: reject any component that starts with '.' or contains separators
    parts = Path(filename).parts
    if not parts or any(p.startswith(".") or p in ("/", "\\") for p in parts):
        abort(400)
    safe_path = _CAR_IMAGES_DIR.joinpath(*parts).resolve()
    if not safe_path.is_relative_to(_CAR_IMAGES_DIR.resolve()):
        abort(400)
    return send_from_directory(str(_CAR_IMAGES_DIR), str(Path(*parts)))


def register(app) -> None:
    """Attach routes to ``app`` keeping the original bare endpoint names."""
    app.add_url_rule("/health", view_func=health)
    app.add_url_rule("/favicon.ico", view_func=favicon)
    app.add_url_rule("/car-images/<path:filename>", view_func=serve_car_image)
