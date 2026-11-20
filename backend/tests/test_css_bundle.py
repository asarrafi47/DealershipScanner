"""The site stylesheet bundle (backend/web/css_bundle.py, 2026-11-20).

Pins: the bundle is the global partials concatenated byte for byte in cascade order
(the order _head_css.html linked them in before the bundle); it is rebuilt when a part
changes and never moves the ?v= stamp; pages link ONE stylesheet, or the same parts in
the same order when the bundle cannot be written; the build script gives it
precompressed siblings that the static view serves.
"""

from __future__ import annotations

import gzip
import os
import re
import shutil
from pathlib import Path

import pytest

from backend.web import css_bundle
from backend.web.css_bundle import CSS_BUNDLE_PARTS, CSS_BUNDLE_PATH, bundle_bytes, ensure_bundle

ROOT = Path(__file__).resolve().parents[2]
STATIC = ROOT / "frontend" / "static"

# The cascade order _head_css.html emitted as 15 <link> tags until 2026-11-20 (itself
# style.css's @import order). The bundle must keep it; new parts may only be appended.
HISTORICAL_ORDER = (
    "css/00-base.css", "css/01-nav.css", "css/02-listings.css", "css/03-car-page.css",
    "css/04-dashboard.css", "css/05-car-tools.css", "css/06-dev.css", "css/07-dealer-admin.css",
    "css/08-marketing.css", "css/09-components.css", "css/10-pages.css", "css/11-overrides.css",
    "css/12-viewers.css", "css/13-landing-auth.css", "css/14-vdp-uniform.css",
)

def _build_script():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "build_static_compressed", ROOT / "scripts" / "build_static_compressed.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_MARKER = re.compile(rb"\n/\* ---- (css/[\w.-]+) ---- \*/\n")


def test_part_order_keeps_the_historical_cascade():
    assert CSS_BUNDLE_PARTS[: len(HISTORICAL_ORDER)] == HISTORICAL_ORDER
    assert len(set(CSS_BUNDLE_PARTS)) == len(CSS_BUNDLE_PARTS)
    for rel in CSS_BUNDLE_PARTS:
        assert (STATIC / rel).is_file(), rel
    # Page-specific sheets stay out of the global bundle.
    assert "css/15-dealership.css" not in CSS_BUNDLE_PARTS
    assert "css/16-admin-pages.css" not in CSS_BUNDLE_PARTS


def test_bundle_is_the_concatenation_of_the_parts_in_order():
    data = bundle_bytes(STATIC)
    pieces = _MARKER.split(data)
    # [header, name0, body0, name1, body1, ...]
    names = tuple(p.decode() for p in pieces[1::2])
    bodies = pieces[2::2]
    assert names == CSS_BUNDLE_PARTS
    bodies[-1] = bodies[-1][:-1]  # the trailing newline after the last part
    for rel, body in zip(names, bodies):
        assert body == (STATIC / rel).read_bytes(), rel
    stripped = b"".join(bodies)
    assert stripped == b"".join((STATIC / rel).read_bytes() for rel in CSS_BUNDLE_PARTS)


def test_no_part_imports_or_leaves_a_comment_open():
    # @import/@charset are only valid at the top of a stylesheet; inside the bundle
    # they would be dropped. An unclosed comment would swallow the next part.
    for rel in CSS_BUNDLE_PARTS:
        text = (STATIC / rel).read_text()
        assert not re.search(r"^\s*@(import|charset)\b", text, re.M), rel
        assert text.count("/*") == text.count("*/"), rel


@pytest.fixture
def static_copy(tmp_path):
    root = tmp_path / "static"
    (root / "css").mkdir(parents=True)
    for rel in CSS_BUNDLE_PARTS:
        shutil.copy2(STATIC / rel, root / rel)
    return root


def test_ensure_bundle_writes_rewrites_and_keeps_the_version_stamp(static_copy):
    from backend.web.static import compute_static_cache_ver

    ver = compute_static_cache_ver(static_copy)
    assert ensure_bundle(static_copy) is True
    target = static_copy / CSS_BUNDLE_PATH
    assert target.read_bytes() == bundle_bytes(static_copy)
    # The bundle's mtime is the newest part's, so writing it never moves ?v=.
    assert compute_static_cache_ver(static_copy) == ver
    first_mtime = target.stat().st_mtime

    # Idempotent.
    assert ensure_bundle(static_copy) is True
    assert target.stat().st_mtime == first_mtime

    # An edited part makes it stale; the next check rebuilds it.
    part = static_copy / CSS_BUNDLE_PARTS[3]
    part.write_text(part.read_text() + "\n.zz-test { color: red; }\n")
    later = first_mtime + 50
    os.utime(part, (later, later))
    assert ensure_bundle(static_copy) is True
    assert b".zz-test { color: red; }" in target.read_bytes()
    assert target.stat().st_mtime == later


def test_ensure_bundle_reports_false_when_it_cannot_write(static_copy, monkeypatch):
    def deny(*a, **k):
        raise OSError("read-only")

    monkeypatch.setattr(css_bundle.os, "replace", deny)
    assert ensure_bundle(static_copy) is False
    assert not (static_copy / CSS_BUNDLE_PATH).exists()
    assert not list((static_copy / "css").glob("*.tmp"))


def test_build_script_writes_the_bundle_and_its_siblings(static_copy):
    build = _build_script().build

    build(static_copy, quiet=True)
    target = static_copy / CSS_BUNDLE_PATH
    assert target.read_bytes() == bundle_bytes(static_copy)
    gz = target.with_name(target.name + ".gz")
    assert gzip.decompress(gz.read_bytes()) == target.read_bytes()
    assert build(static_copy, check=True, quiet=True) == 0


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users.db"))
    monkeypatch.setenv("INVENTORY_DB_PATH", str(tmp_path / "inventory.db"))
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    from backend.main import app

    css_bundle.reset_memo()
    yield app, app.test_client()
    css_bundle.reset_memo()


def _stylesheets(html: str) -> list[str]:
    return re.findall(r'<link rel="stylesheet" href="/static/([^"?]+)\?v=\w+">', html)


def test_pages_link_one_bundled_stylesheet(client):
    _app, c = client
    html = c.get("/login").data.decode()
    assert _stylesheets(html) == [CSS_BUNDLE_PATH]


def test_pages_fall_back_to_the_parts_in_order(client, monkeypatch):
    _app, c = client
    monkeypatch.setattr(css_bundle, "ensure_bundle", lambda root: False)
    css_bundle.reset_memo()
    html = c.get("/login").data.decode()
    assert tuple(_stylesheets(html)) == CSS_BUNDLE_PARTS


def test_bundle_is_served_precompressed_and_immutable(client, static_copy, monkeypatch):
    build = _build_script().build

    app, c = client
    build(static_copy, quiet=True)
    monkeypatch.setattr(app, "static_folder", str(static_copy))
    resp = c.get(f"/static/{CSS_BUNDLE_PATH}?v=1", headers={"Accept-Encoding": "gzip"})
    assert resp.status_code == 200
    assert resp.headers["Content-Encoding"] == "gzip"
    assert resp.content_type.startswith("text/css")
    assert "immutable" in resp.headers["Cache-Control"]
    assert gzip.decompress(resp.data) == (static_copy / CSS_BUNDLE_PATH).read_bytes()
