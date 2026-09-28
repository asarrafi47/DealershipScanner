"""Static asset delivery: cache-buster version, cache headers, precompressed siblings.

The ``?v=`` stamp on every static URL used to be the mtime of the dead
``style.css`` (loaded by nothing), so editing a real partial or script never
changed the query string. That was harmless under Flask's default
``Cache-Control: no-cache`` and becomes a stale-asset bug the moment assets are
cached for a year -- which they now are. These tests pin the contract:

* the version moves when any source file under the static root moves;
* a stamped URL is cached for a year and ``immutable``, an unstamped one is not.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest


def _touch(path: Path, mtime: float) -> None:
    os.utime(path, (mtime, mtime))


def test_version_changes_when_a_static_file_mtime_changes(tmp_path: Path):
    from backend.main import compute_static_cache_ver

    css = tmp_path / "css"
    css.mkdir()
    a = tmp_path / "main.js"
    b = css / "00-base.css"
    a.write_text("// a")
    b.write_text("/* b */")
    base = 1_700_000_000
    _touch(a, base)
    _touch(b, base + 10)

    v1 = compute_static_cache_ver(tmp_path)
    assert v1 == str(base + 10)

    # Editing the *older* file (main.js) past the newest one must move the stamp:
    # the version tracks the newest mtime anywhere in the tree, not one file.
    _touch(a, base + 500)
    v2 = compute_static_cache_ver(tmp_path)
    assert v2 != v1
    assert v2 == str(base + 500)

    # Precompressed siblings are derived artifacts and never bump the stamp.
    gz = tmp_path / "main.js.gz"
    gz.write_bytes(b"\x1f\x8b")
    _touch(gz, base + 9_000)
    assert compute_static_cache_ver(tmp_path) == v2


def test_version_is_stable_when_nothing_changes(tmp_path: Path):
    from backend.main import compute_static_cache_ver

    (tmp_path / "x.js").write_text("x")
    assert compute_static_cache_ver(tmp_path) == compute_static_cache_ver(tmp_path)


def test_empty_static_root_has_a_version(tmp_path: Path):
    from backend.main import compute_static_cache_ver

    assert compute_static_cache_ver(tmp_path) == "1"


@pytest.fixture
def static_client(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users.db"))
    monkeypatch.setenv("INVENTORY_DB_PATH", str(tmp_path / "inventory.db"))
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    from backend.main import app

    static_root = tmp_path / "static"
    static_root.mkdir()
    monkeypatch.setattr(app, "static_folder", str(static_root))
    monkeypatch.setattr(app, "debug", False)
    return app.test_client(), static_root


def test_stamped_static_url_is_immutable_for_a_year(static_client):
    client, root = static_client
    (root / "main.js").write_text("console.log(1);")

    resp = client.get("/static/main.js?v=123")
    assert resp.status_code == 200
    cc = resp.headers["Cache-Control"]
    assert "max-age=31536000" in cc
    assert "immutable" in cc
    assert "public" in cc
    assert resp.headers.get("ETag")

    resp304 = client.get("/static/main.js?v=123", headers={"If-None-Match": resp.headers["ETag"]})
    assert resp304.status_code == 304


def test_unstamped_static_url_is_not_immutable(static_client):
    client, root = static_client
    (root / "placeholder.svg").write_text("<svg/>")

    resp = client.get("/static/placeholder.svg")
    assert resp.status_code == 200
    cc = resp.headers["Cache-Control"]
    assert "immutable" not in cc
    assert "max-age=31536000" not in cc
    assert "max-age=3600" in cc
