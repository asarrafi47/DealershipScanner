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


# ---- precompressed siblings (scripts/build_static_compressed.py + _static_view) ----


def test_gzip_sibling_is_served_when_accepted(static_client):
    import gzip

    client, root = static_client
    src = root / "main.js"
    src.write_text("console.log('hello world');" * 20)
    gz = root / "main.js.gz"
    gz.write_bytes(gzip.compress(src.read_bytes(), mtime=0))
    st = src.stat()
    _touch(gz, st.st_mtime)

    resp = client.get("/static/main.js?v=1", headers={"Accept-Encoding": "gzip, deflate"})
    assert resp.status_code == 200
    assert resp.headers["Content-Encoding"] == "gzip"
    assert resp.headers["Content-Length"] == str(gz.stat().st_size)
    assert "Accept-Encoding" in resp.headers.get("Vary", "")
    assert resp.content_type.startswith(("text/javascript", "application/javascript"))
    assert "immutable" in resp.headers["Cache-Control"]
    assert gzip.decompress(resp.data) == src.read_bytes()

    plain = client.get("/static/main.js?v=1", headers={"Accept-Encoding": "identity"})
    assert plain.status_code == 200
    assert "Content-Encoding" not in plain.headers
    assert plain.data == src.read_bytes()
    # Distinct representations carry distinct validators.
    assert plain.headers["ETag"] != resp.headers["ETag"]


def test_stale_gzip_sibling_is_ignored(static_client):
    import gzip

    client, root = static_client
    src = root / "app.css"
    gz = root / "app.css.gz"
    gz.write_bytes(gzip.compress(b"old bytes", mtime=0))
    _touch(gz, 1_700_000_000)
    src.write_text("body { color: red }")
    _touch(src, 1_700_000_100)

    resp = client.get("/static/app.css?v=1", headers={"Accept-Encoding": "gzip"})
    assert resp.status_code == 200
    assert "Content-Encoding" not in resp.headers
    assert resp.data == src.read_bytes()


def test_brotli_preferred_over_gzip_when_both_exist(static_client):
    import gzip

    client, root = static_client
    src = root / "x.js"
    src.write_text("var x = 1;")
    st = src.stat()
    (root / "x.js.gz").write_bytes(gzip.compress(src.read_bytes(), mtime=0))
    (root / "x.js.br").write_bytes(b"not-really-brotli")
    _touch(root / "x.js.gz", st.st_mtime)
    _touch(root / "x.js.br", st.st_mtime)

    resp = client.get("/static/x.js", headers={"Accept-Encoding": "gzip, br"})
    assert resp.headers["Content-Encoding"] == "br"
    assert resp.data == b"not-really-brotli"

    resp = client.get("/static/x.js", headers={"Accept-Encoding": "gzip"})
    assert resp.headers["Content-Encoding"] == "gzip"


def test_build_script_is_idempotent_and_prunes_orphans(tmp_path: Path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "build_static_compressed",
        Path(__file__).resolve().parents[2] / "scripts" / "build_static_compressed.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    (tmp_path / "a.js").write_text("console.log(1);")
    (tmp_path / "css").mkdir()
    (tmp_path / "css" / "b.css").write_text("body{}")
    (tmp_path / "gone.js.gz").write_bytes(b"orphan")
    (tmp_path / "logo.png").write_bytes(b"\x89PNG")

    first = mod.build(tmp_path, quiet=True)
    assert first >= 3  # two .gz (+ two .br when brotli is present) + one orphan removed
    assert (tmp_path / "a.js.gz").exists()
    assert (tmp_path / "css" / "b.css.gz").exists()
    assert not (tmp_path / "gone.js.gz").exists()
    assert not (tmp_path / "logo.png.gz").exists()
    assert (tmp_path / "a.js.gz").stat().st_mtime == (tmp_path / "a.js").stat().st_mtime

    assert mod.build(tmp_path, quiet=True) == 0
    assert mod.build(tmp_path, check=True, quiet=True) == 0

    # Editing a source makes exactly its siblings stale.
    _touch(tmp_path / "a.js", (tmp_path / "a.js").stat().st_mtime + 60)
    assert mod.build(tmp_path, check=True, quiet=True) == (2 if mod.brotli else 1)
