"""Photo attachments on comments: what the server accepts, and what it stores.

``test_community_api_routes.py`` covers who may post and delete; this covers the
new bytes on the wire. Every assertion here is about a decision that cannot be
made on the client, because the client is the attacker in each of these cases:

* the filename and the ``Content-Type`` are attacker-controlled, so acceptance
  has to come from decoding the file (``backend/utils/comment_images.py``),
* a header can promise 900 megapixels in 300 bytes, so dimensions have to be
  checked before the pixels are read,
* the stored file has to be a re-encode, or EXIF/GPS and any appended payload
  ride along into a URL the whole internet can fetch,
* and the serving route must not be steerable at all.

SQLite isolation mirrors ``test_community_api_routes.py`` — never touches prod
Postgres — and ``COMMENT_UPLOAD_DIR`` is redirected at ``tmp_path``, so a test
run never writes into the real ``comment_uploads/``.
"""
from __future__ import annotations

import io
import json
import os
import struct
import zlib
from pathlib import Path

import pytest
from PIL import Image

from backend.db import comments_db
from backend.db.comments_db import SCOPE_CAR, list_comments
from backend.main import app
from backend.utils import comment_images

CSRF = "comment-attachment-test-csrf-token-32-chars"
CAR_ID = 202
USER_ID = 77


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def uploads(tmp_path, monkeypatch):
    """Redirect the upload directory; yield it so tests can inspect the disk."""
    target = tmp_path / "comment_uploads"
    monkeypatch.setenv("COMMENT_UPLOAD_DIR", str(target))
    return target


@pytest.fixture()
def seeded(tmp_path, monkeypatch, uploads):
    monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "1")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setenv("COMMENT_POST_RPM", "500")
    monkeypatch.setenv("COMMENT_FLAG_RPM", "500")
    from backend.db import inventory_db

    monkeypatch.setattr(inventory_db, "DB_PATH", os.path.join(str(tmp_path), "inv.db"))
    conn = inventory_db.get_conn()
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS cars (id INTEGER PRIMARY KEY, vin TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS dealerships (id INTEGER PRIMARY KEY, name TEXT)")
        cur.execute(f"INSERT INTO cars (id, vin) VALUES ({CAR_ID}, 'TESTVIN0000000002')")
        conn.commit()
    finally:
        conn.close()
    return inventory_db


def _client(user_id: int | None = USER_ID, ip: str = "203.0.113.44"):
    c = app.test_client()
    c.environ_base["REMOTE_ADDR"] = ip
    with c.session_transaction() as sess:
        sess.clear()
        sess["_csrf_token"] = CSRF
        if user_id is not None:
            sess["user_id"] = user_id
    return c


def _post(client, files, body="Sat in it this morning, here is what it looks like."):
    """Multipart POST in the shape ds_comments.js sends (field name ``images``)."""
    data = {"body": body, "images": files}
    return client.post(
        f"/api/cars/{CAR_ID}/comments",
        data=data,
        content_type="multipart/form-data",
        headers={"X-CSRF-Token": CSRF},
    )


def _stored_files(uploads: Path) -> list[Path]:
    return sorted(p for p in uploads.rglob("*") if p.is_file())


# ---------------------------------------------------------------------------
# Sample payloads
# ---------------------------------------------------------------------------

def _png_bytes(width=40, height=30, color=(200, 30, 30)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (width, height), color).save(buf, format="PNG")
    return buf.getvalue()


def _jpeg_with_exif() -> bytes:
    """A JPEG carrying EXIF, including a GPS IFD (the field that leaks a home)."""
    exif = Image.Exif()
    exif[0x010F] = "DealershipScannerTest"   # Make
    exif[0x0110] = "SecretCameraModel"       # Model
    exif[0x8825] = {1: "N", 2: (37.0, 46.0, 30.0), 3: "W", 4: (122.0, 25.0, 10.0)}  # GPSInfo
    buf = io.BytesIO()
    Image.new("RGB", (60, 40), (10, 90, 160)).save(buf, format="JPEG", exif=exif)
    return buf.getvalue()


def _png_with_declared_size(width: int, height: int) -> bytes:
    """A PNG whose IHDR promises ``width`` x ``height`` but carries no real pixels.

    This is the decompression-bomb shape: a few hundred bytes on the wire that
    a decoder would expand to gigabytes. ``Image.open`` reads the IHDR and
    reports the size without decoding, which is precisely why the dimension
    check has to happen there and not after ``load()``.
    """
    def chunk(kind: bytes, payload: bytes) -> bytes:
        body = kind + payload
        return struct.pack(">I", len(payload)) + body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", ihdr)
        + chunk(b"IDAT", zlib.compress(b"\x00" * 64))
        + chunk(b"IEND", b"")
    )


# ---------------------------------------------------------------------------
# (a) Not an image
# ---------------------------------------------------------------------------

def test_a_text_file_named_jpg_is_rejected(seeded, uploads):
    """Neither the extension nor the declared Content-Type is evidence of anything."""
    payload = (io.BytesIO(b"#!/bin/sh\nrm -rf /\n"), "totally_a_photo.jpg", "image/jpeg")
    rv = _post(_client(), [payload])

    assert rv.status_code == 400
    assert rv.get_json()["error"] == "not_an_image"
    # The comment itself must not exist either: a rejected upload leaves nothing.
    assert list_comments(SCOPE_CAR, CAR_ID) == []
    assert _stored_files(uploads) == []


def test_a_valid_image_with_an_appended_payload_does_not_keep_it(seeded, uploads):
    """The classic polyglot: real PNG header, hostile tail. The tail is not pixels.

    Storing the upload verbatim would serve that tail back from our own origin.
    The re-encode rebuilds the file from decoded pixel data, so there is nothing
    for the appended bytes to survive in.
    """
    marker = b"<?php system($_GET[0]); ?>"
    rv = _post(_client(), [(io.BytesIO(_png_bytes() + marker), "polyglot.png", "image/png")])
    assert rv.status_code == 201

    stored = _stored_files(uploads)
    assert len(stored) == 1
    assert marker not in stored[0].read_bytes()


# ---------------------------------------------------------------------------
# (b) Too big — on the wire and in declared pixels
# ---------------------------------------------------------------------------

def test_declared_dimensions_over_the_cap_are_rejected(seeded, uploads):
    """20000 x 2000 is under the megapixel ceiling but over the edge ceiling."""
    bomb = _png_with_declared_size(20000, 2000)
    assert len(bomb) < 1000, "the point is that a bomb is tiny on the wire"

    rv = _post(_client(), [(io.BytesIO(bomb), "wide.png", "image/png")])
    assert rv.status_code == 400
    assert rv.get_json()["error"] == "image_dimensions"
    assert list_comments(SCOPE_CAR, CAR_ID) == []
    assert _stored_files(uploads) == []


def test_a_decompression_bomb_is_rejected_before_it_is_decoded(seeded, uploads):
    """40000 x 40000 = 1.6 gigapixels, ~6 GB decoded, ~300 bytes on the wire."""
    bomb = _png_with_declared_size(40000, 40000)
    rv = _post(_client(), [(io.BytesIO(bomb), "bomb.png", "image/png")])

    assert rv.status_code == 400
    # Either guard is a correct answer: the explicit size check, or Pillow's own
    # MAX_IMAGE_PIXELS (lowered to match) refusing to hand back an image at all.
    assert rv.get_json()["error"] in {"image_dimensions", "not_an_image"}
    assert _stored_files(uploads) == []


def test_a_file_over_the_byte_cap_is_rejected(seeded, uploads):
    """Bytes are capped before decode, so the cost of a rejection stays flat."""
    oversized = _png_bytes() + b"\x00" * (comment_images.MAX_UPLOAD_BYTES + 1)
    rv = _post(_client(), [(io.BytesIO(oversized), "huge.png", "image/png")])

    assert rv.status_code == 400
    assert rv.get_json()["error"] == "image_too_large"
    assert _stored_files(uploads) == []


def test_more_than_four_photos_are_rejected(seeded, uploads):
    files = [
        (io.BytesIO(_png_bytes(color=(i * 20, 10, 10))), f"p{i}.png", "image/png")
        for i in range(comment_images.MAX_ATTACHMENTS_PER_COMMENT + 1)
    ]
    rv = _post(_client(), files)

    assert rv.status_code == 400
    assert rv.get_json()["error"] == "too_many_images"
    assert list_comments(SCOPE_CAR, CAR_ID) == []
    assert _stored_files(uploads) == []


# ---------------------------------------------------------------------------
# (c) A valid upload round-trips
# ---------------------------------------------------------------------------

def test_a_valid_upload_round_trips_and_renders(seeded, uploads):
    """Post two photos, read the thread back, fetch the bytes from the URL given."""
    client = _client()
    files = [
        (io.BytesIO(_png_bytes(40, 30)), "front.png", "image/png"),
        (io.BytesIO(_jpeg_with_exif()), "interior.jpg", "image/jpeg"),
    ]
    rv = _post(client, files)
    assert rv.status_code == 201

    created = rv.get_json()["comment"]
    assert len(created["attachments"]) == 2
    # Order is the order they were attached, and the URL is the only path a
    # client learns -- the on-disk name never leaves the server.
    body = rv.get_data(as_text=True)
    assert "storage_key" not in body
    assert "front.png" not in body and "interior.jpg" not in body

    listed = app.test_client().get(f"/api/cars/{CAR_ID}/comments").get_json()
    attachments = listed["comments"][0]["attachments"]
    assert [a["id"] for a in attachments] == [a["id"] for a in created["attachments"]]
    assert all(a["width"] > 0 and a["height"] > 0 for a in attachments)

    # And the bytes actually come back as a decodable image of the right type.
    for att in attachments:
        served = app.test_client().get(att["url"])
        assert served.status_code == 200
        assert served.mimetype in comment_images.ALLOWED_MIME_TYPES
        with Image.open(io.BytesIO(served.get_data())) as im:
            assert (im.width, im.height) == (att["width"], att["height"])

    assert len(_stored_files(uploads)) == 2


def test_exif_including_gps_does_not_survive_the_round_trip(seeded, uploads):
    """A phone photo of a car carries the coordinates of the driveway it was in."""
    source = _jpeg_with_exif()
    with Image.open(io.BytesIO(source)) as im:
        assert dict(im.getexif()), "fixture must actually carry EXIF"
        assert im.getexif().get_ifd(0x8825), "fixture must actually carry GPS"

    rv = _post(_client(), [(io.BytesIO(source), "phone.jpg", "image/jpeg")])
    assert rv.status_code == 201

    url = rv.get_json()["comment"]["attachments"][0]["url"]
    served = app.test_client().get(url).get_data()
    with Image.open(io.BytesIO(served)) as im:
        assert dict(im.getexif()) == {}
        assert not im.getexif().get_ifd(0x8825)
    assert b"SecretCameraModel" not in served


def test_a_json_comment_still_posts_and_carries_no_attachments(seeded):
    """The JSON path is untouched: uploads are additive, not a replacement."""
    rv = _client().post(
        f"/api/cars/{CAR_ID}/comments",
        data=json.dumps({"body": "No photos, just a note."}),
        content_type="application/json",
        headers={"X-CSRF-Token": CSRF},
    )
    assert rv.status_code == 201
    assert rv.get_json()["comment"]["attachments"] == []


# ---------------------------------------------------------------------------
# (d) The serving route cannot be steered
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "attempt",
    [
        "/comment-images/../../etc/passwd",
        "/comment-images/..%2f..%2fetc%2fpasswd",
        "/comment-images/%2e%2e%2f%2e%2e%2fetc%2fpasswd",
        "/comment-images/....//....//etc/passwd",
        "/comment-images/1/../../../etc/passwd",
        "/comment-images/..\\..\\windows\\win.ini",
        "/comment-images/inventory.db",
    ],
)
def test_the_serving_route_cannot_be_traversed(seeded, uploads, attempt):
    """The URL takes an integer id; there is no path segment to poison.

    Anything that is not an integer never matches the rule, so these are 404 at
    the router, before a view runs. The check below is deliberately "not 200 and
    no file content" rather than "404", so a future rule change that starts
    matching these still fails this test.
    """
    rv = app.test_client().get(attempt)
    assert rv.status_code != 200
    assert b"root:" not in rv.get_data()


def test_an_unknown_or_deleted_attachment_id_is_404(seeded, uploads):
    assert app.test_client().get("/comment-images/999999").status_code == 404


def test_a_storage_key_outside_the_upload_dir_never_resolves(seeded, uploads, tmp_path):
    """Defence in depth: even a poisoned DB row cannot address another file.

    ``storage_key`` is minted server-side and never comes from a request, so
    this is not reachable today. It is asserted anyway because the row is the
    one string in the system that turns into a filesystem read.
    """
    outside = tmp_path / "secret.txt"
    outside.write_text("not yours")
    for key in ("../secret.txt", "/etc/passwd", "ab/../../secret.txt", "AB/" + "f" * 32 + ".jpg"):
        assert comment_images.resolve_stored_path(key) is None


# ---------------------------------------------------------------------------
# Auth and lifecycle
# ---------------------------------------------------------------------------

def test_an_anonymous_visitor_cannot_upload(seeded, uploads):
    """Same rule as posting text: login required, and nothing is written first."""
    rv = _post(_client(user_id=None), [(io.BytesIO(_png_bytes()), "a.png", "image/png")])

    assert rv.status_code == 401
    assert rv.get_json()["error"] == "not_logged_in"
    assert _stored_files(uploads) == []


def test_an_upload_without_the_csrf_header_is_rejected(seeded, uploads):
    """Multipart must not be a way around the check the JSON path passes."""
    client = _client()
    rv = client.post(
        f"/api/cars/{CAR_ID}/comments",
        data={"body": "no csrf on this one", "images": (io.BytesIO(_png_bytes()), "a.png", "image/png")},
        content_type="multipart/form-data",
    )
    assert rv.status_code == 403
    assert _stored_files(uploads) == []


def test_deleting_a_comment_removes_its_photos(seeded, uploads):
    client = _client()
    rv = _post(client, [(io.BytesIO(_png_bytes()), "a.png", "image/png")])
    comment = rv.get_json()["comment"]
    url = comment["attachments"][0]["url"]
    assert app.test_client().get(url).status_code == 200

    deleted = client.delete(
        f"/api/cars/{CAR_ID}/comments/{comment['id']}", headers={"X-CSRF-Token": CSRF}
    )
    assert deleted.status_code == 200
    # Both halves: the row stops resolving and the bytes leave the disk.
    assert app.test_client().get(url).status_code == 404
    assert _stored_files(uploads) == []


def test_another_user_cannot_delete_the_photos(seeded, uploads):
    rv = _post(_client(user_id=1), [(io.BytesIO(_png_bytes()), "a.png", "image/png")])
    comment = rv.get_json()["comment"]

    denied = _client(user_id=2).delete(
        f"/api/cars/{CAR_ID}/comments/{comment['id']}", headers={"X-CSRF-Token": CSRF}
    )
    assert denied.status_code == 403
    assert len(_stored_files(uploads)) == 1
    assert app.test_client().get(comment["attachments"][0]["url"]).status_code == 200


# ---------------------------------------------------------------------------
# Storage location
# ---------------------------------------------------------------------------

def test_the_upload_dir_is_anchored_to_the_repo_root_not_the_cwd(monkeypatch, tmp_path):
    """The bug this repo already paid for once: a data path resolved against cwd.

    ``users.db``/``incomplete_listings.db`` silently split into a root copy and a
    ``backend/`` copy depending on where the process was started. Uploads resolve
    from this file's location instead, and a relative override still resolves
    against the repo root.
    """
    monkeypatch.delenv("COMMENT_UPLOAD_DIR", raising=False)
    repo_root = Path(comment_images.__file__).resolve().parent.parent.parent

    monkeypatch.chdir(tmp_path)
    assert comment_images.upload_dir() == repo_root / "comment_uploads"

    monkeypatch.setenv("COMMENT_UPLOAD_DIR", "var/uploads")
    assert comment_images.upload_dir() == repo_root / "var" / "uploads"


def test_stored_names_are_opaque_and_never_the_uploaded_filename(seeded, uploads):
    rv = _post(_client(), [(io.BytesIO(_png_bytes()), "../../etc/passwd.png", "image/png")])
    assert rv.status_code == 201

    stored = _stored_files(uploads)
    assert len(stored) == 1
    relative = stored[0].relative_to(uploads.resolve())
    assert comment_images._STORAGE_KEY_RE.match(relative.as_posix()), relative
    assert "passwd" not in relative.as_posix()


# ---------------------------------------------------------------------------
# Client/server contract (mirrors test_community_api_routes.py)
# ---------------------------------------------------------------------------

def _widget_js() -> str:
    return (
        Path(__file__).resolve().parents[2] / "frontend" / "static" / "ds_comments.js"
    ).read_text(encoding="utf-8")


def test_the_widget_never_string_builds_an_image_url():
    """Only ``body_html`` -- escaped server-side -- may be written with innerHTML.

    An image URL spliced into markup is a sink: it takes an attacker-influenced
    string and re-parses it as HTML. The widget assigns ``img.src`` as a
    property instead, which cannot become an element no matter what it contains.
    """
    js = _widget_js()
    assert "img.src = a.url" in js
    assert "lightbox.img.src = url" in js

    writes = [
        line.strip()
        for line in js.splitlines()
        if ".innerHTML =" in line and '= ""' not in line
    ]
    assert writes == ['body.innerHTML = c.body_html || "";'], writes


def test_the_multipart_post_still_sends_the_csrf_header_and_no_content_type():
    """Setting Content-Type by hand on FormData drops the multipart boundary.

    The header the server actually checks is sent explicitly either way, so the
    upload path passes exactly the CSRF check the JSON path does.
    """
    js = _widget_js()
    assert 'fd.append("images", files[i])' in js
    assert '"X-CSRF-Token": csrfToken()' in js
    # The JSON branch is the only place a Content-Type is set.
    content_type_writes = [
        line.strip() for line in js.splitlines() if '"Content-Type"' in line
    ]
    assert content_type_writes == ['init.headers["Content-Type"] = "application/json";']


def test_attachments_survive_a_restart(seeded, uploads):
    """Rows, not process memory: a fresh read of the thread still finds them."""
    rv = _post(_client(), [(io.BytesIO(_png_bytes()), "a.png", "image/png")])
    attachment_id = rv.get_json()["comment"]["attachments"][0]["id"]

    # A brand-new read path, nothing cached from the request that wrote it.
    row = comments_db.get_attachment(attachment_id)
    assert row is not None
    assert row["mime_type"] in comment_images.ALLOWED_MIME_TYPES
    assert comment_images.resolve_stored_path(row["storage_key"]).is_file()
