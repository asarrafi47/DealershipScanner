"""Decode, re-encode and store photos attached to a comment.

The bytes for :data:`backend.db.comments_db.comment_attachments` live on local
disk under ``<repo_root>/comment_uploads``; nothing here talks to an external
service or a CDN. Everything a comment upload is allowed to be is decided in
this module, server-side, and every one of those decisions is deliberate:

* **The file is what Pillow says it is.** The filename and the client-sent
  ``Content-Type`` are both attacker-controlled and are used for nothing. A
  submission is an image only if ``Image.open`` reports a format in
  :data:`ALLOWED_FORMATS`.

* **The stored file is not the uploaded file.** Every accepted image is decoded
  to raw pixels and written back out through ``Image.frombytes``, which carries
  no ``info`` dict. That is what strips EXIF (a phone photo of a car carries the
  GPS coordinates of the driveway it was taken in) and what neutralizes a
  polyglot: appended payloads, trailing archives and PNG text chunks are simply
  not part of the pixel data, so they do not survive the round trip. Orientation
  is applied before the strip, so a portrait phone photo does not come out
  sideways once its EXIF tag is gone.

* **Dimensions are checked before the pixels are read.** ``Image.open`` parses a
  header, so a 30000x30000 PNG whose IHDR promises 3.6 GB of pixels is rejected
  from its declared size without ever being decoded. ``Image.MAX_IMAGE_PIXELS``
  is lowered to match as a second line of defence for anything that reaches a
  decoder by another path.

* **The stored name is opaque.** ``storage_key`` is
  ``"<2 hex>/<32 hex>.<ext>"`` from :func:`secrets.token_hex` and is generated
  here. No part of a user-supplied filename ever reaches the filesystem, so
  there is no traversal payload to sanitize -- the shard prefix keeps one
  directory from growing to a million entries.

The upload directory is anchored to the repo root the way
``backend/db/repositories/base_repo.py`` anchors ``inventory.db``, and for the
same reason: resolving a data path against the process's cwd once split this
app's SQLite databases into a root copy and a ``backend/`` copy depending on
where it was started from.
"""

from __future__ import annotations

import os
import re
import secrets
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable

from PIL import Image, ImageOps

# --- limits ---------------------------------------------------------------

# Enough to show a car from four angles; small enough that one comment cannot
# dominate a thread or a phone's data plan.
MAX_ATTACHMENTS_PER_COMMENT = 4

# Per file, measured on the raw upload before anything is decoded.
MAX_UPLOAD_BYTES = 8 * 1024 * 1024

# Slack over the theoretical maximum body (4 x 8 MiB) for multipart boundaries,
# part headers and the comment text itself.
MAX_REQUEST_BYTES = MAX_ATTACHMENTS_PER_COMMENT * MAX_UPLOAD_BYTES + 1024 * 1024

# Decompression-bomb ceiling. A 50 MP source is far beyond any phone camera and
# still ~200 MB decoded; anything larger is rejected from its header.
MAX_SOURCE_PIXELS = 50_000_000
MAX_SOURCE_EDGE = 12_000

# What actually gets written. A comment thumbnail opened full size never needs
# more than this, and it bounds disk growth per upload to a few hundred KB.
MAX_STORED_EDGE = 1600

# Input formats accepted -> (extension, mime) the re-encode writes back out.
# Output is an allowlist, not "whatever came in": a format absent here is never
# produced, so the serving route's Content-Type is drawn from a closed set.
ALLOWED_FORMATS: dict[str, tuple[str, str]] = {
    "JPEG": ("jpg", "image/jpeg"),
    "PNG": ("png", "image/png"),
    "WEBP": ("webp", "image/webp"),
}

ALLOWED_MIME_TYPES = frozenset(mime for _ext, mime in ALLOWED_FORMATS.values())

# Second line of defence behind the explicit size check below. Pillow's own
# default is ~89 MP; lowering it is strictly a tightening for this process.
if Image.MAX_IMAGE_PIXELS is None or Image.MAX_IMAGE_PIXELS > MAX_SOURCE_PIXELS:
    Image.MAX_IMAGE_PIXELS = MAX_SOURCE_PIXELS

# The only shape a stored key may have. Applied when a key is minted AND again
# when one is read back out of the database, so a row edited by any other means
# still cannot address a file outside the upload directory.
_STORAGE_KEY_RE = re.compile(r"^[0-9a-f]{2}/[0-9a-f]{32}\.(?:jpg|png|webp)$")

# backend/utils/ -> backend/ -> repo root. Anchored, never cwd-relative.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent

_DEFAULT_UPLOAD_DIRNAME = "comment_uploads"


class AttachmentError(ValueError):
    """Rejected before anything was written; ``code`` is the API error string."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class PreparedImage:
    """A validated, re-encoded image that has not been written to disk yet."""

    storage_key: str
    data: bytes
    mime_type: str
    width: int
    height: int

    @property
    def byte_size(self) -> int:
        return len(self.data)


# --- storage location -----------------------------------------------------

def upload_dir() -> Path:
    """Absolute directory the bytes live in (``COMMENT_UPLOAD_DIR`` overrides).

    Resolved from this file's location, not the cwd. A relative override is
    still resolved against the repo root rather than wherever the process was
    started, because that difference is exactly what once produced two
    divergent copies of this app's SQLite databases.
    """
    raw = (os.environ.get("COMMENT_UPLOAD_DIR") or "").strip()
    if not raw:
        return _REPO_ROOT / _DEFAULT_UPLOAD_DIRNAME
    path = Path(raw).expanduser()
    return path if path.is_absolute() else (_REPO_ROOT / path)


def resolve_stored_path(storage_key: str | None) -> Path | None:
    """Filesystem path for a stored key, or ``None`` if the key is not one.

    The key is re-matched against :data:`_STORAGE_KEY_RE` and the joined path is
    re-checked for containment. Both are belt and braces -- keys are minted here
    and never come from a request -- but the serving route is the one place a
    stored string turns into a filesystem read, and that is worth two lines.
    """
    if not isinstance(storage_key, str) or not _STORAGE_KEY_RE.match(storage_key):
        return None
    root = upload_dir().resolve()
    candidate = (root / storage_key).resolve()
    if not candidate.is_relative_to(root):
        return None
    return candidate


def _new_storage_key(ext: str) -> str:
    token = secrets.token_hex(16)
    return f"{token[:2]}/{token}.{ext}"


# --- validation + re-encode -----------------------------------------------

def _read_upload(item: Any, index: int) -> bytes:
    """Raw bytes of one Werkzeug ``FileStorage``, size-capped before decoding."""
    stream = getattr(item, "stream", None) or item
    try:
        stream.seek(0, os.SEEK_END)
        size = stream.tell()
        stream.seek(0)
    except (AttributeError, OSError):
        size = None
    if size is not None and size > MAX_UPLOAD_BYTES:
        raise AttachmentError(
            "image_too_large",
            f"Photo {index + 1} is larger than {MAX_UPLOAD_BYTES // (1024 * 1024)} MB.",
        )
    # Read one byte past the cap so a stream that would not report its length
    # (chunked upload) is still bounded rather than trusted.
    raw = stream.read(MAX_UPLOAD_BYTES + 1)
    if not raw:
        raise AttachmentError("empty_image", f"Photo {index + 1} was empty.")
    if len(raw) > MAX_UPLOAD_BYTES:
        raise AttachmentError(
            "image_too_large",
            f"Photo {index + 1} is larger than {MAX_UPLOAD_BYTES // (1024 * 1024)} MB.",
        )
    return raw


def _has_alpha(im: Image.Image) -> bool:
    return im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info)


def _encode_kwargs(fmt: str) -> dict[str, Any]:
    if fmt == "JPEG":
        return {"quality": 88, "optimize": True, "progressive": True}
    if fmt == "PNG":
        return {"optimize": True}
    return {"quality": 88, "method": 4}


def prepare_image(raw: bytes, index: int = 0) -> PreparedImage:
    """Validate one upload and return its re-encoded bytes. Never touches disk.

    Raises :class:`AttachmentError` for anything that is not a decodable image
    in an allowed format, or whose declared dimensions exceed the bomb ceiling.
    """
    too_big = AttachmentError(
        "image_dimensions",
        f"Photo {index + 1} is too large to process "
        f"(over {MAX_SOURCE_EDGE} px on a side or {MAX_SOURCE_PIXELS // 1_000_000} megapixels).",
    )
    not_an_image = AttachmentError(
        "not_an_image", f"Photo {index + 1} is not a JPEG, PNG or WebP image."
    )
    try:
        with Image.open(BytesIO(raw)) as im:
            fmt = (im.format or "").upper()
            if fmt not in ALLOWED_FORMATS:
                raise not_an_image
            width, height = im.size
            # Header-only so far: reject the bomb before a pixel is decoded.
            if width < 1 or height < 1:
                raise not_an_image
            if (
                width > MAX_SOURCE_EDGE
                or height > MAX_SOURCE_EDGE
                or width * height > MAX_SOURCE_PIXELS
            ):
                raise too_big

            ext, mime = ALLOWED_FORMATS[fmt]
            # Apply the orientation tag while it still exists; the re-encode
            # below drops it, and a sideways photo is a bug report.
            oriented = ImageOps.exif_transpose(im) or im
            target_mode = "RGB"
            if fmt != "JPEG" and _has_alpha(oriented):
                target_mode = "RGBA"
            working = oriented.convert(target_mode)
            working.thumbnail((MAX_STORED_EDGE, MAX_STORED_EDGE), Image.LANCZOS)
            # Rebuilding from raw pixels is the strip: frombytes produces an
            # image with an empty info dict, so EXIF/GPS, PNG text chunks and
            # any appended polyglot payload cannot reach the saved file.
            clean = Image.frombytes(working.mode, working.size, working.tobytes())
    except AttachmentError:
        raise
    except Image.DecompressionBombError as exc:
        raise too_big from exc
    except Exception as exc:  # unreadable, truncated, or not an image at all
        raise not_an_image from exc

    buf = BytesIO()
    try:
        clean.save(buf, format=fmt, **_encode_kwargs(fmt))
    except Exception as exc:
        raise AttachmentError(
            "image_unreadable", f"Photo {index + 1} could not be processed."
        ) from exc
    return PreparedImage(
        storage_key=_new_storage_key(ext),
        data=buf.getvalue(),
        mime_type=mime,
        width=clean.width,
        height=clean.height,
    )


def prepare_uploads(files: Iterable[Any]) -> list[PreparedImage]:
    """Validate every uploaded file, or raise on the first one that fails.

    All-or-nothing on purpose: silently dropping the third of four photos would
    look like data loss to the person who attached it.
    """
    items = [f for f in files if f is not None and getattr(f, "filename", "") != ""]
    if not items:
        return []
    if len(items) > MAX_ATTACHMENTS_PER_COMMENT:
        raise AttachmentError(
            "too_many_images",
            f"Attach at most {MAX_ATTACHMENTS_PER_COMMENT} photos per comment.",
        )
    return [prepare_image(_read_upload(item, i), i) for i, item in enumerate(items)]


# --- disk -----------------------------------------------------------------

def store(prepared: PreparedImage) -> Path:
    """Write one prepared image, exclusively so a key collision cannot overwrite."""
    path = resolve_stored_path(prepared.storage_key)
    if path is None:  # unreachable: the key was minted by _new_storage_key
        raise AttachmentError("image_unreadable", "Could not store that photo.")
    path.parent.mkdir(parents=True, exist_ok=True)
    # O_EXCL: 128 bits of token makes a collision implausible, but "implausible"
    # and "silently serves someone else's photo" are different guarantees.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(prepared.data)
    except Exception:
        discard(prepared.storage_key)
        raise
    return path


def discard(storage_key: str | None) -> None:
    """Best-effort unlink. A file that is already gone is the desired state."""
    path = resolve_stored_path(storage_key)
    if path is None:
        return
    try:
        path.unlink()
    except OSError:
        pass
