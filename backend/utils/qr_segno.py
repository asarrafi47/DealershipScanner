"""Encode arbitrary strings (e.g. TOTP otpauth:// URIs) as PNG QR images using Segno (no qrcode / Pillow for QR)."""

from __future__ import annotations

import io

import segno


def png_bytes(*, data: str, box_size: int = 6) -> bytes:
    """Return PNG bytes; error correction defaults to M (Segno default)."""
    s = (data or "").strip()
    if not s:
        return b""
    q = segno.make(s, error="M")
    buf = io.BytesIO()
    # scale=box_size approximately matches pips per module (segno "scale" is module pixel size)
    q.save(buf, kind="png", scale=max(1, int(box_size)))
    return buf.getvalue()
