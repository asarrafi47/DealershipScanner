"""Gzip a body that is produced in pieces, without ever joining the pieces (web speed P19).

``_build_cars_scope_entry`` used to join every stored card into one string, encode it to
bytes and call ``gzip.compress`` on that: for a large metro the transient copies were
116.3 MB; compressing piece by piece keeps the peak at 12.9 MB for the same content
(server-causes 4.4 and the README P19 row).

The output is byte for byte what ``zlib.compress(whole, level, wbits=31)`` returns for the
concatenated pieces (the gzip wrapper zlib writes: header mtime 0, OS byte 3), which is
also what ``gzip.compress(whole, level, mtime=0)`` returns on Python 3.12. Unlike
``gzip.compress`` with its default mtime, the header carries no clock, so two builds of
the same content give the same bytes and the same ETag.
"""

from __future__ import annotations

import zlib
from typing import Iterable

_GZIP_WBITS = 31  # 16 + 15: gzip wrapper around a 32 KB window


def gzip_stream(parts: Iterable[bytes], *, level: int = 6) -> bytes:
    """Gzip the concatenation of *parts* (each ``bytes``) one piece at a time."""
    comp = zlib.compressobj(level, zlib.DEFLATED, _GZIP_WBITS)
    out: list[bytes] = []
    for piece in parts:
        if piece:
            chunk = comp.compress(piece)
            if chunk:
                out.append(chunk)
    out.append(comp.flush())
    return b"".join(out)
