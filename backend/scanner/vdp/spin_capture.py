"""
360-spin asset capture helpers (Impel/SpinCar/WebRotate).

Pure URL-classification and sequence-detection logic for the VDP spin capture step in
``backend/scanner/vdp/core.py``. Everything here is side-effect free and unit-testable;
the Playwright-driving part lives in ``core._vdp_spin_capture``.

Observed Impel URL shapes (live traffic, 2026-07-06):

- exterior frames:   https://cdn.impel.io/swipetospin-viewers/{customer}/{vin}/{stamp}/ec/0-N.jpg
  (N = 0..numImgEC-1; a ``low-res/ec/0-N.jpg`` tier may also load)
- closeups:          .../{stamp}/closeups/cu-N.jpg          (regular gallery photos — NOT spin frames)
- interior pano:     .../{stamp}/pano/pano_f.jpg (+ _b/_l/_r/_u/_d) — a CUBEMAP of square faces,
  not an equirectangular image. Per contract, ``interior_pano`` must be a single equirect URL,
  so cubemap faces are detected and skipped.
- manifest JSON:     https://api.impel.io/spin/{customer}/{vin}
  keys: ``cdn_image_prefix`` (protocol-relative), ``info.options.numImgEC``,
  ``info.views.exterior.load_images_from`` (segment name, "ec").
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

# Hosts/paths that identify a 360-spin provider.
SPIN_PROVIDER_TOKENS: tuple[str, ...] = ("impel", "spincar", "swipetospin", "webrotate")

# Path segments (on spin-provider URLs) that hold spin-viewer-only assets which must never
# reach the vehicle gallery: exterior frame sequences and interior panos, at any res tier.
_SPIN_RESERVED_SEGMENTS: frozenset[str] = frozenset({"ec", "pano"})

# Cubemap face suffixes (filename stem endings) — f/b/l/r/u/d faces of an interior pano.
_CUBE_FACE_RE = re.compile(r"_(?:f|b|l|r|u|d)$", re.I)

# Static viewer chrome that must never count as vehicle imagery (e.g. the Impel
# ``spincar-static/.../spin_pano_cursors/cursor-spin.png`` UI cursor contains "pano").
_SPIN_JUNK_FRAGMENTS: tuple[str, ...] = (
    "spincar-static",
    "/ui/",
    "cursor",
    "icon",
    "sprite",
    "logo",
    "avatar",
    "button",
    "placeholder",
    "feature_tour",
)

_IMG_EXT_RE = re.compile(r"\.(jpe?g|png|webp|avif)$", re.I)
_MIN_SPIN_FRAMES = 8
_MAX_SPIN_FRAMES = 256


def _url_host_path(url: str) -> tuple[str, str]:
    try:
        p = urlparse((url or "").strip())
        return (p.netloc or "").lower(), p.path or ""
    except (ValueError, TypeError):
        return "", ""


def is_spin_provider_url(url: str) -> bool:
    """True when *url* lives on a known 360-spin provider host/path (Impel, SpinCar, WebRotate)."""
    host, path = _url_host_path(url)
    if not host:
        return False
    blob = f"{host}{path.lower()}"
    return any(tok in blob for tok in SPIN_PROVIDER_TOKENS)


def is_spin_reserved_url(url: str) -> bool:
    """
    True when *url* is a spin-viewer-only asset (exterior frame / interior pano) that must be
    kept OUT of the vehicle gallery. Closeups (``/closeups/cu-N.jpg``) are regular photos and
    return False — they stay in the gallery as today.
    """
    if not is_spin_provider_url(url):
        return False
    _, path = _url_host_path(url)
    low = path.lower()
    if "/closeups/" in low:
        return False
    segs = {s for s in low.split("/") if s}
    return bool(segs & _SPIN_RESERVED_SEGMENTS)


def spin_url_key(url: str) -> str:
    """Normalized dedupe/exclusion key: lowercase scheme://host/path (query dropped)."""
    host, path = _url_host_path(url)
    return f"{host}{path}".lower()


def _frame_skeleton_and_index(filename: str) -> tuple[str, int] | None:
    """
    ("0-#.jpg", 12) for "0-12.jpg". Uses the LAST digit run in the stem as the rotation
    index; returns None when the stem has no digits (not a numbered frame).
    """
    m = _IMG_EXT_RE.search(filename)
    if not m:
        return None
    stem, ext = filename[: m.start()], filename[m.start():]
    runs = list(re.finditer(r"\d+", stem))
    if not runs:
        return None
    last = runs[-1]
    try:
        idx = int(last.group(0))
    except ValueError:
        return None
    skeleton = f"{stem[: last.start()]}#{stem[last.end():]}{ext}".lower()
    return skeleton, idx


def _looks_low_res(dir_path: str) -> bool:
    low = dir_path.lower()
    return "low-res" in low or "lowres" in low or "thumb" in low


def extract_spin_assets(urls: list[str]) -> dict[str, Any]:
    """
    Partition observed spin-provider image URLs into an ordered exterior frame sequence and an
    interior pano.

    Returns ``{"spin_frames": [...], "interior_pano": str|None}``. ``spin_frames`` is [] unless
    a numbered same-directory sequence with >= 8 distinct indices exists; frames are ordered by
    their numeric filename index (NOT network arrival order). ``interior_pano`` is set only for
    a single non-cubemap, non-sequence pano candidate (confident equirect).
    """
    out: dict[str, Any] = {"spin_frames": [], "interior_pano": None}
    groups: dict[tuple[str, str, str], dict[int, str]] = {}
    pano_candidates: dict[str, str] = {}

    seen: set[str] = set()
    for u in urls or []:
        if not isinstance(u, str):
            continue
        u = u.strip()
        if not u.lower().startswith("https://") or not is_spin_provider_url(u):
            continue
        key = spin_url_key(u)
        if key in seen:
            continue
        seen.add(key)
        host, path = _url_host_path(u)
        low_path = path.lower()
        if "/closeups/" in low_path:
            continue
        if any(frag in low_path for frag in _SPIN_JUNK_FRAGMENTS):
            continue
        filename = low_path.rsplit("/", 1)[-1]
        dir_path = low_path.rsplit("/", 1)[0] if "/" in low_path else ""
        if not _IMG_EXT_RE.search(filename):
            continue
        if "pano" in low_path:
            pano_candidates[key] = u
            continue
        ski = _frame_skeleton_and_index(filename)
        if ski is None:
            continue
        skeleton, idx = ski
        g = groups.setdefault((host, dir_path, skeleton), {})
        g.setdefault(idx, u)

    # Frame sequence: biggest numbered group with >= 8 distinct indices; prefer full-res tiers.
    best: tuple[tuple[int, int, str], dict[int, str]] | None = None
    for (host, dir_path, skeleton), members in groups.items():
        if len(members) < _MIN_SPIN_FRAMES:
            continue
        rank = (len(members), 0 if _looks_low_res(dir_path) else 1, f"{host}{dir_path}/{skeleton}")
        if best is None or rank > best[0]:
            best = (rank, members)
    if best is not None:
        members = best[1]
        ordered = [members[i] for i in sorted(members)][:_MAX_SPIN_FRAMES]
        out["spin_frames"] = ordered

    # Interior pano: single non-cubemap, non-sequence candidate only.
    remaining: list[str] = []
    cube_faces = 0
    for key, u in pano_candidates.items():
        _, path = _url_host_path(u)
        filename = path.lower().rsplit("/", 1)[-1]
        m = _IMG_EXT_RE.search(filename)
        stem = filename[: m.start()] if m else filename
        if _CUBE_FACE_RE.search(stem):
            cube_faces += 1
            continue
        if _frame_skeleton_and_index(filename) is not None and re.search(r"\d", stem):
            # numbered pano tiles (multi-row/cube tiles) — not a single equirect
            continue
        remaining.append(u)
    if len(remaining) == 1 and cube_faces == 0:
        out["interior_pano"] = remaining[0]
    return out


_IMPEL_MANIFEST_RE = re.compile(r"https://api\.impel\.io/spin/[^/\s?]+/[a-z0-9]+", re.I)
_SWIPETOSPIN_PATH_RE = re.compile(r"swipetospin-viewers/([^/\s]+)/([a-z0-9]{11,17})/", re.I)
_IFRAME_CUSTOMER_RE = re.compile(r"[#!&]customer=([a-z0-9_\-]+)", re.I)
_IFRAME_VIN_RE = re.compile(r"[#!&]vin=([a-z0-9]{11,17})", re.I)


def build_impel_manifest_url_candidates(
    config_urls: list[str],
    asset_urls: list[str],
    iframe_urls: list[str],
    *,
    max_candidates: int = 4,
) -> list[str]:
    """
    Candidate Impel spin-manifest URLs, best first:
    1. observed ``api.impel.io/spin/{customer}/{vin}`` requests (query stripped),
    2. derived from observed ``swipetospin-viewers/{customer}/{vin}/...`` asset paths,
    3. derived from the viewer iframe URL hash (``#!customer=X!vin=Y``).
    """
    out: list[str] = []
    seen: set[str] = set()

    def _push(u: str) -> None:
        k = u.lower()
        if k not in seen and len(out) < max_candidates:
            seen.add(k)
            out.append(u)

    for u in config_urls or []:
        m = _IMPEL_MANIFEST_RE.search(u or "")
        if m:
            _push(m.group(0))
    for u in list(asset_urls or []) + list(iframe_urls or []):
        m = _SWIPETOSPIN_PATH_RE.search(u or "")
        if m:
            _push(f"https://api.impel.io/spin/{m.group(1)}/{m.group(2).lower()}")
    for u in iframe_urls or []:
        mc = _IFRAME_CUSTOMER_RE.search(u or "")
        mv = _IFRAME_VIN_RE.search(u or "")
        if mc and mv:
            _push(f"https://api.impel.io/spin/{mc.group(1)}/{mv.group(1).lower()}")
    return out


def parse_impel_spin_manifest(data: Any) -> dict[str, Any]:
    """
    Build the ordered exterior frame URL list from an Impel spin manifest
    (``api.impel.io/spin/{customer}/{vin}`` JSON).

    Frame URL shape (verified live): ``{cdn_image_prefix}{segment}/0-{i}.jpg`` for
    i in range(numImgEC), segment from ``info.views.exterior.load_images_from`` ("ec").
    Returns ``{"spin_frames": [...], "interior_pano": None}`` — the Impel pano is a cubemap
    (see module docstring), never a single equirect, so it is not surfaced here.
    """
    out: dict[str, Any] = {"spin_frames": [], "interior_pano": None}
    if not isinstance(data, dict):
        return out
    prefix = str(data.get("cdn_image_prefix") or "").strip()
    if prefix.startswith("//"):
        prefix = "https:" + prefix
    if not prefix.lower().startswith("https://"):
        return out
    if not prefix.endswith("/"):
        prefix += "/"
    info = data.get("info") if isinstance(data.get("info"), dict) else {}
    opts = info.get("options") if isinstance(info.get("options"), dict) else {}
    try:
        n = int(opts.get("numImgEC") or 0)
    except (TypeError, ValueError):
        n = 0
    if n < _MIN_SPIN_FRAMES:
        return out
    n = min(n, _MAX_SPIN_FRAMES)
    views = info.get("views") if isinstance(info.get("views"), dict) else {}
    ext = views.get("exterior") if isinstance(views.get("exterior"), dict) else {}
    seg = str(ext.get("load_images_from") or "ec").strip().strip("/") or "ec"
    if not re.fullmatch(r"[\w\-]{1,32}", seg):
        seg = "ec"
    out["spin_frames"] = [f"{prefix}{seg}/0-{i}.jpg" for i in range(n)]
    return out
