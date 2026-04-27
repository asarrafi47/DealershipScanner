"""Shared helpers for parsers."""
import os
import re
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse


def _has_vin(d: dict) -> bool:
    if not isinstance(d, dict):
        return False
    return "vin" in d or "VIN" in d


def get_total_count(obj) -> int | None:
    """Extract totalCount / pageInfo.totalCount / totalPages from API response. Returns None if not found."""
    if not isinstance(obj, dict):
        return None
    v = obj.get("totalCount") or obj.get("total_count") or obj.get("totalRecords")
    if v is not None and isinstance(v, (int, float)):
        return int(v)
    pi = obj.get("pageInfo") or obj.get("page_info") or obj.get("pagination")
    if isinstance(pi, dict):
        v = pi.get("totalCount") or pi.get("total") or pi.get("totalRecords")
        if v is not None and isinstance(v, (int, float)):
            return int(v)
        total_pages = pi.get("totalPages") or pi.get("total_pages")
        per = pi.get("pageSize") or pi.get("perPage") or pi.get("pageSize")
        if total_pages is not None and per is not None and isinstance(total_pages, (int, float)) and isinstance(per, (int, float)):
            return int(total_pages) * int(per)
    return None


def _vin_dict_count(lst: list) -> int:
    return sum(1 for i in lst if isinstance(i, dict) and _has_vin(i))


def _collect_vehicle_list_candidates(obj, min_vin_count: int) -> list[tuple[list, int, int]]:
    """Every list with >= min_vin_count VIN-bearing dicts: (list, vin_count, len(list))."""
    found: list[tuple[list, int, int]] = []

    def walk(o: Any) -> None:
        if o is None:
            return
        if isinstance(o, list):
            vc = _vin_dict_count(o)
            if vc >= min_vin_count:
                found.append((o, vc, len(o)))
            for item in o:
                walk(item)
        elif isinstance(o, dict):
            for v in o.values():
                walk(v)

    walk(obj)
    return found


def find_vehicle_list(obj, min_vin_count: int = 3) -> list | None:
    """
    Recursively search JSON for lists that contain at least min_vin_count dicts with
    ``vin`` / ``VIN``. When several qualify (e.g. compare widget vs inventory), returns
    the list with the most VIN-bearing dicts, then the longest list as tie-break.
    """
    cands = _collect_vehicle_list_candidates(obj, min_vin_count)
    if not cands:
        return None
    cands.sort(key=lambda t: (-t[1], -t[2]))
    return cands[0][0]


def _get_nested(obj: dict, *paths) -> any:
    """Try each path (e.g. ('price',), ('pricing', 'internetPrice')) and return first non-None."""
    for path in paths:
        cur = obj
        for key in path:
            if not isinstance(cur, dict):
                break
            cur = cur.get(key)
        if cur is not None and cur != "":
            return cur
    return None


def extract_price(obj: dict) -> float:
    """Price: prefer nested pricing.internetPrice, then msrp, salePrice, price. Strips $ and commas."""
    v = _get_nested(
        obj,
        ("pricing", "internetPrice"),
        ("pricing", "msrp"),
        ("pricing", "salePrice"),
        ("pricing", "price"),
        ("internetPrice",),
        ("msrp",),
        ("salePrice",),
        ("askingPrice",),
        ("price",),
        ("Price",),
        ("sellingPrice",),
        ("internet_Price",),
        ("internet_price",),
        ("listPrice",),
    )
    price = norm_float(v)
    raw = v
    if price == 0 or (isinstance(raw, str) and "contact" in (raw or "").lower()):
        fallback = _get_nested(
            obj,
            ("pricing", "msrp"),
            ("pricing", "internetPrice"),
            ("pricing", "salePrice"),
            ("msrp",),
            ("internetPrice",),
            ("sellingPrice",),
            ("internet_Price",),
            ("internet_price",),
            ("price",),
        )
        price = norm_float(fallback)
    return price


def _is_placeholder_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return True
    u = url.lower()
    return "coming-soon" in u or "placeholder" in u or "no-image" in u or "default" in u


_RESIZE_QUERY_KEYS_DROP = frozenset(
    {
        "w",
        "width",
        "h",
        "height",
        "dw",
        "dh",
        "size",
        "p",
        "quality",
        "q",
        "thumbnail",
        "thumb",
    }
)

_IMAGE_KEY_SUBSTRINGS = (
    "image",
    "photo",
    "picture",
    "media",
    "gallery",
    "spin",
    "thumb",
    "asset",
    "hero",
    "vehiclephoto",
    "imageurl",
    "imageurls",
    "photourl",
    "photolist",
    "mediaset",
    "carousel",
    "viewer",
    "360",
)


def inventory_gallery_max() -> int:
    """Max gallery URLs per vehicle (inventory + VDP merge cap). Override with SCANNER_INVENTORY_GALLERY_MAX."""
    try:
        return max(4, int((os.environ.get("SCANNER_INVENTORY_GALLERY_MAX") or "48").strip()))
    except ValueError:
        return 48


def normalize_image_url_https(url: str) -> str:
    """Normalize scheme-relative and http→https for CDN hosts (best-effort)."""
    u = (url or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("http://"):
        return "https://" + u[7:]
    return u


def strip_obvious_resize_query_params(url: str) -> str:
    """Drop common width/height/thumb query params when safe (absolute URLs only)."""
    u = (url or "").strip()
    if not u or "?" not in u:
        return u
    try:
        p = urlparse(u)
        if not p.scheme or not p.netloc:
            return u
        pairs = [
            (k, v)
            for k, v in parse_qsl(p.query, keep_blank_values=True)
            if k.lower() not in _RESIZE_QUERY_KEYS_DROP
        ]
        new_q = urlencode(pairs)
        return urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, p.fragment))
    except (ValueError, TypeError):
        return u


def _canonical_image_dedupe_key(url: str) -> str:
    """Group thumb vs large variants for dedupe_prefer_large."""
    nu = normalize_image_url_https(url.strip()).lower()
    nu = strip_obvious_resize_query_params(nu).lower()
    try:
        p = urlparse(nu)
        return f"{p.netloc}{p.path}"
    except (ValueError, TypeError):
        return nu


def _image_url_prefer_score(url: str) -> int:
    """Heuristic: larger dimensions in query/path → higher score."""
    u = normalize_image_url_https(url)
    score = min(200, len(u))
    low = u.lower()
    if "xxlarge" in low or "xlarge" in low or "full" in low or "hires" in low or "1920" in low:
        score += 80
    if "large" in low and "small" not in low:
        score += 25
    if "thumb" in low or "thumbnail" in low or "small" in low or "icon" in low:
        score -= 40
    m = re.findall(r"(?:width|w|h|height)=(\d+)", low)
    for g in m:
        try:
            score += min(120, int(g) // 10)
        except ValueError:
            pass
    return score


def dedupe_urls_order_prefer_large(urls: list[str], *, max_len: int) -> list[str]:
    """
    Dedupe by canonical path (after stripping obvious resize params), preserving first
    position in list but swapping in a later URL if it has a higher prefer score.
    Output order is stable by first canonical occurrence.
    """
    key_to_idx: dict[str, int] = {}
    out: list[str] = []
    for u in urls:
        if not isinstance(u, str):
            continue
        nu = normalize_image_url_https(u.strip())
        if not nu.startswith("https://"):
            continue
        if _is_placeholder_url(nu):
            continue
        k = _canonical_image_dedupe_key(nu)
        if k not in key_to_idx:
            key_to_idx[k] = len(out)
            out.append(nu)
            if len(out) >= max_len:
                break
            continue
        i = key_to_idx[k]
        if _image_url_prefer_score(nu) > _image_url_prefer_score(out[i]):
            out[i] = nu
    return out[:max_len]


def _looks_like_media_url(s: str) -> bool:
    if not s or not isinstance(s, str):
        return False
    t = s.strip()
    if not t.startswith(("http://", "https://", "//", "/")):
        return False
    low = t.lower().split("?", 1)[0]
    return bool(re.search(r"\.(jpe?g|png|webp|gif|avif)(\s*$)", low, re.I))


def harvest_image_urls_from_json(
    obj: Any,
    base_url: str,
    *,
    max_urls: int,
    max_nodes: int = 14000,
) -> list[str]:
    """
    Walk nested dict/list JSON and collect http(s) image URLs from common vehicle-media keys
    and string leaves that look like image URLs.
    """
    seen_ids: set[int] = set()
    out: list[str] = []
    nodes = 0

    def push_url(raw: str) -> None:
        if len(out) >= max_urls:
            return
        u = clean_image_url(raw.strip(), base_url)
        u = normalize_image_url_https(u)
        if not u.startswith("http") or _is_placeholder_url(u):
            return
        if u not in out:
            out.append(u)

    def extract_from_value(val: Any) -> None:
        if len(out) >= max_urls or nodes >= max_nodes:
            return
        if isinstance(val, str):
            if _looks_like_media_url(val):
                push_url(val)
            return
        if isinstance(val, list):
            for it in val:
                if len(out) >= max_urls:
                    return
                if isinstance(it, dict):
                    u = _image_url_from_media_item(it, base_url)
                    if u.startswith("http"):
                        push_url(u)
                elif isinstance(it, str) and _looks_like_media_url(it):
                    push_url(it)
            return
        if isinstance(val, dict):
            walk(val)

    def walk(x: Any) -> None:
        nonlocal nodes
        if len(out) >= max_urls or nodes >= max_nodes:
            return
        nodes += 1
        if isinstance(x, dict):
            i = id(x)
            if i in seen_ids:
                return
            seen_ids.add(i)
            for k, v in x.items():
                if len(out) >= max_urls or nodes >= max_nodes:
                    return
                lk = str(k).lower().replace("_", "").replace("-", "")
                if any(h in lk for h in _IMAGE_KEY_SUBSTRINGS):
                    extract_from_value(v)
                walk(v)
        elif isinstance(x, list):
            for it in x:
                walk(it)
        elif isinstance(x, str) and _looks_like_media_url(x):
            push_url(x)

    walk(obj)
    return dedupe_urls_order_prefer_large(out, max_len=max_urls)


def _image_url_from_media_item(item, base_url: str) -> str:
    """Get URL from a single image dict (url, URL, or uri). Returns empty if placeholder."""
    if not isinstance(item, dict):
        return ""
    u = item.get("url") or item.get("URL") or item.get("uri")
    if u and isinstance(u, str) and not _is_placeholder_url(u):
        return clean_image_url(u, base_url)
    return ""


def extract_gallery_urls(obj: dict, base_url: str, *, max_images: int | None = None) -> list[str]:
    """
    Collect ordered unique photo URLs (http/https) from common CDK / Dealer.com shapes,
    then deep-harvest from the same vehicle JSON (nested media keys and image-like strings).
    Used by dealer.on and any JSON where listing rows include multiple media items.
    """
    mx = max_images if max_images is not None else inventory_gallery_max()
    seen: set[str] = set()
    out: list[str] = []

    def add(u: str) -> None:
        u = normalize_image_url_https((u or "").strip())
        if not u.startswith("https://") or u in seen:
            return
        seen.add(u)
        out.append(u)

    candidates = (
        obj.get("images"),
        obj.get("Images"),
        obj.get("photos"),
        obj.get("Photos"),
        obj.get("media"),
        obj.get("Media"),
        obj.get("vehiclePhotos"),
        obj.get("pictures"),
    )
    for images in candidates:
        if not isinstance(images, list):
            continue
        for it in images[: mx + 8]:
            if isinstance(it, dict):
                u = _image_url_from_media_item(it, base_url)
            elif isinstance(it, str):
                u = clean_image_url(it, base_url)
            else:
                u = ""
            nu = normalize_image_url_https(u)
            if nu.startswith("https://"):
                add(nu)
            if len(out) >= mx:
                shallow = dedupe_urls_order_prefer_large(out, max_len=mx)
                harvested = harvest_image_urls_from_json(obj, base_url, max_urls=mx)
                return dedupe_urls_order_prefer_large(shallow + harvested, max_len=mx)

    hero = extract_image_url(obj, base_url)
    nh = normalize_image_url_https(hero)
    if nh.startswith("https://"):
        add(nh)
    shallow = dedupe_urls_order_prefer_large(out, max_len=mx)
    harvested = harvest_image_urls_from_json(obj, base_url, max_urls=mx)
    return dedupe_urls_order_prefer_large(shallow + harvested, max_len=mx)


def extract_image_url(obj: dict, base_url: str) -> str:
    """Images: images[0].uri/url first (Dealer.com nested), then primaryImage, photoUrl, etc. Checks images exists and length > 0."""
    images = obj.get("images") or obj.get("Images")
    if isinstance(images, list) and len(images) > 0:
        for idx in [0, 1]:
            if idx >= len(images):
                break
            url = _image_url_from_media_item(images[idx], base_url)
            if url:
                return url
        first = images[0]
        url = _image_url_from_media_item(first, base_url)
        if url:
            return url
    v = _get_nested(obj, ("primaryImage", "url"), ("image", "url"), ("photoUrl",), ("imageUrl",), ("image_url",))
    if v and isinstance(v, str) and not _is_placeholder_url(v):
        return clean_image_url(v, base_url)
    v = obj.get("photo") or obj.get("thumbnail") or obj.get("image") or ""
    return clean_image_url(v if isinstance(v, str) else "", base_url)


def extract_mileage(obj: dict) -> int:
    """Mileage: odometer first, then mileage, then attributes.mileage. Parsed as integer."""
    v = _get_nested(
        obj,
        ("odometer",),
        ("mileage",),
        ("Mileage",),
        ("attributes", "mileage"),
        ("attributes", "odometer"),
        ("miles",),
        ("kms",),
    )
    return norm_int(v)


def clean_image_url(url: str, base_url: str) -> str:
    if not url or not isinstance(url, str):
        return ""
    url = url.strip()
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        base = base_url.rstrip("/")
        if "://" in base:
            return base + url
        return "https:" + base + url
    return url


def find_tracking_attr(arr, name_key: str, value_key: str = "value"):
    """Find object in array where obj[name_key] == name_key (or matches). Return obj[value_key] or None. Defensive."""
    if not arr or not isinstance(arr, list):
        return None
    want = (name_key or "").strip().lower()
    if not want:
        return None
    for item in arr:
        if not isinstance(item, dict):
            continue
        n = item.get("name") or item.get("key") or item.get("id")
        if n is None:
            continue
        if str(n).strip().lower() == want:
            return item.get(value_key) or item.get("value")
    return None


def norm_float(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return 0.0


def norm_int(v) -> int:
    if v is None:
        return 0
    try:
        s = str(v).replace(",", "").strip()
        return int(float(s)) if s else 0
    except (ValueError, TypeError):
        return 0


def norm_str(v) -> str:
    if v is None:
        return ""
    return str(v).strip()
