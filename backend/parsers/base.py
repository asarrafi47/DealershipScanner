"""Shared helpers for parsers."""
from urllib.parse import urljoin


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


def find_vehicle_list(obj, min_vin_count: int = 3) -> list | None:
    """
    Recursively search JSON for the first list that contains at least min_vin_count
    dictionaries with a 'vin' or 'VIN' key. Works with Dealer.com, Algolia, etc.
    """
    if obj is None:
        return None
    if isinstance(obj, list):
        vin_count = sum(1 for i in obj if isinstance(i, dict) and _has_vin(i))
        if vin_count >= min_vin_count:
            return obj
        for item in obj:
            found = find_vehicle_list(item, min_vin_count)
            if found is not None:
                return found
        return None
    if isinstance(obj, dict):
        for v in obj.values():
            found = find_vehicle_list(v, min_vin_count)
            if found is not None:
                return found
        return None
    return None


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
            ("price",),
        )
        price = norm_float(fallback)
    return price


def _is_placeholder_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return True
    u = url.lower()
    return "coming-soon" in u or "placeholder" in u or "no-image" in u or "default" in u


def _image_url_from_media_item(item, base_url: str) -> str:
    """Get URL from a single image dict (url, URL, or uri). Returns empty if placeholder."""
    if not isinstance(item, dict):
        return ""
    u = item.get("url") or item.get("URL") or item.get("uri")
    if u and isinstance(u, str) and not _is_placeholder_url(u):
        return clean_image_url(u, base_url)
    return ""


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
