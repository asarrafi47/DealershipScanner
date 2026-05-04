"""Read-only client for U.S. EPA FuelEconomy.gov REST/XML endpoints."""
from __future__ import annotations

import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any

BASE = "https://www.fueleconomy.gov/ws/rest/vehicle"
DEFAULT_UA = "DealershipScanner/1.0 (+https://example.local; vehicle reference ingest)"


def _local_tag(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[-1]
    return tag


def _fetch(url: str, *, timeout: int = 90) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": DEFAULT_UA})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def parse_menu_items(xml_text: str) -> list[tuple[str, str]]:
    root = ET.fromstring(xml_text)
    out: list[tuple[str, str]] = []
    for item in root.findall(".//menuItem"):
        t = (item.findtext("text") or "").strip()
        v = (item.findtext("value") or "").strip()
        if t and v:
            out.append((t, v))
    return out


def menu_years(sleep_s: float = 0.0) -> list[int]:
    xml = _fetch(f"{BASE}/menu/year")
    if sleep_s:
        time.sleep(sleep_s)
    years: list[int] = []
    for _, v in parse_menu_items(xml):
        try:
            years.append(int(v))
        except ValueError:
            continue
    return sorted(set(years))


def menu_makes(year: int, sleep_s: float = 0.0) -> list[tuple[str, str]]:
    q = urllib.parse.urlencode({"year": str(year)})
    xml = _fetch(f"{BASE}/menu/make?{q}")
    if sleep_s:
        time.sleep(sleep_s)
    return parse_menu_items(xml)


def menu_models(year: int, make: str, sleep_s: float = 0.0) -> list[tuple[str, str]]:
    q = urllib.parse.urlencode({"year": str(year), "make": make})
    xml = _fetch(f"{BASE}/menu/model?{q}")
    if sleep_s:
        time.sleep(sleep_s)
    return parse_menu_items(xml)


def menu_options(year: int, make: str, model: str, sleep_s: float = 0.0) -> list[tuple[str, str]]:
    q = urllib.parse.urlencode({"year": str(year), "make": make, "model": model})
    xml = _fetch(f"{BASE}/menu/options?{q}")
    if sleep_s:
        time.sleep(sleep_s)
    return parse_menu_items(xml)


def vehicle_record(vehicle_id: int, sleep_s: float = 0.0) -> dict[str, Any]:
    xml = _fetch(f"{BASE}/{int(vehicle_id)}")
    if sleep_s:
        time.sleep(sleep_s)
    root = ET.fromstring(xml)
    if _local_tag(root.tag) == "vehicle":
        veh = root
    else:
        veh = root.find("vehicle")
    if veh is None:
        return {}
    out: dict[str, Any] = {}
    for child in list(veh):
        tag = _local_tag(child.tag)
        text = (child.text or "").strip()
        out[tag] = text
    return out


def fetch_with_retries(
    fn,
    *args,
    retries: int = 4,
    backoff_s: float = 1.5,
    **kwargs,
):
    last: Exception | None = None
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ET.ParseError) as e:
            last = e
            time.sleep(backoff_s * (attempt + 1))
    assert last is not None
    raise last
