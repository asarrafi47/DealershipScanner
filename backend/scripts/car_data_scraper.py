#!/usr/bin/env python3
"""
Standalone automotive data scraper → CSV aligned with Ram-style option dictionaries.

Outputs: `{Year}_{Make}_{Model}_Complete_Options.csv` (under `--output-dir`, default cwd).

Usage:
  python car_data_scraper.py --vehicle "2023 Ram 1500"
  python car_data_scraper.py --year 2023 --make Ram --model "1500"
  python car_data_scraper.py --batch-nhtsa
  python car_data_scraper.py --batch-nhtsa -j 8 --output-dir ./csv_out
  python car_data_scraper.py --batch-nhtsa --all-nhtsa-makes --output-dir ./csv_out
  python car_data_scraper.py --batch-nhtsa --min-year 2022 --max-year 2024 --output-dir ./csv_out

Batch mode calls the free NHTSA vPIC API ``GetModelsForMakeYear`` for years 2020–2025 (override with
flags). By default it uses a **curated list of mainstream U.S.-market passenger OEMs** (Detroit, Stellantis,
German volume/luxury, Japanese, Korean, Tesla/Rivian/Lucid, etc.) — not buses, aircraft, or niche VPIC
noise. Use ``--all-nhtsa-makes`` only if you need the full VPIC union (car + Truck + MPV), then run
Playwright for each (year, make, model).

Dependencies: playwright (required). playwright_stealth (optional, softer bot signals).

Environment:
  HEADLESS=1           Single-vehicle mode: run Chromium headless (default: headed for debugging).
                       Batch mode (--batch-nhtsa) defaults to headless unless you pass --headed.
  BATCH_CONCURRENCY    Batch mode: parallel Playwright contexts (default 4).
  SCRAPER_TIMEOUT_MS   Navigation timeout (default 45000)
  NHTSA_PAUSE_SEC      Delay between NHTSA HTTP calls (default 0.08)
  BATCH_SCRAPE_PAUSE_SEC  Optional delay after each batch scrape (default 0; still respects semaphore)
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import random
import re
import sys
import time
from typing import Any
from urllib.parse import quote, quote_plus
from urllib.request import Request, urlopen

# -----------------------------------------------------------------------------
# Optional stealth (same pattern as many Playwright projects)
# -----------------------------------------------------------------------------
try:
    from playwright_stealth import Stealth

    _STEALTH_CM = Stealth().use_async
    _HAS_STEALTH = True
except ImportError:
    _STEALTH_CM = None
    _HAS_STEALTH = False

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("car_data_scraper")


CSV_COLUMNS = [
    "Year",
    "Make",
    "Model",
    "Trim",
    "engineOptions",
    "transmissionOptions",
    "drivetrainOptions",
    "exteriorColors",
    "Packages",
    "packageDetails",
    "Options",
    "optionDetails",
]

# -----------------------------------------------------------------------------
# NHTSA vPIC — GetModelsForMakeYear/make/{MAKE}/modelyear/{YEAR}?format=json
# Tuple: (API make path segment, Make column value written to CSV)
#
# Curated for typical U.S. retail cars/light trucks (passenger segment). Covers Detroit / Stellantis,
# VAG + Mercedes + BMW Group, Japanese & Korean volume + luxury, Volvo/JLR, EV natives. Omits
# supercar-only marques, buses, RV builders, and other non-mainstream VPIC makes.
# -----------------------------------------------------------------------------
NHTSA_PREDEFINED_MAKES: list[tuple[str, str]] = [
    ("Acura", "Acura"),
    ("Alfa Romeo", "Alfa Romeo"),
    ("Audi", "Audi"),
    ("BMW", "BMW"),
    ("Buick", "Buick"),
    ("Cadillac", "Cadillac"),
    ("Chevrolet", "Chevrolet"),
    ("Chrysler", "Chrysler"),
    ("Dodge", "Dodge"),
    ("FIAT", "FIAT"),
    ("Ford", "Ford"),
    ("Genesis", "Genesis"),
    ("GMC", "GMC"),
    ("Honda", "Honda"),
    ("Hyundai", "Hyundai"),
    ("Infiniti", "Infiniti"),
    ("Jaguar", "Jaguar"),
    ("Jeep", "Jeep"),
    ("Kia", "Kia"),
    ("Land Rover", "Land Rover"),
    ("Lexus", "Lexus"),
    ("Lincoln", "Lincoln"),
    ("Lucid", "Lucid"),
    ("Mazda", "Mazda"),
    ("Mercedes-Benz", "Mercedes"),
    ("MINI", "MINI"),
    ("Mitsubishi", "Mitsubishi"),
    ("Nissan", "Nissan"),
    ("Polestar", "Polestar"),
    ("Porsche", "Porsche"),
    ("Ram", "Ram"),
    ("Rivian", "Rivian"),
    ("Subaru", "Subaru"),
    ("Tesla", "Tesla"),
    ("Toyota", "Toyota"),
    ("Volkswagen", "Volkswagen"),
    ("Volvo", "Volvo"),
]

# GetMakesForVehicleType/{type} — union these to approximate “all light-vehicle makes” in VPIC.
NHTSA_VEHICLE_TYPES_FOR_ALL_MAKES: tuple[str, ...] = (
    "car",
    "Truck",
    "Multipurpose Passenger Vehicle (MPV)",
)


def _is_probable_junk_model(model_name: str) -> bool:
    n = model_name.lower()
    if len(model_name.strip()) < 2:
        return True
    junk = (
        "trailer",
        " llc",
        "radiator",
        "travel park",
        "cranford",
        "tanks",
        "affordable aluminum",
    )
    return any(j in n for j in junk)


def fetch_models_for_make_year(api_make: str, year: int, *, timeout_sec: float = 45.0) -> list[str]:
    """
    Call NHTSA GetModelsForMakeYear for one make/year; returns distinct Model_Name values.
    """
    enc = quote(api_make, safe="")
    url = f"https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMakeYear/make/{enc}/modelyear/{year}?format=json"
    req = Request(url, headers={"User-Agent": "SarrafiCollectionCarDataScraper/1.1"})
    try:
        with urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        log.warning("NHTSA request failed year=%s make=%s: %s", year, api_make, e)
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("NHTSA invalid JSON year=%s make=%s", year, api_make)
        return []
    out: list[str] = []
    for row in data.get("Results") or []:
        mn = row.get("Model_Name") or row.get("model_name") or ""
        mn = str(mn).strip()
        if not mn or _is_probable_junk_model(mn):
            continue
        out.append(mn)
    pause = float(os.environ.get("NHTSA_PAUSE_SEC") or "0.08")
    time.sleep(pause)
    return _uniq_preserve(out)


def fetch_makes_for_vehicle_type(vehicle_type: str, *, timeout_sec: float = 45.0) -> list[str]:
    """Call NHTSA GetMakesForVehicleType for one VPIC vehicle type string."""
    enc = quote(vehicle_type, safe="")
    url = f"https://vpic.nhtsa.dot.gov/api/vehicles/GetMakesForVehicleType/{enc}?format=json"
    req = Request(url, headers={"User-Agent": "SarrafiCollectionCarDataScraper/1.1"})
    try:
        with urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        log.warning("NHTSA GetMakesForVehicleType failed type=%r: %s", vehicle_type, e)
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("NHTSA invalid JSON GetMakesForVehicleType type=%r", vehicle_type)
        return []
    out: list[str] = []
    for row in data.get("Results") or []:
        mn = row.get("MakeName") or ""
        mn = str(mn).strip()
        if mn:
            out.append(mn)
    pause = float(os.environ.get("NHTSA_PAUSE_SEC") or "0.08")
    time.sleep(pause)
    return out


def enumerate_all_nhtsa_makes() -> list[tuple[str, str]]:
    """
    Distinct MakeName values from VPIC car + Truck + MPV (case-insensitive dedupe).
    Each tuple is (api_make, csv_make) with the same string — suitable for GetModelsForMakeYear paths.
    """
    canon: dict[str, str] = {}
    for vt in NHTSA_VEHICLE_TYPES_FOR_ALL_MAKES:
        for mn in fetch_makes_for_vehicle_type(vt):
            key = mn.lower()
            if key not in canon:
                canon[key] = mn
    pairs = sorted((canon[k], canon[k]) for k in sorted(canon.keys()))
    log.info(
        "NHTSA all-makes: %d distinct makes from types %s",
        len(pairs),
        ", ".join(NHTSA_VEHICLE_TYPES_FOR_ALL_MAKES),
    )
    return pairs


def enumerate_nhtsa_batch(
    min_year: int,
    max_year: int,
    makes: list[tuple[str, str]] | None = None,
) -> list[tuple[int, str, str]]:
    """
    Build list of (year, make_for_csv, model) using NHTSA only (no Playwright).
    """
    mk_rows = makes if makes is not None else NHTSA_PREDEFINED_MAKES
    tasks: list[tuple[int, str, str]] = []
    for year in range(min_year, max_year + 1):
        for api_make, csv_make in mk_rows:
            try:
                models = fetch_models_for_make_year(api_make, year)
            except Exception as e:
                log.exception("NHTSA enumeration error %s %s: %s", year, api_make, e)
                continue
            for mdl in models:
                tasks.append((year, csv_make, mdl))
    log.info("NHTSA batch: %d (year, make, model) tuples to scrape", len(tasks))
    return tasks


def _sanitize_filename_part(s: str) -> str:
    s = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "", s.strip())
    return re.sub(r"\s+", "_", s)[:120]


def parse_vehicle_arg(vehicle: str | None, year: int | None, make: str | None, model: str | None) -> tuple[int, str, str]:
    if vehicle:
        m = re.match(r"^\s*(\d{4})\s+(.+?)\s+(.+?)\s*$", vehicle.strip())
        if not m:
            raise ValueError(f'Could not parse --vehicle "{vehicle}". Use e.g. "2023 Ram 1500".')
        y = int(m.group(1))
        mk = m.group(2).strip()
        md = m.group(3).strip()
        return y, mk, md
    if year and make and model:
        return int(year), make.strip(), model.strip()
    raise ValueError("Provide --vehicle \"YYYY Make Model\" OR --year --make --model")


def _uniq_preserve(xs: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in xs:
        t = re.sub(r"\s+", " ", x.strip())
        if len(t) < 2 or t.lower() in seen:
            continue
        seen.add(t.lower())
        out.append(t[:500])
    return out[:40]


def extract_bullet_like_lines(text: str, keywords: tuple[str, ...]) -> list[str]:
    """Pull lines that look like trim/engine/color bullets from noisy page text."""
    lines = []
    for raw in text.splitlines():
        line = raw.strip()
        if len(line) < 4 or len(line) > 280:
            continue
        low = line.lower()
        if any(k in low for k in keywords):
            line = re.sub(r"^[\-*•\d\.\)\s]+", "", line)
            if len(line) >= 4:
                lines.append(line)
    return _uniq_preserve(lines)


_ENGINE_KW = ("engine", "liter", " v6", " v8", "turbo", "diesel", "hybrid", "electric", " hp", "kw")
_TRANS_KW = ("transmission", "speed", "cvt", "dct", "automatic", "manual")
_DRIVE_KW = ("awd", "fwd", "rwd", "4wd", "4x4", "4x2", "drivetrain", "wheel drive")


def heuristic_extract_from_plaintext(blob: str) -> dict[str, list[str]]:
    """Best-effort splits when HTML tables are not structured."""
    engines = extract_bullet_like_lines(blob, _ENGINE_KW)
    trans = extract_bullet_like_lines(blob, _TRANS_KW)
    drives = extract_bullet_like_lines(blob, _DRIVE_KW)
    colors = extract_bullet_like_lines(blob, ("color", "paint", "metallic", "pearl", "black", "white", "red", "blue", "grey", "gray", "silver"))
    trims = extract_bullet_like_lines(blob, ("trim", "xl", "slt", "lari", "limited", "sport", "premium", "base"))
    return {
        "engines": engines[:15],
        "transmissions": trans[:15],
        "drivetrains": drives[:15],
        "colors": colors[:25],
        "trims": trims[:20],
    }


def extract_package_like_phrases(text: str) -> tuple[list[str], list[str]]:
    """
    Turn snippet-heavy text into parallel package names + details (same length).
    """
    names: list[str] = []
    details: list[str] = []
    # Sentence split for option-like clauses
    for chunk in re.split(r"(?<=[\.\!\?])\s+", text):
        chunk = chunk.strip()
        if len(chunk) < 25 or len(chunk) > 600:
            continue
        low = chunk.lower()
        if any(x in low for x in ("package", "group", "equipment", "option", "adds", "includes", "standard")):
            title = chunk[:80].strip()
            names.append(title)
            details.append(chunk[:500])
    # Pair down to unique by title
    out_n, out_d = [], []
    seen = set()
    for n, d in zip(names, details):
        key = n.lower()[:60]
        if key in seen:
            continue
        seen.add(key)
        out_n.append(n)
        out_d.append(d)
    return out_n[:30], out_d[:30]


async def safe_goto(page, url: str, *, timeout_ms: int) -> bool:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await asyncio.sleep(random.uniform(0.4, 1.2))
        return True
    except (PlaywrightTimeoutError, PlaywrightError) as e:
        log.warning("Navigation failed %s: %s", url[:120], e)
        return False


async def ddg_search_snippets(page, query: str, *, timeout_ms: int, max_snippets: int = 12) -> str:
    """Fetch DuckDuckGo HTML results and concatenate snippet text."""
    qurl = f"https://duckduckgo.com/?q={quote_plus(query)}&ia=web"
    if not await safe_goto(page, qurl, timeout_ms=timeout_ms):
        return ""
    texts: list[str] = []
    try:
        # DuckDuckGo layout varies; try several selectors.
        for sel in (
            "[data-result='snippet']",
            ".result__snippet",
            ".web-result snippet",
            "[data-testid='snippet']",
            ".snippet",
        ):
            loc = page.locator(sel)
            n = await loc.count()
            if n == 0:
                continue
            for i in range(min(n, max_snippets)):
                try:
                    t = await loc.nth(i).inner_text(timeout=5000)
                    if t and len(t.strip()) > 20:
                        texts.append(t.strip())
                except Exception as e:
                    log.debug("snippet[%s] %s: %s", sel, i, e)
            if texts:
                break
    except Exception as e:
        log.warning("DDG snippet scrape issue: %s", e)
    return "\n".join(texts)


async def find_wikipedia_vehicle_url(page, year: int, make: str, model: str, *, timeout_ms: int) -> str | None:
    """Resolve a Wikipedia article URL from DuckDuckGo result links (snippets rarely contain full URLs)."""
    qurl = f"https://duckduckgo.com/?q={quote_plus(f'{year} {make} {model} site:en.wikipedia.org')}&ia=web"
    if not await safe_goto(page, qurl, timeout_ms=timeout_ms):
        return None
    try:
        hrefs = await page.evaluate(
            """() => {
              const out = [];
              for (const a of document.querySelectorAll('a[href*="en.wikipedia.org/wiki/"]')) {
                try {
                  const u = new URL(a.href);
                  if (u.hostname.endsWith('wikipedia.org') && u.pathname.startsWith('/wiki/')) {
                    out.push(u.origin + u.pathname);
                  }
                } catch (e) {}
              }
              return [...new Set(out)];
            }"""
        )
        if hrefs:
            return str(hrefs[0]).split("#")[0]
    except Exception as e:
        log.warning("Could not parse Wikipedia links from DDG: %s", e)
    # Last resort: guess title slug (often wrong)
    slug = f"{make}_{model}".replace(" ", "_")
    return f"https://en.wikipedia.org/wiki/{quote_plus(slug).replace('+', '_')}"


async def scrape_wikipedia_article(page, url: str, *, timeout_ms: int) -> str:
    if not url:
        return ""
    if not await safe_goto(page, url, timeout_ms=timeout_ms):
        return ""
    try:
        # Prefer MediaWiki content; fallback body text.
        for sel in ("#mw-content-text", "main", "article", "body"):
            loc = page.locator(sel)
            if await loc.count() == 0:
                continue
            txt = await loc.first.inner_text(timeout=15000)
            if txt and len(txt) > 200:
                return txt
    except Exception as e:
        log.warning("Wikipedia text extract failed: %s", e)
    try:
        return await page.evaluate("() => document.body.innerText.slice(0, 120000)")
    except Exception:
        return ""


async def primary_scrape_pipeline(
    context,
    year: int,
    make: str,
    model: str,
    *,
    timeout_ms: int,
) -> dict[str, Any]:
    """Wikipedia + heuristics first."""
    page = await context.new_page()
    try:
        wiki_url = await find_wikipedia_vehicle_url(page, year, make, model, timeout_ms=timeout_ms)
        log.info("Primary Wikipedia candidate: %s", wiki_url)
        article = await scrape_wikipedia_article(page, wiki_url or "", timeout_ms=timeout_ms)
        heur = heuristic_extract_from_plaintext(article)
        pkg_names, pkg_details = extract_package_like_phrases(article)
        return {
            "source_label": "wikipedia+heuristic",
            "wiki_url": wiki_url,
            "raw_text_sample": article[:2000],
            "engines": heur["engines"],
            "transmissions": heur["transmissions"],
            "drivetrains": heur["drivetrains"],
            "colors": heur["colors"],
            "trims": heur["trims"],
            "packages": pkg_names,
            "package_details": pkg_details,
            "options": [],
            "option_details": [],
        }
    finally:
        await page.close()


async def fallback_search_pipeline(
    context,
    year: int,
    make: str,
    model: str,
    *,
    timeout_ms: int,
    need_packages: bool,
    need_options: bool,
) -> dict[str, list[str]]:
    """Google-it style: DuckDuckGo queries for missing narrative fields."""
    page = await context.new_page()
    out: dict[str, list[str]] = {"packages": [], "package_details": [], "options": [], "option_details": []}
    try:
        queries: list[str] = []
        if need_packages:
            queries.append(f"{year} {make} {model} trim packages equipment groups options list")
        if need_options:
            queries.append(f"{year} {make} {model} optional equipment standalone options description")
        combined_text = ""
        for q in queries:
            blob = await ddg_search_snippets(page, q, timeout_ms=timeout_ms, max_snippets=15)
            combined_text += "\n" + blob
        pn, pd = extract_package_like_phrases(combined_text)
        out["packages"] = pn
        out["package_details"] = pd[: len(pn)]
        # Options as shorter clauses
        opt_names: list[str] = []
        opt_det: list[str] = []
        for sentence in re.split(r"(?<=[\.\!\?])\s+", combined_text):
            s = sentence.strip()
            if 30 < len(s) < 400 and any(
                k in s.lower() for k in ("sunroof", "tow", "camera", "wheel", "audio", "assist", "heated", "ventilated")
            ):
                opt_names.append(s[:90])
                opt_det.append(s[:500])
        out["options"] = _uniq_preserve(opt_names)[:25]
        out["option_details"] = (opt_det + [""] * len(out["options"]))[: len(out["options"])]
        return out
    finally:
        await page.close()


def pad_rectangular_row(
    year: int,
    make: str,
    model: str,
    *,
    trims: list[str],
    engines: list[str],
    transmissions: list[str],
    drivetrains: list[str],
    colors: list[str],
    packages: list[str],
    package_details: list[str],
    options: list[str],
    option_details: list[str],
) -> list[dict[str, Any]]:
    """
    Build rows where every column list is padded to the same length (max over all lists).
    Missing entries become empty string so CSV columns stay aligned.
    """
    cols = {
        "Trim": trims,
        "engineOptions": engines,
        "transmissionOptions": transmissions,
        "drivetrainOptions": drivetrains,
        "exteriorColors": colors,
        "Packages": packages,
        "packageDetails": package_details,
        "Options": options,
        "optionDetails": option_details,
    }
    lengths = [len(v) for v in cols.values()]
    n = max(lengths + [1])
    def pad(lst: list[str]) -> list[str]:
        lst = list(lst)[:n]
        return lst + [""] * (n - len(lst))

    trims = pad(trims)
    engines = pad(engines)
    trans = pad(transmissions)
    drives = pad(drivetrains)
    colors = pad(colors)
    pkgs = pad(packages)
    pkgd = pad(package_details)
    opts = pad(options)
    optd = pad(option_details)

    rows = []
    for i in range(n):
        rows.append(
            {
                "Year": str(year),
                "Make": make,
                "Model": model,
                "Trim": trims[i],
                "engineOptions": engines[i],
                "transmissionOptions": trans[i],
                "drivetrainOptions": drives[i],
                "exteriorColors": colors[i],
                "Packages": pkgs[i],
                "packageDetails": pkgd[i],
                "Options": opts[i],
                "optionDetails": optd[i],
            }
        )
    # Sparse duplicate-Year/Make/Model like hand-built Ram sample: blank inherited columns on subsequent rows
    for idx, r in enumerate(rows):
        if idx == 0:
            continue
        r["Year"] = ""
        r["Make"] = ""
        r["Model"] = ""
    return rows


async def scrape_vehicle_to_csv(
    context,
    year: int,
    make: str,
    model: str,
    *,
    timeout_ms: int,
    output_dir: str,
) -> str:
    """
    Run Wikipedia + DDG pipelines using an existing Playwright browser context; write one CSV.
    """
    try:
        primary = await primary_scrape_pipeline(context, year, make, model, timeout_ms=timeout_ms)
    except Exception as e:
        log.exception("Primary scrape failed (continuing with empty primary): %s", e)
        primary = {
            "engines": [],
            "transmissions": [],
            "drivetrains": [],
            "colors": [],
            "trims": [],
            "packages": [],
            "package_details": [],
            "options": [],
            "option_details": [],
        }

    need_pkg = len(primary.get("packages") or []) < 2 or len(primary.get("package_details") or []) < 2
    need_opt = True
    try:
        fb = await fallback_search_pipeline(
            context,
            year,
            make,
            model,
            timeout_ms=timeout_ms,
            need_packages=need_pkg,
            need_options=need_opt,
        )
    except Exception as e:
        log.exception("Fallback search failed: %s", e)
        fb = {}

    def merge_lists(a: list[str], b: list[str]) -> list[str]:
        return _uniq_preserve([*(a or []), *(b or [])])

    engines = merge_lists(primary.get("engines"), [])
    trans = merge_lists(primary.get("transmissions"), [])
    drives = merge_lists(primary.get("drivetrains"), [])
    colors = merge_lists(primary.get("colors"), [])
    trims = merge_lists(primary.get("trims"), [])
    pkgs = merge_lists(primary.get("packages"), fb.get("packages") or [])
    pkgdet = merge_lists(primary.get("package_details"), fb.get("package_details") or [])
    opts = merge_lists(primary.get("options"), fb.get("options") or [])
    optdet = merge_lists(primary.get("option_details"), fb.get("option_details") or [])

    rows = pad_rectangular_row(
        year,
        make,
        model,
        trims=trims or [""],
        engines=engines or [""],
        transmissions=trans or [""],
        drivetrains=drives or [""],
        colors=colors or [""],
        packages=pkgs or [""],
        package_details=pkgdet or [""],
        options=opts or [""],
        option_details=optdet or [""],
    )

    os.makedirs(output_dir, exist_ok=True)
    out_name = f"{year}_{_sanitize_filename_part(make)}_{_sanitize_filename_part(model)}_Complete_Options.csv"
    out_path = os.path.abspath(os.path.join(output_dir, out_name))
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        for row in rows:
            w.writerow(row)
    log.info("Wrote %s (%d rows)", out_path, len(rows))
    return out_path


async def run_scrape(
    year: int,
    make: str,
    model: str,
    *,
    headless: bool,
    timeout_ms: int,
    output_dir: str,
) -> str:
    """Single vehicle: launch browser, scrape, exit."""

    async def _body(p):
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1365, "height": 900},
            locale="en-US",
        )
        try:
            return await scrape_vehicle_to_csv(
                context, year, make, model, timeout_ms=timeout_ms, output_dir=output_dir
            )
        finally:
            await context.close()
            await browser.close()

    if _HAS_STEALTH:
        async with _STEALTH_CM(async_playwright()) as p:
            return await _body(p)
    log.warning("playwright_stealth not installed — using plain Playwright context.")
    async with async_playwright() as p:
        return await _body(p)


def _resolve_batch_concurrency(cli_value: int | None) -> int:
    if cli_value is not None:
        return max(1, cli_value)
    raw = os.environ.get("BATCH_CONCURRENCY", "").strip()
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            log.warning("Invalid BATCH_CONCURRENCY=%r — using 4", raw)
    return 4


async def run_nhtsa_batch_scrape(
    *,
    headless: bool,
    timeout_ms: int,
    output_dir: str,
    min_year: int,
    max_year: int,
    makes: list[tuple[str, str]] | None,
    concurrency: int,
) -> list[str]:
    """
    Enumerate models via NHTSA for each year/make, then scrape with parallel browser contexts
    (bounded by ``concurrency``) sharing one Chromium process.
    """
    todo = enumerate_nhtsa_batch(min_year, max_year, makes=makes)
    batch_pause = float(os.environ.get("BATCH_SCRAPE_PAUSE_SEC") or "0")
    sem = asyncio.Semaphore(max(1, concurrency))
    ua = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    )

    async def _body(p):
        browser = await p.chromium.launch(headless=headless)

        async def run_one(i: int, year: int, make: str, model: str) -> str | None:
            async with sem:
                context = await browser.new_context(
                    user_agent=ua,
                    viewport={"width": 1365, "height": 900},
                    locale="en-US",
                )
                try:
                    log.info(
                        "Batch [%d/%d] %d %s %s (≤%d concurrent)",
                        i + 1,
                        len(todo),
                        year,
                        make,
                        model,
                        concurrency,
                    )
                    path = await scrape_vehicle_to_csv(
                        context,
                        year,
                        make,
                        model,
                        timeout_ms=timeout_ms,
                        output_dir=output_dir,
                    )
                    return path
                except Exception as e:
                    log.exception("Scrape failed for %d %s %s: %s", year, make, model, e)
                    return None
                finally:
                    await context.close()
                    if batch_pause > 0:
                        await asyncio.sleep(batch_pause)

        try:
            results = await asyncio.gather(
                *(run_one(i, y, mk, md) for i, (y, mk, md) in enumerate(todo))
            )
        finally:
            await browser.close()
        return [r for r in results if r]

    if _HAS_STEALTH:
        async with _STEALTH_CM(async_playwright()) as p:
            return await _body(p)
    async with async_playwright() as p:
        return await _body(p)


def resolve_headless(*, batch_mode: bool, headed_flag: bool) -> bool:
    """Batch runs headless by default so DuckDuckGo/Wikipedia navigations do not flash windows."""
    if headed_flag:
        return False
    if batch_mode:
        return True
    return os.environ.get("HEADLESS", "").strip().lower() in ("1", "true", "yes")


def main() -> int:
    ap = argparse.ArgumentParser(description="Scrape vehicle option hints → CSV (standalone tool).")
    ap.add_argument("--vehicle", type=str, default=None, help='e.g. "2023 Ram 1500"')
    ap.add_argument("--year", type=int, default=None)
    ap.add_argument("--make", type=str, default=None)
    ap.add_argument("--model", type=str, default=None)
    ap.add_argument(
        "--batch-nhtsa",
        action="store_true",
        help="Enumerate models via NHTSA vPIC (curated U.S. mainstream makes by default; years 2020–2025) then scrape each.",
    )
    ap.add_argument(
        "--all-nhtsa-makes",
        action="store_true",
        help="With --batch-nhtsa: use VPIC makes for car+Truck+MPV (hundreds of makes; very large run).",
    )
    ap.add_argument(
        "--headed",
        action="store_true",
        help="Show the Chromium window (batch mode is headless by default; single-vehicle defaults to headed unless HEADLESS=1).",
    )
    ap.add_argument(
        "-j",
        "--concurrency",
        type=int,
        default=None,
        metavar="N",
        help="Batch only: number of vehicles to scrape in parallel (default: BATCH_CONCURRENCY env or 4).",
    )
    ap.add_argument("--min-year", type=int, default=2020, help="Batch mode: first model year (inclusive).")
    ap.add_argument("--max-year", type=int, default=2025, help="Batch mode: last model year (inclusive).")
    ap.add_argument(
        "--output-dir",
        type=str,
        default=".",
        help="Directory for generated CSV files (default: current directory).",
    )
    args = ap.parse_args()

    headless = resolve_headless(batch_mode=bool(args.batch_nhtsa), headed_flag=bool(args.headed))
    timeout_ms = int(os.environ.get("SCRAPER_TIMEOUT_MS") or "45000")
    out_dir = os.path.abspath(args.output_dir)

    if args.batch_nhtsa:
        if args.min_year > args.max_year:
            log.error("--min-year must be <= --max-year")
            return 2
        if args.all_nhtsa_makes:
            batch_makes = enumerate_all_nhtsa_makes()
            makes_desc = f"all VPIC makes ({len(batch_makes)}): car+Truck+MPV"
        else:
            batch_makes = NHTSA_PREDEFINED_MAKES
            makes_desc = (
                f"{len(batch_makes)} curated mainstream makes: "
                + ", ".join(csv_make for _, csv_make in batch_makes)
            )
        conc = _resolve_batch_concurrency(args.concurrency)
        log.info(
            "NHTSA batch | years %d–%d | %s | headless=%s concurrency=%d stealth=%s | out=%s",
            args.min_year,
            args.max_year,
            makes_desc,
            headless,
            conc,
            _HAS_STEALTH,
            out_dir,
        )
        try:
            paths = asyncio.run(
                run_nhtsa_batch_scrape(
                    headless=headless,
                    timeout_ms=timeout_ms,
                    output_dir=out_dir,
                    min_year=args.min_year,
                    max_year=args.max_year,
                    makes=batch_makes,
                    concurrency=conc,
                )
            )
        except KeyboardInterrupt:
            log.error("Interrupted.")
            return 130
        except Exception as e:
            log.exception("Fatal: %s", e)
            return 1
        print(f"wrote {len(paths)} file(s) under {out_dir}")
        return 0

    try:
        year, make, model = parse_vehicle_arg(args.vehicle, args.year, args.make, args.model)
    except ValueError as e:
        log.error("%s", e)
        log.error("Use --vehicle \"YYYY Make Model\" or --batch-nhtsa for API-driven runs.")
        return 2

    log.info(
        "Target: %d %s %s | headless=%s stealth=%s | out=%s",
        year,
        make,
        model,
        headless,
        _HAS_STEALTH,
        out_dir,
    )

    try:
        out = asyncio.run(
            run_scrape(
                year,
                make,
                model,
                headless=headless,
                timeout_ms=timeout_ms,
                output_dir=out_dir,
            )
        )
    except KeyboardInterrupt:
        log.error("Interrupted.")
        return 130
    except Exception as e:
        log.exception("Fatal: %s", e)
        return 1

    print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
