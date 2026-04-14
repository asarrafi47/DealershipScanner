"""
BMW USA official dealer locator intake.

Strategy (prefer structured data over DOM scraping):
1) Optional JSON endpoint overrides (data/oem/bmw_endpoint_overrides.json).
2) Fetch dealer-locator HTML; parse __NEXT_DATA__ / embedded JSON.
3) Scan linked JS bundles for bmwusa/bmw/cloudfront URLs pointing at JSON or /bin/services.
4) GET candidate URLs and parse GeoJSON or dealer arrays.
5) Playwright: capture JSON responses (GeoJSON or dealer-shaped payloads) from network.

All raw responses are returned to the caller for persistence — nothing is discarded.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import requests

from scrapers.oem.discovery import (
    collect_script_src,
    extract_embedded_json_blobs,
    mine_urls_from_js_bundle,
)
from scrapers.oem.bmw_debug import extract_html_title, write_debug_artifact
from scrapers.oem.bmw_parse_trace import explain_parse_outcome, top_level_keys
from scrapers.oem.http import oem_requests_session

logger = logging.getLogger("scrapers.oem.bmw")

BMW_USA_LOCATOR_URL = "https://www.bmwusa.com/dealer-locator.html"
DEFAULT_BRAND = "BMW"


@dataclass
class BMWIntakeBundle:
    """Everything collected in one ingest pass (for raw persistence)."""

    source_locator_url: str
    scraped_at_iso: str
    records: list[dict[str, Any]] = field(default_factory=list)
    raw_payloads: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    debug_report: dict[str, Any] | None = None


def _endpoint_overrides_path(project_root: Path) -> Path:
    return project_root / "data" / "oem" / "bmw_endpoint_overrides.json"


def load_extra_endpoint_urls(project_root: Path) -> list[str]:
    p = _endpoint_overrides_path(project_root)
    if not p.is_file():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    urls = data.get("extra_urls_to_try") or []
    return [u for u in urls if isinstance(u, str) and u.startswith("http")]


def _digits_phone(s: str | None) -> str:
    if not s:
        return ""
    return re.sub(r"\D", "", s)[:10]


def _clean_str(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _coords_from_geojson_feature(feat: dict[str, Any]) -> tuple[float | None, float | None]:
    geom = feat.get("geometry") or {}
    if geom.get("type") != "Point":
        return None, None
    coords = geom.get("coordinates")
    if not isinstance(coords, list) or len(coords) < 2:
        return None, None
    try:
        lon = float(coords[0])
        lat = float(coords[1])
    except (TypeError, ValueError):
        return None, None
    return lat, lon


def _pick_website(props: dict[str, Any]) -> str:
    for key in (
        "url",
        "website",
        "dealerUrl",
        "dealer_url",
        "webSite",
        "homepage",
        "dealerWebsite",
    ):
        v = _clean_str(props.get(key))
        if v and v.startswith("http"):
            return v
    return ""


def _pick_name(props: dict[str, Any]) -> str:
    for key in ("dealerName", "name", "dealer_name", "title", "locationName"):
        v = _clean_str(props.get(key))
        if v:
            return v
    return ""


def _pick_address_fields(props: dict[str, Any]) -> tuple[str, str, str, str]:
    street = _clean_str(
        props.get("street")
        or props.get("address1")
        or props.get("addressLine1")
        or props.get("streetAddress")
    )
    city = _clean_str(props.get("city") or props.get("town"))
    state = _clean_str(props.get("state") or props.get("stateCode") or props.get("region"))
    zip_ = _clean_str(
        props.get("zip") or props.get("zipCode") or props.get("postalCode") or props.get("postcode")
    )
    if not street and props.get("address"):
        street = _clean_str(props.get("address"))
    return street, city, state, zip_


def _pick_phone(props: dict[str, Any]) -> str:
    for key in ("phone", "tel", "telephone", "phoneNumber", "mainPhone"):
        v = _clean_str(props.get(key))
        if v:
            return v
    return ""


def record_from_geojson_feature(
    feat: dict[str, Any],
    *,
    source_locator_url: str,
    scraped_at_iso: str,
    raw_fragment: dict[str, Any],
) -> dict[str, Any] | None:
    props = feat.get("properties")
    if not isinstance(props, dict):
        props = {}
    name = _pick_name(props)
    if not name:
        return None
    street, city, state, zip_ = _pick_address_fields(props)
    lat, lon = _coords_from_geojson_feature(feat)
    phone = _pick_phone(props)
    website = _pick_website(props)
    return {
        "dealer_name": name,
        "brand": DEFAULT_BRAND,
        "street": street,
        "city": city,
        "state": state,
        "zip": zip_,
        "phone": phone,
        "website": website,
        "latitude": lat,
        "longitude": lon,
        "source_locator_url": source_locator_url,
        "scraped_at": scraped_at_iso,
        "raw_source_payload": raw_fragment,
    }


def record_from_flat_dict(
    obj: dict[str, Any],
    *,
    source_locator_url: str,
    scraped_at_iso: str,
    raw_fragment: dict[str, Any],
) -> dict[str, Any] | None:
    name = _pick_name(obj)
    if not name:
        return None
    street, city, state, zip_ = _pick_address_fields(obj)
    lat = obj.get("latitude") or obj.get("lat")
    lon = obj.get("longitude") or obj.get("lng") or obj.get("lon")
    try:
        lat_f = float(lat) if lat is not None else None
    except (TypeError, ValueError):
        lat_f = None
    try:
        lon_f = float(lon) if lon is not None else None
    except (TypeError, ValueError):
        lon_f = None
    return {
        "dealer_name": name,
        "brand": DEFAULT_BRAND,
        "street": street,
        "city": city,
        "state": state,
        "zip": zip_,
        "phone": _pick_phone(obj),
        "website": _pick_website(obj),
        "latitude": lat_f,
        "longitude": lon_f,
        "source_locator_url": source_locator_url,
        "scraped_at": scraped_at_iso,
        "raw_source_payload": raw_fragment,
    }


def parse_json_to_records(
    data: Any,
    *,
    source_locator_url: str,
    scraped_at_iso: str,
    source_hint: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Returns (records, raw_payload_entries)."""
    records: list[dict[str, Any]] = []
    raws: list[dict[str, Any]] = [{"source": source_hint, "payload": data}]

    if isinstance(data, dict) and data.get("type") == "FeatureCollection":
        feats = data.get("features") or []
        if isinstance(feats, list):
            for feat in feats:
                if not isinstance(feat, dict):
                    continue
                rec = record_from_geojson_feature(
                    feat,
                    source_locator_url=source_locator_url,
                    scraped_at_iso=scraped_at_iso,
                    raw_fragment={"type": "Feature", "hint": source_hint},
                )
                if rec:
                    records.append(rec)
        return records, raws

    candidates: list[dict[str, Any]] = []
    if isinstance(data, list):
        for el in data:
            if isinstance(el, dict):
                candidates.append(el)
    elif isinstance(data, dict):
        for key in ("dealers", "items", "locations", "results", "data", "outlets"):
            v = data.get(key)
            if isinstance(v, list):
                for el in v:
                    if isinstance(el, dict):
                        candidates.append(el)
                break

    for obj in candidates:
        rec = record_from_flat_dict(
            obj,
            source_locator_url=source_locator_url,
            scraped_at_iso=scraped_at_iso,
            raw_fragment={"hint": source_hint},
        )
        if rec:
            records.append(rec)

    return records, raws


def _merge_dedupe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Dedupe by name + zip + phone digits (intake-level)."""
    seen: set[tuple[str, str, str]] = set()
    out: list[dict[str, Any]] = []
    for r in records:
        name = (r.get("dealer_name") or "").strip().lower()
        zip5 = (r.get("zip") or "").strip()[:5]
        ph = _digits_phone(r.get("phone"))
        key = (name, zip5, ph)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _fetch_json_url(
    session: requests.Session,
    url: str,
    timeout: int | tuple[float, float],
) -> tuple[Any | None, str | None]:
    try:
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        logger.debug("fetch_json failed %s: %s", url, e)
        return None, str(e)


def _attempt_parse_with_trace(
    data: Any,
    *,
    source_locator_url: str,
    scraped_at_iso: str,
    source_hint: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    """Parse + compact trace for debug artifacts."""
    recs, raw = parse_json_to_records(
        data,
        source_locator_url=source_locator_url,
        scraped_at_iso=scraped_at_iso,
        source_hint=source_hint,
    )
    trace = {
        "source_hint": source_hint,
        "top_level_keys": top_level_keys(data),
        "parsed_row_count": len(recs),
        "outcome": "parsed_rows" if recs else "rejected",
        "rejection_reason": explain_parse_outcome(data, recs, source_hint=source_hint),
    }
    return recs, raw, trace


def ingest_bmw_usa_requests(
    *,
    project_root: Path,
    timeout: int = 60,
    verify_ssl: bool = True,
    max_js_bundles: int = 8,
    debug: bool = False,
    extra_test_urls: list[str] | None = None,
) -> BMWIntakeBundle:
    """Collect BMW USA dealers using requests + JSON discovery (no browser)."""
    from datetime import datetime, timezone

    scraped_at = datetime.now(timezone.utc).isoformat()
    bundle = BMWIntakeBundle(
        source_locator_url=BMW_USA_LOCATOR_URL,
        scraped_at_iso=scraped_at,
    )
    dbg: dict[str, Any] = {
        "mode": "requests",
        "locator_url": BMW_USA_LOCATOR_URL,
        "timeout_connect_sec": 20,
        "timeout_read_sec": max(90, timeout),
        "verify_ssl": verify_ssl,
        "extra_test_urls": list(extra_test_urls or []),
    }
    session = oem_requests_session(timeout=timeout, verify_ssl=verify_ssl)
    connect_s, read_s = 20, max(90, timeout)

    try:
        r = session.get(BMW_USA_LOCATOR_URL, timeout=(connect_s, read_s))
        r.raise_for_status()
        html = r.text
        dbg["page_fetch"] = {
            "ok": True,
            "status_code": r.status_code,
            "final_url": r.url,
            "content_length": len(html),
            "page_title": extract_html_title(html),
        }
    except Exception as e:
        bundle.notes.append(f"locator_page_fetch_failed:{e!s}")
        bundle.raw_payloads.append(
            {"kind": "locator_html_error", "error": str(e), "url": BMW_USA_LOCATOR_URL}
        )
        dbg["page_fetch"] = {"ok": False, "error": str(e), "error_type": type(e).__name__}
        if debug:
            bundle.debug_report = dbg
        return bundle

    bundle.raw_payloads.append(
        {
            "kind": "locator_html",
            "url": BMW_USA_LOCATOR_URL,
            "bytes": len(html),
        }
    )

    payload_attempts: list[dict[str, Any]] = []

    # Embedded JSON (Next.js / hydration)
    for label, blob in extract_embedded_json_blobs(html):
        recs, raw, tr = _attempt_parse_with_trace(
            blob,
            source_locator_url=BMW_USA_LOCATOR_URL,
            scraped_at_iso=scraped_at,
            source_hint=f"embedded:{label}",
        )
        bundle.records.extend(recs)
        bundle.raw_payloads.extend(raw)
        if debug:
            payload_attempts.append(tr)

    dbg["embedded_script_attempts"] = payload_attempts
    dbg["script_urls"] = collect_script_src(html, BMW_USA_LOCATOR_URL)

    # Scan JS bundles for JSON endpoints
    discovered: list[str] = []
    bundle_fetch_errors: list[dict[str, str]] = []
    for src in collect_script_src(html, BMW_USA_LOCATOR_URL)[:max_js_bundles]:
        try:
            jr = session.get(src, timeout=(connect_s, read_s))
            jr.raise_for_status()
            discovered.extend(mine_urls_from_js_bundle(jr.text))
        except Exception as e:
            logger.debug("bundle fetch %s: %s", src, e)
            if debug:
                bundle_fetch_errors.append({"url": src, "error": str(e)})

    dbg["js_bundle_fetch_errors"] = bundle_fetch_errors
    discovered.extend(load_extra_endpoint_urls(project_root))
    if extra_test_urls:
        discovered = list(dict.fromkeys([*extra_test_urls, *discovered]))
    else:
        discovered = list(dict.fromkeys(discovered))

    dbg["candidate_urls_after_dedupe"] = discovered[:200]
    dbg["candidate_url_count"] = len(discovered)

    url_attempts: list[dict[str, Any]] = []
    for u in discovered:
        data, fetch_err = _fetch_json_url(session, u, (connect_s, read_s))
        if data is None:
            if debug:
                url_attempts.append(
                    {
                        "url": u,
                        "outcome": "fetch_or_json_failed",
                        "error": fetch_err,
                        "top_level_keys": None,
                    }
                )
            continue
        recs, raw, tr = _attempt_parse_with_trace(
            data,
            source_locator_url=BMW_USA_LOCATOR_URL,
            scraped_at_iso=scraped_at,
            source_hint=f"url:{u}",
        )
        tr["url"] = u
        if debug:
            url_attempts.append(tr)
        bundle.records.extend(recs)
        bundle.raw_payloads.extend(raw)

    dbg["url_parse_attempts"] = url_attempts

    if not bundle.records:
        bundle.notes.append(
            "no_records_from_requests_try_playwright: "
            "Run `python -m oem_intake bmw ingest --playwright` or add URLs to data/oem/bmw_endpoint_overrides.json"
        )

    bundle.records = _merge_dedupe_records(bundle.records)
    dbg["rows_extracted"] = len(bundle.records)
    dbg["notes"] = list(bundle.notes)
    if debug:
        bundle.debug_report = dbg
    return bundle


def _json_body_dealer_heuristic(data: Any, url: str) -> bool:
    low = url.lower()
    blob_s = str(data).lower()
    return bool(
        (isinstance(data, dict) and data.get("type") == "FeatureCollection")
        or "dealer" in low
        or "locator" in low
        or "outlet" in blob_s
        or ("features" in blob_s and "geometry" in blob_s)
    )


def ingest_bmw_usa_playwright(
    *,
    timeout_ms: int = 180_000,
    verify_ssl: bool = True,
    debug: bool = False,
    zip_seeds: tuple[str, ...] = ("92606", "90807"),
    selector_overrides: dict[str, str] | None = None,
    ai_selector_assist: bool = False,
    llm_base_url: str | None = None,
    llm_model: str | None = None,
) -> BMWIntakeBundle:
    """Load locator in a real browser and capture dealer-related JSON responses."""
    from datetime import datetime, timezone

    from SCRAPING.constants import USER_AGENT

    scraped_at = datetime.now(timezone.utc).isoformat()
    bundle = BMWIntakeBundle(
        source_locator_url=BMW_USA_LOCATOR_URL,
        scraped_at_iso=scraped_at,
    )

    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        bundle.notes.append(f"playwright_missing:{e!s}")
        return bundle

    if debug:
        from scrapers.oem.bmw_locator_discovery import run_deep_locator_discovery
        ai_adjudicator = None
        if ai_selector_assist:
            try:
                from llm.providers.ollama_client import OpenAICompatibleClient

                client = OpenAICompatibleClient(base_url=llm_base_url)

                def ai_adjudicator(evidence: dict[str, Any]) -> dict[str, Any]:
                    system = (
                        "You are a UI selector adjudicator. Do not browse or invent DOM. "
                        "Use only provided evidence and return strict JSON with keys: "
                        "locator_input_candidates, location_mode_candidates, list_results_candidates, "
                        "dealer_result_container_candidates, notes_on_page_state, likely_wrong_elements_to_avoid. "
                        "Each candidate item must include selector_or_description, confidence, rationale."
                    )
                    user = json.dumps(
                        {
                            "task": "Identify selectors for BMW dealer locator workflow.",
                            "evidence": evidence,
                            "requirements": {
                                "prefer_css_selector_strings": True,
                                "avoid_generic_textbox": True,
                                "max_candidates_per_group": 6,
                            },
                        },
                        ensure_ascii=False,
                    )
                    return client.complete_json(
                        system=system,
                        user=user,
                        model=llm_model,
                        temperature=0.0,
                    )
            except Exception as e:
                bundle.notes.append(f"ai_selector_assist_unavailable:{e!s}")
                ai_adjudicator = None

        combined, deep_report = run_deep_locator_discovery(
            BMW_USA_LOCATOR_URL=BMW_USA_LOCATOR_URL,
            scraped_at_iso=scraped_at,
            timeout_ms=timeout_ms,
            verify_ssl=verify_ssl,
            user_agent=USER_AGENT,
            parse_json_to_records_fn=parse_json_to_records,
            zip_codes=zip_seeds,
            selector_overrides=selector_overrides,
            ai_selector_adjudicator=ai_adjudicator,
        )
        bundle.records = _merge_dedupe_records(combined)
        bundle.raw_payloads.append(
            {
                "kind": "playwright_deep_discovery",
                "row_count": len(bundle.records),
                "json_parser_hits": deep_report.get("json_parser_rows_found", 0),
            }
        )
        bundle.notes.append("playwright_deep_discovery_html_interaction_dom")
        if not bundle.records:
            bundle.notes.append("deep_discovery_no_parseable_rows_check_shortlist_and_html_scripts")
        deep_report["rows_extracted"] = len(bundle.records)
        deep_report["dealer_heuristic_captured_count"] = 0
        deep_report["playwright_network_json"] = deep_report.get("network_all_phases_sample", [])[:200]
        bundle.debug_report = deep_report
        return bundle

    captured: list[tuple[str, Any]] = []
    network_log: list[dict[str, Any]] = []
    nav_info: dict[str, Any] = {}

    def _maybe_capture(resp: Any) -> None:
        try:
            if resp.status >= 400:
                return
            url = resp.url
            ct = (resp.headers.get("content-type") or "").lower()
            looks_json = (
                "json" in ct
                or url.lower().endswith(".json")
                or "/api/" in url.lower()
                or "graphql" in url.lower()
            )
            try:
                body = resp.body()
            except Exception:
                return
            if len(body) < 40:
                return
            if len(body) > 2_500_000:
                return
            if not looks_json and not debug:
                return
            if not looks_json and debug and body[:1] not in (b"{", b"["):
                return
            try:
                text = body.decode("utf-8", "replace")
                data = json.loads(text)
            except Exception as je:
                if debug:
                    network_log.append(
                        {
                            "url": url,
                            "status": resp.status,
                            "content_type": ct,
                            "bytes": len(body),
                            "outcome": "not_json",
                            "decode_error": str(je)[:200],
                        }
                    )
                return

            recs, _, tr = _attempt_parse_with_trace(
                data,
                source_locator_url=BMW_USA_LOCATOR_URL,
                scraped_at_iso=scraped_at,
                source_hint=f"playwright:{url}",
            )
            entry = {
                "url": url,
                "status": resp.status,
                "content_type": ct,
                "bytes": len(body),
                "top_level_keys": top_level_keys(data),
                "parsed_row_count": len(recs),
                "parse_trace": tr,
            }
            if debug:
                network_log.append(entry)

            if _json_body_dealer_heuristic(data, url) or recs:
                captured.append((url, data))
        except Exception:
            return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=USER_AGENT,
            ignore_https_errors=not verify_ssl,
            locale="en-US",
        )
        page = ctx.new_page()
        page.on("response", _maybe_capture)
        try:
            resp = page.goto(
                BMW_USA_LOCATOR_URL,
                wait_until="load",
                timeout=timeout_ms,
            )
            nav_info = {
                "final_url": page.url,
                "status": resp.status if resp else None,
                "page_title": page.title(),
            }
        except Exception as e:
            bundle.notes.append(f"playwright_goto_failed:{e!s}")
            nav_info = {"error": str(e), "error_type": type(e).__name__}
        page.wait_for_timeout(10_000)
        try:
            page.wait_for_load_state("networkidle", timeout=60_000)
        except Exception:
            pass
        try:
            nav_info["page_title_after_wait"] = page.title()
            nav_info["final_url_after_wait"] = page.url
        except Exception:
            pass
        ctx.close()
        browser.close()

    bundle.raw_payloads.append(
        {"kind": "playwright_captured_responses", "count": len(captured), "network_json_seen": len(network_log)}
    )
    for url, data in captured:
        bundle.raw_payloads.append({"kind": "playwright_json", "url": url, "payload": data})
        recs, _ = parse_json_to_records(
            data,
            source_locator_url=BMW_USA_LOCATOR_URL,
            scraped_at_iso=scraped_at,
            source_hint=f"playwright:{url}",
        )
        bundle.records.extend(recs)

    bundle.records = _merge_dedupe_records(bundle.records)
    if not bundle.records:
        bundle.notes.append("playwright_captured_no_dealer_rows; site layout may have changed")

    if debug:
        bundle.debug_report = {
            "mode": "playwright",
            "locator_url": BMW_USA_LOCATOR_URL,
            "timeout_ms": timeout_ms,
            "verify_ssl": verify_ssl,
            "navigation": nav_info,
            "dealer_heuristic_captured_count": len(captured),
            "playwright_network_json": network_log[:500],
            "rows_extracted": len(bundle.records),
            "notes": list(bundle.notes),
        }
    return bundle


def ingest_bmw_usa(
    *,
    project_root: Path,
    prefer_playwright: bool = False,
    timeout: int = 60,
    verify_ssl: bool = True,
    debug: bool = False,
    extra_test_urls: list[str] | None = None,
    zip_seeds: tuple[str, ...] = ("92606", "90807"),
    selector_overrides: dict[str, str] | None = None,
    ai_selector_assist: bool = False,
    llm_base_url: str | None = None,
    llm_model: str | None = None,
) -> BMWIntakeBundle:
    """
    Full BMW USA ingest. Prefer Playwright when requests finds nothing (optional).
    """
    tw = max(180_000, timeout * 1000)
    if prefer_playwright:
        return ingest_bmw_usa_playwright(
            timeout_ms=tw,
            verify_ssl=verify_ssl,
            debug=debug,
            zip_seeds=zip_seeds,
            selector_overrides=selector_overrides,
            ai_selector_assist=ai_selector_assist,
            llm_base_url=llm_base_url,
            llm_model=llm_model,
        )

    b = ingest_bmw_usa_requests(
        project_root=project_root,
        timeout=timeout,
        verify_ssl=verify_ssl,
        debug=debug,
        extra_test_urls=extra_test_urls,
    )
    if not b.records:
        logger.info("Requests ingest returned 0 rows; retrying with Playwright capture")
        b2 = ingest_bmw_usa_playwright(
            timeout_ms=tw,
            verify_ssl=verify_ssl,
            debug=debug,
            zip_seeds=zip_seeds,
            selector_overrides=selector_overrides,
            ai_selector_assist=ai_selector_assist,
            llm_base_url=llm_base_url,
            llm_model=llm_model,
        )
        b2.notes.extend(b.notes)
        b2.raw_payloads = b.raw_payloads + b2.raw_payloads
        merged = _merge_dedupe_records(b.records + b2.records)
        b2.records = merged
        if debug and (b.debug_report or b2.debug_report):
            b2.debug_report = {
                "combined": True,
                "requests_phase": b.debug_report,
                "playwright_phase": b2.debug_report,
            }
        return b2
    if debug and b.debug_report:
        b.debug_report["extra_test_urls"] = list(extra_test_urls or [])
    return b


def load_fixture_records(project_root: Path) -> BMWIntakeBundle:
    """Offline sample dealers for tests / dry runs."""
    from datetime import datetime, timezone

    p = project_root / "data" / "oem" / "fixtures" / "bmw_locator_sample.json"
    if not p.is_file():
        raise FileNotFoundError(f"BMW fixture missing: {p}")
    scraped_at = datetime.now(timezone.utc).isoformat()
    data = json.loads(p.read_text(encoding="utf-8"))
    bundle = BMWIntakeBundle(
        source_locator_url=str(data.get("source_locator_url") or BMW_USA_LOCATOR_URL),
        scraped_at_iso=scraped_at,
    )
    items = data.get("records") or []
    for item in items:
        if not isinstance(item, dict):
            continue
        bundle.records.append(
            {
                **item,
                "source_locator_url": bundle.source_locator_url,
                "scraped_at": scraped_at,
                "raw_source_payload": {"fixture": True},
            }
        )
    bundle.raw_payloads.append({"kind": "fixture", "path": str(p)})
    bundle.notes.append("fixture_mode")
    return bundle
