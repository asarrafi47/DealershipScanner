#!/usr/bin/env python3
"""Verbose HTTP discovery probe for one dealership: everything the site answered,
written to workspace/dealer_logs/<dealer_id>/discovery.md (readable) and
discovery_<stamp>.json (raw), so a failed synthesis can be analysed without
re-running it. Process: docs/NETWORK_SCAN_PROCESS.md step 1.

What it records, in order:
  1. homepage fetch: redirect chain, status, bytes, title, server / cf-ray headers,
     challenge verdict and which markers matched
  2. platform fingerprint: every template's detect() result
  3. page signals: JSON-LD Vehicle count, script hosts, inline API hints, inline JSON keys
  4. inventory path probes: status / bytes / VIN count / JSON-LD vehicles per common path
  5. synthesis: candidates with url / method / pagination; per candidate the page-1
     replay: status, top-level JSON keys, rows parsed, VINs, error text
  6. exceptions with tracebacks

Usage:
  python -m backend.scripts.discovery_probe --dealers a,b
  python -m backend.scripts.discovery_probe --from-file workspace/pipeline/synth_failures.txt
"""
from __future__ import annotations

import argparse
import json
import os
import logging
import re
import sys
import time
import traceback
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

ROOT = Path(__file__).resolve().parents[2]
LOG_ROOT = Path(os.environ.get("DEALER_LOGS_ROOT") or (ROOT / "workspace" / "dealer_logs"))
_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
_VIN_RE = re.compile(r"\b[A-HJ-NPR-Z0-9]{17}\b")
_API_HINTS = ("algolia", "typesense", "carscommerce", "vhcliaa", "dealeron", "ws-inv-data", "inventory.json",
              "wp-json", "dealerinspire", "dealer.com", "dealereprocess", "sincro", "cdk", "fox", "dealerfire",
              "dealersocket", "roadster", "gubagoo", "autoipacket", "getauto", "dealerspike", "vinsolutions",
              "teamvelocity", "sm360", "d2cmedia", "nabthat", "overfuel", "motive", "jazel", "ebizautos",
              "autorevo", "carsforsale", "dealercarsearch", "autofunds", "dealercenter", "frazer")
_INV_PATHS = ("/inventory/", "/new-vehicles/", "/used-vehicles/", "/searchnew.aspx", "/searchused.aspx",
              "/inventory/new/", "/inventory/used/", "/new-inventory/", "/used-inventory/", "/cars-for-sale/",
              "/vehicles/", "/all-inventory/", "/inventory.json", "/search/used/")


def _get(url: str, referer: str | None = None, timeout: float = 25.0) -> dict[str, Any]:
    """One impersonated GET with the redirect chain and headers we care about."""
    from curl_cffi import requests as cr

    hdr = {"User-Agent": _UA, "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8"}
    if referer:
        hdr["Referer"] = referer
        hdr["Sec-Fetch-Site"] = "same-origin"
    t0 = time.time()
    try:
        r = cr.get(url, headers=hdr, impersonate="chrome", timeout=timeout, allow_redirects=True)
    except Exception as exc:  # noqa: BLE001
        return {"url": url, "error": f"{type(exc).__name__}: {str(exc)[:200]}", "secs": round(time.time() - t0, 2)}
    text = r.text or ""
    title = re.search(r"<title[^>]*>(.*?)</title>", text, re.S | re.I)
    chain = []
    try:
        chain = [f"{h.status_code} {h.url}" for h in (r.history or [])]
    except Exception:  # noqa: BLE001
        pass
    return {
        "url": url, "final_url": str(r.url), "status": r.status_code, "bytes": len(text), "secs": round(time.time() - t0, 2),
        "title": (title.group(1).strip()[:120] if title else None),
        "server": r.headers.get("server"), "cf_ray": bool(r.headers.get("cf-ray")), "content_type": (r.headers.get("content-type") or "")[:60],
        "redirects": chain, "text": text,
    }


def _challenge_report(html: str) -> dict[str, Any]:
    from backend.scanner.recipe_synth import _CHALLENGE_BEACON_MARKERS, _CHALLENGE_MARKERS, looks_like_challenge

    low = (html or "").lower()
    return {
        "verdict": looks_like_challenge(html or ""),
        "strong_markers": [m for m in _CHALLENGE_MARKERS if m in low],
        "beacon_markers": [m for m in _CHALLENGE_BEACON_MARKERS if m in low],
    }


def _fingerprint_report(html: str, url: str) -> dict[str, Any]:
    from backend.scanner.recipe_synth import PLATFORM_TEMPLATES, fingerprint_platform

    per: dict[str, Any] = {}
    for t in PLATFORM_TEMPLATES:
        try:
            per[t.name] = bool(t.detect(html, url))
        except Exception as exc:  # noqa: BLE001
            per[t.name] = f"error: {str(exc)[:80]}"
    return {"platform": fingerprint_platform(html, url), "detect": per}


def _signals(html: str) -> dict[str, Any]:
    low = html.lower()
    hosts = Counter(urlparse(s).hostname or "" for s in re.findall(r"<script[^>]+src=[\"']([^\"']+)", html, re.I))
    hints = {h: low.count(h) for h in _API_HINTS if h in low}
    ld_vehicle = len(re.findall(r'"@type"\s*:\s*"(?:Vehicle|Car)"', html))
    inline = Counter(re.findall(r"(window\.[A-Za-z_.]{3,40}\s*=\s*[\{\[]|var\s+[A-Za-z_]{3,30}\s*=\s*[\{\[]|__NEXT_DATA__|__NUXT__|data-vehicle=)", html))
    return {
        "script_hosts": [h for h, _ in hosts.most_common(15) if h],
        "api_hints": hints,
        "jsonld_vehicles": ld_vehicle,
        "vins_in_html": len(set(_VIN_RE.findall(html))),
        "inline_json": [k for k, _ in inline.most_common(12)],
        "generator": (re.search(r'<meta[^>]+name=["\']generator["\'][^>]+content=["\']([^"\']+)', html, re.I) or [None, None])[1] if re.search(r'name=["\']generator["\']', html, re.I) else None,
    }


def _probe_paths(origin: str) -> list[dict[str, Any]]:
    out = []
    for p in _INV_PATHS:
        r = _get(origin + p, referer=origin + "/", timeout=20)
        row = {k: r.get(k) for k in ("url", "final_url", "status", "bytes", "secs", "title", "error")}
        text = r.get("text") or ""
        row["vins"] = len(set(_VIN_RE.findall(text)))
        row["jsonld_vehicles"] = len(re.findall(r'"@type"\s*:\s*"(?:Vehicle|Car)"', text))
        row["challenge"] = _challenge_report(text)["verdict"] if text else None
        out.append(row)
        time.sleep(0.8)
    return out


_MANIFEST_NAMES: dict[str, str] | None = None


def _manifest_name(dealer_id: str) -> str:
    """The roster name the scanner itself passes to the gate (dealers.json)."""
    global _MANIFEST_NAMES
    if _MANIFEST_NAMES is None:
        _MANIFEST_NAMES = {}
        try:
            raw = json.loads((ROOT / "dealers.json").read_text())
            items = raw.get("dealers") if isinstance(raw, dict) else raw
            for d in items or []:
                if isinstance(d, dict) and d.get("id") and d.get("name"):
                    _MANIFEST_NAMES[str(d["id"])] = str(d["name"])
        except Exception:  # noqa: BLE001
            pass
    return _MANIFEST_NAMES.get(dealer_id, "")


def _synth_report(dealer_id: str, url: str, html: str, platform: str | None) -> dict[str, Any]:
    from backend.parsers import parse
    from backend.scanner import recipes as rmod
    from backend.scanner.recipe_synth import synthesize_recipes

    rep: dict[str, Any] = {"candidates": []}
    captured: list[str] = []

    class _Grab(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                captured.append(f"{record.levelname} {record.name}: {record.getMessage()}"[:800])
            except Exception:  # noqa: BLE001
                pass

    grab = _Grab(level=logging.INFO)
    roots = [logging.getLogger(n) for n in ("scanner", "backend.parsers", "backend.scanner")]
    saved_levels = [(lg, lg.level) for lg in roots]
    for lg in roots:
        lg.addHandler(grab)
        if lg.level > logging.INFO or lg.level == logging.NOTSET:
            lg.setLevel(logging.INFO)
    try:
        cands = synthesize_recipes(dealer_id, url, html, platform)
    except Exception:  # noqa: BLE001
        rep["error"] = traceback.format_exc()[-1500:]
        return rep
    finally:
        for lg, lvl in saved_levels:
            lg.removeHandler(grab)
            lg.setLevel(lvl)
        rep["synth_log"] = captured[-40:]
    if platform == "carscommerce":
        from backend.scanner.recipe_synth import _extract_carscommerce_key, _extract_ccid

        rep["carscommerce"] = {"ccid": _extract_ccid(html), "page_key": bool(_extract_carscommerce_key(html))}
    origin = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
    try:
        from backend.scanner.dealer_place import learn_place, place_kwargs

        place_full = learn_place(dealer_id, url, html)
        place = place_kwargs(place_full)
        rep["place"] = {**place, "source": place_full.get("place_source")} if place else {}
        from backend.scanner.dealer_place import name_from_html

        rep["site_name"] = name_from_html(html)
    except Exception as exc:  # noqa: BLE001
        place = {}
        rep["place_error"] = str(exc)[:120]
    for c in cands:
        item: dict[str, Any] = {"url": c.url, "method": c.method, "pagination": c.pagination, "provider_hint": c.provider_hint,
                                "auth_header_names": sorted((c.auth_headers or {}).keys())}
        try:
            _b = json.loads(c.post_template) if c.post_template else None
            if isinstance(_b, dict) and (_b.get("facetFilters") or _b.get("filters")):
                item["body_filters"] = {"facetFilters": _b.get("facetFilters"), "filters": _b.get("filters")}
        except ValueError:
            pass
        try:
            body = json.loads(c.post_template) if c.post_template else None
            if body is not None:
                body = rmod._mutate_for_page(c, body, 0)
            page_url = rmod._url_for_page(c, 0)
            status, parsed = rmod._replay_request(c, body, origin, page_url)
            item["page1"] = {"url": page_url, "status": status,
                             "type": type(parsed).__name__,
                             "keys": (sorted(parsed.keys())[:20] if isinstance(parsed, dict) else None),
                             "len": (len(parsed) if isinstance(parsed, (list, str)) else None)}
            if parsed is not None:
                rejected: list[dict] = []
                rows = list(parse(c.provider_hint or platform or "", parsed, base_url=origin, dealer_id=dealer_id,
                                  dealer_name=_manifest_name(dealer_id) or rep.get("site_name") or dealer_id,
                                  dealer_url=origin, rejected_out=rejected, **place))
                item["page1"]["rows_parsed"] = len(rows)
                item["page1"]["rows_rejected"] = len(rejected)
                item["page1"]["vins"] = len({str(r.get("vin") or "").upper() for r in rows if r.get("vin")})
                if rejected:
                    reasons: dict[str, int] = {}
                    for r in rejected[:400]:
                        key = str(r.get("_rooftop_reject") or "rejected")[:80]
                        reasons[key] = reasons.get(key, 0) + 1
                    item["page1"]["reject_reasons"] = dict(sorted(reasons.items(), key=lambda kv: -kv[1])[:8])
                    item["page1"]["rejected_stores"] = sorted({(_rt_name(r.get("_rooftop")) or str(r.get("dealer_name") or ""))[:60]
                                                              for r in rejected[:400]} - {""})[:12]
                if isinstance(parsed, dict):
                    item["page1"]["sample"] = json.dumps(parsed)[:600]
                elif isinstance(parsed, str):
                    item["page1"]["sample"] = parsed[:400]
        except Exception:  # noqa: BLE001
            item["error"] = traceback.format_exc()[-1500:]
        rep["candidates"].append(item)
    return rep


def _rt_name(rt: Any) -> str:
    """Label of the rooftop stamp a group parser attached (dict name / "City, ST" / str)."""
    if isinstance(rt, dict):
        for k in ("name", "dealer_name", "title", "rooftop"):
            if rt.get(k):
                return str(rt[k])
        city, st = rt.get("city") or rt.get("dealer_city"), rt.get("state") or rt.get("dealer_state")
        return f"{city}, {st}" if city and st else ""
    return str(rt or "")


def probe_dealer(dealer_id: str, url: str, *, paths: bool = True) -> dict[str, Any]:
    stamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    report: dict[str, Any] = {"dealer_id": dealer_id, "url": url, "stamp": stamp}
    try:
        home = _get(url)
        html = home.pop("text", "") or ""
        report["homepage"] = home
        report["challenge"] = _challenge_report(html)
        if html:
            report["fingerprint"] = _fingerprint_report(html, url)
            report["signals"] = _signals(html)
            report["synth"] = _synth_report(dealer_id, url, html, report["fingerprint"]["platform"])
        origin = f"{urlparse(home.get('final_url') or url).scheme}://{urlparse(home.get('final_url') or url).netloc}"
        if paths:
            report["paths"] = _probe_paths(origin)
    except Exception:  # noqa: BLE001
        report["error"] = traceback.format_exc()[-2000:]
    report["classification"] = _classify(report)
    _write_logs(dealer_id, report)
    return report


def _classify(rep: dict[str, Any]) -> str:
    home = rep.get("homepage") or {}
    if home.get("error"):
        return "homepage_error"
    if home.get("status") != 200:
        return f"homepage_http_{home.get('status')}"
    if (rep.get("challenge") or {}).get("verdict"):
        return "challenge_page"
    fp = (rep.get("fingerprint") or {}).get("platform")
    synth = rep.get("synth") or {}
    if not fp:
        return "unknown_platform"
    if not synth.get("candidates"):
        return f"no_candidates_for_{fp}"
    if any((c.get("page1") or {}).get("vins") for c in synth["candidates"]):
        return f"replay_ok_{fp}"
    statuses = {str((c.get("page1") or {}).get("status")) for c in synth["candidates"]}
    return f"replay_empty_{fp}_status_{'/'.join(sorted(statuses))}"


def browser_capture(dealer_id: str, url: str, rep: dict[str, Any] | None = None) -> dict[str, Any]:
    """Phase 1 of docs/HTTP_ONLY_SCANS_PLAN.md: the discovery browser. Captures the
    SRP's inventory endpoints, promotes them to recipes, then re-validates over
    HTTP and logs everything to discovery.md. Never writes car rows."""
    from datetime import datetime, timezone

    from backend.scanner.discovery_capture import capture_endpoints_sync

    stamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    provider = ((rep or {}).get("fingerprint") or {}).get("platform") or "dealer_dot_com"
    paths = None
    if provider in ("carscommerce", "dealer_inspire"):
        # The profiler labels CarsCommerce-fed sites dealer_dot_com and the capture
        # then walks /new-inventory/index.htm, /used-inventory/index.htm: Dealer
        # Inspire 404s those, so the used SRP (Algolia "lightning") was never
        # opened (claremontcdjr-com, group1fordofsouthaustin-com 2026-09-26).
        from backend.scanner.phases.site_profile import INVENTORY_PATHS_DEALER_INSPIRE

        paths = list(INVENTORY_PATHS_DEALER_INSPIRE)
    cap = capture_endpoints_sync(dealer_id, url, _manifest_name(dealer_id) or dealer_id, provider, paths=paths)
    verify: dict[str, Any] = {}
    validation = cap.get("validation") or {}
    if validation:
        # promote_from_ledger already judged the captured set against the site's
        # own count (recipe_validation.gate_recipes) and logged the full report
        # to discovery.md; keep the per-recipe lines for the capture JSON.
        for c in validation.get("recipes") or []:
            verify[str(c.get("url") or "")[:100]] = {
                "ok": bool(c.get("vins")), "info": {"vins": c.get("vins"), "site_total": c.get("site_total"),
                                                     "conditions": c.get("per_condition"), "error": c.get("error") or None},
            }
    else:
        # Nothing was captured: re-check whatever recipes the dealer already has.
        try:
            from backend.scanner.recipe_validation import validate_recipe_set
            from backend.scanner.recipes import load_recipes

            from urllib.parse import urlparse as _up

            _o = f"{_up(url).scheme}://{_up(url).netloc}"
            live = [x for x in load_recipes(dealer_id) if not x.stale][:6]
            if live:
                rep = validate_recipe_set(dealer_id, live, base_url=_o, dealer_name=_manifest_name(dealer_id) or dealer_id)
                validation = {**rep.summary(), "status": rep.status}
                for c in rep.recipes:
                    verify[c.url[:100]] = {"ok": bool(c.vins), "info": {"vins": c.vins, "site_total": c.site_total,
                                                                          "conditions": c.per_condition, "error": c.error or None}}
        except Exception as exc:  # noqa: BLE001
            verify["_error"] = str(exc)[:160]
    cap["verify"] = verify
    cap["validation"] = validation
    d = LOG_ROOT / dealer_id
    d.mkdir(parents=True, exist_ok=True)
    (d / f"capture_{stamp.replace(':', '').replace('-', '')}.json").write_text(json.dumps(cap, indent=1, default=str), encoding="utf-8")
    L = [f"## {stamp} browser capture (discovery) — {cap.get('mode')}",
         f"- profile: {json.dumps(cap.get('profile'))}; paths: {cap.get('paths')}",
         f"- intercept records: {cap.get('records')}; endpoints captured: {len(cap.get('endpoints') or [])}; recipes {cap.get('recipes_before')} -> {cap.get('recipes_after')} ({cap.get('seconds')}s)"]
    for ep in cap.get("endpoints") or []:
        L.append(f"  - {ep['method']} {ep['url']} rows={ep['vehicle_rows']} total={ep['total_count']} type={ep['content_type']} hint={ep['provider_hint']}")
    if validation:
        L.append(f"- recipe validation: {str(validation.get('verdict') or '').upper()} {'; '.join(validation.get('reasons') or [])[:200]}"
                 f" (VINs {validation.get('vins_total')}, site total {validation.get('site_total')}, coverage {validation.get('coverage')},"
                 f" per condition {json.dumps(validation.get('per_condition'))})")
    for u_, v in verify.items():
        L.append(f"  - validate {u_}: {'ok' if v.get('ok') else 'FAIL'} {json.dumps(v.get('info'))[:220]}")
    for e in cap.get("errors") or []:
        L.append(f"  - error: {e}")
    with (d / "discovery.md").open("a", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n\n")
    return cap


def _write_logs(dealer_id: str, rep: dict[str, Any]) -> None:
    d = LOG_ROOT / dealer_id
    d.mkdir(parents=True, exist_ok=True)
    stamp = rep["stamp"]
    (d / f"discovery_{stamp.replace(':', '').replace('-', '')}.json").write_text(json.dumps(rep, indent=1, default=str), encoding="utf-8")
    home = rep.get("homepage") or {}
    L = [f"## {stamp} discovery probe — {rep['classification']}", f"- url: {rep['url']}",
         f"- homepage: status {home.get('status')} {home.get('bytes')} bytes in {home.get('secs')}s, title {home.get('title')!r}, server {home.get('server')}, cf-ray {home.get('cf_ray')}, final {home.get('final_url')}"]
    if home.get("redirects"):
        L.append(f"- redirects: {' -> '.join(home['redirects'])}")
    if home.get("error"):
        L.append(f"- homepage error: {home['error']}")
    ch = rep.get("challenge") or {}
    L.append(f"- challenge: {ch.get('verdict')} strong={ch.get('strong_markers')} beacon={ch.get('beacon_markers')}")
    fp = rep.get("fingerprint") or {}
    if fp:
        L.append(f"- fingerprint: {fp.get('platform')}; detect: " + ", ".join(f"{k}={v}" for k, v in (fp.get("detect") or {}).items()))
    sg = rep.get("signals") or {}
    if sg:
        L.append(f"- signals: jsonld_vehicles={sg.get('jsonld_vehicles')} vins_in_html={sg.get('vins_in_html')} generator={sg.get('generator')}")
        L.append(f"  - script hosts: {', '.join(sg.get('script_hosts') or [])}")
        L.append(f"  - api hints: {json.dumps(sg.get('api_hints'))}")
        L.append(f"  - inline json: {', '.join(sg.get('inline_json') or [])}")
    sy = rep.get("synth") or {}
    if sy:
        if rep.get("place"):
            L.append(f"- store place for attribution: {json.dumps(rep['place'])}")
        if sy.get("carscommerce"):
            L.append(f"- carscommerce: {json.dumps(sy['carscommerce'])}")
        if sy.get("site_name"):
            L.append(f"- site names itself: {sy['site_name']!r}")
        if sy.get("error"):
            L.append(f"- synth error:\n```\n{sy['error']}\n```")
        for line in sy.get("synth_log") or []:
            L.append(f"- synth log: {line}")
        for c in sy.get("candidates") or []:
            p1 = c.get("page1") or {}
            L.append(f"- candidate {c.get('method')} {c.get('url')} pagination={c.get('pagination')} auth={c.get('auth_header_names')}" + (f" filters={json.dumps(c.get('body_filters'))}" if c.get("body_filters") else ""))
            L.append(f"  - page 1: status {p1.get('status')} type {p1.get('type')} keys {p1.get('keys')} rows_parsed {p1.get('rows_parsed')} vins {p1.get('vins')}")
            if p1.get("rows_rejected"):
                L.append(f"  - page 1 gate: rejected {p1.get('rows_rejected')} reasons {json.dumps(p1.get('reject_reasons'))} stores {p1.get('rejected_stores')}")
            if p1.get("sample"):
                L.append(f"  - sample: `{p1['sample'][:300]}`")
            if c.get("error"):
                L.append(f"  - error:\n```\n{c['error']}\n```")
    for p in rep.get("paths") or []:
        L.append(f"- path {p.get('url')}: {p.get('status')} {p.get('bytes')}B vins={p.get('vins')} jsonld={p.get('jsonld_vehicles')} challenge={p.get('challenge')} title={p.get('title')!r}" + (f" error={p['error']}" if p.get("error") else ""))
    if rep.get("error"):
        L.append(f"- probe error:\n```\n{rep['error']}\n```")
    L.append(f"- raw: discovery_{stamp.replace(':', '').replace('-', '')}.json")
    p = d / "discovery.md"
    head = "" if p.exists() else f"# {dealer_id} — discovery\n\nProcess: docs/NETWORK_SCAN_PROCESS.md\n\n"
    with p.open("a", encoding="utf-8") as fh:
        fh.write(head + "\n".join(L) + "\n\n")
    idx = LOG_ROOT / "_learning" / "errors_index.md"
    idx.parent.mkdir(parents=True, exist_ok=True)
    with idx.open("a", encoding="utf-8") as fh:
        fh.write(f"- {stamp} {rep['classification']} -> {dealer_id} (workspace/dealer_logs/{dealer_id}/discovery.md)\n")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dealers", default="")
    ap.add_argument("--from-file", default="", help="file whose lines start with a dealer_id")
    ap.add_argument("--manifest", default=str(ROOT / "dealers.json"))
    ap.add_argument("--no-paths", action="store_true")
    ap.add_argument("--browser-capture", action="store_true",
                    help="Discovery only: open the SRP in a headless browser, capture the inventory endpoints and promote them to recipes (the one sanctioned browser use — docs/HTTP_ONLY_SCANS_PLAN.md)")
    ap.add_argument("--capture-always", action="store_true", help="with --browser-capture: capture even when the HTTP probe already replays")
    args = ap.parse_args()
    ids = [d.strip() for d in args.dealers.split(",") if d.strip()]
    if args.from_file:
        ids += [ln.split()[0] for ln in Path(args.from_file).read_text().splitlines() if ln.strip()]
    raw = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    items = raw.get("dealers") if isinstance(raw, dict) else raw
    urls = {d["dealer_id"]: d["url"] for d in items if isinstance(d, dict) and d.get("dealer_id") and d.get("url")}
    missing = [did for did in ids if did not in urls]
    if missing:
        try:
            from backend.scripts.dealer_pipeline import dealers_from_db

            urls.update({k: v["url"] for k, v in dealers_from_db(missing).items() if v.get("url")})
        except Exception as exc:  # noqa: BLE001
            print(f"db url lookup failed: {str(exc)[:100]}", flush=True)
    for did in ids:
        u = urls.get(did)
        if not u:
            print(f"{did:36s} not in manifest or db", flush=True)
            continue
        rep = probe_dealer(did, u, paths=not args.no_paths)
        print(f"{did:36s} {rep['classification']:40s} {(rep.get('homepage') or {}).get('status')} {(rep.get('fingerprint') or {}).get('platform')}", flush=True)
        if args.browser_capture and (args.capture_always or not str(rep["classification"]).startswith("replay_ok")):
            cap = browser_capture(did, u, rep)
            print(f"{did:36s} browser-capture: {cap.get('records', 0)} record(s), {len(cap.get('endpoints') or [])} endpoint(s), "
                  f"recipes {cap.get('recipes_before')} -> {cap.get('recipes_after')} ({cap.get('seconds')}s){' errors=' + '; '.join(cap['errors']) if cap.get('errors') else ''}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
