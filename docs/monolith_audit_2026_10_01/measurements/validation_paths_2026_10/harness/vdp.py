"""P0B.1 VDP part: chrome-only vs profile rotation, explicit Chrome UA vs the profile's own UA.

10 dealers x 5 VDPs (vdp_urls.psv, picked read-only from local cars). Per VDP, in a
per-VDP random order:
  today      backend.scanner.vdp.prefetch._fetch_html (curl_cffi chrome + Chrome/126 UA, requests fallback)
  c124_ua    curl_cffi chrome124 + the prefetch headers (explicit Chrome/126 UA)
  saf_ua     curl_cffi safari17_0 + the prefetch headers (explicit Chrome UA: the mismatch replay/VDP send today)
  saf_own    curl_cffi safari17_0, prefetch headers minus User-Agent (the profile's own UA)
  chrome_own curl_cffi chrome, prefetch headers minus User-Agent
Rotation (chrome -> chrome124 -> safari17_0, explicit UA) clears when today's chrome attempt,
c124_ua or saf_ua cleared. Same guards and per-host pacing as measure.py (imported).
Also: feed-replay UA probe (page 1 of one recipe per dealer whose replay needed impersonation),
profile x {explicit Chrome/124 UA, own UA}.
"""
from __future__ import annotations

import json
import os
import random
import sys
import time

sys.argv = [sys.argv[0], "--no-dealers"] + sys.argv[1:]
import measure as M  # noqa: E402  (applies guards, read-only DB assert, paced transport)

from backend.scanner.vdp import prefetch as PF  # noqa: E402

RUN = M.RUN
OUT = os.path.join(RUN, "vdp_results.json")


def headers_for(url: str, explicit_ua: bool) -> dict:
    from urllib.parse import urlparse
    p = urlparse(url)
    h = {"User-Agent": PF._UA, "Accept": "text/html,application/xhtml+xml", "Referer": f"{p.scheme}://{p.netloc}/",
         "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Dest": "document"}
    if not explicit_ua:
        h.pop("User-Agent")
    return h


def probe(url: str, profile: str, explicit_ua: bool) -> dict:
    cffi = M.NC.import_curl_cffi()
    n0 = len(M.WIRE)
    try:
        resp = M.NC.send(cffi, "GET", url, via_get=True, headers=headers_for(url, explicit_ua), impersonate=profile,
                         timeout=15.0, proxies=M.NC.scanner_proxies())
        ctype = str(resp.headers.get("content-type") or "")
        text = resp.text or ""
        ok = resp.status_code == 200 and "html" in ctype and not M.NC.looks_like_challenge(text)
        return {"status": resp.status_code, "cleared": ok, "challenge": M.NC.looks_like_challenge(text) if "html" in ctype else False,
                "bytes": len(text), "vin_in_page": None}
    except Exception as e:  # noqa: BLE001
        return {"status": None, "cleared": False, "error": (type(e).__name__ + ": " + str(e))[:120]}
    finally:
        pass


def today(url: str) -> dict:
    n0 = len(M.WIRE)
    html = PF._fetch_html(url)
    wire = M.WIRE[n0:]
    st = PF._LAST_STATUS.pop(url, None)
    err = PF._LAST_ERROR.pop(url, None) if hasattr(PF, "_LAST_ERROR") else None
    chrome = wire[0] if wire else {}
    chrome_ok = bool(chrome) and chrome.get("lib") == "curl_cffi" and chrome.get("status") == 200 and not chrome.get("challenge") \
        and "html" in str(chrome.get("ctype") or "")
    return {"status": st, "cleared": html is not None, "chrome_attempt_cleared": chrome_ok,
            "attempts": [{"lib": w.get("lib"), "profile": w.get("profile"), "status": w.get("status"), "challenge": w.get("challenge")} for w in wire],
            "error": str(err)[:120] if err else None}


def run_vdp():
    rows = []
    for line in open(os.path.join(RUN, "vdp_urls.psv")):
        p = line.rstrip("\n").split("|")
        if len(p) >= 3:
            rows.append({"dealer": p[0], "vin": p[1], "url": "|".join(p[2:])})
    done = {}
    if os.path.exists(OUT):
        done = json.load(open(OUT))
    res = done.get("vdp", [])
    seen = {r["url"] for r in res}
    for r in rows:
        if r["url"] in seen:
            continue
        rng = random.Random("vdp-" + r["url"])
        steps = ["today", "c124_ua", "saf_ua", "saf_own", "chrome_own"]
        rng.shuffle(steps)
        M.CUR["phase"] = "vdp"
        M.CUR["recipe"] = r["url"]
        out = {**r, "order": steps}
        for s in steps:
            if s == "today":
                out[s] = today(r["url"])
            elif s == "c124_ua":
                out[s] = probe(r["url"], "chrome124", True)
            elif s == "saf_ua":
                out[s] = probe(r["url"], "safari17_0", True)
            elif s == "saf_own":
                out[s] = probe(r["url"], "safari17_0", False)
            elif s == "chrome_own":
                out[s] = probe(r["url"], "chrome", False)
        out["t"] = M.now_iso()
        res.append(out)
        done["vdp"] = res
        json.dump(done, open(OUT + ".tmp", "w"), indent=0)
        os.replace(OUT + ".tmp", OUT)
        print(M.now_iso(), "vdp", r["dealer"], {k: (out[k].get("status"), out[k].get("cleared")) for k in steps}, flush=True)
    return done


def run_feed_ua(dealers: list[str]):
    """Page 1 of the first live recipe of each dealer under every profile, explicit UA vs own UA,
    with the replay's own header set (recipes._replay_request)."""
    done = json.load(open(OUT)) if os.path.exists(OUT) else {}
    res = done.get("feed_ua", [])
    seen = {r["dealer"] for r in res}
    for did in dealers:
        if did in seen:
            continue
        path = os.path.join(M.RESULTS, did + ".json")
        if not os.path.exists(path):
            continue
        meas = json.load(open(path))
        # the recipe whose replay needed impersonation
        imp = [w for w in meas.get("wire") or [] if w.get("lib") == "curl_cffi" and w.get("phase") in ("set2", "set40")]
        if not imp:
            continue
        rows = json.load(open(os.path.join(M.MAIN, "workspace/recipes", did + ".json")))
        recs = [x for x in M.R._rows_to_recipes(rows) if not x.stale]
        target = next((x for x in recs if x.url[:100] in imp[0]["url"] or imp[0]["url"].startswith(x.url[:60])), recs[0] if recs else None)
        if target is None:
            continue
        base = meas.get("base_url") or ""
        tmpl = json.loads(target.post_template) if target.post_template else None
        body = M.R._mutate_for_page(target, tmpl, 0) if tmpl is not None else None
        url = M.R._url_for_page(target, 0)
        payload = json.dumps(body) if body is not None else None
        cffi = M.NC.import_curl_cffi()
        out = {"dealer": did, "url": url, "method": target.method, "pagination": target.pagination, "probes": []}
        combos = [(p, ua) for p in M.NC.IMPERSONATE_PROFILES for ua in (True, False)]
        random.Random("feedua-" + did).shuffle(combos)
        M.CUR["phase"] = "feed_ua"
        M.CUR["recipe"] = url
        for prof, ua in combos:
            h = {"User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/124.0.0.0 Safari/537.36"),
                 "Accept": "application/json, text/plain, */*", "Origin": base.rstrip("/"), "Referer": base.rstrip("/") + "/",
                 **target.auth_headers}
            if target.method != "GET":
                h["Content-Type"] = "application/json"
            if not ua:
                h.pop("User-Agent")
            kw = {"headers": h, "timeout": 20.0, "impersonate": prof}
            if target.method != "GET":
                kw["data"] = payload
            try:
                resp = M.NC.send(cffi, target.method, url, via_get=target.method == "GET", **kw)
                text = resp.text or ""
                out["probes"].append({"profile": prof, "explicit_ua": ua, "status": resp.status_code, "bytes": len(text),
                                      "challenge": M.NC.looks_like_challenge(text) if "<html" in text[:2000].lower() else False})
            except Exception as e:  # noqa: BLE001
                out["probes"].append({"profile": prof, "explicit_ua": ua, "status": None, "error": str(e)[:120]})
        if target.method == "GET":
            # D-OD3 header probe: plain requests with replay's headers (control) vs synth's
            # navigation header set + same-site Referer (what _dep_fetch_page sends).
            req = M.NC.import_requests()
            from urllib.parse import urlparse as _up
            pp = _up(url)
            nav = {**M.SH._browser_headers(), "Referer": f"{pp.scheme}://{pp.netloc}/", "Sec-Fetch-Site": "same-origin"}
            rep_h = {"User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                                    "Chrome/124.0.0.0 Safari/537.36"),
                     "Accept": "application/json, text/plain, */*", "Origin": base.rstrip("/"), "Referer": base.rstrip("/") + "/",
                     **target.auth_headers}
            hp = [("requests_replay_headers", rep_h), ("requests_nav_headers", nav)]
            random.Random("hdr-" + did).shuffle(hp)
            for label, hh in hp:
                try:
                    resp = M.NC.send(req, "GET", url, via_get=True, headers=hh, timeout=20.0)
                    text = resp.text or ""
                    out["probes"].append({"profile": label, "explicit_ua": True, "status": resp.status_code, "bytes": len(text),
                                          "challenge": M.NC.looks_like_challenge(text) if "<html" in text[:2000].lower() else False})
                except Exception as e:  # noqa: BLE001
                    out["probes"].append({"profile": label, "explicit_ua": True, "status": None, "error": str(e)[:120]})
        res.append(out)
        done["feed_ua"] = res
        json.dump(done, open(OUT + ".tmp", "w"), indent=0)
        os.replace(OUT + ".tmp", OUT)
        print(M.now_iso(), "feed_ua", did, [(p["profile"], p["explicit_ua"], p.get("status")) for p in out["probes"]], flush=True)


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    mode = args[0] if args else "vdp"
    if mode == "vdp":
        run_vdp()
    elif mode == "feed_ua":
        run_feed_ua(args[1:])
    print(M.now_iso(), "VDP DONE", flush=True)
