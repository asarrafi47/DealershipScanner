"""P0B.1 online part: validation paths on a live sample, with zero writes.

- Every writer the validation path could reach is replaced by a function that
  raises WriteAttempt (self-tested at start).
- The DB session is read-only (PGOPTIONS default_transaction_read_only=on,
  asserted at start).
- Every wire request (requests, curl_cffi, urllib) is paced per host
  (P0B1_HOST_GAP seconds, default 1.5) and logged.
- Recipes are read with plain json.load from the main checkout.
- Resumable: one results/<dealer>.json per finished dealer.
"""
from __future__ import annotations

import copy
import json
import os
import random
import sys
import time
import traceback
import urllib.error
from datetime import datetime, timezone
from urllib.parse import urlparse

RUN = "/private/tmp/claude-501/phase0/p0b1_run"
WT = "/private/tmp/claude-501/phase0/p0b1"
MAIN = "/Users/asarrafi/Projects/DealershipScanner"
os.chdir(RUN)
sys.path.insert(0, WT)
os.environ["DEALER_LOGS_ROOT"] = RUN + "/fake_logs"
os.environ["SCANNER_SYNTH_FETCH_DELAY"] = "0"  # pacing happens per host at the transport below
assert "default_transaction_read_only=on" in (os.environ.get("PGOPTIONS") or ""), "PGOPTIONS must force read-only"
assert os.environ.get("INVENTORY_DATABASE_URL"), "INVENTORY_DATABASE_URL must be exported"

HOST_GAP = float(os.environ.get("P0B1_HOST_GAP") or 1.5)
RESULTS = os.path.join(RUN, "results")
os.makedirs(RESULTS, exist_ok=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ── write guards ───────────────────────────────────────────────────────────────
class WriteAttempt(RuntimeError):
    pass


def _deny(name):
    def f(*a, **k):
        raise WriteAttempt(f"P0B.1 harness: write attempt via {name}")
    f.__name__ = "deny_" + name.rsplit(".", 1)[-1]
    return f


import backend.scanner.recipes as R  # noqa: E402
import backend.scanner.recipe_store as RS  # noqa: E402
import backend.scanner.recipe_validation as RV  # noqa: E402
import backend.attribution.disown as AD  # noqa: E402
import backend.scanner.rooftop_disown as RDO  # noqa: E402
import backend.scanner.dealer_place as DP  # noqa: E402
import backend.scanner.synth.http as SH  # noqa: E402
from backend.scanner.net import client as NC  # noqa: E402
from backend.attribution.place import gate_place, store_place  # noqa: E402

GUARDED = [
    (R, ["save_recipes", "mark_stale", "record_stale_status", "clear_stale_status", "load_recipes",
         "promote_from_ledger", "try_fetch_via_recipes"]),
    (RS, ["db_save_recipes", "set_scan_hints"]),
    (RV, ["gate_recipes", "write_discovery_log", "record_recipe_status"]),
    (AD, ["disown_foreign_rooftop_vins"]),
    (RDO, ["disown_foreign_rooftop_vins"]),
    (DP, ["learn_place"]),
]
for mod, names in GUARDED:
    for n in names:
        setattr(mod, n, _deny(f"{mod.__name__}.{n}"))
GUARD_SELFTEST = {}
for mod, names in GUARDED:
    for n in names:
        try:
            getattr(mod, n)("selftest", [], {})
            GUARD_SELFTEST[f"{mod.__name__}.{n}"] = "DID NOT RAISE"
        except WriteAttempt:
            GUARD_SELFTEST[f"{mod.__name__}.{n}"] = "raises WriteAttempt"
assert all(v == "raises WriteAttempt" for v in GUARD_SELFTEST.values()), GUARD_SELFTEST

# DB session must be read-only
from backend.db.inventory_pg import pg_connect  # noqa: E402
_c = pg_connect()
try:
    _cur = _c.cursor()
    _cur.execute("SHOW default_transaction_read_only")
    DB_RO = _cur.fetchone()[0]
finally:
    _c.close()
assert DB_RO == "on", DB_RO

# ── transport: per-host pacing + wire log ──────────────────────────────────────
WIRE: list[dict] = []
CUR = {"dealer": "", "phase": "", "recipe": ""}
_last_host: dict[str, float] = {}


def _pace_host(url: str) -> float:
    host = (urlparse(url).hostname or "").lower()
    wait = _last_host.get(host, 0.0) + HOST_GAP - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    _last_host[host] = time.monotonic()
    return max(0.0, wait)


_orig_send = NC.send


def paced_send(lib, method, url, *, via_get, **kw):
    _pace_host(url)
    hdrs = kw.get("headers") or {}
    rec = {"t": now_iso(), "phase": CUR["phase"], "recipe": CUR["recipe"][:120],
           "lib": "curl_cffi" if "curl_cffi" in getattr(lib, "__name__", "") else "requests",
           "method": method, "url": url[:220], "profile": kw.get("impersonate"),
           "ua_explicit": any(k.lower() == "user-agent" for k in hdrs)}
    t0 = time.monotonic()
    try:
        resp = _orig_send(lib, method, url, via_get=via_get, **kw)
    except Exception as e:  # noqa: BLE001
        rec.update(status=None, error=(type(e).__name__ + ": " + str(e))[:160], secs=round(time.monotonic() - t0, 2))
        WIRE.append(rec)
        raise
    ctype = ""
    try:
        ctype = str(resp.headers.get("content-type") or "")
    except Exception:  # noqa: BLE001
        pass
    challenge = False
    nbytes = None
    try:
        nbytes = len(resp.content or b"")
        if "html" in ctype.lower() or resp.status_code in (403, 429, 503):
            challenge = NC.looks_like_challenge(resp.text or "")
    except Exception:  # noqa: BLE001
        pass
    rec.update(status=int(resp.status_code), bytes=nbytes, ctype=ctype[:40], challenge=challenge,
               secs=round(time.monotonic() - t0, 2))
    WIRE.append(rec)
    return resp


NC.send = paced_send

_orig_open = SH.open_url


class _Resp:
    def __init__(self, resp, raw):
        self._r, self._raw = resp, raw
        self.headers = resp.headers
        self.status = getattr(resp, "status", 200)

    def read(self, *a):
        return self._raw

    def geturl(self):
        return self._r.geturl()


def paced_open(req, timeout=25.0, **kw):
    url = req.full_url if hasattr(req, "full_url") else str(req)
    _pace_host(url)
    rec = {"t": now_iso(), "phase": CUR["phase"], "recipe": CUR["recipe"][:120], "lib": "urllib",
           "method": req.get_method() if hasattr(req, "get_method") else "GET", "url": url[:220], "profile": None,
           "ua_explicit": True}
    t0 = time.monotonic()
    try:
        resp = _orig_open(req, timeout=timeout, **kw)
        raw = resp.read()
    except urllib.error.HTTPError as e:
        rec.update(status=int(e.code), secs=round(time.monotonic() - t0, 2))
        WIRE.append(rec)
        raise
    except Exception as e:  # noqa: BLE001
        rec.update(status=None, error=(type(e).__name__ + ": " + str(e))[:160], secs=round(time.monotonic() - t0, 2))
        WIRE.append(rec)
        raise
    ctype = str(resp.headers.get("content-type") or "")
    challenge = False
    if "html" in ctype.lower():
        try:
            challenge = NC.looks_like_challenge(raw.decode("utf-8", "replace"))
        except Exception:  # noqa: BLE001
            pass
    rec.update(status=int(getattr(resp, "status", 200) or 200), bytes=len(raw), ctype=ctype[:40], challenge=challenge,
               secs=round(time.monotonic() - t0, 2))
    WIRE.append(rec)
    return _Resp(resp, raw)


SH.open_url = paced_open

# ── replay fetch with a per-dealer 200-response cache (polite: one fetch per distinct request) ──
_orig_replay = R._replay_request
CACHE: dict = {}
LOGICAL: list[dict] = []


def cached_fetch(recipe, body, base_url, url=None):
    key = (recipe.method, url or recipe.url, json.dumps(body, sort_keys=True, default=str))
    if key in CACHE:
        LOGICAL.append({"phase": CUR["phase"], "url": (url or recipe.url)[:200], "status": 200, "cached": True})
        return 200, copy.deepcopy(CACHE[key])
    st, parsed = _orig_replay(recipe, body, base_url, url)
    LOGICAL.append({"phase": CUR["phase"], "url": (url or recipe.url)[:200], "status": st, "cached": False,
                    "parsed": parsed is not None})
    if st == 200 and parsed is not None:
        CACHE[key] = copy.deepcopy(parsed)
    return st, parsed


RV._replay_request = cached_fetch  # validate_recipe's generic JSON path (same function the scan replays with)

# per-recipe VIN sets from every _check_recipe call
_orig_check = RV._check_recipe
CHECKS: list[dict] = []


def check_wrap(recipe, **kw):
    out = _orig_check(recipe, **kw)
    CHECKS.append({"phase": CUR["phase"], "url": recipe.url, "key": list(recipe.key()), "pages": kw.get("pages"),
                   "vins": sorted(out[1]), "by_cond": {c: len(s) for c, s in out[2].items()}})
    return out


RV._check_recipe = check_wrap

# ── dealers ───────────────────────────────────────────────────────────────────
def load_dealers():
    raw = json.load(open(os.path.join(MAIN, "dealers.json")))
    items = raw.get("dealers") if isinstance(raw, dict) else raw
    man = {d["dealer_id"]: d for d in items if isinstance(d, dict) and d.get("dealer_id")}
    db = {}
    for line in open(os.path.join(RUN, "db_dealers.psv")):
        p = line.rstrip("\n").split("|")
        if len(p) >= 3 and p[0]:
            db[p[0]] = {"dealer_id": p[0], "url": p[1], "name": "|".join(p[2:])}
    return man, db


def one_condition_ok():
    try:
        text = open(os.path.join(MAIN, "workspace/pipeline/one_condition_ok.txt")).read()
    except OSError:
        return set()
    return {ln.split("#", 1)[0].strip() for ln in text.splitlines() if ln.split("#", 1)[0].strip()}


def wire_summary(rows):
    by = {}
    for w in rows:
        k = str(w.get("status"))
        by[k] = by.get(k, 0) + 1
    return {"requests": len(rows), "statuses": by,
            "challenges": sum(1 for w in rows if w.get("challenge")),
            "impersonated": sum(1 for w in rows if w.get("lib") == "curl_cffi"),
            "errors": sum(1 for w in rows if w.get("error"))}


def measure(did: str, man, db, ok_ids, rng) -> dict:
    WIRE.clear(); CACHE.clear(); LOGICAL.clear(); CHECKS.clear()
    d = man.get(did) or db.get(did) or {}
    url = str(d.get("url") or "").rstrip("/")
    name = str(d.get("name") or did)
    rows = json.load(open(os.path.join(MAIN, "workspace/recipes", did + ".json")))
    recs = R._rows_to_recipes(rows)  # pure conversion (infers pagination for 'none'), no IO
    live = [r for r in recs if not r.stale]
    if not url and live:
        p = urlparse(live[0].url)
        url = f"{p.scheme}://{p.netloc}"
    place = gate_place(store_place(url, did))  # registry + hinted street, read-only
    out = {"dealer": did, "name": name, "base_url": url, "place_keys": sorted(place), "started": now_iso(),
           "live_recipes": len(live), "stale_recipes": len(recs) - len(live), "steps": {}, "errors": []}
    order = ["set2", "set40", "count"]
    rng.shuffle(order)
    out["order"] = order
    for step in order:
        CUR["phase"] = step
        t0 = time.monotonic()
        n0 = len(WIRE)
        try:
            if step in ("set2", "set40"):
                pages = 2 if step == "set2" else RV._VALIDATE_MAX_PAGES
                CUR["recipe"] = "(set)"
                rep = RV.validate_recipe_set(did, live, cached_fetch, base_url=url, dealer_name=name, place=place or None,
                                             pages=pages, one_condition_ok=ok_ids)
                out["steps"][step] = {"report": rep.to_dict(), "status": rep.status}
            else:
                counts = []
                idx = list(range(len(live)))
                rng.shuffle(idx)
                for i in idx:
                    r = live[i]
                    CUR["recipe"] = r.url
                    t1 = time.monotonic()
                    m0 = len(WIRE)
                    try:
                        n = RV.validate_recipe(r, url, did, name, place=place or None)
                        err = None
                    except WriteAttempt:
                        raise
                    except Exception as e:  # noqa: BLE001
                        n, err = None, (type(e).__name__ + ": " + str(e))[:200]
                    counts.append({"i": i, "url": r.url, "n": n, "error": err, "secs": round(time.monotonic() - t1, 1),
                                   "wire": wire_summary(WIRE[m0:])})
                out["steps"][step] = {"counts": counts}
        except WriteAttempt as e:
            out["errors"].append({"step": step, "write_attempt": str(e), "tb": traceback.format_exc()[-1500:]})
        except Exception as e:  # noqa: BLE001
            out["errors"].append({"step": step, "error": (type(e).__name__ + ": " + str(e))[:300],
                                  "tb": traceback.format_exc()[-1500:]})
        out["steps"].setdefault(step, {})["secs"] = round(time.monotonic() - t0, 1)
        out["steps"][step]["wire"] = wire_summary(WIRE[n0:])
    out["recipes"] = [{"i": i, "url": r.url, "method": r.method, "pagination": r.pagination, "provider_hint": r.provider_hint,
                       "platform": RV.platform_of(r), "vehicle_rows": r.vehicle_rows, "total_count": r.total_count,
                       "pins": sorted(RV.recipe_condition_filter(r)), "key": list(r.key())} for i, r in enumerate(live)]
    out["checks"] = CHECKS[:]
    out["logical"] = LOGICAL[:]
    out["wire"] = WIRE[:]
    out["finished"] = now_iso()
    return out


def main():
    sample = json.load(open(os.path.join(RUN, "sample.json")))
    only = [a for a in sys.argv[1:] if not a.startswith("-")]
    if only:
        sample = [d for d in sample if d in only]
    man, db = load_dealers()
    ok_ids = one_condition_ok()
    meta = {"started": now_iso(), "host_gap_s": HOST_GAP, "guard_selftest": GUARD_SELFTEST, "db_read_only": DB_RO,
            "worktree_rev": "dbdbf5cba", "one_condition_ok": sorted(ok_ids)}
    json.dump(meta, open(os.path.join(RESULTS, "_meta_%d.json" % int(time.time())), "w"), indent=1)
    for did in sample:
        path = os.path.join(RESULTS, did + ".json")
        if os.path.exists(path):
            continue
        rng = random.Random(f"p0b1-{did}")
        CUR["dealer"] = did
        print(now_iso(), "start", did, flush=True)
        t0 = time.monotonic()
        try:
            res = measure(did, man, db, ok_ids, rng)
        except WriteAttempt as e:
            res = {"dealer": did, "fatal_write_attempt": str(e), "tb": traceback.format_exc()[-2000:]}
        except Exception as e:  # noqa: BLE001
            res = {"dealer": did, "fatal": (type(e).__name__ + ": " + str(e))[:300], "tb": traceback.format_exc()[-2000:]}
        tmp = path + ".tmp"
        json.dump(res, open(tmp, "w"), indent=0, default=str)
        os.replace(tmp, path)
        print(now_iso(), "done", did, round(time.monotonic() - t0), "s", "wire", len(res.get("wire") or []),
              "errors", len(res.get("errors") or []), flush=True)
    print(now_iso(), "ALL DONE", flush=True)


if __name__ == "__main__":
    main()
