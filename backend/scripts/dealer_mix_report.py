#!/usr/bin/env python3
"""Dealer mix report: franchise vs independent vs national retailer vs marketplace.

Read-only. Measures the real inventory database for docs/independent_dealers/ANALYSIS.md:
the registry (``dealerships``) against the scan roster (``cars``, ``dealer_recipes``,
``scan_runs``), and per dealer kind (``backend/discovery/dealer_kind.py``): cars, recipes,
the last scan result, registry rows never scanned and a freshness histogram.

Safety: reads ``INVENTORY_DATABASE_URL``; connects with ``default_transaction_read_only=on``
and a statement timeout, sets the session READ ONLY, runs SELECT statements only and prints
aggregates and dealer ids (public business domains). It prints no VIN, no dealer name, no
address, no phone and no URL with a query string. A missing table or column prints one line
and the report goes on.

Run (repo root): ``python -m backend.scripts.dealer_mix_report``. On Railway the owner pipes
``dealer_kind.py`` followed by this file into ``python3 -`` in the web container
(docs/independent_dealers/ANALYSIS.md section 4 has the exact command). The two files are
concatenated, so this file must not use a ``__future__`` import.
"""
import json
import os
import re
import sys
from collections import Counter, defaultdict

if "classify_dealer" not in globals():  # run from a checkout, not concatenated
    try:
        from backend.discovery.dealer_kind import KINDS, classify_dealer, host_from_dealer_id
    except ImportError:  # pragma: no cover - started as a plain file path
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
        from backend.discovery.dealer_kind import KINDS, classify_dealer, host_from_dealer_id

STATEMENT_TIMEOUT_MS = 30000
LIST_CAP = 80
SMALL_LOT = 20  # MIN_ROWS_UNKNOWN in backend/scanner/pipeline/constants.py
AGE_BINS = (("0-1d", 0, 1), ("2-3d", 2, 3), ("4-7d", 4, 7), ("8-14d", 8, 14),
            ("15-30d", 15, 30), ("31-60d", 31, 60), ("61d+", 61, 10 ** 6))

# --- SQL: SELECT only ------------------------------------------------------------------
_AGE = ("CASE WHEN scraped_at ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' "
        "THEN CURRENT_DATE - substr(scraped_at, 1, 10)::date END")
_ACTIVE = "COALESCE(listing_active, 1) = 1"
_COND = "lower(COALESCE(condition, ''))"
SQL_CARS = (
    "SELECT dealer_id, COUNT(*) AS rows_all, "
    f"COUNT(*) FILTER (WHERE {_ACTIVE}) AS active, "
    f"COUNT(*) FILTER (WHERE {_ACTIVE} AND {_COND} LIKE 'new%') AS active_new, "
    f"COUNT(*) FILTER (WHERE {_ACTIVE} AND ({_COND} LIKE '%used%' OR {_COND} LIKE '%pre%' "
    f"OR {_COND} LIKE '%certif%' OR {_COND} = 'cpo')) AS active_used, "
    f"array_agg(DISTINCT lower(btrim(make))) FILTER (WHERE {_ACTIVE} AND make IS NOT NULL "
    "AND btrim(make) <> '') AS makes, "
    "MAX(dealer_name) AS name, "
    + ", ".join(
        f"COUNT(*) FILTER (WHERE {_ACTIVE} AND age BETWEEN {lo} AND {hi}) AS \"{lab}\""
        for lab, lo, hi in AGE_BINS
    )
    + f", COUNT(*) FILTER (WHERE {_ACTIVE} AND age IS NULL) AS age_unknown "
    f"FROM (SELECT dealer_id, listing_active, condition, make, dealer_name, {_AGE} AS age "
    "FROM cars WHERE dealer_id IS NOT NULL AND btrim(dealer_id) <> '') c "
    "GROUP BY dealer_id"
)
SQL_RECIPES = (
    "SELECT dealer_id, MAX(COALESCE(recipe_count, 0)) AS recipe_count, "
    "MAX(provider_hint) AS provider, MAX(scan_hints) AS scan_hints "
    "FROM dealer_recipes WHERE dealer_id IS NOT NULL AND btrim(dealer_id) <> '' "
    "GROUP BY dealer_id"
)
SQL_SCANS = (
    "SELECT DISTINCT ON (dealer_id) dealer_id, finished_at, "
    "(error IS NOT NULL AND btrim(error) <> '') AS has_error, upserted, provider "
    "FROM scan_runs WHERE dealer_id IS NOT NULL ORDER BY dealer_id, finished_at DESC"
)
SQL_REGISTRY_FULL = (
    "SELECT id, name, website_url, dealer_website_url, state, oem_brand, google_primary_type, "
    "source_dmv, source_osm, source_web FROM dealerships "
    "WHERE is_active = 1 AND duplicate_of_id IS NULL"
)
SQL_REGISTRY_MIN = (
    "SELECT id, name, website_url, NULL AS dealer_website_url, state, NULL AS oem_brand, "
    "NULL AS google_primary_type, 0 AS source_dmv, 0 AS source_osm, 0 AS source_web "
    "FROM dealerships"
)
# Active cars in the public-incomplete index: hidden on the production site
# (backend/db/repositories/data_quality_repo.py listings_include_incomplete_cars).
SQL_HIDDEN = (
    "SELECT c.dealer_id, COUNT(*) AS hidden FROM incomplete_listings i "
    "JOIN cars c ON c.id = i.car_id "
    "WHERE COALESCE(c.listing_active, 1) = 1 AND c.dealer_id IS NOT NULL GROUP BY c.dealer_id"
)
QUERIES = (SQL_CARS, SQL_RECIPES, SQL_SCANS, SQL_REGISTRY_FULL, SQL_REGISTRY_MIN, SQL_HIDDEN)

# Hosts that are listing marketplaces, not a dealer's own site (registry website check).
_MARKET_HOST_RE = re.compile(
    r"(^|\.)(cars\.com|autotrader\.com|cargurus\.com|truecar\.com|carfax\.com|carsforsale\.com|"
    r"facebook\.com|craigslist\.org|offerup\.com|ebay\.com|autolist\.com|edmunds\.com|kbb\.com)$"
)


def url_to_dealer_id(url):
    """The scanner's dealer id for a site URL (backend/scanner/manifest.py _host_to_dealer_id)."""
    s = re.sub(r"^https?://(www\.)?", "", (url or "").strip(), flags=re.I).split("/")[0]
    return s.lower().replace(".", "-")


def _host(url):
    s = re.sub(r"^https?://", "", (url or "").strip(), flags=re.I).split("/")[0].lower()
    return s[4:] if s.startswith("www.") else s


def _hint_flags(raw):
    try:
        h = json.loads(raw) if raw else {}
    except (TypeError, ValueError):
        return {"unparsed"}
    if not isinstance(h, dict):
        return set()
    return {k for k in ("skip_reason", "requires_browser", "needs_http_proxy") if h.get(k)}


def last_scan_result(scan):
    """A coarse result from scan_runs (the triage verdict lives in the run bundle, not here)."""
    if not scan:
        return "never_scanned"
    if scan.get("has_error"):
        return "error"
    n = int(scan.get("upserted") or 0)
    if n == 0:
        return "no_rows"
    if n < SMALL_LOT:
        return "under_20_rows"
    return "rows"


def summarize(cars, recipes, scans, registry, hidden=None):
    """Pure aggregation. Each argument is a list of dicts keyed like the SQL columns, or None
    when that table could not be read. Returns a JSON-able dict of counts and dealer ids."""
    cars_by = {r["dealer_id"]: r for r in (cars or [])}
    rec_by = {r["dealer_id"]: r for r in (recipes or [])}
    scan_by = {r["dealer_id"]: r for r in (scans or [])}
    hidden_by = {r["dealer_id"]: int(r.get("hidden") or 0) for r in (hidden or [])}
    reg_ids = defaultdict(list)
    for r in registry or []:
        did = url_to_dealer_id(r.get("dealer_website_url") or r.get("website_url"))
        if did:
            reg_ids[did].append(r)
    with_recipe = {d for d, r in rec_by.items() if int(r.get("recipe_count") or 0) > 0}
    active = {d for d, r in cars_by.items() if int(r.get("active") or 0) > 0}
    known = set(cars_by) | set(rec_by) | set(scan_by)

    def kind_of(did):
        c = cars_by.get(did) or {}
        reg = (reg_ids.get(did) or [{}])[0]
        return classify_dealer(
            dealer_id=did,
            name=c.get("name") or reg.get("name"),
            rows_new=c.get("active_new"),
            rows_used=c.get("active_used"),
            makes=c.get("makes") if c else None,
        )

    out = {
        "tables": {"cars": cars is not None, "dealer_recipes": recipes is not None,
                   "scan_runs": scans is not None, "dealerships": registry is not None,
                   "incomplete_listings": hidden is not None},
        "sets": {
            "registry_rows": len(registry or []),
            "registry_rows_with_site": sum(1 for r in registry or []
                                           if (r.get("dealer_website_url") or r.get("website_url"))),
            "registry_ids": len(reg_ids),
            "dealers_in_cars": len(cars_by),
            "dealers_with_active_cars": len(active),
            "dealers_with_recipe": len(with_recipe),
            "dealers_ever_scanned": len(scan_by),
            "fleet_roster_estimate": len(active),
            "recipe_only_dealers": len(with_recipe - active),
            "registry_ids_with_active_cars": len(set(reg_ids) & active),
            "registry_ids_scanned": len(set(reg_ids) & set(scan_by)),
            "registry_ids_never_seen": len(set(reg_ids) - known),
            "active_dealers_not_in_registry": len(active - set(reg_ids)),
        },
        "kinds": {},
        "signals": defaultdict(Counter),
        "registry_never_seen": {},
        "lists": defaultdict(list),
    }
    per = {k: {"dealers": 0, "with_active": 0, "active_cars": 0, "new": 0, "used": 0,
               "recipe": Counter(), "hints": Counter(), "last_scan": Counter(),
               "platform": Counter(), "fresh": Counter(), "small_lots": 0, "hidden": 0}
           for k in KINDS}
    for did in sorted(known):
        kind, signal = kind_of(did)
        out["signals"][kind][signal.split(":")[0]] += 1
        p = per[kind]
        c = cars_by.get(did) or {}
        n_act = int(c.get("active") or 0)
        p["dealers"] += 1
        p["active_cars"] += n_act
        p["new"] += int(c.get("active_new") or 0)
        p["used"] += int(c.get("active_used") or 0)
        p["hidden"] += hidden_by.get(did, 0)
        if n_act:
            p["with_active"] += 1
            if n_act < SMALL_LOT:
                p["small_lots"] += 1
        for lab, _, _ in AGE_BINS:
            p["fresh"][lab] += int(c.get(lab) or 0)
        p["fresh"]["unknown"] += int(c.get("age_unknown") or 0)
        rec = rec_by.get(did)
        p["recipe"]["present" if did in with_recipe else "absent"] += 1
        for f in _hint_flags((rec or {}).get("scan_hints")):
            p["hints"][f] += 1
        scan = scan_by.get(did)
        res = last_scan_result(scan) if scans is not None else "scan_runs_missing"
        p["last_scan"][res] += 1
        plat = (rec or {}).get("provider") or (scan or {}).get("provider") or "none"
        p["platform"][str(plat)[:40]] += 1
        if kind != "franchise" and len(out["lists"][kind]) < LIST_CAP:
            out["lists"][kind].append({
                "dealer_id": did[:80], "active": n_act, "new": int(c.get("active_new") or 0),
                "used": int(c.get("active_used") or 0), "makes": len(c.get("makes") or []),
                "recipe": did in with_recipe, "last_scan": res, "signal": signal[:40],
            })
    for k in KINDS:
        p = per[k]
        out["kinds"][k] = {**{x: p[x] for x in ("dealers", "with_active", "active_cars", "new",
                                                "used", "small_lots", "hidden")},
                           "recipe": dict(p["recipe"]), "hints": dict(p["hints"]),
                           "last_scan": dict(p["last_scan"]),
                           "platform": dict(p["platform"].most_common(8)),
                           "fresh": {lab: p["fresh"][lab] for lab, _, _ in AGE_BINS}
                           | {"unknown": p["fresh"]["unknown"]}}
    never = {k: {"rows": 0, "by_state": Counter(), "site_is_marketplace": 0, "no_site": 0}
             for k in KINDS}
    for r in registry or []:
        url = r.get("dealer_website_url") or r.get("website_url")
        did = url_to_dealer_id(url)
        if did and did in known:
            continue
        kind, _ = classify_dealer(name=r.get("name"), host=_host(url) if url else None)
        bucket = never[kind]
        bucket["rows"] += 1
        bucket["by_state"][(r.get("state") or "??")[:2].upper()] += 1
        if not url:
            bucket["no_site"] += 1
        elif _MARKET_HOST_RE.search(_host(url)):
            bucket["site_is_marketplace"] += 1
    out["registry_never_seen"] = {k: {**{x: v[x] for x in ("rows", "site_is_marketplace", "no_site")},
                                      "top_states": dict(v["by_state"].most_common(10))}
                                  for k, v in never.items()}
    out["signals"] = {k: dict(v) for k, v in out["signals"].items()}
    out["lists"] = dict(out["lists"])
    return out


def render(rep):
    lines = ["DEALER MIX REPORT (read-only; counts and dealer ids only)"]
    missing = [t for t, ok in rep["tables"].items() if not ok]
    if missing:
        lines.append("missing tables or columns: " + ", ".join(missing))
    lines.append("== sets")
    lines += [f"  {k}: {v}" for k, v in rep["sets"].items()]
    lines.append("== per kind (dealers seen in cars, recipes or scan_runs)")
    for k, v in rep["kinds"].items():
        lines.append(f"  {k}: dealers {v['dealers']}, with active cars {v['with_active']}, "
                     f"active cars {v['active_cars']} (new {v['new']}, used {v['used']}), "
                     f"lots under {SMALL_LOT} cars {v['small_lots']}, "
                     f"active cars hidden as incomplete in production {v['hidden']}")
        for part in ("recipe", "hints", "last_scan", "platform", "fresh"):
            lines.append(f"    {part}: {json.dumps(v[part], sort_keys=part in ('recipe', 'hints', 'last_scan'))}")
    lines.append("== rule that fired, per kind")
    for k, v in sorted(rep["signals"].items()):
        lines.append(f"  {k}: {json.dumps(v, sort_keys=True)}")
    lines.append("== registry rows never seen by the scanner (no cars, recipe or scan run)")
    for k, v in rep["registry_never_seen"].items():
        lines.append(f"  {k}: rows {v['rows']}, site is a marketplace page {v['site_is_marketplace']}, "
                     f"no site {v['no_site']}, top states {json.dumps(v['top_states'])}")
    for k, items in rep["lists"].items():
        lines.append(f"== {k} dealers (first {LIST_CAP})")
        for it in items:
            lines.append("  " + " ".join(f"{a}={it[a]}" for a in it))
    lines.append("END DEALER MIX REPORT")
    return "\n".join(lines)


def _fetch(conn, label, sqls):
    """Run the first query of ``sqls`` that works; None (and one printed line) when none does."""
    for i, sql in enumerate(sqls):
        more = " (trying a smaller query)" if i + 1 < len(sqls) else ""
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                cols = [d.name for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            conn.commit()  # ends the read-only transaction
            return rows
        except Exception as exc:  # noqa: BLE001 - one line per missing table or column
            conn.rollback()
            print(f"skip {label}: {type(exc).__name__}: {str(exc).splitlines()[0][:160]}{more}", flush=True)
    return None


def connect(url, timeout_ms=STATEMENT_TIMEOUT_MS):
    import psycopg

    conn = psycopg.connect(
        url,
        options=f"-c default_transaction_read_only=on -c statement_timeout={int(timeout_ms)}",
        connect_timeout=15,
        application_name="dealer_mix_report",
    )
    conn.read_only = True
    with conn.cursor() as cur:
        cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        cur.execute(f"SET statement_timeout = {int(timeout_ms)}")
    conn.commit()
    return conn


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    url = (os.environ.get("INVENTORY_DATABASE_URL") or "").strip()
    if not url:
        print("INVENTORY_DATABASE_URL is not set; nothing to read.")
        return 2
    conn = connect(url)
    try:
        cars = _fetch(conn, "cars", [SQL_CARS])
        recipes = _fetch(conn, "dealer_recipes", [SQL_RECIPES])
        scans = _fetch(conn, "scan_runs", [SQL_SCANS])
        registry = _fetch(conn, "dealerships", [SQL_REGISTRY_FULL, SQL_REGISTRY_MIN])
        hidden = _fetch(conn, "incomplete_listings", [SQL_HIDDEN])
    finally:
        conn.close()
    rep = summarize(cars, recipes, scans, registry, hidden)
    print(json.dumps(rep, sort_keys=True) if "--json" in args else render(rep))
    return 0


if __name__ == "__main__":
    sys.exit(main())
