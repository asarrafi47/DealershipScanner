"""DB metrics for the clean-room acceptance tool. Every function takes a verified
read-only ``Target`` and runs on Postgres and on SQLite (the tests use SQLite).

Anything regex-shaped is classified in Python over a narrow column scan, so the
SQL stays portable; the one JSON join (``nhtsa_vpic_cache``) is dialect-aware.
Every query is bounded by ``statement_timeout`` (120 s) on Postgres.
"""

from __future__ import annotations

import collections
import json
import re
from typing import Any
from urllib.parse import urlsplit

from backend.scripts.cleanroom.db import Target
from backend.scripts.cleanroom.recompute import is_used

VIN17_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")
DICT_TRIM_PREFIX = "{'_type'"
TOKEN_COLOR = "[["
PLACEHOLDER_SVG = "/static/placeholder.svg"
# Tokens the incomplete-listings findings (2.4) saw on placeholder photos that the
# parser rule (backend/parsers/base.py:_is_placeholder_url) does not catch.
EXTRA_PLACEHOLDER_TOKENS = ("unavailable_stockphoto", "photo_unavailable", "photo-not-available",
                            "photo_not_available", "/default.jpg")
DRIVE_4X = ("AWD", "4WD")
VPIC_2WD_PREFIXES = ("4x2", "FWD", "RWD")


# ---------------------------------------------------------------------------
# DB state and identity
# ---------------------------------------------------------------------------

def db_state(t: Target) -> dict[str, Any]:
    r = t.one("SELECT count(*), max(id), max(scraped_at) FROM cars WHERE listing_active = 1", label="db_state")
    return {"active": r[0], "max_id": r[1], "max_scraped_at": None if r[2] is None else str(r[2])}


def db_counts(t: Target) -> dict[str, Any]:
    tot = t.one(
        "SELECT count(*), "
        "sum(CASE WHEN listing_active = 1 THEN 1 ELSE 0 END), "
        "sum(CASE WHEN coalesce(listing_active, 1) = 0 THEN 1 ELSE 0 END), "
        "sum(CASE WHEN listing_removed_at IS NOT NULL THEN 1 ELSE 0 END), "
        "count(DISTINCT vin) FROM cars",
        label="db_counts",
    )
    by = t.rows(
        "SELECT dealer_id, "
        "sum(CASE WHEN listing_active = 1 THEN 1 ELSE 0 END), "
        "sum(CASE WHEN coalesce(listing_active, 1) = 0 THEN 1 ELSE 0 END) "
        "FROM cars GROUP BY dealer_id",
        label="db_counts",
    )
    return {
        "total_rows": int(tot[0] or 0),
        "active": int(tot[1] or 0),
        "inactive": int(tot[2] or 0),
        "removed_at_set": int(tot[3] or 0),
        "distinct_vins": int(tot[4] or 0),
        "_active_by_dealer": {d: int(a or 0) for d, a, _ in by if a},
        "_inactive_by_dealer": {d: int(i or 0) for d, _, i in by if i},
    }


# ---------------------------------------------------------------------------
# VIN-owner conflicts
# ---------------------------------------------------------------------------

def conflicts(t: Target, top: int = 10) -> dict[str, Any]:
    if not t.has_table("vin_owner_conflicts"):
        return {"present": False}
    r = t.one("SELECT count(*), count(DISTINCT vin), count(DISTINCT owner_dealer_id), "
              "count(DISTINCT claimant_dealer_id) FROM vin_owner_conflicts", label="conflicts")
    pairs = t.rows("SELECT owner_dealer_id, claimant_dealer_id, count(*) FROM vin_owner_conflicts "
                   "GROUP BY owner_dealer_id, claimant_dealer_id ORDER BY 3 DESC, 1, 2", label="conflicts")
    return {
        "present": True,
        "rows": int(r[0]),
        "vins": int(r[1]),
        "pairs": len(pairs),
        "owners": int(r[2]),
        "claimants": int(r[3]),
        "top_pairs": [{"owner": o, "claimant": c, "rows": int(n)} for o, c, n in pairs[:top]],
        "_pairs": [(o, c, int(n)) for o, c, n in pairs],
    }


def conflict_groups(pairs: list[tuple[str, str, int]], min_rows: int = 20) -> dict[str, Any]:
    """Connected components of owner/claimant pairs; keep those with >= min_rows rows."""
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for o, c, _ in pairs:
        ro, rc = find(o), find(c)
        if ro != rc:
            parent[ro] = rc
    comp_rows: collections.Counter[str] = collections.Counter()
    comp_dealers: dict[str, set[str]] = collections.defaultdict(set)
    for o, c, n in pairs:
        root = find(o)
        comp_rows[root] += n
        comp_dealers[root].update((o, c))
    keep = sorted((r for r in comp_rows if comp_rows[r] >= min_rows), key=lambda r: (-comp_rows[r], r))
    groups = [{"rows": comp_rows[r], "dealers": sorted(comp_dealers[r])} for r in keep]
    return {
        "min_rows": min_rows,
        "components": len(comp_rows),
        "components_dealers": sum(len(v) for v in comp_dealers.values()),
        "groups": len(groups),
        "dealers": sum(len(g["dealers"]) for g in groups),
        "rows": sum(g["rows"] for g in groups),
        "_groups": groups,
    }


# ---------------------------------------------------------------------------
# CF-7 class-(b) DB checks (conflicts RCA section 6, plan N11.3)
# ---------------------------------------------------------------------------

_DON_RE = re.compile(r"/inventoryphotos/(\d+)/")
_DDC_ACCOUNT_RE = re.compile(r"pictures\.dealer\.com/s/([^/]+)/|/s/([a-z0-9_-]+)/\d+/", re.I)
_CHAPMAN_LOT_RE = re.compile(r"/vehicles/([A-Z0-9]{2,4})/", re.I)
_VDP_CITY_RE = re.compile(r"----([A-Za-z_]+)--[A-Z]{2}/?$")


def _first_image(row: dict[str, Any]) -> str:
    img = str(row.get("image_url") or "")
    if img.startswith("http"):
        return img
    g = row.get("gallery")
    if isinstance(g, str) and g.strip().startswith("["):
        try:
            for u in json.loads(g):
                if isinstance(u, str) and u.startswith("http"):
                    return u
        except ValueError:
            pass
    return img


def _dealer_rows(t: Target, dealer_id: str, cols: str) -> list[dict[str, Any]]:
    return list(t.dict_rows(f"SELECT {cols} FROM cars WHERE dealer_id = %s AND listing_active = 1",
                            (dealer_id,), label="cf7"))


def _host(url: Any) -> str:
    try:
        return (urlsplit(str(url or "")).hostname or "").lower()
    except ValueError:
        return ""


def cf7_checks(t: Target) -> dict[str, Any]:
    out: dict[str, Any] = {}
    # (1) mbbeverlyhills-com rows served from mbontario's host (cross-wired recipe)
    mbb = _dealer_rows(t, "mbbeverlyhills-com", "vin, source_url")
    on_host = [r for r in mbb if _host(r["source_url"]) == "www.mbontario.com"]
    claimed: set[str] = set()
    if t.has_table("vin_owner_conflicts"):
        claimed = {v for (v,) in t.rows(
            "SELECT DISTINCT vin FROM vin_owner_conflicts WHERE owner_dealer_id = %s AND claimant_dealer_id = %s",
            ("mbbeverlyhills-com", "mbontario-com"), label="cf7")}
    out["mbbeverlyhills_mbontario_host"] = {
        "dealer_id": "mbbeverlyhills-com",
        "host": "www.mbontario.com",
        "dealer_active": len(mbb),
        "total": len(on_host),
        "conflict_rows_claimant_mbontario": sum(1 for r in on_host if r["vin"] in claimed),
    }
    out["mbontario_active"] = len(_dealer_rows(t, "mbontario-com", "vin"))

    def stock_suffix(dealer: str, suffixes: tuple[str, ...]) -> dict[str, Any]:
        rows = _dealer_rows(t, dealer, "stock_number")
        c = collections.Counter()
        for r in rows:
            s = str(r.get("stock_number") or "").strip().upper()
            for suf in suffixes:
                if s.endswith(suf):
                    c[suf] += 1
        return {"dealer_active": len(rows), "rows": sum(c.values()), "by_suffix": dict(sorted(c.items()))}

    out["cleveland_nissan_ch_da_br"] = stock_suffix("cleveland-nissan-com", ("CH", "DA", "BR"))
    out["mtnviewnissan_ch"] = stock_suffix("mtnviewnissan-com", ("CH",))

    rows = _dealer_rows(t, "chapmanfordaz-com", "image_url, gallery")
    lots = collections.Counter()
    for r in rows:
        m = _CHAPMAN_LOT_RE.search(_first_image(r))
        if m:
            lots[m.group(1).upper()] += 1
    out["chapmanfordaz_cae_cay_cav"] = {
        "dealer_active": len(rows),
        "rows": sum(lots[k] for k in ("CAE", "CAY", "CAV")),
        "by_lot": {k: lots.get(k, 0) for k in ("CAE", "CAY", "CAV")},
    }

    def don_accounts(dealer: str, accounts: tuple[str, ...]) -> dict[str, Any]:
        rows = _dealer_rows(t, dealer, "image_url, gallery")
        c = collections.Counter()
        for r in rows:
            m = _DON_RE.search(_first_image(r))
            if m and m.group(1) in accounts:
                c[m.group(1)] += 1
        return {"dealer_active": len(rows), "rows": sum(c.values()),
                "by_account": {a: c.get(a, 0) for a in accounts}}

    out["bentleycadillac_don_18411_21776_19779"] = don_accounts("bentleycadillac-com", ("18411", "21776", "19779"))
    out["landers"] = {
        "landersmclartychevrolet_don_20573_20574": don_accounts("landersmclartychevrolet-com", ("20573", "20574")),
        "landersmclartynissanhuntsville_don_20624": don_accounts("landersmclartynissanhuntsville-com", ("20624",)),
        "landersmclartysubaru_don_24529": don_accounts("landersmclartysubaru-net", ("24529",)),
    }
    out["landers"]["rows"] = sum(v["rows"] for v in out["landers"].values() if isinstance(v, dict))

    rows = _dealer_rows(t, "bmwofmonrovia-net", "image_url, gallery")
    acc = collections.Counter()
    for r in rows:
        m = _DDC_ACCOUNT_RE.search(_first_image(r))
        acc[(m.group(1) or m.group(2)).lower() if m else "(no dealer.com account)"] += 1
    out["bmwofmonrovia_non_sonicbmwmonrovia"] = {
        "dealer_active": len(rows),
        "rows": sum(n for a, n in acc.items() if a not in ("sonicbmwmonrovia", "(no dealer.com account)")),
        "no_account": acc.get("(no dealer.com account)", 0),
        "top_accounts": dict(acc.most_common(8)),
    }

    rows = _dealer_rows(t, "5starford-com", "source_url")
    city = collections.Counter()
    for r in rows:
        m = _VDP_CITY_RE.search(str(r.get("source_url") or ""))
        city[m.group(1) if m else "(no city)"] += 1
    out["5starford_non_nrh_city"] = {
        "dealer_active": len(rows),
        "rows": sum(n for c, n in city.items() if c not in ("North_Richland_Hills", "(no city)")),
        "no_city": city.get("(no city)", 0),
        "by_city": dict(city.most_common(8)),
    }

    rows = _dealer_rows(t, "fletcherjones-com", "description")
    out["fletcherjones_henderson"] = {
        "dealer_active": len(rows),
        "rows": sum(1 for r in rows if "mercedes-benz of henderson" in str(r.get("description") or "").lower()),
    }
    return out


# ---------------------------------------------------------------------------
# one pass over active cars: parser junk, prices, VIN shape, photos, headline
# ---------------------------------------------------------------------------

_SCAN_COLS = ("id, vin, dealer_id, make, model, trim, price, msrp, mileage, condition, is_cpo, exterior_color, "
              "interior_color, image_url, gallery, engine_description, engine_l, drivetrain, fuel_type, "
              "transmission, transmission_type, body_style, cylinders")
_HTTP_IMG_RE = re.compile(r"^(https?://|/car-images/)", re.I)
_GALLERY_HTTP_RE = re.compile(r'"(https?://|/car-images/)', re.I)


def _blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def column_scan(t: Target) -> dict[str, Any]:
    try:
        from backend.parsers.base import _is_placeholder_url
    except Exception:  # the parser rule is optional context, not a definition
        _is_placeholder_url = None
    c: collections.Counter[str] = collections.Counter()
    dealers: dict[str, set[str]] = collections.defaultdict(set)
    per_dealer: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    price_models: dict[tuple[str, float], set[str]] = collections.defaultdict(set)
    price_cars: collections.Counter[tuple[str, float]] = collections.Counter()
    tokens: collections.Counter[str] = collections.Counter()
    token_dealers: dict[str, set[str]] = collections.defaultdict(set)
    for r in t.dict_rows(f"SELECT {_SCAN_COLS} FROM cars WHERE listing_active = 1", label="column_scan"):
        did = r.get("dealer_id") or "?"
        pd = per_dealer[did]
        pd["active"] += 1
        c["active"] += 1
        trim = r.get("trim")
        if isinstance(trim, str) and trim.startswith(DICT_TRIM_PREFIX):
            c["dict_trim"] += 1
            dealers["dict_trim"].add(did)
            pd["dict_trim"] += 1
        if isinstance(r.get("exterior_color"), str) and TOKEN_COLOR in r["exterior_color"]:
            c["token_color"] += 1
            dealers["token_color"].add(did)
            pd["token_color"] += 1
        if isinstance(r.get("interior_color"), str) and TOKEN_COLOR in r["interior_color"]:
            c["token_interior_color"] += 1
        price = r.get("price")
        if price is not None:
            p = float(price)
            if 10 <= p < 1000:
                c["junk_10_999"] += 1
                dealers["junk_10_999"].add(did)
                pd["junk_price"] += 1
                price_models[(did, p)].add(str(r.get("model") or ""))
                price_cars[(did, p)] += 1
            elif 0 < p < 10:
                c["price_lt10"] += 1
            elif p <= 0:
                c["price_le0"] += 1
            elif 1000 <= p < 3000:
                c["price_1k_3k"] += 1
            elif p > 1_000_000:
                c["price_gt_1m"] += 1
        vin = str(r.get("vin") or "")
        if len(vin) != 17:
            c["vin_not_17"] += 1
            dealers["vin_not_17"].add(did)
            pd["vin_not_17"] += 1
        elif re.search(r"[IOQ]", vin.upper()):
            c["vin_17_with_ioq"] += 1
            dealers["vin_17_with_ioq"].add(did)
        if not VIN17_RE.match(vin):
            c["vin_bad_shape"] += 1
        img = str(r.get("image_url") or "")
        gal = str(r.get("gallery") or "")
        has_photo = bool(_HTTP_IMG_RE.match(img) or _GALLERY_HTTP_RE.search(gal))
        if not has_photo:
            c["no_photo"] += 1
            pd["no_photo"] += 1
        if img == PLACEHOLDER_SVG:
            c["placeholder_svg"] += 1
        if img.startswith("http"):
            low = img.lower()
            hit = next((tok for tok in EXTRA_PLACEHOLDER_TOKENS if tok in low), None)
            if hit:
                tokens[hit] += 1
                token_dealers[hit].add(did)
                pd["http_placeholder"] += 1
            if _is_placeholder_url is not None and _is_placeholder_url(img):
                c["http_placeholder_parser_rule"] += 1
        used = is_used(r)
        mi = r.get("mileage")
        if used and mi == 0:
            c["used_zero_mi"] += 1
            pd["used_zero_mi"] += 1
        elif used and mi is None:
            c["used_null_mi"] += 1
        elif (not used) and mi is None:
            c["nonused_null_mi"] += 1
        pd["new" if not used else "used"] += 1
        # headline.sql columns
        c["price_missing"] += price is None and r.get("msrp") is None
        c["trim_blank"] += _blank(trim)
        c["ext_blank"] += _blank(r.get("exterior_color"))
        c["int_blank"] += _blank(r.get("interior_color"))
        c["engine_raw_blank"] += _blank(r.get("engine_description")) and _blank(r.get("engine_l"))
        c["drive_raw_blank"] += _blank(r.get("drivetrain"))
        c["fuel_blank"] += _blank(r.get("fuel_type"))
        c["fuel_injection"] += bool(re.search("injection", str(r.get("fuel_type") or ""), re.I))
        c["trans_raw_blank"] += _blank(r.get("transmission")) and _blank(r.get("transmission_type"))
        c["body_blank"] += _blank(r.get("body_style"))
        c["cyl_null"] += r.get("cylinders") is None
    repeated = [(k, n) for k, n in price_cars.items() if len(price_models[k]) >= 5]
    return {
        "parser_junk": {
            "dict_trim": {"rows": c["dict_trim"], "dealers": len(dealers["dict_trim"])},
            "token_color": {"rows": c["token_color"], "dealers": len(dealers["token_color"])},
            "token_interior_color_rows": c["token_interior_color"],
            "definition": "trim starting \"{'_type'\"; exterior_color containing '[['",
        },
        "junk_prices": {
            "price_10_999": c["junk_10_999"],
            "price_10_999_dealers": len(dealers["junk_10_999"]),
            "price_lt10": c["price_lt10"],
            "price_le0": c["price_le0"],
            "price_1k_3k": c["price_1k_3k"],
            "price_gt_1m": c["price_gt_1m"],
            "repeated_value_pairs": len(repeated),
            "repeated_value_cars": sum(n for _, n in repeated),
            "repeated_rule": "same dealer and price in $10-$999 across >= 5 distinct models",
        },
        "vin_quality": {
            "not_17_chars": c["vin_not_17"],
            "not_17_dealers": len(dealers["vin_not_17"]),
            "len17_with_i_o_q": c["vin_17_with_ioq"],
            "len17_with_i_o_q_dealers": len(dealers["vin_17_with_ioq"]),
            "bad_shape_total": c["vin_bad_shape"],
        },
        "placeholder_images": {
            "no_http_photo": c["no_photo"],
            "stored_placeholder_svg": c["placeholder_svg"],
            "http_extra_tokens": dict(tokens.most_common()),
            "http_extra_tokens_dealers": {k: len(v) for k, v in token_dealers.items()},
            "http_extra_tokens_total": sum(tokens.values()),
            "http_parser_rule": c["http_placeholder_parser_rule"],
        },
        "mileage": {
            "used_zero": c["used_zero_mi"],
            "used_null": c["used_null_mi"],
            "new_or_blank_condition_null": c["nonused_null_mi"],
        },
        "headline": {k: c[k] for k in ("active", "price_missing", "trim_blank", "ext_blank", "int_blank",
                                       "no_photo", "engine_raw_blank", "drive_raw_blank", "fuel_blank",
                                       "fuel_injection", "trans_raw_blank", "body_blank", "cyl_null",
                                       "used_zero_mi", "used_null_mi", "nonused_null_mi", "junk_10_999",
                                       "price_lt10", "price_le0", "price_1k_3k", "price_gt_1m")},
        "_per_dealer": {d: dict(v) for d, v in per_dealer.items()},
    }


# ---------------------------------------------------------------------------
# drivetrain vs vPIC (fact precedence, FP08)
# ---------------------------------------------------------------------------

def drivetrain_vs_vpic(t: Target) -> dict[str, Any]:
    if not t.has_table("nhtsa_vpic_cache"):
        return {"present": False}
    dt = t.json_text("v.response_json", "Results", 0, "DriveType")
    rows = t.rows(
        f"SELECT c.dealer_id, c.drivetrain, {dt} FROM cars c JOIN nhtsa_vpic_cache v ON v.vin = c.vin "
        "WHERE c.listing_active = 1",
        label="drivetrain_vs_vpic",
    )
    hits = [(d, drv, vp) for d, drv, vp in rows
            if drv in DRIVE_4X and isinstance(vp, str) and vp.startswith(VPIC_2WD_PREFIXES)]
    by = collections.Counter(d for d, _, _ in hits)
    return {
        "present": True,
        "decoded_active": len(rows),
        "rows": len(hits),
        "dealers": len(by),
        "by_pair": dict(collections.Counter(f"{drv}|{vp}" for _, drv, vp in hits).most_common()),
        "definition": "active cars stored AWD/4WD whose vPIC Results[0].DriveType starts 4x2/FWD/RWD",
        "_by_dealer": dict(by),
    }


# ---------------------------------------------------------------------------
# body.py: body-style vocabulary
# ---------------------------------------------------------------------------

_CANON = {"SUV", "Truck", "Sedan", "Coupe", "Van", "Hatchback", "Convertible", "Wagon", "Minivan", "Crossover",
          "Pickup"}
_CATEGORY_RE = re.compile(r"^(cars?|midsize cars|compact( cars)?|large cars|subcompact cars|minicompact cars|"
                          r"two seaters|trucks|suvs|vans|passenger cars?|mid-?size)$", re.I)


def body_style_vocabulary(t: Target) -> dict[str, Any]:
    from backend.utils.field_clean import coerce_body_style_stored

    rows = t.rows("SELECT body_style, trim, count(*) FROM cars WHERE listing_active = 1 GROUP BY body_style, trim",
                  label="body_style")
    out: collections.Counter[str] = collections.Counter()
    for b, tr, n in rows:
        n = int(n)
        if not (b or "").strip():
            out["blank"] += n
            continue
        v = coerce_body_style_stored(b) or ""
        if v in _CANON:
            out["canonical"] += n
        elif _CATEGORY_RE.match(v.strip()):
            out["category"] += n
        elif tr and v.strip().lower() == tr.strip().lower():
            out["trim_copy"] += n
        elif re.search(r"cab|crew|van", v, re.I):
            out["cab_van_label"] += n
        else:
            out["other_noncanonical"] += n
    res = dict(out)
    res["noncanonical_total"] = sum(out[k] for k in ("category", "trim_copy", "cab_van_label", "other_noncanonical"))
    return res


# ---------------------------------------------------------------------------
# coverage and first-seen against a reference DB
# ---------------------------------------------------------------------------

def vin_first_seen(t: Target) -> tuple[dict[str, str], dict[str, int], Any]:
    """(vin -> earliest first_seen_at, vin -> max listing_active, max first_seen_at) over every row."""
    first: dict[str, str] = {}
    active: dict[str, int] = {}
    mx = None
    for vin, fs, la in t.rows("SELECT vin, first_seen_at, listing_active FROM cars", label="ref_vins"):
        if vin is None:
            continue
        active[vin] = max(active.get(vin, 0), int(la or 0))
        if fs is None:
            continue
        s = str(fs)
        if vin not in first or s < first[vin]:
            first[vin] = s
        if mx is None or s > mx:
            mx = s
    return first, active, mx


CHANGE_BUCKETS = ("le_-90pct", "-90_to_-30pct", "-30_to_-10pct", "-10_to_+10pct", "+10_to_+30pct", "ge_+30pct")


def change_bucket(ch: float) -> str:
    """Bucket a relative change (db - ref) / ref the way dealer_records section 3 does."""
    if ch <= -0.9:
        return CHANGE_BUCKETS[0]
    if ch <= -0.3:
        return CHANGE_BUCKETS[1]
    if ch < -0.1:  # exactly -10% counts as "-10..+10" (reproduces dealer_records section 3)
        return CHANGE_BUCKETS[2]
    if ch < 0.1:
        return CHANGE_BUCKETS[3]
    if ch < 0.3:
        return CHANGE_BUCKETS[4]
    return CHANGE_BUCKETS[5]


def coverage_vs_ref(db_active_by_dealer: dict[str, int], ref_active_by_dealer: dict[str, int],
                    db_vins: set[str], ref_active: dict[str, int], roster: set[str] | None = None) -> dict[str, Any]:
    dealers = set(roster) if roster else (set(db_active_by_dealer) | set(ref_active_by_dealer))
    buckets: dict[str, dict[str, int]] = {name: {"dealers": 0, "ref_active": 0, "db_active": 0}
                                          for name in CHANGE_BUCKETS}
    no_ref = []
    for d in sorted(dealers):
        a, o = db_active_by_dealer.get(d, 0), ref_active_by_dealer.get(d, 0)
        if not o:
            no_ref.append(d)
            continue
        b = buckets[change_bucket((a - o) / o)]
        b["dealers"] += 1
        b["ref_active"] += o
        b["db_active"] += a
    in_ref_any = sum(1 for v in db_vins if v in ref_active)
    return {
        "dealers_compared": len(dealers) - len(no_ref),
        "dealers_without_ref_rows": len(no_ref),
        "ref_active_total": sum(ref_active_by_dealer.get(d, 0) for d in dealers),
        "db_active_total": sum(db_active_by_dealer.get(d, 0) for d in dealers),
        "change_buckets": buckets,
        "db_active_vins": len(db_vins),
        "db_active_vins_in_ref_any_status": in_ref_any,
        "db_active_vins_not_in_ref": len(db_vins) - in_ref_any,
        "db_active_vins_ref_retired": sum(1 for v in db_vins if v in ref_active and not ref_active[v]),
        "ref_active_vins_not_in_db_active": sum(1 for v, a in ref_active.items() if a and v not in db_vins),
    }


def active_vins(t: Target) -> set[str]:
    return {v for (v,) in t.rows("SELECT vin FROM cars WHERE listing_active = 1", label="active_vins") if v}


def index_rows(t: Target) -> list[tuple[Any, Any]]:
    if not t.has_table("incomplete_listings"):
        return []
    return t.rows("SELECT il.car_id, il.missing_fields_json FROM incomplete_listings il "
                  "JOIN cars c ON c.id = il.car_id AND c.listing_active = 1", label="index_rows")
