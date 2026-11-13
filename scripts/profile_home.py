"""Time every part of a signed-in GET /home on a throwaway Postgres with fake data.

    scripts/cloud/with_pg.sh -- python scripts/profile_home.py [--json-out FILE]

Builds a scratch inventory database (never a real one), seeds synthetic cars and one fake
shopper with views, compares, saves and a ZIP, then makes cold and warm requests through the
Flask test client. It prints, per request: total ms, ms inside the route function, ms in the
other request hooks, Postgres connections opened, SQL statements run, and a table of ms per
named component (wrapped functions), cold and warm.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

N_CARS = 3000


def _seed(inv, n: int) -> list[int]:
    makes = [("Honda", "Civic"), ("Honda", "Accord"), ("Toyota", "Camry"), ("Toyota", "RAV4"),
             ("Ford", "F-150"), ("Tesla", "Model 3"), ("BMW", "X5"), ("Kia", "Soul")]
    rows = []
    for i in range(n):
        mk, md = makes[i % len(makes)]
        rows.append({
            "title": f"{2015 + i % 10} {mk} {md}", "year": 2015 + i % 10, "make": mk, "model": md,
            "trim": "Sport", "price": 15000 + (i * 37) % 30000, "mileage": 5000 + (i * 91) % 90000,
            "image_url": f"https://img.example.test/{i}.jpg",
            "dealer_name": f"Synthetic Dealer {i % 20}", "dealer_url": f"https://d{i % 20}.example.test/",
            "dealer_id": f"syn-dealer-{i % 20}", "fuel_type": "Gas", "transmission": "Automatic",
            "drivetrain": "FWD", "exterior_color": "Blue", "interior_color": "Black",
            "gallery": json.dumps([f"https://img.example.test/{i}.jpg"]),
            "scraped_at": "2026-11-01T00:00:00Z",
        })
    inv.add_cars(rows)
    with inv.connect() as c:
        return [r[0] for r in c.execute("SELECT id FROM cars ORDER BY id").fetchall()]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json-out")
    ap.add_argument("--warm-runs", type=int, default=5)
    ap.add_argument("--connect-delay-ms", type=float, default=25.0,
                    help="sleep per Postgres connection to mimic production (measured 20-38 ms)")
    args = ap.parse_args()

    from backend.tests import pg_harness as pgh

    admin = pgh.admin_dsn()
    if admin is None:
        print("TEST_PG_ADMIN_DSN missing: run through scripts/cloud/with_pg.sh", file=sys.stderr)
        return 2
    pgh.assert_safe_admin_dsn(admin)
    tmpl = pgh.build_template(admin)
    name, dsn = pgh.clone(tmpl)
    tmp = Path(tempfile.mkdtemp(prefix="homeprof_"))
    env = {
        "INVENTORY_DATABASE_URL": dsn, "DATABASE_URL": "", "INVENTORY_SQLITE_TESTS": "",
        "INVENTORY_SCHEMA_CHECK": "strict", "INVENTORY_AUTO_MIGRATE": "",
        "FLASK_ENV": "development", "BILLING_STRIPE_ENABLED": "0",
        "USERS_DB_PATH": str(tmp / "users.db"), "DEV_USERS_DB_PATH": str(tmp / "dev.db"),
        "DEALER_PORTAL_DB_PATH": str(tmp / "portal.db"),
        "USERS_DB_ENCRYPTION_KEY": "", "DEV_USERS_DB_ENCRYPTION_KEY": "",
        "ALLOW_UNENCRYPTED_USER_DB": "1",
    }
    os.environ.update(env)
    pgh.reset_pg_process_caches()
    try:
        inv = pgh.PgInventory(dsn, name)
        ids = _seed(inv, N_CARS)
        import backend.main as main_mod

        importlib.reload(main_mod)
        app = main_mod.app
        app.config["TESTING"] = True
        from backend.db.repositories import saved_cars_repo
        from backend.db.user_history_db import record_car_view, record_compare_session
        from backend.db.users_db import init_users_db, save_user

        init_users_db()
        uid = save_user("synthetic_shopper", "shopper@example.invalid", "Synthetic-pass-1!", role="shopper")
        for cid in ids[:30]:
            record_car_view(uid, cid)
        record_compare_session(uid, ids[40:43])
        for cid in ids[50:56]:
            saved_cars_repo.save_car(uid, cid)

        # ---- instrumentation -------------------------------------------------
        import psycopg

        from backend.routes import home_dashboard as hd

        stats = {"connects": 0, "queries": 0}
        comp: dict[str, float] = {}

        orig_connect = psycopg.connect

        def counting_connect(*a, **k):
            stats["connects"] += 1
            time.sleep(args.connect_delay_ms / 1000.0)
            conn = orig_connect(*a, **k)
            return conn

        psycopg.connect = counting_connect  # type: ignore[assignment]

        def timed(mod, attr, label):
            fn = getattr(mod, attr, None)
            if fn is None:  # a component that only exists in some versions of the code
                return

            def w(*a, **k):
                t = time.perf_counter()
                try:
                    return fn(*a, **k)
                finally:
                    comp[label] = comp.get(label, 0.0) + (time.perf_counter() - t) * 1000

            setattr(mod, attr, w)

        timed(hd, "_recommendations_for_user", "reco (cache wrapper)")
        timed(hd, "_recommendations_for_user_uncached", "reco (uncached build)")
        timed(hd, "hidden_dealer_ids_for_user", "hidden dealers lookup")
        timed(hd, "get_saved_car_ids", "saved ids")
        timed(hd, "get_cars_by_ids", "get_cars_by_ids (all callers)")
        timed(hd, "serialize_cars_for_listings_grid", "serialize grid (all callers)")
        timed(hd, "_home_lists_for_user", "saved+viewed+compared lists")
        timed(hd, "get_recent_history_ids", "history sqlite: both lists")
        timed(hd, "get_recent_viewed_car_ids", "history sqlite: viewed ids (reco)")
        timed(hd, "get_recent_compared_car_ids", "history sqlite: compared ids")
        timed(hd, "search_cars_by_make_model_pairs", "similar-cars SQL search")
        timed(hd, "_render_personal_home", "whole route body incl. template")

        client = app.test_client()
        with client.session_transaction() as s:
            s["user_id"] = uid
            s["listings_geo_zip"] = "92618"
            s["listings_geo_radius_mi"] = 50.0

        results = []

        def one(label):
            stats["connects"] = 0
            comp.clear()
            t = time.perf_counter()
            r = client.get("/home")
            ms = (time.perf_counter() - t) * 1000
            body = r.get_data(as_text=True)
            results.append({
                "label": label, "status": r.status_code, "total_ms": round(ms, 1),
                "connects": stats["connects"], "bytes": len(body),
                "components": {k: round(v, 1) for k, v in comp.items()},
            })

        one("cold (first request in process)")
        for i in range(args.warm_runs):
            one(f"warm {i + 1}")
        hd._invalidate_reco_cache(uid)
        one("right after a car view (signal)")
        time.sleep(1.5)  # let a background rebuild finish, if the code has one
        one("after that, warm again")
        hd._reco_cache.clear()
        one("reco cache empty, rest warm")

        print(f"{'request':38} {'status':>6} {'total ms':>9} {'pg connects':>12}")
        for r in results:
            print(f"{r['label']:38} {r['status']:>6} {r['total_ms']:>9} {r['connects']:>12}")
        labels = sorted({k for r in results for k in r["components"]})
        print("\nms per component (inclusive, calls summed)")
        print(f"{'component':40}" + "".join(f"{r['label'][:12]:>14}" for r in results[:2]) + f"{'warm median':>14}")
        for k in labels:
            warm = [r["components"].get(k, 0.0) for r in results[1:1 + args.warm_runs]]
            print(f"{k:40}" + "".join(f"{r['components'].get(k, 0.0):>14}" for r in results[:2])
                  + f"{round(statistics.median(warm), 1):>14}")
        if args.json_out:
            Path(args.json_out).write_text(json.dumps(results, indent=2))
    finally:
        pgh.drop(admin, name)
        pgh.drop(admin, tmpl.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
