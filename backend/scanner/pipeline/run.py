"""``run(args)``: the dealer pipeline's numbered steps (roster, recipes, scan, vPIC, assess +
reconcile, lifecycle, triage). backend/scripts/dealer_pipeline.py parses the arguments
and calls it. Moved verbatim from dealer_pipeline.main (audit F11)."""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from backend.scanner.pipeline.assess import assess
from backend.scanner.pipeline.constants import BASELINE_DAYS
from backend.scanner.pipeline.db import _assess_conn, _conn_alive, _rows, chromium_process_count, get_conn, wait_for_db
from backend.scanner.pipeline.dealer_logs import log_discovery, log_scan_run, write_instructions_if_first_success
from backend.scanner.pipeline.lifecycle import run_lifecycle_pass
from backend.scanner.pipeline.reconcile import reconcile_dealer
from backend.scanner.pipeline.recipes import ensure_recipe
from backend.scanner.pipeline.roster import dealer_from_manifest, dealers_from_db, load_manifest_dealers
from backend.scanner.pipeline.runner import run_discovery_capture, run_http_only_scan, wait_for_scanner_lock
from backend.scanner.pipeline.triage import platform_cluster_lines, triage_table, write_needs_discovery, write_slow_dealers
from backend.scanner.pipeline.vpic import vpic_for_dealers


def run(args) -> int:
    manifest = load_manifest_dealers(args.manifest)
    if args.dealers:
        ids = [d.strip() for d in args.dealers.split(",") if d.strip()]
    else:
        ids = [d["dealer_id"] for i, d in enumerate(manifest) if i % max(1, args.shard_count) == args.shard_index]
        if args.limit:
            ids = ids[: args.limit]
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc).replace(microsecond=0)
    baseline_since = (started - timedelta(days=BASELINE_DAYS)).isoformat()
    since_iso = args.since or started.isoformat()

    conn = get_conn()
    known: dict[str, int] = {}
    for did in ids:
        # Rows seen in the 30 days before this run. Every active row would count
        # cars nobody has seen since July (Right Toyota: 3,034 "active", 1,367 seen
        # in September) and the floor then calls a full 1,273-car replay no_rows.
        known[did] = int(_rows(conn, "SELECT COUNT(*) AS n FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at >= ?",
                                (did, baseline_since))[0]["n"])
    conn.close()

    recipe_info: dict[str, dict[str, Any]] = {}
    db_dealers = dealers_from_db([did for did in ids if not dealer_from_manifest(did, manifest)])
    discovered: dict[str, dict[str, Any]] = {}
    dealers_by_id: dict[str, dict[str, Any]] = {}
    for did in ids:
        # 72 of the 222 stale active dealers (2026-09-26) are not in dealers.json
        # at all; their url and name live on their own rows. The manifest is a
        # convenience, never the source of truth for a dealer we already sell.
        d = dealer_from_manifest(did, manifest) or db_dealers.get(did) or {"dealer_id": did, "url": ""}
        dealers_by_id[did] = d
        if not d.get("url"):
            recipe_info[did] = {"had_recipes": 0, "synth": "not_in_manifest_or_db"}
            continue
        try:
            recipe_info[did] = ensure_recipe(d, force=args.force_synth)
            if (not args.no_discover and not recipe_info[did].get("had_recipes")
                    and not str(recipe_info[did].get("synth", "")).startswith("saved")):
                # Phase 1 of docs/HTTP_ONLY_SCANS_PLAN.md: the ONE sanctioned browser
                # use, in its own process, only for a dealer no HTTP template could
                # describe. It writes recipes + discovery.md, never car rows.
                discovered[did] = run_discovery_capture(did)
                if discovered[did].get("recipes_after", 0) > 0:
                    recipe_info[did]["synth"] = f"browser_capture_{discovered[did]['recipes_after']}"
                    recipe_info[did]["had_recipes"] = discovered[did]["recipes_after"]
                recipe_info[did]["discovery"] = discovered[did]
        except Exception as exc:  # noqa: BLE001
            import traceback as _tb

            recipe_info[did] = {"had_recipes": 0, "synth": f"error:{str(exc)[:100]}", "traceback": _tb.format_exc()[-1200:]}
        print(f"recipe  {did:36s} {json.dumps(recipe_info[did])[:140]}", flush=True)
        # Logs now, not at the end: the discovery entry for every dealer, and the
        # verbose probe (everything the site answered) for every synthesis failure.
        try:
            log_discovery(d, recipe_info[did], started.strftime("%Y-%m-%d %H:%M UTC"))
            synth = str(recipe_info[did].get("synth") or "")
            if d.get("url") and not recipe_info[did].get("had_recipes") and synth and not synth.startswith("saved"):
                from backend.scripts.discovery_probe import probe_dealer

                rep = probe_dealer(did, d["url"], paths=True)
                recipe_info[did]["probe"] = rep.get("classification")
                print(f"probe   {did:36s} {rep.get('classification')}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"log     discovery {did}: {str(exc)[:100]}", flush=True)

    if not args.skip_scan:
        scannable = [did for did in ids if recipe_info[did].get("had_recipes") or str(recipe_info[did].get("synth", "")).startswith("saved")]
        skipped = [did for did in ids if did not in scannable]
        if skipped:
            print(f"scan    skipping {len(skipped)} dealer(s) with no recipe: {', '.join(skipped)[:200]}", flush=True)
        chromium_leaks: list[dict[str, Any]] = []
        for i in range(0, len(scannable), args.batch):
            batch = scannable[i:i + args.batch]
            t0 = time.time()
            if not wait_for_db(args.lock_wait):
                print(f"scan    database unreachable for {args.lock_wait}s; stopping before batch {i // args.batch + 1}", flush=True)
                break
            # Another pipeline's shard frees the lock between ITS batches and grabs
            # it again a second later; on 2026-09-24 the mini's Stevenson batch lost
            # that race ("Scanner already running (lock held by pid …)"), the scanner
            # exited 0 and both dealers were logged as "no scan_runs row". Re-wait and
            # relaunch whenever the log says the lock was taken first.
            rc = -1
            for attempt in range(1, 7):
                if not wait_for_scanner_lock(args.lock_wait):
                    print("scan    scanner lock still held; aborting remaining batches", flush=True)
                    break
                log_path = out_dir / "scanner.log"
                mark = log_path.stat().st_size if log_path.exists() else 0
                chrome_before = chromium_process_count()
                rc = run_http_only_scan(batch, concurrency=len(batch), log_path=log_path, timeout_sec=args.scan_timeout)
                chrome_after = chromium_process_count()
                if chrome_after > max(chrome_before, 0):
                    print(f"scan    BROWSER LEAK: chromium processes {chrome_before} -> {chrome_after} during batch {', '.join(batch)[:80]}", flush=True)
                    chromium_leaks.append({"batch": batch, "before": chrome_before, "after": chrome_after})
                try:
                    with log_path.open("rb") as fh:
                        fh.seek(mark)
                        tail = fh.read().decode("utf-8", "replace")
                except OSError:
                    tail = ""
                if "Scanner already running" in tail and "refusing to start" in tail:
                    print(f"scan    lost the lock race (attempt {attempt}); re-waiting", flush=True)
                    time.sleep(5)
                    continue
                break
            else:
                print("scan    gave up after 6 lock races", flush=True)
            if rc == -1:
                break
            print(f"scan    batch {i // args.batch + 1}: {', '.join(batch)[:120]} rc={rc} in {time.time() - t0:.0f}s", flush=True)
        if not args.no_vpic and scannable:
            try:
                vp = vpic_for_dealers(scannable)
                print(f"vpic    {json.dumps(vp)[:200]}", flush=True)
            except Exception as exc:  # noqa: BLE001
                print(f"vpic    failed: {str(exc)[:120]}", flush=True)

    stamp = started.strftime("%Y-%m-%d %H:%M UTC")
    conn = _assess_conn()
    results = []
    try:
        for did in ids:
            if not _conn_alive(conn):
                print("assess  database connection dropped; reconnecting", flush=True)
                conn = _assess_conn()
            r = assess(conn, did, since_iso, known[did], recipe_info[did])
            try:
                # Retire against the rows stamped to THIS store, never the raw feed
                # count: a group feed's 1,521 rows with 2 kept retired 158 real
                # cars on 2026-09-28 (mbbeverlyhills-com, fjmercedes-com).
                rows_kept = int(r.get("rows_stamped") if r.get("rows_stamped") is not None else (r.get("rows") or 0))
                r["reconcile"] = reconcile_dealer(conn, did, since_iso, known[did], rows_kept, str(r.get("verdict")),
                                                  dry_run=args.no_reconcile)
            except Exception as exc:  # noqa: BLE001
                r["reconcile"] = {"error": str(exc)[:120]}
            results.append(r)
            try:
                log_scan_run(r, stamp)
                write_instructions_if_first_success(r, stamp)
            except Exception as exc:  # noqa: BLE001
                print(f"log     scan_runs {did}: {str(exc)[:80]}", flush=True)
    finally:
        conn.close()

    # 5. lifecycle — after the main batches, never before (it must not delay the fleet)
    lifecycle_summary: dict[str, Any] = {"enabled": False}
    for r in results:
        r.setdefault("lifecycle", "none")
    if not args.no_lifecycle and not args.skip_scan:
        try:
            lifecycle_summary = run_lifecycle_pass(results, dealers_by_id, known, out_dir=out_dir, stamp=stamp, no_discover=args.no_discover,
                                                   batch=args.batch, scan_timeout=args.scan_timeout, lock_wait=args.lock_wait,
                                                   no_vpic=args.no_vpic, no_reconcile=args.no_reconcile)
            lifecycle_summary["enabled"] = True
        except Exception as exc:  # noqa: BLE001 - the triage must still be written
            import traceback as _tb

            lifecycle_summary = {"enabled": True, "error": str(exc)[:200], "traceback": _tb.format_exc()[-1200:]}
            print(f"lifecyc pass failed: {str(exc)[:160]}", flush=True)
    needs = write_needs_discovery(results, out_dir)

    live_recipes = sum(1 for r in results if (r.get("recipe") or {}).get("had_recipes") or str((r.get("recipe") or {}).get("synth", "")).startswith(("saved", "browser_capture")))
    triage = {
        "started": started.isoformat(), "dealers": results,
        "chromium_leaks": list(locals().get("chromium_leaks", [])) + list(lifecycle_summary.get("chromium_leaks") or []),
        "recipe_coverage": {"dealers": len(results), "with_recipe": live_recipes},
        "counts": {v: sum(1 for r in results if r.get("verdict") == v) for v in ("ok", "thin", "inaccurate", "no_rows", "error", "no_recipe")},
        "needs_discovery": [r["dealer_id"] for r in results if r.get("verdict") in ("no_rows", "error", "no_recipe")],
        "thin": [r["dealer_id"] for r in results if r.get("verdict") == "thin"],
        "inaccurate": [r["dealer_id"] for r in results if r.get("verdict") == "inaccurate"],
        "lifecycle": {**lifecycle_summary, "by_dealer": {r["dealer_id"]: r.get("lifecycle", "none") for r in results},
                      "needs_discovery_file": str(out_dir / "needs_discovery.txt") if needs else None},
    }
    # 6. platform clustering: which of this run's failing dealers share a platform
    #    nobody has a template for (item 3 of the plan; the code notices, not a human)
    triage["platform_clusters"] = platform_cluster_lines(triage["needs_discovery"])
    (out_dir / "triage.json").write_text(json.dumps(triage, indent=1, default=str), encoding="utf-8")
    lines = triage_table(results)
    (out_dir / "triage.md").write_text("\n".join(lines + [""] + triage["platform_clusters"]) + "\n", encoding="utf-8")
    try:
        write_slow_dealers(results, out_dir, started)
    except Exception as exc:  # noqa: BLE001
        print(f"log     slow_dealers: {str(exc)[:100]}", flush=True)
    print("\n".join(lines))
    for line in triage["platform_clusters"]:
        print(line, flush=True)
    print(json.dumps(triage["counts"]))
    return 0
