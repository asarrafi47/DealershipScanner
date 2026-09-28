# Execution plan, from 2026-09-28

Owner-approved order. Update the status column as items land.

| # | Step | Status |
|---|---|---|
| 1 | **Ship 1.4.1.** Chapman Ford deadlock retry in the upsert; logs for the 12 platform-migrated dealers; timing-fingerprint backfill on the real DB; full suite; production image tested in local Docker; check production `schema_migrations` (local chain stops at V019); deploy web to Railway; run V023 + `build_listings_grid_cards` once; apply the planner statistics and work_mem on production Postgres; smoke-test production. | done 2026-09-28: live on sarraficars.com, prod inventory replaced from local (backup in workspace/backups), cards built with FLASK_ENV=production |
| 2 | **Scanning on Railway, not home machines.** The scan is HTTP-only now (no Chromium in the scan image). Measure its real CPU/memory per shard, price a nightly fleet cycle on Railway, then deploy the slim scanner image and a scheduled nightly job there. Home machines (mini, gaming laptop) are no longer part of the plan. | in progress |
| 3 | **Cleanup.** Rewrite then delete the scrapers' browser halves (HTTP_ONLY_SCANS_PLAN Phase 2); completeness follow-ups F07, F08, F11, F16-F20; remaining visual fixes (M6, IH-10/ES-6, SA-05, ES-10/SA-09, plus the "Next" list); efficiency items (connection pool, index-friendly make filter, CSS bundles, listings facet blob, card payload trim). | queued |
| 4 | **Features** (docs/COMPETITOR_FEATURE_GAPS.md): price-drop alerts on saved cars, new-listing alerts on saved searches, a saved-search page, a price-history chart, deal badges on result cards. | queued |
| 5 | Owner's separate scanning/discovery task. | after 4 |

Decisions recorded 2026-09-28:
- Listings no longer ship the whole fleet: default to the shopper's ZIP + radius, no cars until a search starts, results must stay fast (measured: largest metro 0.99 s cold, 4 ms warm).
- Production runs on Railway, web and scanning both; no k3s, no home cluster.
