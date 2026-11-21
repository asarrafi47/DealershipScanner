# N6 RESUME (integrator)

Branch phase/N6 (cut from phase/N3 = 1.5.8). All unit branches merged serially; docs committed in one integrator commit on top of 51ba6d475.

| Unit | Status | Unit head | Merge sha | Acceptance |
|---|---|---|---|---|
| N6.1 | DONE | daa8ba420 | 58bd6f7e0 | code met; copy/cars items deferred |
| N6.3 | DONE | 1e94059a0 | a2a662fda | code met; real-DB deferred |
| N6.5 | DONE | ff13a43c4 | de006140b | met; heal not applied |
| N6.6 | DONE | d855e7c5f | 780d9ddab | code met; re-grade deferred |
| N6.7 | PARTIAL | 0c072fff1 | ad4c08ae8 | repeated-value rule repair-only; dry-run counts deferred |
| N6.8 | DONE | 028d16f93 | 02ebe3805 | helper/alarm met; 50-image sample deferred |
| N6.9 | DONE | e2223549c | 7bed5efd6 | tests met; cleanroom measurement deferred |
| P8A.2 | DONE | c7033de26 | 9347e6a29 | met |
| N6.2 | DONE | bdc686f28 | 3557db5ff | unit tests met; cars_cr_n6 scan deferred |
| N6.4 | DONE | 6dbccb2d0 | fdddafbed | code met; 1,280 reclass count deferred |
| P8A.6 | DONE | be288d577 | 51ba6d475 | met; 15-car live check deferred |

## Merge results
All 11 merged, only conflict was a trivial import in listing_completeness.py (N6.8). Targeted tests passed after each merge. Full suite not run after the final merge.

## Known failing
backend/tests/test_upsert_vehicles_golden.py (phases assertion). Pre-existing on phase/N6 base for N6.2/N6.4 reports; N6.7 adds a log line; N6.4 adds 4 index columns. Regenerate golden and review diff.

## Deferred: needs copy (createdb -T not allowed this run)
N6.1 stale flags 4,581 to 0 and 199k rebuild timing; N6.3 admin count; N6.4 ~1,280 reclass; N6.6 re-grade; N6.7 dry-run counts 234+205+1; N6.9 1,374/497/1,019 counts; N6.2 3-dealer cars_cr_n6 scan (MAIN, D-N25, needs owner OK to git switch phase/N6); P8A.6 15 cars; N6 re-baseline (exit gate).

## Open owner asks
- OK to run the local rebuild on `cars` (retired index rows 8,738 to 0) and to apply heal scripts locally (heal_vdp_junk_values, repair_junk_prices --apply with backup). Prod applies are N12.10.
- OK/createdb copies of cars_cleanroom and cars for the deferred acceptance.
- Decide: follow-up unit to wire the N6.7 repeated-value rule; persisting the withheld-price note; whether INDEX_RULES_VERSION needs a further bump for N6.5/N6.7/N6.8 (N6.4 already bumped to 2026-10-n6.4).
- Review SHARED_IMAGE_MIN_CARS=4 (N6.8 flagged 22,877 cars, plan expected ~2,970).
- Not done: VERSION bump, release tag (integrator for release).
