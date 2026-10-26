# N9 Retirement single writer, provenance, two-strike, honest accounting, pause safety - RESUME

Branch phase/N9 (head 4cb2f367c), merged into integ/draft at 0dd5b4739, no conflicts. Draft only.

| Unit | Head | Status |
|---|---|---|
| P5A.1, P5A.3 | 6b5c76393 | DONE |
| P5A.2 | ef014e54b | DONE |
| P5B.1, P5B.2 | 24b0e5695 | DONE (live re-runs deferred to N9.14) |
| P5A.4 | 915311f2a | DONE |
| P5A.5 | 3dadac888 | DONE (3 cleanroom xfails remain) |
| P5A.6, P5B.7 | bc3892ddc | DONE |
| P5B.3, P5B.4 | 88c882840 | DONE (V032/V033) |
| P5A.7 | 0ab266184 | DONE |
| N9.12 | 9fa3e45b3 | DONE |
| N9.13 | 0aeea7f01 | DONE (simulated pause) |
| N9.15 | 4cb2f367c | DONE |
| N9.14, N9.16 | - | SKIPPED (data ops, owner approval) |

## Open after merge
- Two failing tests on integ/draft: test_reconcile_interim_buckets.py::test_pipeline_main_pass_retires_no_used_rows and ::test_lifecycle_retry_uses_its_own_bucket_counts. They get 'thin' (no_site_total_check) instead of 'inaccurate'. Cause: N9.12 treats a run with no replay walk report as thin. Fix: seed a walk report in the scan_runs summary of those tests (as test_recipe_lifecycle does). Passed 21/21 before the merge.
- test_no_sqlite.py::test_no_new_sqlite_beyond_allowlist fails: allowlist lacks assess.py (11 vs 9), resume.py, runner.py, test_fleet_scan_resume.py, test_runner_pause_safe.py. Regenerate with --write after review or move to PG twins.
- test_upsert_vehicles_golden fails (spec_backfill call order); one report says it also fails on the unmodified base, not confirmed on integ/draft.
- Migration numbers: V032/V033 replace the plan's V028/V029. Renumber the plan's N9.16 (`--target 33`), exit gate (max version 33), release notes and risk table. phase/N30 and phase/N37 use V035/V036; N8's V030 is already on integ/draft.
- test_cleanroom_shapes_2026_10.py: Coastline, Napleton, SCL shapes are still xfail; wire them to the two-strike layer.
- Behavior notes: a group feed whose total counts every rooftop now walks up to the cap (150). Delta scan's old lot-wide 80% gate is gone (policy decides at share 0.8). Allow-list moved 2 ids, not 3. pipeline_w2 writer name is unused.

## Deferred checks
- PG tier for reconcile twins, pipeline report-only and V032/V033 on a TEST_PG_ADMIN_DSN server (one chunk ran 18 PG tests green, others not run).
- N9.14 scratch-DB fleet scan: 21 to 0 in-run retirements; 3/1/1/0 strikes then retirements; FP02 dealers, 5starford (1,338), autosavvy (5,406), 13 cap walks tagged page_cap.
- Live placeholder-photo gate on groovetoyota and mymetrohonda; kearnymesasubaru coverage drop.
- Real SIGSTOP of a 3-dealer run (N9.13) and kill/resume of a real fleet (N9.15).
- N9.16 dry-run output for owner review (default dry run; never --apply in draft).

## Owner asks
- OK for N9.14 (`git switch`, cars_cr_n9 scratch scan) and N9.16 apply.
- Confirm renumbering to V032/V033 and SCANNER_STRIKE_GAP_HOURS=20, SCANNER_RECONCILE_BUCKET_SHARE 0.6/0.8, replay cap 150.
