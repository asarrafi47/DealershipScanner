# N18 One fetch policy and VDP prefetch coverage (12A extended) - RESUME

Branch phase/N18, merged into integ/draft at 9bca4fc20. Draft only.

| Unit | Head | Status |
|---|---|---|
| P12A.1 | 0d6dccb90 | DONE |
| P12A.3, P12A.6 | 075b8ec17 | DONE |
| P12A.4 | 53bb0e15d | DONE |
| P12A.5 | 50d3579eb | DONE code (V035); acceptance sample deferred |
| N18.8, N18.exit-grep | 5646fcbfb | DONE code; exit grep has debt table |
| P12A.2 | - | BLOCKED (needs N18.1 numbers, N13 merged) |
| N18.1, P12A.5-measure, N18.8-f5, N18.release | - | SKIPPED (owner/data/prod ops) |

## Deferred checks
- PG tier: V035 chain-head and migrations-superset tests skipped (TEST_PG_ADMIN_DSN unset); vdp_attempts lazy DDL has no PG test.
- Home-IP 20-dealer sample: fetched share >=30%, 25% drop in price gaps outside the new-arrival window.
- Re-run: guessed-URL 404s <100, 0 page fields without VIN match, F5 verdict after two-scan evidence.
- Shrink the 14-file debt table in test_no_raw_http_in_scanner_grep (window_sticker 4, synth/http 3, gap_fill, dealer_venom, dealer_inspire, recipes.py:1826, post_scan/pipeline.py:858 ...).
- Page-bound sites (dealer_inspire/venom/eprocess *_from_page, autowall cookie session) left for P17B.

## Owner asks
- D-OD3, D-OD7, D-OD8, D-OD13 (accepted as recommended); approve the home-IP burst (N18.1).
- Approve the Docker smoke of the scanner-nightly stage, then scanner-nightly deploy (minor release).
- Plan assigned no migration number for P12A.5; V035 was used, confirm.
