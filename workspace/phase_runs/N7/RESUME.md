# N7 Postgres harness core - RESUME

Branch phase/N7, merged into integ/draft at 0037f5518 (no conflicts). Draft only; no push, no CI run.

| Unit | Head | Status |
|---|---|---|
| P14A.1 | 733f11c32 | DONE |
| P14A.2 | 53cb18036 | DONE |
| P14A.3 | 53cb18036 | DONE in code, CI unverified |
| P14A.4 | 3366fb507 | DONE |
| P14A.5 | 63f37e5ac | DONE |
| P14A.6 | 63f37e5ac | DONE |
| P14A.7 | 2b9740fbf | DONE |
| P14A.8 | - | BLOCKED, builder returned nothing; redo (pin W1-W3, portal retire path) |
| N7.9 | - | BLOCKED, builder returned nothing; redo (needs N1.4 shapes) |

## Open after merge
- test_pg_harness.py::test_every_module_level_ready_flag_is_reset_or_reviewed fails on integ/draft: 6 module-level flags added by integ code (first: backend.listings.dealer_locator._GOOGLE_CACHE_MAX, likely a cap) must be reset or added to _SWEEP_REVIEWED.
- test_upsert_vehicles_golden.py::test_upsert_vehicles_golden fails: phase sequence differs from the golden after integ changes; regenerate golden or fix.
- PG tier tests were skipped at merge (no TEST_PG_ADMIN_DSN); run with TEST_PG_ADMIN_DSN='postgresql:///postgres?host=/tmp'.

## Deferred checks
- CI green, 0 skipped pg tests, 8 min or less; broken migration turns it red (needs a branch push).
- pgvector/pgvector:0.8.1-pg18 image tag unverified.
- Median clone+drop 0.314 s vs 0.5 s budget (test asserts < 2.0 s).

## Owner asks
- Update ruleset context pytest-integration -> pytest-pg (gh api PUT).
- Approve (or not) a CI repeat job for P14A.7.
- pg_harness.py must not be edited by later units.
