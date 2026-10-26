# N6b Efficiency quick wins - RESUME

Branch phase/N6b (cut from phase/N6). All 7 units merged --no-ff, no conflicts. No VERSION bump; CHANGELOG lines under [Unreleased].

| Unit | Branch | Head | What | Tests |
|---|---|---|---|---|
| E1 | unit/E1b | 77975ff5 | bcrypt cost 4 under pytest (BCRYPT_TEST_LOW_COST) | password_hash, paid_access_parity, auth tests pass |
| E2 | unit/E2b | 7a0fa931 | lru_cache on adapt_sql_for_postgres_execute | 7 pass |
| E3 | unit/E3b | 5bfd3e1e | facet rebuild throttle (300 s) + paint memo | facet/filter/perf/golden pass |
| E4 | unit/E4b | 9620e17b | registry backfill to startup thread | 8 + 23 pass |
| E5 | unit/E5b | f1f6cffa | pg_connect connect_timeout; comment fixes | test_db_connect 25 pass |
| E6 | unit/E6b | 52dfb787 | active-time scan timeout, wall/active logs, caffeinate docs | ~95 + 45 pass |
| E7 | unit/E7b | 8289ff17 | bake pgeocode cache in Dockerfile.web | not built (no Docker) |

## Measured gains
- E1: test_paid_access_parity 33.9 s -> 3.9 s.
- E2: 7.9 KB statement 0.553 ms -> 0.0015 ms per call (~360x).
- E4: removes ~210 s one-time stall from first geo-coords request (not measured here).
- E3, E5, E6, E7: not measured.

## Merge results
All clean. Post-merge per-unit test files re-run and passed. Full suite NOT run.

## Notes
- E3 behavior change: facet values may lag a new scan by up to 5 min (FACET_REBUILD_MIN_INTERVAL_S=0 disables).
- E4: no cross-worker lock; fine at GUNICORN_WORKERS=1. Rows stay unstamped for first minutes after restart. Skipped under pytest / DS_SKIP_REGISTRY_BACKFILL=1.
- E6: macOS monotonic already excludes sleep, so the 21 error verdicts' true source is unconfirmed. Per-dealer asyncio.wait_for unchanged. N9.13 re-queue of killed dealers and the real SIGSTOP test are not done.
- E7: Docker image build untested; RUN is best effort.
- E5: GUNICORN_WORKERS default stays 3 in entrypoint; prod overrides to 1.
- Next: run full pytest in chunks, then release per docs/RELEASING.md (owner pushes).
