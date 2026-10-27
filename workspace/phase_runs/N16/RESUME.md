# N16 Every fact writer through precedence (Phase 8B) - RESUME

Branch phase/N16 (head d76cb0b51), merged into integ/draft at 83e738cb0. Draft only.

| Unit | Head | Status |
|---|---|---|
| P8B.1 | e92e8c297 | DONE |
| P8B.2 | cf390c342 | DONE |
| P8B.9 | f26af0a7f | DONE |
| P8B.3 | e310d8a16 | DONE |
| P8B.4a | 157099318 | DONE |
| P8B.4b | d7bb57277 | DONE |
| P8B.6a | dc39c9fe4 | DONE |
| P8B.6b | 03e32b856 | DONE |
| P8B.5 | ddad8033c | DONE |
| P8B.7 | 38b19835f | DONE |
| P8B.8 | 6cb64dbfa | DONE |
| N16.X1 | d76cb0b51 | DONE (tests) |
| N16.X2, X3 | - | BLOCKED (data ops, owner deploy) |

## Deferred checks
- PG tier (TEST_PG_ADMIN_DSN): 5 skipped on integ/draft (upsert rescan x3, dictionary e2e, sticker entry point), PG golden, PG heal tests in test_vpic_facts_provenance.
- Re-measure drivetrain/transmission gaps (P8B.4b), noncanonical body <=1% (P8B.6b), inline-decode coverage (P8B.4a).
- test_no_sqlite.py has 2 failures from earlier chunks (new constructs in three test files, stale allowlist entries); reconcile.
- test_vehicle_facts_precedence::test_no_db_or_network_imports looked flaky in a wide sweep.
- Fact-precedence cycle test (N17 P8C.3).

## Owner asks
- Review the 12 KNOWN_RAW_WRITERS exceptions in test_no_raw_fact_updates_grep.py (vpic_facts and enrichment/service first).
- Decide a precedence-level EV engine_l/engine_description veto and the Hybrid-vs-vPIC MHEV gap.
- Confirm: same-rank stored source lets the feed win (P8B.1); provenance label epa_catalog vs epa_dictionary (P8B.5); trim_heuristic rank 5 added to precedence.py (P8B.2).
- Run the fleet rescan and deploy (X2/X3) when ready.
