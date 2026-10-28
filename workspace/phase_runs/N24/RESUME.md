# N24 Forced-induction resolution (10C) - RESUME

Branch phase/N24, merged into integ/draft at d098da777 (head 7d1c07799). Draft only.

| Unit | Head | Status |
|---|---|---|
| P10C.1, P10C.8 | edecd14f9 | DONE code; P10C.8 baselines pending |
| P10C.5 | (phase/N24) | DONE fixtures; vehicles.csv measurement pending |
| P10C.2 | a0923fa52 | DONE |
| P10C.4, P10C.6 | 3ef138113 | DONE |
| P10C.3 | f707264ff | DONE (test_no_sqlite allowlist issue) |
| P10C.7 | b845fbe59 | DONE, pg cycle test not run |
| P10C.9 | 7d1c07799 | DONE tool, pg tests pass |
| P10C.5m, P10C.8b, P10C.9b | - | blocked data ops, not started |
| P10C.rel | - | blocked prod op, not started |

## Deferred checks
- P10C.8: record baselines for epa_master_fi_contradicts_flags, car_fi_boost_on_na_family, car_fi_missing_on_boosted_family after V036 flag columns are loaded; then drop INDUCTION_INVARIANT_IDS exemption; run seconds-long local --sql-only
- P10C.5: scratch measurement vs pinned vehicles.csv (831 false positives to under 100, true positives within 2%)
- pg-tier tests not run in merge (no TEST_PG_ADMIN_DSN): P10C.7 cycle test, golden pg tier
- P10C.9 dry run on real fleet (D-ET2 numbers) after P10B.4; never --apply without owner OK
- Fix test_no_sqlite failures from P10C.3 (linker.py qmark count; sqlite3 import in test_catalog_linker_forced_induction.py)
- Leftover harness databases pytest_* on the local server (cache, not cleaned)

## Owner asks
- Provide pinned vehicles.csv for the P10C.5 measurement
- Review heal dry run and conflict sample; approve any apply
- Approve release step (minor) and Docker smoke
